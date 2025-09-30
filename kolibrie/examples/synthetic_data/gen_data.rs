/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(unused_imports)]
#![allow(unused_variables)]
use crossbeam::channel::{bounded, Sender};
use rayon::prelude::*;
use rand::distr::Uniform;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const POSITIONS: &[&str] = &["Manager", "Developer", "Salesperson"];
const TOTAL_EMPLOYEES: usize = 10000; // 1 billion
const BATCH_SIZE: usize = 100_000; // Number of records per batch
const CHANNEL_CAPACITY: usize = 100; // Number of batches that can be queued

fn main() -> std::io::Result<()> {
    // Start timing the operation
    let start_time = Instant::now();

    // Calculate the number of batches needed
    let num_batches = (TOTAL_EMPLOYEES + BATCH_SIZE - 1) / BATCH_SIZE;

    println!(
        "Starting generation of {} RDF triples in {} batches...",
        TOTAL_EMPLOYEES, num_batches
    );

    // Set up crossbeam channel with bounded capacity to limit memory usage
    let (sender, receiver) = bounded::<String>(CHANNEL_CAPACITY);

    // Atomic counter for tracking progress
    let progress_counter = AtomicUsize::new(0);

    // Spawn the writer thread
    let writer_handle = std::thread::spawn(move || -> std::io::Result<()> {
        // Open the output file with a buffered writer (16 MB buffer)
        let file = File::create("synthetic_employee_data_10K.rdf")?;
        let mut writer = BufWriter::with_capacity(16 * 1024 * 1024, file);

        // Write XML declaration
        writer.write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")?;

        // Write rdf:RDF start tag with namespaces
        writer.write_all(
            b"<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" \
xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" \
xmlns:socrata=\"http://www.socrata.com/rdf/terms#\" \
xmlns:dcat=\"http://www.w3.org/ns/dcat#\" \
xmlns:ods=\"http://open-data-standards.github.com/2012/01/open-data-standards#\" \
xmlns:dcterm=\"http://purl.org/dc/terms/\" \
xmlns:geo=\"http://www.w3.org/2003/01/geo/wgs84_pos#\" \
xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\" \
xmlns:foaf=\"http://xmlns.com/foaf/0.1/\" \
xmlns:dsbase=\"https://data.cityofchicago.org/resource/\" \
xmlns:ds=\"https://data.cityofchicago.org/resource/xzkq-xp2w/\">\n",
        )?;

        let mut total_written = 0usize;
        let mut last_report = Instant::now();

        // Receive batches from the channel and write them to the file
        while let Ok(batch) = receiver.recv() {
            writer.write_all(batch.as_bytes())?;
            total_written += BATCH_SIZE;

            // Update progress every 100 million records
            if total_written % 10000 == 0 {
                let elapsed = last_report.elapsed();
                println!(
                    "[Writer] Written {} employees in {:.2?} ({:.2} employees/sec)",
                    total_written,
                    elapsed,
                    10000 as f64 / elapsed.as_secs_f64()
                );
                last_report = Instant::now();
            }
        }

        // Write the closing rdf:RDF tag
        writer.write_all(b"</rdf:RDF>\n")?;
        writer.flush()?;
        println!("[Writer] Completed writing all RDF triples.");
        Ok(())
    });

    // Use a scoped thread pool to manage parallel data generation
    rayon::scope(|s| {
        s.spawn(|_| {
            // Create a parallel iterator over batch indices
            (0..num_batches).into_par_iter().for_each_with(sender.clone(), |s, batch_idx| {
                // Calculate the range of employee IDs for this batch
                let start_id = batch_idx * BATCH_SIZE + 1;
                let end_id = ((batch_idx + 1) * BATCH_SIZE).min(TOTAL_EMPLOYEES);
                let current_batch_size = end_id - start_id + 1;

                // Initialize a thread-local RNG
                let mut rng = rand::rngs::StdRng::from_os_rng();
                let position_dist = Uniform::try_from(0..POSITIONS.len()).unwrap();
                let salary_dist = Uniform::try_from(30_000..150_000).unwrap();

                // Preallocate a buffer string for the batch
                let mut buffer = String::with_capacity(current_batch_size * 512); // Approximate size

                // Generate RDF triples for each employee in the batch
                for employee_id in start_id..=end_id {
                    let employee_uri = format!("http://example.org/employee{}", employee_id);
                    let position = POSITIONS[rng.sample(&position_dist)];
                    let salary = rng.sample(&salary_dist);

                    buffer.push_str("  <rdf:Description rdf:about=\"");
                    buffer.push_str(&employee_uri);
                    buffer.push_str("\">\n");

                    buffer.push_str("    <foaf:name>");
                    buffer.push_str(&employee_uri);
                    buffer.push_str("</foaf:name>\n");

                    buffer.push_str("    <foaf:title>");
                    buffer.push_str(position);
                    buffer.push_str("</foaf:title>\n");

                    buffer.push_str("    <foaf:workplaceHomepage>http://example.org/company</foaf:workplaceHomepage>\n");

                    buffer.push_str("    <ds:full_or_part_time>F</ds:full_or_part_time>\n");

                    buffer.push_str("    <ds:salary_or_hourly>SALARY</ds:salary_or_hourly>\n");

                    buffer.push_str("    <ds:annual_salary>");
                    buffer.push_str(&salary.to_string());
                    buffer.push_str("</ds:annual_salary>\n");

                    buffer.push_str("  </rdf:Description>\n");
                }

                // Send the serialized batch to the writer thread
                if let Err(e) = s.send(buffer) {
                    eprintln!("Failed to send batch {}: {}", batch_idx, e);
                }

                // Update progress
                let prev = progress_counter.fetch_add(current_batch_size, Ordering::SeqCst) + current_batch_size;
                if prev % 10000 < current_batch_size {
                    let elapsed = start_time.elapsed();
                    println!(
                        "[Generator] Progress: {} / {} employees written ({:.2}%)",
                        prev,
                        TOTAL_EMPLOYEES,
                        (prev as f64 / TOTAL_EMPLOYEES as f64) * 100.0
                    );
                }
            });

            // Drop the sender to signal completion
            drop(sender);
        });
    });

    // Wait for the writer thread to finish
    writer_handle
        .join()
        .expect("Writer thread panicked")?;

    let total_time = start_time.elapsed();
    println!(
        "Successfully written {} employees in {:.2?}",
        TOTAL_EMPLOYEES, total_time
    );

    Ok(())
}
