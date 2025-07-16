/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 * 
 * 
 * 
 * NOTE 1: We are using the benchmark dataset from:
 *  Waterloo SPARQL Diversity Test Suite (WatDiv) v0.6
 *  Source: https://dsg.uwaterloo.ca/watdiv/
 * 
 * NOTE 2: Before running with the 10M-triple dataset, ensure you have:
 *   1) Downloaded `watdiv.10M.nt` into a `benchmark_dataset` directory
 *      at the project root.
 *   2) Created the `benchmark_dataset` directory next to `kolibrie/`.
 *      (e.g., `mkdir benchmark_dataset && mv watdiv.10M.nt benchmark_dataset/`)
 * 
 * NOTE 3: The watdiv.10M.nt file is approximately 1.5 GB in size.
 * 
 */

use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn parse_large_ntriples_file(file_path: &str) -> Result<SparqlDatabase, Box<dyn std::error::Error>> {
    println!("Starting to parse N-Triples file: {}", file_path);
    let start_time = Instant::now();
    
    let mut db = SparqlDatabase::new();
    
    // Much smaller buffer and more aggressive memory management
    let file = File::open(file_path)?;
    let reader = BufReader::with_capacity(64 * 1024, file); // Reduced buffer size
    
    let mut line_count = 0;
    let mut batch_lines = Vec::new();
    const BATCH_SIZE: usize = 1_000; // Much smaller batch size
    
    for line_result in reader.lines() {
        let line = line_result?;
        
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        
        batch_lines.push(line);
        line_count += 1;
        
        if batch_lines.len() >= BATCH_SIZE {
            // Process batch immediately
            let batch_data = batch_lines.join("\n");
            db.parse_ntriples(&batch_data);
            
            // Aggressive cleanup
            batch_lines.clear();
            batch_lines.shrink_to_fit();
            
            // Force garbage collection every 100k triples
            if line_count % 100_000 == 0 {
                println!("Processed {} triples", line_count);
                std::hint::black_box(());
                
                // Optional: Force a small delay to allow system cleanup
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
    
    // Process remaining batch
    if !batch_lines.is_empty() {
        let batch_data = batch_lines.join("\n");
        db.parse_ntriples(&batch_data);
    }
    
    println!("Finished parsing {} triples in {:.2} seconds", 
             line_count, start_time.elapsed().as_secs_f64());
    
    Ok(db)
}

fn run_sample_query(db: &mut SparqlDatabase) {
    println!("\nRunning sample query...");
    let query_start = Instant::now();

    let sparql_query = r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
    PREFIX sorg: <http://schema.org/>
    SELECT ?v0 ?v2 ?v3 
    WHERE {
        ?v0 wsdbm:subscribes ?v1 .
        ?v2 sorg:caption ?v3 .
        ?v0 wsdbm:likes ?v2
    }"#;
    
    let _ = execute_query_rayon_parallel2_redesign_streaming(sparql_query, db);
    let query_time = query_start.elapsed();
    
    println!("Query executed in {:.3} seconds", query_time.as_secs_f64());
}

fn main() {
    // Set current directory to the root of the project
    std::env::set_current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))
        .expect("Failed to set project root as current directory");
    let file_path = "../benchmark_dataset/watdiv.10M.nt";
    
    match parse_large_ntriples_file(file_path) {
        Ok(mut db) => {
            println!("Successfully processed N-Triples file");
            run_sample_query(&mut db);
        }
        Err(e) => {
            eprintln!("Error processing file '{}': {}", file_path, e);
            println!("File not found or error occurred. Running simple example instead...");
        }
    }
}