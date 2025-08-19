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
    const BATCH_SIZE: usize = 10_000; // Much smaller batch size
    
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

    // Build indexes after parsing - this is where the magic happens
    println!("Building indexes...");
    let index_start = Instant::now();
    db.build_all_indexes();
    println!("Indexes built in {:.2} seconds", index_start.elapsed().as_secs_f64());
    
    Ok(db)
}

fn run_sample_query(db: &mut SparqlDatabase) {
    let query_start = Instant::now();

    let sparql_query = r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
 PREFIX sorg: <http://schema.org/>
 PREFIX dc: <http://purl.org/dc/terms/>
 PREFIX foaf: <http://xmlns.com/foaf/>
 PREFIX gr: <http://purl.org/goodrelations/>
 PREFIX gn: <http://www.geonames.org/ontology#>
 PREFIX mo: <http://purl.org/ontology/mo/>
 PREFIX og: <http://ogp.me/ns#>
 PREFIX rev: <http://purl.org/stuff/rev#>
 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
 SELECT ?v0 
 WHERE {
   ?v0	wsdbm:likes	?v1 .
   ?v0	wsdbm:friendOf	?v2 .
   ?v0	dc:Location	?v3 .
   ?v0	foaf:age	?v4 .
   ?v0	wsdbm:gender	?v5 .
   ?v0	foaf:givenName	?v6 .
 }"#;

//     let sparql_query = "PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
// PREFIX sorg: <http://schema.org/>
// PREFIX dc: <http://purl.org/dc/terms/>
// PREFIX foaf: <http://xmlns.com/foaf/>
// PREFIX gr: <http://purl.org/goodrelations/>
// PREFIX gn: <http://www.geonames.org/ontology#>
// PREFIX mo: <http://purl.org/ontology/mo/>
// PREFIX og: <http://ogp.me/ns#>
// PREFIX rev: <http://purl.org/stuff/rev#>
// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
// SELECT ?v0 ?v4 ?v6 ?v7 
// WHERE {
// 	?v0	sorg:caption	?v1 .
// 	?v0	sorg:text	?v2 .
// 	?v0	sorg:contentRating	?v3 .
// 	?v0	rev:hasReview	?v4 .
// 	?v4	rev:title	?v5 .
// 	?v4	rev:reviewer	?v6 .
// 	?v7	sorg:actor	?v6 .
// 	?v7	sorg:language	?v8 .
// }";
    
//     let sparql_query = "PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
// PREFIX sorg: <http://schema.org/>
// PREFIX dc: <http://purl.org/dc/terms/>
// PREFIX foaf: <http://xmlns.com/foaf/>
// PREFIX gr: <http://purl.org/goodrelations/>
// PREFIX gn: <http://www.geonames.org/ontology#>
// PREFIX mo: <http://purl.org/ontology/mo/>
// PREFIX og: <http://ogp.me/ns#>
// PREFIX rev: <http://purl.org/stuff/rev#>
// PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
// PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
// SELECT ?v0 ?v3 ?v4 ?v8 
// WHERE {
// 	?v0	sorg:legalName	?v1 .
// 	?v0	gr:offers	?v2 .
// 	?v2	sorg:eligibleRegion	wsdbm:Country5 .
// 	?v2	gr:includes	?v3 .
// 	?v4	sorg:jobTitle	?v5 .
// 	?v4	foaf:homepage	?v6 .
// 	?v4	wsdbm:makesPurchase	?v7 .
// 	?v7	wsdbm:purchaseFor	?v3 .
// 	?v3	rev:hasReview	?v8 .
// 	?v8	rev:totalVotes	?v9 .
// }";
    
    // Use the existing Volcano Optimizer instead of the streaming function
    let _ = execute_query_rayon_parallel2_volcano(sparql_query, db);
    let query_time = query_start.elapsed();

    // result limited to 10 for demonstration purposes
    // for result in result.iter().take(10) {
    //     if let [v0] = &result[..] {
    //         println!("{}", v0);
    //     }
    // }
    
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
