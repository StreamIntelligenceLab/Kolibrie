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

fn parse_large_ntriples_file(
    file_path: &str,
) -> Result<SparqlDatabase, Box<dyn std::error::Error>> {
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
            db.parse_ntriples_and_add(&batch_data);

            // Aggressive cleanup
            batch_lines.clear();
            batch_lines.shrink_to_fit();

            // Progress info every 100k triples
            if line_count % 100_000 == 0 {
                println!("Processed {} triples", line_count);
                std::hint::black_box(());

                // Optional: small delay to let the system breathe
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }

    // Process remaining batch
    if !batch_lines.is_empty() {
        let batch_data = batch_lines.join("\n");
        db.parse_ntriples_and_add(&batch_data);
    }
    db.get_or_build_stats();

    println!(
        "Finished parsing {} triples in {:.2} seconds",
        line_count,
        start_time.elapsed().as_secs_f64()
    );

    // Build indexes after parsing - this is where the magic happens
    println!("Building indexes...");
    let index_start = Instant::now();
    db.build_all_indexes();
    println!("Indexes built in {:.2} seconds", index_start.elapsed().as_secs_f64());

    Ok(db)
}

fn run_all_queries(db: &mut SparqlDatabase) {
    const ITERATIONS: usize = 20;

    // (name, query)
    let queries: &[(&str, &str)] = &[
        // C1
        (
            "C1",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v4 ?v6 ?v7
            WHERE {
            ?v0	sorg:caption	?v1 .
            ?v0	sorg:text	?v2 .
            ?v0	sorg:contentRating	?v3 .
            ?v0	rev:hasReview	?v4 .
            ?v4	rev:title	?v5 .
            ?v4	rev:reviewer	?v6 .
            ?v7	sorg:actor	?v6 .
            ?v7	sorg:language	?v8 .
}
"#,
        ),
        // C2
        (
            "C2",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v3 ?v4 ?v8 WHERE {
            ?v0	sorg:legalName	?v1 .
            ?v0	gr:offers	?v2 .
            ?v2	sorg:eligibleRegion	wsdbm:Country5 .
            ?v2	gr:includes	?v3 .
            ?v4	sorg:jobTitle	?v5 .
            ?v4	foaf:homepage	?v6 .
            ?v4	wsdbm:makesPurchase	?v7 .
            ?v7	wsdbm:purchaseFor	?v3 .
            ?v3	rev:hasReview	?v8 .
            ?v8	rev:totalVotes	?v9 .
}
"#,
        ),
        // C3
        (
            "C3",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0
            WHERE {
            ?v0	wsdbm:likes	?v1 .
            ?v0	wsdbm:friendOf	?v2 .
            ?v0	dc:Location	?v3 .
            ?v0	foaf:age	?v4 .
            ?v0	wsdbm:gender	?v5 .
            ?v0	foaf:givenName	?v6 .
}
"#,
        ),
        // F1
        (
            "F1",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v2 ?v3 ?v4 ?v5 WHERE {
            ?v0	og:tag	<http://db.uwaterloo.ca/~galuc/wsdbm/Topic7> .
            ?v0	rdf:type	?v2 .
            ?v3	sorg:trailer	?v4 .
            ?v3	sorg:keywords	?v5 .
            ?v3	wsdbm:hasGenre	?v0 .
            ?v3	rdf:type	wsdbm:ProductCategory2 .
}
"#,
        ),
        // F2
        (
            "F2",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6 ?v7 WHERE {
            ?v0	foaf:homepage	?v1 .
            ?v0	og:title	?v2 .
            ?v0	rdf:type	?v3 .
            ?v0	sorg:caption	?v4 .
            ?v0	sorg:description	?v5 .
            ?v1	sorg:url	?v6 .
            ?v1	wsdbm:hits	?v7 .
            ?v0	wsdbm:hasGenre	<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre1> .
}
"#,
        ),
        // F3
        (
            "F3",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6 WHERE {
            ?v0	sorg:contentRating	?v1 .
            ?v0	sorg:contentSize	?v2 .
            ?v0	wsdbm:hasGenre	<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre5> .
            ?v4	wsdbm:makesPurchase	?v5 .
            ?v5	wsdbm:purchaseDate	?v6 .
            ?v5	wsdbm:purchaseFor	?v0 .
}
"#,
        ),
        // F4
        (
            "F4",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22/rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v2 ?v4 ?v5 ?v6 ?v7 ?v8 WHERE {
            ?v0	foaf:homepage	?v1 .
            ?v2	gr:includes	?v0 .
            ?v0	og:tag	<http://db.uwaterloo.ca/~galuc/wsdbm/Topic1> .
            ?v0	sorg:description	?v4 .
            ?v0	sorg:contentSize	?v8 .
            ?v1	sorg:url	?v5 .
            ?v1	wsdbm:hits	?v6 .
            ?v1	sorg:language	wsdbm:Language0 .
            ?v7	wsdbm:likes	?v0 .
}
"#,
        ),
        // F5
        (
            "F5",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v3 ?v4 ?v5 ?v6 WHERE {
            ?v0	gr:includes	?v1 .
            <http://db.uwaterloo.ca/~galuc/wsdbm/Retailer1>	gr:offers	?v0 .
            ?v0	gr:price	?v3 .
            ?v0	gr:validThrough	?v4 .
            ?v1	og:title	?v5 .
            ?v1	rdf:type	?v6 .
}
"#,
        ),
        // L1
        (
            "L1",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v2 ?v3 WHERE {
            ?v0 wsdbm:subscribes <http://db.uwaterloo.ca/~galuc/wsdbm/Website546> .
            ?v2 sorg:caption ?v3 .
            ?v0 wsdbm:likes ?v2 .
}
"#,
        ),
        // L2
        (
            "L2",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v1 ?v2 WHERE {
            <http://db.uwaterloo.ca/~galuc/wsdbm/City0> gn:parentCountry ?v1 .
            ?v2 wsdbm:likes wsdbm:Product0 .
            ?v2 sorg:nationality ?v1 .
}
"#,
        ),
        // L3
        (
            "L3",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 WHERE {
            ?v0	wsdbm:likes	?v1 .
            ?v0	wsdbm:subscribes	<http://db.uwaterloo.ca/~galuc/wsdbm/Website546> .
}
"#,
        ),
        // L4
        (
            "L4",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v2 WHERE {
            ?v0 <http://ogp.me/ns#tag> <http://db.uwaterloo.ca/~galuc/wsdbm/Topic10> .
            ?v0 <http://schema.org/caption> ?v2 .
}
"#,
        ),
        // L5
        (
            "L5",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v3 WHERE {
            ?v0	sorg:jobTitle	?v1 .
            <http://db.uwaterloo.ca/~galuc/wsdbm/City0>	gn:parentCountry	?v3 .
            ?v0	sorg:nationality	?v3 .
}
"#,
        ),
        // S1
        (
            "S1",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22/rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v3 ?v4 ?v5 ?v6 ?v7 ?v8 ?v9 WHERE {
            ?v0	gr:includes	?v1 .
            <http://db.uwaterloo.ca/~galuc/wsdbm/Retailer0>	gr:offers	?v0 .
            ?v0	gr:price	?v3 .
            ?v0	gr:serialNumber	?v4 .
            ?v0	gr:validFrom	?v5 .
            ?v0	gr:validThrough	?v6 .
            ?v0	sorg:eligibleQuantity	?v7 .
            ?v0	sorg:eligibleRegion	?v8 .
            ?v0	sorg:priceValidUntil	?v9 .
}
"#,
        ),
        // S2
        (
            "S2",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22/rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v3 WHERE {
            ?v0	dc:Location	?v1 .
            ?v0	sorg:nationality	<http://db.uwaterloo.ca/~galuc/wsdbm/Country6> .
            ?v0	wsdbm:gender	?v3 .
            ?v0	rdf:type	wsdbm:Role2 .
}
"#,
        ),
        // S3
        (
            "S3",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22/rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v2 ?v3 ?v4 WHERE {
            ?v0	rdf:type	<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory4> .
            ?v0	sorg:caption	?v2 .
            ?v0	wsdbm:hasGenre	?v3 .
            ?v0	sorg:publisher	?v4 .
}
"#,
        ),
        // S4
        (
            "S4",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22/rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v2 ?v3 WHERE {
            ?v0	foaf:age	<http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup4> .
            ?v0	foaf:familyName	?v2 .
            ?v3	mo:artist	?v0 .
            ?v0	sorg:nationality	wsdbm:Country1 .
}
"#,
        ),
        // S5
        (
            "S5",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22/rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v2 ?v3 WHERE {
            ?v0	rdf:type	<http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2> .
            ?v0	sorg:description	?v2 .
            ?v0	sorg:keywords	?v3 .
            ?v0	sorg:language	wsdbm:Language0 .
}
"#,
        ),
        // S6
        (
            "S6",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22/rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v2 WHERE {
            ?v0	mo:conductor	?v1 .
            ?v0	rdf:type	?v2 .
            ?v0	wsdbm:hasGenre	<http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre26> .
}
"#,
        ),
        // S7
        (
            "S7",
            r#"PREFIX wsdbm: <http://db.uwaterloo.ca/~galuc/wsdbm/>
            PREFIX sorg: <http://schema.org/>
            PREFIX dc:   <http://purl.org/dc/terms/>
            PREFIX foaf: <http://xmlns.com/foaf/>
            PREFIX gr:   <http://purl.org/goodrelations/>
            PREFIX gn:   <http://www.geonames.org/ontology#>
            PREFIX mo:   <http://purl.org/ontology/mo/>
            PREFIX og:   <http://ogp.me/ns#>
            PREFIX rev:  <http://purl.org/stuff/rev#>
            PREFIX rdf:  <http://www.w3.org/1999/02/22/rdf-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?v0 ?v1 ?v2 WHERE {
            ?v0	rdf:type	?v1 .
            ?v0	sorg:text	?v2 .
            <http://db.uwaterloo.ca/~galuc/wsdbm/User7>	wsdbm:likes	?v0 .
}
"#,
        ),
    ];

    for (name, query) in queries.iter() {
        println!("==============================================");
        println!("Running query {} ({} iterations)...", name, ITERATIONS);

        let mut total_time = 0.0;
        // let mut last_result:Vec<Vec<String>> = Vec::new();

        for _ in 0..ITERATIONS {
            let start = Instant::now();
            let _ = execute_query_rayon_parallel2_volcano(query, db);
            let elapsed = start.elapsed().as_secs_f64();
            total_time += elapsed;
        }

        let avg = total_time / (ITERATIONS as f64);
        println!("Average time for {}: {:.6} seconds", name, avg);
    }
}

fn main() {
    // Set current directory to the root of the project
    std::env::set_current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))
    .expect("Failed to set project root as current directory");

    let file_path = "../benchmark_dataset/watdiv.10M.nt";

    match parse_large_ntriples_file(file_path) {
        Ok(mut db) => {
            println!("Successfully processed N-Triples file");
            run_all_queries(&mut db);
        }
        Err(e) => {
            eprintln!("Error processing file '{}': {}", file_path, e);
            println!(
                "File not found or error occurred. \
Make sure ../benchmark_dataset/watdiv.10M.nt exists."
            );
        }
    }
}
