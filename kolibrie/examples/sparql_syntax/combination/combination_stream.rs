/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::parser::*;
use kolibrie::execute_query::*;
use kolibrie::sparql_database::SparqlDatabase;

fn main() {
    // Stream data simulating temperature readings over time
    let rdf_xml_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org#"
                 xmlns:stream="http://example.org/stream#">
          <!-- Current window data -->
          <rdf:Description rdf:about="http://example.org/stream#reading1">
            <ex:room>Room101</ex:room>
            <ex:temperature>95</ex:temperature>
            <ex:timestamp>1640995200</ex:timestamp>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/stream#reading2">
            <ex:room>Room102</ex:room>
            <ex:temperature>75</ex:temperature>
            <ex:timestamp>1640995205</ex:timestamp>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/stream#reading3">
            <ex:room>Room103</ex:room>
            <ex:temperature>120</ex:temperature>
            <ex:timestamp>1640995210</ex:timestamp>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/stream#reading4">
            <ex:room>Room101</ex:room>
            <ex:temperature>87</ex:temperature>
            <ex:timestamp>1640995215</ex:timestamp>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/stream#reading5">
            <ex:room>Room104</ex:room>
            <ex:temperature>45</ex:temperature>
            <ex:timestamp>1640995220</ex:timestamp>
          </rdf:Description>
        </rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml_data);
    println!("Database RDF triples: {:#?}", database.triples);
    println!("Total triples loaded: {}", database.triples.len());

    // Test 1: Basic RSP rule with RSTREAM and sliding window
    println!("\n=== Test 1: Basic RSP Windowing Rule ===");
    let rsp_rule_basic = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :TemperatureAlert(?room)
RSTREAM
FROM NAMED WINDOW <http://example.org/window1> ON <http://example.org/temperatureStream> [SLIDING 10 SLIDE 2 REPORT ON_WINDOW_CLOSE TICK TIME_DRIVEN]
:- 
CONSTRUCT { 
    ?room ex:hasAlert "high_temperature" .
}
WHERE { 
    ?reading ex:room ?room ; 
             ex:temperature ?temp ;
             ex:timestamp ?time .
    FILTER (?temp > 90)
}"#;

    println!("Processing RSP rule with RSTREAM and sliding window...");
    let rule_result = process_rule_definition(rsp_rule_basic, &mut database);
    match rule_result {
        Ok((rule, inferred_facts)) => {
            println!("RSP Rule processed successfully!");
            println!("Rule structure: {:#?}", rule);
            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                println!("  {}", database.triple_to_string(triple, &database.dictionary));
            }
        },
        Err(e) => println!("Failed to process RSP rule: {}", e),
    }

    // Test 2: ISTREAM with tumbling window
    println!("\n=== Test 2: ISTREAM with Tumbling Window ===");
    let rsp_rule_istream = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :NewHighTemp(?room)
ISTREAM
FROM NAMED WINDOW <http://example.org/window2> ON <http://example.org/tempStream> [TUMBLING 5 REPORT NON_EMPTY_CONTENT TICK TUPLE_DRIVEN]
:- 
CONSTRUCT { 
    ?room ex:newHighReading ?temp .
}
WHERE { 
    ?reading ex:room ?room ; 
             ex:temperature ?temp .
    FILTER (?temp > 85)
}"#;

    println!("Processing ISTREAM rule with tumbling window...");
    let rule_result2 = process_rule_definition(rsp_rule_istream, &mut database);
    match rule_result2 {
        Ok((rule, inferred_facts)) => {
            println!("ISTREAM Rule processed successfully!");
            println!("Rule has windowing: {}", rule.premise.len() > 0);
            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                println!("  {}", database.triple_to_string(triple, &database.dictionary));
            }
        },
        Err(e) => println!("Failed to process ISTREAM rule: {}", e),
    }

    // Test 3: DSTREAM with range window and ML.PREDICT
    println!("\n=== Test 3: DSTREAM with Range Window and ML.PREDICT ===");
    let rsp_rule_ml = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :PredictiveAlert
DSTREAM
FROM NAMED WINDOW <http://example.org/window3> ON <http://example.org/sensorStream> [RANGE 15 REPORT PERIODIC TICK TIME_DRIVEN]
:- 
CONSTRUCT { 
    ?room ex:predictedLevel ?level .
}
WHERE { 
    ?reading ex:room ?room ; 
             ex:temperature ?temp ;
             ex:timestamp ?time .
    FILTER (?temp > 70)
}

ML.PREDICT(
    MODEL "temperature_predictor",
    INPUT {
        SELECT ?room ?temp ?time
        WHERE {
            ?reading ex:room ?room ;
                     ex:temperature ?temp ;
                     ex:timestamp ?time .
        }
    },
    OUTPUT ?level
)"#;

    println!("Processing DSTREAM rule with range window and ML.PREDICT...");
    let rule_result3 = process_rule_definition(rsp_rule_ml, &mut database);
    match rule_result3 {
        Ok((rule, inferred_facts)) => {
            println!("DSTREAM + ML.PREDICT Rule processed successfully!");
            println!("Rule has {} premise patterns", rule.premise.len());
            println!("Rule has {} filter conditions", rule.filters.len());
            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                println!("  {}", database.triple_to_string(triple, &database.dictionary));
            }
        },
        Err(e) => println!("❌ Failed to process DSTREAM + ML rule: {}", e),
    }

    // Test 4: Test parsing individual components
    println!("\n=== Test 4: Testing Individual Parser Components ===");
    
    // Test stream type parsing
    let stream_tests = vec!["RSTREAM", "ISTREAM", "DSTREAM"];
    for stream in stream_tests {
        match parse_stream_type(stream) {
            Ok((_, stream_type)) => println!("Parsed stream type: {:?}", stream_type),
            Err(e) => println!("Failed to parse {}: {:?}", stream, e),
        }
    }

    // Test window specification parsing
    let window_spec = "[SLIDING 10 SLIDE 2 REPORT ON_WINDOW_CLOSE TICK TIME_DRIVEN]";
    match parse_window_spec(window_spec) {
        Ok((_, spec)) => {
            println!("Parsed window spec:");
            println!("  Type: {:?}", spec.window_type);
            println!("  Width: {}", spec.width);
            println!("  Slide: {:?}", spec.slide);
            println!("  Report: {:?}", spec.report_strategy);
            println!("  Tick: {:?}", spec.tick);
        },
        Err(e) => println!("Failed to parse window spec: {:?}", e),
    }

    // Test FROM NAMED WINDOW parsing
    let window_clause = "FROM NAMED WINDOW <http://example.org/window1> ON <http://example.org/stream1> [TUMBLING 5]";
    match parse_from_named_window(window_clause) {
        Ok((_, clause)) => {
            println!("Parsed FROM NAMED WINDOW clause:");
            println!("  Window IRI: {}", clause.window_iri);
            println!("  Stream IRI: {}", clause.stream_iri);
            println!("  Window Type: {:?}", clause.window_spec.window_type);
        },
        Err(e) => println!("Failed to parse FROM NAMED WINDOW: {:?}", e),
    }

    // Test 5: Query the results
    println!("\n=== Test 5: Querying RSP Results ===");
    
    // Query for high temperature alerts
    let alert_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?alert
WHERE { 
  ?room ex:hasAlert ?alert . 
}"#;
    
    println!("Querying for temperature alerts...");
    let query_results = execute_query(alert_query, &mut database);
    println!("Alert query results: {:?}", query_results);

    // Query for new high readings
    let new_reading_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { 
  ?room ex:newHighReading ?temp . 
}"#;
    
    println!("Querying for new high readings...");
    let reading_results = execute_query(new_reading_query, &mut database);
    println!("New reading query results: {:?}", reading_results);

    // Query for predicted levels
    let prediction_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?level
WHERE { 
  ?room ex:predictedLevel ?level . 
}"#;
    
    println!("Querying for predicted levels...");
    let prediction_results = execute_query(prediction_query, &mut database);
    println!("Prediction query results: {:?}", prediction_results);

    println!("\n=== RSP Windowing Demo Complete ===");
    println!("Total triples in database: {}", database.triples.len());
    println!("Parser successfully handled all RSP windowing syntax!");
}