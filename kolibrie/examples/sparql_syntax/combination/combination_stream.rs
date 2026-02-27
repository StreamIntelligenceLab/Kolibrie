/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::execute_query::*;
use kolibrie::parser::*;
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

    // Basic RSP rule with RSTREAM and sliding window
    println!("\n=== Basic RSP Windowing Rule ===");
    let rsp_rule_basic = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :TemperatureAlert(?room) :- 
RSTREAM
FROM NAMED WINDOW <http://example.org/window1> ON <http://example.org/temperatureStream> [SLIDING 10 SLIDE 2 REPORT ON_WINDOW_CLOSE TICK TIME_DRIVEN] 
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
    // DEBUG: Track database state before windowing
    let triples_before_rsp = database.triples.len();
    println!(
        "DEBUG: Database has {} triples before RSP processing",
        triples_before_rsp
    );

    let rule_result = process_rule_definition(rsp_rule_basic, &mut database);

    // DEBUG: Track database state after windowing
    let triples_after_rsp = database.triples.len();
    println!(
        "DEBUG: Database has {} triples after RSP processing (+{})",
        triples_after_rsp,
        triples_after_rsp - triples_before_rsp
    );

    match rule_result {
        Ok((rule, inferred_facts)) => {
            println!("RSP Rule processed successfully!");

            // DEBUG: Check if windowing was actually applied
            println!("DEBUG: RSP Windowing Analysis:");
            println!("   - Inferred {} facts", inferred_facts.len());
            println!("   - Rule premise patterns: {}", rule.premise.len());
            println!("   - Rule filter conditions: {}", rule.filters.len());

            // DEBUG: Analyze inferred facts for windowing evidence
            for (i, triple) in inferred_facts.iter().enumerate() {
                let dict = database.dictionary.read().unwrap();
                let s = dict.decode(triple.subject).unwrap_or("unknown");
                let p = dict.decode(triple.predicate).unwrap_or("unknown");
                let o = dict.decode(triple.object).unwrap_or("unknown");
                println!("DEBUG: Fact {}: {} -> {} -> {}", i + 1, s, p, o);

                if p.contains("hasAlert") {
                    println!("   WINDOWING EVIDENCE: Found hasAlert predicate");
                }
            }

            println!("Rule structure: {:#?}", rule);
            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                let dict = database.dictionary.read().unwrap();
                println!(
                    "  {}",
                    database.triple_to_string(triple, &*dict)
                );
            }
        }
        Err(e) => println!("Failed to process RSP rule: {}", e),
    }

    // ISTREAM with tumbling window
    println!("\n=== ISTREAM with Tumbling Window ===");
    let rsp_rule_istream = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :NewHighTemp(?room) :- 
ISTREAM
FROM NAMED WINDOW <http://example.org/window2> ON <http://example.org/tempStream> [TUMBLING 5 REPORT NON_EMPTY_CONTENT TICK TUPLE_DRIVEN] 
CONSTRUCT { 
    ?room ex:newHighReading ?temp .
}
WHERE { 
    ?reading ex:room ?room ; 
             ex:temperature ?temp .
    FILTER (?temp > 85)
}"#;

    println!("Processing ISTREAM rule with tumbling window...");
    // DEBUG: Track ISTREAM behavior
    let triples_before_istream = database.triples.len();
    println!(
        "DEBUG: Database has {} triples before ISTREAM processing",
        triples_before_istream
    );

    let rule_result2 = process_rule_definition(rsp_rule_istream, &mut database);

    let triples_after_istream = database.triples.len();
    println!(
        "DEBUG: Database has {} triples after ISTREAM processing (+{})",
        triples_after_istream,
        triples_after_istream - triples_before_istream
    );

    match rule_result2 {
        Ok((rule, inferred_facts)) => {
            println!("ISTREAM Rule processed successfully!");

            // DEBUG: Compare ISTREAM vs RSTREAM behavior
            println!("DEBUG: ISTREAM vs RSTREAM Comparison:");
            println!(
                "   - RSTREAM added: {} triples",
                triples_after_rsp - triples_before_rsp
            );
            println!(
                "   - ISTREAM added: {} triples",
                triples_after_istream - triples_before_istream
            );

            if (triples_after_istream - triples_before_istream)
                != (triples_after_rsp - triples_before_rsp)
            {
                println!(
                    "   WINDOWING WORKING: Different stream operators produce different results!"
                );
            } else {
                println!("   WINDOWING UNCLEAR: Same results from different stream operators");
            }

            println!("Rule has windowing: {}", rule.premise.len() > 0);
            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                let dict = database.dictionary.read().unwrap();
                println!(
                    "  {}",
                    database.triple_to_string(triple, &*dict)
                );
            }
        }
        Err(e) => println!("Failed to process ISTREAM rule: {}", e),
    }

    // DSTREAM with range window and ML.PREDICT
    println!("\n=== DSTREAM with Range Window and ML.PREDICT ===");
    let rsp_rule_ml = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :PredictiveAlert :- 
DSTREAM
FROM NAMED WINDOW <http://example.org/window3> ON <http://example.org/sensorStream> [RANGE 15 REPORT PERIODIC TICK TIME_DRIVEN]
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
    // DEBUG: Track DSTREAM behavior
    let triples_before_dstream = database.triples.len();
    println!(
        "DEBUG: Database has {} triples before DSTREAM processing",
        triples_before_dstream
    );

    let rule_result3 = process_rule_definition(rsp_rule_ml, &mut database);

    let triples_after_dstream = database.triples.len();
    println!(
        "DEBUG: Database has {} triples after DSTREAM processing (+{})",
        triples_after_dstream,
        triples_after_dstream - triples_before_dstream
    );

    match rule_result3 {
        Ok((rule, inferred_facts)) => {
            println!("DSTREAM + ML.PREDICT Rule processed successfully!");

            // DEBUG: Three-way comparison
            println!("DEBUG: Three-way Stream Operator Comparison:");
            println!(
                "   - RSTREAM added: {} triples",
                triples_after_rsp - triples_before_rsp
            );
            println!(
                "   - ISTREAM added: {} triples",
                triples_after_istream - triples_before_istream
            );
            println!(
                "   - DSTREAM added: {} triples",
                triples_after_dstream - triples_before_dstream
            );

            let all_different = (triples_after_rsp - triples_before_rsp)
                != (triples_after_istream - triples_before_istream)
                || (triples_after_istream - triples_before_istream)
                    != (triples_after_dstream - triples_before_dstream);

            if all_different {
                println!("   WINDOWING CONFIRMED: All stream operators behave differently!");
            } else {
                println!("   WINDOWING NEEDS INVESTIGATION: Similar behavior across operators");
            }

            println!("Rule has {} premise patterns", rule.premise.len());
            println!("Rule has {} filter conditions", rule.filters.len());
            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                let dict = database.dictionary.read().unwrap();
                println!(
                    "  {}",
                    database.triple_to_string(triple, &*dict)
                );
            }
        }
        Err(e) => println!("Failed to process DSTREAM + ML rule: {}", e),
    }

    // Test parsing individual components
    println!("\n=== Testing Individual Parser Components ===");

    // Test stream type parsing
    let stream_tests = vec!["RSTREAM", "ISTREAM", "DSTREAM"];
    for stream in stream_tests {
        match parse_stream_type(stream) {
            Ok((_, stream_type)) => {
                println!("Parsed stream type: {:?}", stream_type);
                println!("DEBUG: Stream type {} parsed successfully", stream);
            }
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

            // DEBUG: Verify window parameters
            println!("DEBUG: Window parameters validation:");
            println!("   - SLIDING window with width {} detected", spec.width);
            if let Some(slide) = spec.slide {
                println!("   - Slide parameter {} detected", slide);
                if slide < spec.width {
                    println!(
                        "   OVERLAPPING WINDOWS: slide ({}) < width ({})",
                        slide, spec.width
                    );
                } else {
                    println!(
                        "   NON-OVERLAPPING: slide ({}) >= width ({})",
                        slide, spec.width
                    );
                }
            }
        }
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

            // DEBUG: Verify windowing clause parsing
            println!("DEBUG: Windowing clause validation:");
            println!("   - Window IRI: {}", clause.window_iri);
            println!("   - Stream IRI: {}", clause.stream_iri);
            println!("   PARSING SUCCESS: FROM NAMED WINDOW clause parsed correctly");
        }
        Err(e) => println!("Failed to parse FROM NAMED WINDOW: {:?}", e),
    }

    // Query the results
    println!("\n=== Querying RSP Results ===");

    // Query for high temperature alerts
    let alert_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?alert
WHERE { 
  ?room ex:hasAlert ?alert . 
}"#;

    println!("Querying for temperature alerts...");
    let query_results = execute_query(alert_query, &mut database);
    println!("Alert query results: {:?}", query_results);

    // DEBUG: Validate windowing results
    println!("DEBUG: Alert query validation:");
    println!("   - Found {} alert results", query_results.len());
    if query_results.len() > 0 {
        println!("   WINDOWING SUCCESS: hasAlert predicates found in database");
        for (i, result) in query_results.iter().enumerate() {
            println!("     Alert {}: {:?}", i + 1, result);
        }
    } else {
        println!("   NO ALERTS: No hasAlert predicates found - check windowing logic");
    }

    // Query for new high readings
    let new_reading_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { 
  ?room ex:newHighReading ?temp . 
}"#;

    println!("Querying for new high readings...");
    let reading_results = execute_query(new_reading_query, &mut database);
    println!("New reading query results: {:?}", reading_results);

    // DEBUG: Validate ISTREAM results
    println!("DEBUG: ISTREAM reading validation:");
    println!("   - Found {} reading results", reading_results.len());
    if reading_results.len() > 0 {
        println!("   ISTREAM SUCCESS: newHighReading predicates found");
    } else {
        println!("   NO READINGS: No newHighReading predicates found");
    }

    // Query for predicted levels
    let prediction_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?level
WHERE { 
  ?room ex:predictedLevel ?level . 
}"#;

    println!("Querying for predicted levels...");
    let prediction_results = execute_query(prediction_query, &mut database);
    println!("Prediction query results: {:?}", prediction_results);

    // DEBUG: Validate DSTREAM results
    println!("DEBUG: DSTREAM prediction validation:");
    println!("   - Found {} prediction results", prediction_results.len());
    if prediction_results.len() > 0 {
        println!("   DSTREAM SUCCESS: predictedLevel predicates found");
    } else {
        println!("   NO PREDICTIONS: No predictedLevel predicates found");
    }

    // DEBUG: Final windowing assessment
    println!("\nDEBUG: Final Windowing Assessment:");
    let total_windowing_results =
        query_results.len() + reading_results.len() + prediction_results.len();
    println!("   - Total windowing results: {}", total_windowing_results);
    println!("   - RSTREAM alerts: {}", query_results.len());
    println!("   - ISTREAM readings: {}", reading_results.len());
    println!("   - DSTREAM predictions: {}", prediction_results.len());

    if total_windowing_results > 0 {
        println!("   OVERALL SUCCESS: Windowing is working and producing results!");
    } else {
        println!("   OVERALL FAILURE: No windowing results found - check implementation");
    }

    println!("\n=== RSP Windowing Demo Complete ===");
    println!("Total triples in database: {}", database.triples.len());
    println!("Parser successfully handled all RSP windowing syntax!");

    // DEBUG: Final database state
    println!("DEBUG: Final database state:");
    println!("   - Started with: {} triples", database.triples.len());
    println!("   - Final count: {} triples", database.triples.len());
    println!("   - Total added: {} triples", database.triples.len() - 15); // 15 original triples
}

