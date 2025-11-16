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

fn main() {
    let query = r#"RETRIEVE SOME ACTIVE STREAM ?s FROM <http://my.org/catalog>
WITH {
    ?s a :Stream .
    ?s :hasDescriptor ?descriptor .
    ?descriptor :hasMetaData ?meta.
    ?meta :hasLocation <:somelocation>.
    ?meta :hasCoverage <:someArea>.
}
REGISTER RSTREAM <http://out/stream> AS
SELECT *
FROM NAMED WINDOW :wind ON ?s [RANGE PT10M STEP PT1M]
FROM NAMED WINDOW :wind2 ON :uri2 [RANGE PT5M STEP PT30S]
WHERE {
    WINDOW :wind {
        ?obs a ssn:Observation .
        ?obs ssn:hasSimpleResult ?value .
        ?obs ssn:obsevedProperty ?prop .
        ?prop a :Temperature .
    }
    WINDOW :wind2 {
        ?obs2 a ssn:Observation .
        ?obs2 ssn:hasSimpleResult ?value2 .
        ?obs2 ssn:obsevedProperty ?prop2 .
        ?prop2 a :CO2 .
    }
}"#;
    match parse_combined_query(query) {
        Ok((remaining, parsed_query)) => {
            println!("Query parsed successfully!");
            println!("Remaining input: '{}'", remaining.trim());
            println!();
            
            // Print RETRIEVE clause details
            if let Some(retrieve) = &parsed_query.retrieve_clause {
                println!("RETRIEVE Clause:");
                println!("  Mode: {:?}", retrieve.mode);
                println!("  State: {:?}", retrieve.state);
                println!("  Variable: {}", retrieve.variable);
                println!("  From IRI: {}", retrieve.from_iri);
                println!("  Graph patterns ({} triples):", retrieve.graph_pattern.len());
                for (i, (s, p, o)) in retrieve.graph_pattern.iter().enumerate() {
                    println!("    {}: {} {} {}", i + 1, s, p, o);
                }
                println!();
            }
            
            // Print REGISTER clause details
            if let Some(register) = &parsed_query.register_clause {
                println!("REGISTER Clause:");
                println!("  Stream Type: {:?}", register.stream_type);
                println!("  Output Stream IRI: {}", register.output_stream_iri);
                println!("  Window Clauses ({} windows):", register.query.window_clause.len());
                for (i, window) in register.query.window_clause.iter().enumerate() {
                    println!("    Window {}:", i + 1);
                    println!("      Window IRI: {}", window.window_iri);
                    println!("      Stream IRI: {}", window.stream_iri);
                    println!("      Window Type: {:?}", window.window_spec.window_type);
                    println!("      Width: {}", window.window_spec.width);
                    if let Some(slide) = window.window_spec.slide {
                        println!("      Slide: {}", slide);
                    }
                    if let Some(report) = window.window_spec.report_strategy {
                        println!("      Report Strategy: {}", report);
                    }
                    if let Some(tick) = window.window_spec.tick {
                        println!("      Tick: {}", tick);
                    }
                }
                println!();

                // Print window-specific queries
                println!("  Window Queries ({} blocks):", register.query.window_blocks.len());
                for (i, window_block) in register.query.window_blocks.iter().enumerate() {
                    println!("    Window Block {} ({}): {} triples", 
                        i + 1, window_block.window_name, window_block.patterns.len());
                    for (j, (s, p, o)) in window_block.patterns.iter().enumerate() {
                        println!("      {}: {} {} {}", j + 1, s, p, o);
                    }
                    println!();
                }
            }
            
            println!("All components parsed successfully!");
        },
        Err(e) => {
            println!("Error: {:?}", e);
        }
    }
}
