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
    ?s hasDescriptor ?descriptor .
    ?descriptor :hasMetaData ?meta.
    ?meta :hasLocation <:somelocation>.
    ?meta :hasCoverage <:someArea>.
}
REGISTER RSTREAM <http://out/stream> AS
SELECT *
FROM NAMED WINDOW :wind ON ?s [RANGE PT10M STEP PT1M]
WHERE {
    WINDOW :wind {
        ?obs a ssn:Observation .
        ?obs ssn:hasSimpleResult ?value .
        ?obs ssn:obsevedProperty ?prop .
        ?prop a :Temperature .
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
            println!("All components parsed successfully!");
        },
        Err(e) => {
            println!("Error: {:?}", e);
            
        }
    }
}
