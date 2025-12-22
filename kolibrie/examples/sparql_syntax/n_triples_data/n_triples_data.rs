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
use kolibrie::sparql_database::*;

fn n_triple_simple_query() {
    let mut db = SparqlDatabase::new();

    // N-Triples format
    let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> .
<http://example.org/jane> <http://example.org/name> "Jane Doe" .
<http://example.org/john> <http://example.org/name> "John Smith" .
<http://example.org/jane> <http://example.org/age> "25"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/john> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
    "#;

    // Use the parse_ntriples function
    db.parse_ntriples_and_add(ntriples_data);
    db.get_or_build_stats();

    let sparql_query = r#"
    PREFIX ex: <http://example.org/> 
    SELECT ?name 
    WHERE {
        ?person ex:hasFriend ?friend . 
        ?friend ex:name ?name
    }"#;

    let results = execute_query_rayon_parallel2_volcano(sparql_query, &mut db);
    println!("Query Results:");
    for result in results {
        if let [name] = &result[..] {
            println!("Friend's name: {}", name);
        }
    }
}

fn main() {
    n_triple_simple_query();
}
