/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

fn n3_simple_query() {
    let mut db = SparqlDatabase::new();

    let n3_data = r#"
        @prefix ex: <http://example.org/> .
        ex:john ex:hasFriend ex:jane .
        ex:jane ex:name "Jane Doe" .
        ex:john ex:name "John Smith" .
    "#;

    db.parse_n3(n3_data);

    let sparql_query = r#"
    PREFIX ex: <http://example.org/> 
    SELECT ?name 
    WHERE {
        ?person ex:hasFriend ?friend . 
        ?friend ex:name ?name
    }"#;

    let results = execute_query(sparql_query, &mut db);
    println!("Results:");
    for result in results {
        if let [name] = &result[..] {
            println!("{}", name);
        }
    }
}

fn main() {
    n3_simple_query();
}