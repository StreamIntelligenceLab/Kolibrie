/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

// Simple select
fn simple_select() {
    let rdf_data = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:ex="http://example.org/">
        <rdf:Description rdf:about="http://example.org/John">
            <ex:name>John</ex:name>
            <ex:age>42</ex:age>
            <ex:knows rdf:resource="http://example.org/Alice"/>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org/Alice">
            <ex:name>Alice</ex:name>
            <ex:age>30</ex:age>
        </rdf:Description>
        </rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    let sparql = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?name ?age
    WHERE {
        ?person ex:name ?name ; 
                ex:age ?age
    }"#;

    let results = execute_query(sparql, &mut database);

    // Now the main function is responsible for printing the results
    println!("Results: {:?}", results);
}

fn main() {
  simple_select();
}