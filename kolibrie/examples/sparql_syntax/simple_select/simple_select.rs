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
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/">
          <rdf:Description rdf:about="http://example.org/peter">
            <ex:worksAt rdf:resource="http://example.org/kulak"/>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/kulak">
            <ex:located rdf:resource="http://example.org/kortrijk"/>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/charlotte">
            <ex:worksAt rdf:resource="http://example.org/ughent"/>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/ughent">
            <ex:located rdf:resource="http://example.org/ghent"/>
          </rdf:Description>
        </rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    let sparql = r#"PREFIX ex: <http://example.org/> SELECT ?person ?location WHERE {?person ex:worksAt ?location}"#;

    let results = execute_query(sparql, &mut database);

    // Now the main function is responsible for printing the results
    println!("Results:");
    for result in results {
        if let [person, location] = &result[..] {
            println!("?person = {}, ?location = {}", person, location);
        }
    }
}

fn main() {
  simple_select();
}