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

// Adding FILTER
fn filter_char() {
    let rdf_data = r##"
<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:dc="http://purl.org/dc/elements/1.1/">

  <rdf:Description rdf:about="#book1">
    <dc:title>To Kill a Mockingbird</dc:title>
    <dc:creator>Harper Lee</dc:creator>
    <dc:date>1960</dc:date>
  </rdf:Description>

  <rdf:Description rdf:about="#book2">
    <dc:title>Pride and Prejudice</dc:title>
    <dc:creator>Jane Austen</dc:creator>
    <dc:date>1813</dc:date>
  </rdf:Description>

</rdf:RDF>
    "##;
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    let sparql = r#"
    PREFIX dc: <http://purl.org/dc/elements/1.1/> 
    SELECT ?title ?author 
    WHERE {
      ?book dc:title ?title . 
      ?book dc:creator ?author 
      FILTER (?author = "Jane Austen")
    }"#;

    let results = execute_query(sparql, &mut database);

    // Now the main function is responsible for printing the results
    println!("Results:");
    for result in results {
        if let [title, author] = &result[..] {
            println!("?title = {} ?author = {}", title, author);
        }
    }
}

fn main() {
    filter_char();
}