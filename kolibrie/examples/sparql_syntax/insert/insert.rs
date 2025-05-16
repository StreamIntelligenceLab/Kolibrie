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

fn insert() {
    // Define the RDF/XML string representing the inserted triples
    let rdf_xml = r#"
<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/JohnDoe">
    <ex:age>30</ex:age>
  </rdf:Description>
</rdf:RDF>
"#;

    // Initialize a sample database (assuming SparqlDatabase and Triple are implemented)
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml);

    // Define an INSERT SPARQL query
    let sparql_query = r#"PREFIX ex: <http://example.org/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> INSERT {<http://example.org/JohnDoe> ex:occupation "Software Developer"} WHERE {<http://example.org/JohnDoe> ex:age "30"}"#;

    // Execute the query on the database
    let _results = execute_query(sparql_query, &mut database);

    database.debug_print_triples();
}

fn main() {
    insert();
}

