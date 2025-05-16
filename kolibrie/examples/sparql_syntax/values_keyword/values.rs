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

fn values() {
    let rdf_data = r#"
<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns:ex="http://example.org/">

    <rdf:Description rdf:about="http://example.org/person1">
        <ex:worksAt rdf:resource="http://example.org/companyA"/>
    </rdf:Description>

    <rdf:Description rdf:about="http://example.org/person2">
        <ex:worksAt rdf:resource="http://example.org/companyB"/>
    </rdf:Description>

    <rdf:Description rdf:about="http://example.org/person3">
        <ex:worksAt rdf:resource="http://example.org/companyA"/>
    </rdf:Description>

</rdf:RDF>
"#;
    // Create an instance of SparqlDatabase
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    // Sample SPARQL query using VALUES clause
    let sparql_query = r#"PREFIX ex: <http://example.org/> SELECT ?person ?company WHERE {?person ex:worksAt ?company} VALUES ?company { ex:companyA ex:companyB }"#;

    // Execute the query on the database
    let results = execute_query(sparql_query, &mut database);

    // Display the results
    println!("Query Results:");
    for result in results {
        println!("{:?}", result);
    }
}

fn main() {
    values();
}
