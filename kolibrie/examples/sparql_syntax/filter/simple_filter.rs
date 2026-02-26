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

fn select() {
    // Define the RDF/XML string representing the inserted triples
    let rdf_xml = r#"
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://example.org/">
    <rdf:Description rdf:about="http://example.org/person1">
        <ex:hasOccupation>Engineer</ex:hasOccupation>
    </rdf:Description>
    <rdf:Description rdf:about="http://example.org/person2">
        <ex:hasOccupation>Artist</ex:hasOccupation>
    </rdf:Description>
    <rdf:Description rdf:about="http://example.org/person3">
        <ex:hasOccupation>Doctor</ex:hasOccupation>
    </rdf:Description>
</rdf:RDF>
"#;

    // Initialize a sample database (assuming SparqlDatabase and Triple are implemented)
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml);

    // Define an INSERT SPARQL query
    let sparql_query = r#"PREFIX ex: <http://example.org/> SELECT ?person WHERE {?person ex:hasOccupation "Engineer"}"#;

    // Execute the query on the database
    let results = execute_query(sparql_query, &mut database);

    println!("{:?}", results);

    let dict = database.dictionary.read().unwrap();
    for triple in &database.triples {
        let subject = dict.decode(triple.subject).unwrap_or_default();
        let predicate = dict.decode(triple.predicate).unwrap_or_default();
        let object = dict.decode(triple.object).unwrap_or_default();
        println!("Triple: ({}, {}, {})", subject, predicate, object);
    }
    drop(dict);
    

    // Output the results (if any)
    for result in results {
        if let [person] = &result[..] {
            println!("{}", person);
        }
    }
}

fn main() {
    select();
}

