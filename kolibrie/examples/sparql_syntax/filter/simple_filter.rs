/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::prelude::*;

fn select() {
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

    let mut database = KolibrieDB::new();
    database.parse_rdf(rdf_xml);

    let sparql_query = r#"PREFIX ex: <http://example.org/> SELECT ?person WHERE {?person ex:hasOccupation "Engineer"}"#;

    let results = execute(sparql_query, &mut database)
        .expect("query should run")
        .into_rows();

    println!("{:?}", results);

    let dict = database.dictionary();
    for triple in database.query_default_triples(None, None, None) {
        let subject = dict.decode(triple.subject).unwrap_or_default();
        let predicate = dict.decode(triple.predicate).unwrap_or_default();
        let object = dict.decode(triple.object).unwrap_or_default();
        println!("Triple: ({}, {}, {})", subject, predicate, object);
    }
    drop(dict);

    for result in results {
        if let [person] = &result[..] {
            println!("{}", person);
        }
    }
}

fn main() {
    select();
}
