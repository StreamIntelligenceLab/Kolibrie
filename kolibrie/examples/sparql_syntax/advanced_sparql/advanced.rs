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

fn query() {
    let rdf_data = r#"
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/Person">
    <rdf:type rdf:resource="http://www.w3.org/2000/01/rdf-schema#Class"/>
    <rdfs:label>Person</rdfs:label>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/Location">
    <rdf:type rdf:resource="http://www.w3.org/2000/01/rdf-schema#Class"/>
    <rdfs:label>Location</rdfs:label>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/City">
    <rdf:type rdf:resource="http://www.w3.org/2000/01/rdf-schema#Class"/>
    <rdfs:label>City</rdfs:label>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/worksAt">
    <rdf:type rdf:resource="http://www.w3.org/2000/01/rdf-schema#Property"/>
    <rdfs:domain rdf:resource="http://example.org/Person"/>
    <rdfs:range rdf:resource="http://example.org/Location"/>
    <rdfs:label>works at</rdfs:label>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/located">
    <rdf:type rdf:resource="http://www.w3.org/2000/01/rdf-schema#Property"/>
    <rdfs:domain rdf:resource="http://example.org/Location"/>
    <rdfs:range rdf:resource="http://example.org/City"/>
    <rdfs:label>located in</rdfs:label>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/peter">
    <rdf:type rdf:resource="http://example.org/Person"/>
    <ex:worksAt rdf:resource="http://example.org/kulak"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/charlotte">
    <rdf:type rdf:resource="http://example.org/Person"/>
    <ex:worksAt rdf:resource="http://example.org/ughent"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/kulak">
    <rdf:type rdf:resource="http://example.org/Location"/>
    <ex:located rdf:resource="http://example.org/kortrijk"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/ughent">
    <rdf:type rdf:resource="http://example.org/Location"/>
    <ex:located rdf:resource="http://example.org/ghent"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/kortrijk">
    <rdf:type rdf:resource="http://example.org/City"/>
    <rdfs:label>Kortrijk</rdfs:label>
  </rdf:Description>
  <rdf:Description rdf:about="http://example.org/ghent">
    <rdf:type rdf:resource="http://example.org/City"/>
    <rdfs:label>Ghent</rdfs:label>
  </rdf:Description>
</rdf:RDF> 
        "#;
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    let sparql = r#"PREFIX ex: <http://example.org/> SELECT ?person ?location ?city WHERE {?person ex:worksAt ?location . ?location ex:located ?city}"#;

    let results = execute_query(sparql, &mut database);

    println!("Results:");
    for result in results {
        if let [person, location, city] = &result[..] {
            println!(
                "?person = {} location = {} city = {}",
                person, location, city
            );
        }
    }
}

fn main() {
  query();
}