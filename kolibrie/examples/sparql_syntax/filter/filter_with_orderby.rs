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

fn main() {
    let rdf_data = r#"
    <?xml version="1.0" encoding="UTF-8"?>
    <rdf:RDF 
        xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" 
        xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#" 
        xmlns:foaf="http://xmlns.com/foaf/0.1/"
        xmlns:time="http://www.w3.org/2006/time#"
        xmlns:ex="http://example.org/vocab#">
        
        <rdf:Description rdf:about="http://example.org/event1">
            <ex:name>Conference A</ex:name>
            <ex:location>New York</ex:location>
            <time:hasBeginning>2025-01-10T09:00:00</time:hasBeginning>
            <time:hasEnd>2025-01-12T17:00:00</time:hasEnd>
            <ex:attendees>120</ex:attendees>
            <ex:type>Technical</ex:type>
        </rdf:Description>
        
        <rdf:Description rdf:about="http://example.org/event2">
            <ex:name>Workshop B</ex:name>
            <ex:location>San Francisco</ex:location>
            <time:hasBeginning>2025-02-15T10:00:00</time:hasBeginning>
            <time:hasEnd>2025-02-15T16:00:00</time:hasEnd>
            <ex:attendees>45</ex:attendees>
            <ex:type>Business</ex:type>
        </rdf:Description>
        
        <rdf:Description rdf:about="http://example.org/event3">
            <ex:name>Seminar C</ex:name>
            <ex:location>London</ex:location>
            <time:hasBeginning>2025-03-20T14:00:00</time:hasBeginning>
            <time:hasEnd>2025-03-21T12:00:00</time:hasEnd>
            <ex:attendees>75</ex:attendees>
            <ex:type>Academic</ex:type>
        </rdf:Description>
        
        <rdf:Description rdf:about="http://example.org/event4">
            <ex:name>Meetup D</ex:name>
            <ex:location>Berlin</ex:location>
            <time:hasBeginning>2025-04-05T18:00:00</time:hasBeginning>
            <time:hasEnd>2025-04-05T21:00:00</time:hasEnd>
            <ex:attendees>30</ex:attendees>
            <ex:type>Social</ex:type>
        </rdf:Description>
        
        <rdf:Description rdf:about="http://example.org/event5">
            <ex:name>Conference E</ex:name>
            <ex:location>Tokyo</ex:location>
            <time:hasBeginning>2025-05-12T09:00:00</time:hasBeginning>
            <time:hasEnd>2025-05-14T17:00:00</time:hasEnd>
            <ex:attendees>200</ex:attendees>
            <ex:type>Technical</ex:type>
        </rdf:Description>
    </rdf:RDF>
    "#;
    
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    let sparql = r#"PREFIX ex: <http://example.org/vocab#>
    SELECT ?name ?type ?attendees
    WHERE {
        ?event ex:name ?name .
        ?event ex:type ?type .
        ?event ex:attendees ?attendees .
        FILTER (?type = "Technical" || ?type = "Academic")
    }
    ORDER BY ?attendees"#;

    let results = execute_query(sparql, &mut database);
    for result in results {
        if let [name, type_, attendees] = &result[..] {
            println!("Name: {}, Type: {}, Attendees: {}", name, type_, attendees);
        }
    }
}
