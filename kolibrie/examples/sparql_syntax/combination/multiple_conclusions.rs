/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::parser::*;
use kolibrie::execute_query::*;
use kolibrie::sparql_database::SparqlDatabase;
use datalog::knowledge_graph::KnowledgeGraph;

fn main() {
    std::env::set_var("RUST_BACKTRACE", "1");
    let rdf_xml_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org#"
                 xmlns:alert="http://example.org/alerts#">
          <rdf:Description rdf:about="http://example.org#Room101">
            <ex:temperature>75</ex:temperature>
            <ex:room>Room101</ex:room>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Sensor1">
            <ex:room>Room101</ex:room>
            <ex:temperature>90</ex:temperature>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Room102">
            <ex:temperature>35</ex:temperature>
            <ex:room>Room102</ex:room>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Sensor2">
            <ex:room>Room102</ex:room>
            <ex:temperature>70</ex:temperature>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Room103">
            <ex:temperature>45</ex:temperature>
            <ex:room>Room103</ex:room>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Sensor3">
            <ex:room>Room103</ex:room>
            <ex:temperature>190</ex:temperature>
          </rdf:Description>
        </rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml_data);
    println!("Database RDF triples loaded.");

    let mut kg = KnowledgeGraph::new();
    for triple in database.triples.iter() {
        let subject = database.dictionary.decode(triple.subject);
        let predicate = database.dictionary.decode(triple.predicate);
        let object = database.dictionary.decode(triple.object);
        kg.add_abox_triple(&subject.unwrap(), &predicate.unwrap(), &object.unwrap());
    }
    println!("KnowledgeGraph ABox loaded.");

    let combined_query_input = r#"PREFIX ex: <http://example.org#>
PREFIX alert: <http://example.org/alerts#>
RULE :OverheatingAlert(?room, ?temp) :- 
    WHERE { 
        ?reading ex:room ?room ; 
                 ex:temperature ?temp .
        FILTER (?temp > 80)
    } 
    => 
    { 
        ?room ex:overheatingAlert true ;
             alert:status "Critical" ;
             alert:timestamp "2025-04-19T13:49:18Z" ;
             alert:requiresAction true .
    }.
SELECT ?room ?temp ?status ?timestamp ?action
WHERE { 
    :OverheatingAlert(?room, ?temp) .
    ?room alert:status ?status .
    ?room alert:timestamp ?timestamp .
    ?room alert:requiresAction ?action .
}"#;

    // Process the combined query
    match process_combined_query(combined_query_input, &mut database, &mut kg) {
        Ok((rule_result, inferred_facts, prefixes)) => {
            println!("Combined query processed successfully.");
            println!("Prefixes: {:?}", prefixes);

            if let Some(rule) = rule_result {
                println!("Rule processed: {:?}", rule);
            }

            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                println!("{}", database.triple_to_string(triple, &database.dictionary));
            }
        }
        Err(error) => {
            eprintln!("Error processing combined query: {}", error);
        }
    }

    // Execute the SELECT query
    let query_results = execute_query(combined_query_input, &mut database);
    println!("Query results: {:?}", query_results);
}