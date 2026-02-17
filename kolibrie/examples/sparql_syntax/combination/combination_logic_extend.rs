/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::parser::*;
use kolibrie::execute_query::*;
use kolibrie::sparql_database::SparqlDatabase;

fn main() {
  std::env::set_var("RUST_BACKTRACE", "1");
  let rdf_xml_data = r#"
      <?xml version="1.0"?>
      <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
               xmlns:ex="http://example.org#">
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
  println!("Database RDF triples: {:#?}", database.triples);

  // Define the rule separately
  let rule_definition = r#"PREFIX ex: <http://example.org#>
RULE :OverheatingAlert(?room, ?temp) :- 
  CONSTRUCT { 
      ?room ex:overheatingAlert true .
  }
  WHERE { 
      ?reading ex:room ?room ; 
               ex:temperature ?temp .
      FILTER (?temp > 80)
  }"#;

  // Process the rule definition
  let rule_result = process_rule_definition(rule_definition, &mut database);
  
  if let Ok((rule, inferred_facts)) = rule_result {
      println!("Rule processed successfully.");
      println!("Rule details: {:#?}", rule);
      
      println!("Inferred {} new fact(s):", inferred_facts.len());
      let dict = database.dictionary.read().unwrap();
      for triple in inferred_facts.iter() {
          println!("{}", database.triple_to_string(triple, &dict));
      }
  } else {
      println!("Failed to process rule definition: {:?}", rule_result.err());
      return; // Exit if rule processing failed
  }
  
  // Define the SELECT query separately
  let select_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { 
  ?room ex:overheatingAlert true . 
}"#;
  
  // Execute the SELECT query that uses the rule
  let query_results = execute_query(select_query, &mut database);
  println!("Query results: {:?}", query_results);
}
