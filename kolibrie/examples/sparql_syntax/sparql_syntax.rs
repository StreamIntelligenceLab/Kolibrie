/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::parser::*;
use kolibrie::sparql_database::*;
use std::time::Instant;
use kolibrie::volcano_optimizer::*;


fn volcano_optimizer_sparql() {
    // Step 1: Initialize the database
    let mut database = SparqlDatabase::new();

    // Step 2: Generate RDF data with 1_000_000 triples
    let mut rdf_data = String::from(
        r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://example.org/">"#,
    );

    for i in 0..1_000_000 {
        let person_uri = format!("http://example.org/person{}", i);
        let loved_person_uri = format!("http://example.org/person{}", (i + 1) % 1_000_000);

        rdf_data.push_str(&format!(
            r#"
            <rdf:Description rdf:about="{person}">
                <ex:loves rdf:resource="{loved_person}"/>
            </rdf:Description>"#,
            person = person_uri,
            loved_person = loved_person_uri,
        ));
    }

    rdf_data.push_str("</rdf:RDF>");

    database.parse_rdf(&rdf_data);
    database.build_all_indexes();

    // Step 3: Define the SPARQL query
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?loved_person
    WHERE {
        <http://example.org/person5> ex:loves ?loved_person .
        ?loved_person ex:loves <http://example.org/person7>
    }"#;

    // Step 4: Parse the SPARQL query
    if let Ok((_, (_, variables, patterns, filters, _, prefixes, _, _, _, _, _, _))) =
        parse_sparql_query(sparql_query)
    {
        // Merge prefixes into the database
        database.prefixes.extend(prefixes.clone());

        // Extract variables for the logical plan
        let variables_for_plan: Vec<(&str, &str)> = variables
            .iter()
            .map(|(agg_type, var, _)| (*agg_type, *var))
            .collect();

        // Build the logical plan
        let logical_plan = build_logical_plan(
            variables_for_plan,
            patterns.clone(),
            filters.clone(),
            &prefixes.clone(),
            &mut database,
            Vec::new(),
            None,
        );

        // Step 5: Initialize the optimizer and find the best physical plan
        let mut optimizer = VolcanoOptimizer::new(&database);
        let physical_plan = optimizer.find_best_plan(&logical_plan);

        println!("Logical Plan: {:?}", logical_plan);
        println!("Optimized Physical Plan: {:?}", physical_plan);

        // Step 6: Execute the physical plan
        let start = Instant::now();
        let results = physical_plan.execute(&mut database);
        let duration = start.elapsed();

        // Step 7: Extract and print the selected variables
        let selected_vars: Vec<String> = variables
            .iter()
            .filter(|(agg_type, _, _)| *agg_type == "VAR")
            .map(|(_, var, _)| var.to_string())
            .collect();

        println!("Query execution time: {:?}", duration);
        println!("Results:");
        for result in results {
            for var in &selected_vars {
                if let Some(value) = result.get(var) {
                    println!("{} = {}", var, value);
                }
            }
            println!("---");
        }
    } else {
        eprintln!("Failed to parse the SPARQL query.");
    }
}

fn main() {
    volcano_optimizer_sparql();
}
