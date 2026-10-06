/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(dead_code)]

use kolibrie::execute_query::{execute_query_rayon_parallel2_volcano, execute_sparql_query};
use kolibrie::sparql_database::SparqlDatabase;

/// Normalizes a solution sequence into a comparable bag
pub fn bag(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// Runs a query and returns its solutions as a bag
pub fn query(database: &mut SparqlDatabase, sparql: &str) -> Vec<Vec<String>> {
    bag(execute_query_rayon_parallel2_volcano(sparql, database))
}

/// Runs a query and returns its solutions in the sequence the engine produced
pub fn ordered(database: &mut SparqlDatabase, sparql: &str) -> Vec<Vec<String>> {
    execute_query_rayon_parallel2_volcano(sparql, database)
}

/// Runs a query through the `Result`-returning entry point
pub fn query_checked(
    database: &mut SparqlDatabase,
    sparql: &str,
) -> Result<Vec<Vec<String>>, String> {
    execute_sparql_query(sparql, database).map(bag)
}

/// Builds a database from N-Triples-style statements in the default graph
pub fn database_with(triples: &str) -> SparqlDatabase {
    let mut database = SparqlDatabase::new();
    load(&mut database, triples);
    database
}

/// Inserts N-Triples-style statements into an existing database
pub fn load(database: &mut SparqlDatabase, triples: &str) {
    kolibrie::execute_query::execute_sparql_update(
        &format!("INSERT DATA {{\n{}\n}}", triples),
        database,
    )
    .expect("test fixture data must load");
}

/// Shorthand for a one-column expected result
pub fn rows1(values: &[&str]) -> Vec<Vec<String>> {
    bag(values.iter().map(|v| vec![v.to_string()]).collect())
}

/// Shorthand for a two-column expected result
pub fn rows2(values: &[(&str, &str)]) -> Vec<Vec<String>> {
    bag(values
        .iter()
        .map(|(a, b)| vec![a.to_string(), b.to_string()])
        .collect())
}
