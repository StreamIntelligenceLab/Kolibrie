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
#![allow(unreachable_pub)]

use kolibrie::database::KolibrieDB;
use kolibrie::execute_query::{execute, execute_sparql_query};

/// Normalizes a solution sequence into a comparable bag
pub fn bag(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// Runs a query and returns its solutions as a bag
pub fn query(database: &mut KolibrieDB, sparql: &str) -> Vec<Vec<String>> {
    bag(execute(sparql, database)
        .expect("query should run")
        .into_rows())
}

/// Runs a query and returns its solutions in the sequence the engine produced
pub fn ordered(database: &mut KolibrieDB, sparql: &str) -> Vec<Vec<String>> {
    execute(sparql, database)
        .expect("query should run")
        .into_rows()
}

/// Runs a query through the `Result`-returning entry point
pub fn query_checked(
    database: &mut KolibrieDB,
    sparql: &str,
) -> Result<Vec<Vec<String>>, kolibrie::execute_query::QueryError> {
    execute_sparql_query(sparql, database).map(bag)
}

/// Builds a database from N-Triples-style statements in the default graph
pub fn database_with(triples: &str) -> KolibrieDB {
    let mut database = KolibrieDB::new();
    load(&mut database, triples);
    database
}

/// Inserts N-Triples-style statements into an existing database
pub fn load(database: &mut KolibrieDB, triples: &str) {
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
