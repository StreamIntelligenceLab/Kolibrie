/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! A result that spans more than one decode chunk reports the right values

use kolibrie::database::KolibrieDB;
use kolibrie::execute_query::execute;
use std::collections::BTreeSet;

/// Comfortably more than one decode chunk, which is 1024 rows
const ROWS: usize = 3_000;

fn database_with_distinct_objects() -> KolibrieDB {
    let mut database = KolibrieDB::new();
    let mut data = String::with_capacity(ROWS * 80);
    for i in 0..ROWS {
        data.push_str(&format!("<urn:s{i}> <urn:p> <urn:o{i}> .\n"));
    }
    kolibrie::execute_query::execute_sparql_update(
        &format!("INSERT DATA {{\n{data}\n}}"),
        &mut database,
    )
    .expect("fixture must load");
    database
}

#[test]
fn every_row_past_the_first_chunk_keeps_its_own_value() {
    let mut database = database_with_distinct_objects();
    let solutions =
        execute("SELECT ?s ?o WHERE { ?s <urn:p> ?o }", &mut database).expect("query should run");

    assert_eq!(
        solutions.len(),
        ROWS,
        "the fixture must span several chunks"
    );

    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    for solution in solutions.iter() {
        seen.insert((
            solution.get("s").expect("?s is bound").to_string(),
            solution.get("o").expect("?o is bound").to_string(),
        ));
    }

    let expected: BTreeSet<(String, String)> = (0..ROWS)
        .map(|i| (format!("urn:s{i}"), format!("urn:o{i}")))
        .collect();
    assert_eq!(
        seen.len(),
        ROWS,
        "every row must be distinct: a repeated pair means a chunk read another \
         chunk's terms"
    );
    assert_eq!(
        seen, expected,
        "the values read back must be the ones stored"
    );
}

#[test]
fn materialising_a_multi_chunk_result_agrees_with_borrowing_it() {
    let mut database = database_with_distinct_objects();
    let borrowed: Vec<Vec<Option<String>>> =
        execute("SELECT ?s ?o WHERE { ?s <urn:p> ?o }", &mut database)
            .expect("query should run")
            .iter()
            .map(|solution| {
                solution
                    .values()
                    .map(|value| value.map(str::to_string))
                    .collect()
            })
            .collect();
    let materialised = execute("SELECT ?s ?o WHERE { ?s <urn:p> ?o }", &mut database)
        .expect("query should run")
        .into_rows();

    assert_eq!(borrowed.len(), ROWS);
    assert_eq!(materialised.len(), ROWS);
    for (row, (borrowed, materialised)) in borrowed.iter().zip(&materialised).enumerate() {
        let borrowed: Vec<&str> = borrowed
            .iter()
            .map(|value| value.as_deref().unwrap_or_default())
            .collect();
        let materialised: Vec<&str> = materialised.iter().map(String::as_str).collect();
        assert_eq!(
            borrowed, materialised,
            "row {row} differs between read paths"
        );
    }
}

#[test]
fn equal_results_compare_equal_with_different_term_tables() {
    let mut database = KolibrieDB::new();
    kolibrie::execute_query::execute_sparql_update(
        "INSERT DATA { <urn:s1> <urn:p> \"a\" . <urn:s2> <urn:p> \"b\" . <urn:s3> <urn:p> \"c\" . }",
        &mut database,
    )
    .expect("fixture must load");

    let limited = execute(
        "SELECT ?o WHERE { ?s <urn:p> ?o } ORDER BY ?o LIMIT 2",
        &mut database,
    )
    .expect("query should run");
    let filtered = execute(
        "SELECT ?o WHERE { ?s <urn:p> ?o FILTER(?o != \"c\") } ORDER BY ?o",
        &mut database,
    )
    .expect("query should run");

    assert_eq!(limited.len(), 2, "LIMIT keeps two");
    assert_eq!(filtered.len(), 2, "FILTER keeps the same two");
    assert_eq!(
        limited, filtered,
        "equality must compare values, not how they happen to be interned"
    );
}
