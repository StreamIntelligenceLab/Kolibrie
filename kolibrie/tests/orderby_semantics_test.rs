/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod common;

use common::{ordered, query, rows1};
use kolibrie::sparql_database::SparqlDatabase;

/// Three values chosen so numeric and lexical comparison disagree cyclically
fn cyclic_database() -> SparqlDatabase {
    common::database_with(
        r#"<urn:a> <urn:v> 9 .
<urn:b> <urn:v> 100 .
<urn:c> <urn:v> "50x" ."#,
    )
}

fn numeric_database() -> SparqlDatabase {
    common::database_with(
        r#"<urn:n1> <urn:v> 10 .
<urn:n2> <urn:v> 9 .
<urn:n3> <urn:v> 100 ."#,
    )
}

fn subjects(rows: Vec<Vec<String>>) -> Vec<String> {
    rows.into_iter().map(|row| row[0].clone()).collect()
}

#[test]
fn numbers_sort_numerically_not_lexically() {
    let mut database = numeric_database();
    assert_eq!(
        subjects(ordered(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:v> ?v } ORDER BY ?v",
        )),
        vec!["urn:n2", "urn:n1", "urn:n3"]
    );
}

#[test]
fn descending_reverses_the_numeric_order() {
    let mut database = numeric_database();
    assert_eq!(
        subjects(ordered(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:v> ?v } ORDER BY DESC(?v)",
        )),
        vec!["urn:n3", "urn:n1", "urn:n2"]
    );
}

#[test]
fn numeric_and_non_numeric_values_get_a_consistent_order() {
    let mut database = cyclic_database();
    // Numbers first, in numeric order; then everything else lexically
    assert_eq!(
        subjects(ordered(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:v> ?v } ORDER BY ?v",
        )),
        vec!["urn:a", "urn:b", "urn:c"]
    );
}

#[test]
fn descending_reverses_the_mixed_order() {
    let mut database = cyclic_database();
    assert_eq!(
        subjects(ordered(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:v> ?v } ORDER BY DESC(?v)",
        )),
        vec!["urn:c", "urn:b", "urn:a"]
    );
}

#[test]
fn ordering_a_large_mixed_column_is_deterministic_and_does_not_panic() {
    // A wide spread of values whose numeric and lexical orders disagree
    let mut triples = String::new();
    let values = [
        "9", "100", "50x", "1", "20", "3.5", "abc", "007", "-4", "1e3", "z", "0", "10",
        "2busy", "", "  ", "999999999999999999999", "0.0001", "Zebra", "apple",
    ];
    for (index, value) in values.iter().enumerate() {
        triples.push_str(&format!("<urn:s{}> <urn:v> \"{}\" .\n", index, value));
    }
    let mut database = common::database_with(&triples);

    let first = ordered(
        &mut database,
        "SELECT ?s ?v WHERE { ?s <urn:v> ?v } ORDER BY ?v",
    );
    let second = ordered(
        &mut database,
        "SELECT ?s ?v WHERE { ?s <urn:v> ?v } ORDER BY ?v",
    );
    assert_eq!(first.len(), values.len());
    assert_eq!(first, second, "ORDER BY must be deterministic");
}

#[test]
fn order_by_the_second_key_breaks_ties_on_the_first() {
    let mut database = common::database_with(
        r#"<urn:t1> <urn:g> 1 .
<urn:t1> <urn:v> 20 .
<urn:t2> <urn:g> 1 .
<urn:t2> <urn:v> 10 .
<urn:t3> <urn:g> 0 .
<urn:t3> <urn:v> 30 ."#,
    );
    assert_eq!(
        subjects(ordered(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:g> ?g . ?s <urn:v> ?v } ORDER BY ?g ?v",
        )),
        vec!["urn:t3", "urn:t2", "urn:t1"]
    );
}

#[test]
fn a_subquery_orders_before_it_limits() {
    let mut database = cyclic_database();
    // The subquery must keep the two smallest values under the same total
    assert_eq!(
        query(
            &mut database,
            "SELECT ?s WHERE { { SELECT ?s ?v WHERE { ?s <urn:v> ?v } ORDER BY ?v LIMIT 2 } }",
        ),
        rows1(&["urn:a", "urn:b"])
    );
}

#[test]
fn a_descending_subquery_orders_before_it_limits() {
    let mut database = cyclic_database();
    assert_eq!(
        query(
            &mut database,
            "SELECT ?s WHERE { { SELECT ?s ?v WHERE { ?s <urn:v> ?v } ORDER BY DESC(?v) LIMIT 2 } }",
        ),
        rows1(&["urn:b", "urn:c"])
    );
}

#[test]
fn subquery_and_top_level_ordering_agree_on_numbers() {
    let mut database = numeric_database();
    assert_eq!(
        query(
            &mut database,
            "SELECT ?s WHERE { { SELECT ?s ?v WHERE { ?s <urn:v> ?v } ORDER BY ?v LIMIT 2 } }",
        ),
        rows1(&["urn:n1", "urn:n2"])
    );
}
