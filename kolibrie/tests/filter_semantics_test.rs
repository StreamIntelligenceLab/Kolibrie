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

use common::{query, rows1};
use kolibrie::sparql_database::SparqlDatabase;

fn people() -> SparqlDatabase {
    common::database_with(
        r#"<urn:alice> <urn:name> "Alice" .
<urn:alice> <urn:age> 30 .
<urn:bob> <urn:name> "Bob" .
<urn:bob> <urn:age> 25 .
<urn:carol> <urn:name> "Carol" .
<urn:carol> <urn:age> 40 ."#,
    )
}

fn names_where(database: &mut SparqlDatabase, filter: &str) -> Vec<Vec<String>> {
    query(
        database,
        &format!(
            "SELECT ?s WHERE {{ ?s <urn:name> ?n . FILTER({}) }}",
            filter
        ),
    )
}

// ---------------------------------------------------------------------------

#[test]
fn greater_than_orders_non_numeric_operands_lexically() {
    let mut database = people();
    assert_eq!(
        names_where(&mut database, r#"?n > "Bob""#),
        rows1(&["urn:carol"])
    );
}

#[test]
fn less_than_orders_non_numeric_operands_lexically() {
    let mut database = people();
    assert_eq!(
        names_where(&mut database, r#"?n < "Bob""#),
        rows1(&["urn:alice"])
    );
}

#[test]
fn greater_or_equal_does_not_admit_every_non_numeric_operand() {
    let mut database = people();
    // Zero-coercion made this true for "Alice" as well, since 0.0 >= 0.0
    assert_eq!(
        names_where(&mut database, r#"?n >= "Bob""#),
        rows1(&["urn:bob", "urn:carol"])
    );
}

#[test]
fn numeric_comparison_is_unaffected() {
    let mut database = people();
    assert_eq!(
        query(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:age> ?age . FILTER(?age > 26) }",
        ),
        rows1(&["urn:alice", "urn:carol"])
    );
}

#[test]
fn comparing_a_string_against_a_number_is_an_error_and_drops_the_row() {
    let mut database = people();
    assert_eq!(names_where(&mut database, r#"?n > 5"#), Vec::<Vec<String>>::new());
}

// ---------------------------------------------------------------------------

#[test]
fn negating_an_unbound_comparison_does_not_admit_every_row() {
    let mut database = people();
    // `?missing` is never bound, so the comparison errors and `!error` stays an error
    assert_eq!(
        names_where(&mut database, "!(?missing > 100)"),
        Vec::<Vec<String>>::new()
    );
}

#[test]
fn negating_a_division_by_zero_does_not_admit_every_row() {
    let mut database = people();
    assert_eq!(
        query(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:age> ?age . FILTER(!(?age / 0 > 1)) }",
        ),
        Vec::<Vec<String>>::new()
    );
}

#[test]
fn negating_a_satisfiable_comparison_still_inverts_it() {
    let mut database = people();
    assert_eq!(
        query(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:age> ?age . FILTER(!(?age > 26)) }",
        ),
        rows1(&["urn:bob"])
    );
}

// ---------------------------------------------------------------------------

#[test]
fn true_or_error_is_true() {
    let mut database = people();
    assert_eq!(
        names_where(&mut database, r#"?n = "Alice" || ?missing > 100"#),
        rows1(&["urn:alice"])
    );
}

#[test]
fn error_or_true_is_true() {
    let mut database = people();
    assert_eq!(
        names_where(&mut database, r#"?missing > 100 || ?n = "Alice""#),
        rows1(&["urn:alice"])
    );
}

#[test]
fn false_and_error_is_false() {
    let mut database = people();
    assert_eq!(
        names_where(&mut database, r#"?n = "Zed" && ?missing > 100"#),
        Vec::<Vec<String>>::new()
    );
}

#[test]
fn error_and_false_is_false() {
    let mut database = people();
    assert_eq!(
        names_where(&mut database, r#"?missing > 100 && ?n = "Zed""#),
        Vec::<Vec<String>>::new()
    );
}

#[test]
fn error_and_true_stays_an_error_and_drops_the_row() {
    let mut database = people();
    assert_eq!(
        names_where(&mut database, r#"?missing > 100 && ?n = "Alice""#),
        Vec::<Vec<String>>::new()
    );
}

#[test]
fn an_unbound_comparison_alone_drops_every_row() {
    let mut database = people();
    assert_eq!(
        names_where(&mut database, "?missing > 100"),
        Vec::<Vec<String>>::new()
    );
}

#[test]
fn nested_boolean_expressions_keep_their_shape() {
    let mut database = people();
    assert_eq!(
        query(
            &mut database,
            r#"SELECT ?s WHERE {
                ?s <urn:name> ?n .
                ?s <urn:age> ?age .
                FILTER((?age > 26 && ?n != "Zed") || ?n = "Bob")
            }"#,
        ),
        rows1(&["urn:alice", "urn:bob", "urn:carol"])
    );
}

// ---------------------------------------------------------------------------

#[test]
fn subquery_filter_negating_an_unbound_comparison_admits_no_row() {
    let mut database = people();
    assert_eq!(
        query(
            &mut database,
            r#"SELECT ?s WHERE {
                { SELECT ?s WHERE { ?s <urn:name> ?n . FILTER(!(?missing > 100)) } }
            }"#,
        ),
        Vec::<Vec<String>>::new()
    );
}

#[test]
fn subquery_filter_orders_non_numeric_operands_lexically() {
    let mut database = people();
    assert_eq!(
        query(
            &mut database,
            r#"SELECT ?s WHERE {
                { SELECT ?s WHERE { ?s <urn:name> ?n . FILTER(?n > "Bob") } }
            }"#,
        ),
        rows1(&["urn:carol"])
    );
}
