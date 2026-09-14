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

use common::{query, rows1, rows2};
use kolibrie::sparql_database::SparqlDatabase;

/// Two departments, four employees, with a repeated salary so that grouping
fn staff() -> SparqlDatabase {
    common::database_with(
        r#"<urn:e1> <urn:dept> <urn:sales> .
<urn:e1> <urn:salary> 100 .
<urn:e1> <urn:name> "Ann" .
<urn:e2> <urn:dept> <urn:sales> .
<urn:e2> <urn:salary> 200 .
<urn:e2> <urn:name> "Bea" .
<urn:e3> <urn:dept> <urn:eng> .
<urn:e3> <urn:salary> 500 .
<urn:e3> <urn:name> "Cy" .
<urn:e4> <urn:dept> <urn:sales> .
<urn:e4> <urn:salary> 100 .
<urn:e4> <urn:name> "Dee" ."#,
    )
}

const STAFF_PATTERN: &str = "?e <urn:dept> ?d . ?e <urn:salary> ?salary . ?e <urn:name> ?name";

// ---------------------------------------------------------------------------

#[test]
fn a_non_grouped_variable_is_not_carried_out_of_the_group() {
    let mut database = staff();
    // `?name` is neither a grouping key nor an aggregate, so it has no value
    assert_eq!(
        query(
            &mut database,
            &format!(
                "SELECT ?name (SUM(?salary) AS ?total) WHERE {{ {} }} GROUP BY ?d",
                STAFF_PATTERN
            ),
        ),
        rows2(&[("", "400"), ("", "500")])
    );
}

#[test]
fn the_grouping_key_is_carried_out_of_the_group() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!(
                "SELECT ?d (SUM(?salary) AS ?total) WHERE {{ {} }} GROUP BY ?d",
                STAFF_PATTERN
            ),
        ),
        rows2(&[("urn:eng", "500"), ("urn:sales", "400")])
    );
}

#[test]
fn duplicate_values_still_contribute_to_the_aggregate() {
    let mut database = staff();
    // Sales holds 100, 200 and 100; a set-based sum would report 300
    assert_eq!(
        query(
            &mut database,
            &format!(
                "SELECT (SUM(?salary) AS ?total) WHERE {{ {} }} GROUP BY ?d",
                STAFF_PATTERN
            ),
        ),
        rows1(&["400", "500"])
    );
}

#[test]
fn count_reports_the_group_size() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!(
                "SELECT ?d (COUNT(?e) AS ?headcount) WHERE {{ {} }} GROUP BY ?d",
                STAFF_PATTERN
            ),
        ),
        rows2(&[("urn:eng", "1"), ("urn:sales", "3")])
    );
}

#[test]
fn each_group_aggregates_only_its_own_rows() {
    let mut database = staff();
    // Sales holds 100, 200 and 100; engineering holds 500 alone
    assert_eq!(
        query(
            &mut database,
            &format!(
                "SELECT ?d (MAX(?salary) AS ?top) WHERE {{ {} }} GROUP BY ?d",
                STAFF_PATTERN
            ),
        ),
        rows2(&[("urn:eng", "500"), ("urn:sales", "200")])
    );
}

#[test]
fn averaging_uses_every_row_in_the_group() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!(
                "SELECT ?d (AVG(?salary) AS ?mean) WHERE {{ {} }} GROUP BY ?d",
                STAFF_PATTERN
            ),
        ),
        rows2(&[
            ("urn:eng", "500"),
            ("urn:sales", &(400.0f64 / 3.0).to_string()),
        ])
    );
}

// ---------------------------------------------------------------------------

#[test]
fn min_over_numbers_compares_numerically() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!("SELECT (MIN(?salary) AS ?m) WHERE {{ {} }}", STAFF_PATTERN),
        ),
        rows1(&["100"])
    );
}

#[test]
fn max_over_numbers_compares_numerically() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!("SELECT (MAX(?salary) AS ?m) WHERE {{ {} }}", STAFF_PATTERN),
        ),
        rows1(&["500"])
    );
}

#[test]
fn min_over_non_numeric_terms_returns_the_smallest_term() {
    let mut database = staff();
    // Discarding every non-numeric term left this aggregate with no value
    assert_eq!(
        query(
            &mut database,
            &format!("SELECT (MIN(?name) AS ?m) WHERE {{ {} }}", STAFF_PATTERN),
        ),
        rows1(&["Ann"])
    );
}

#[test]
fn max_over_non_numeric_terms_returns_the_largest_term() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!("SELECT (MAX(?name) AS ?m) WHERE {{ {} }}", STAFF_PATTERN),
        ),
        rows1(&["Dee"])
    );
}

// ---------------------------------------------------------------------------

#[test]
fn summing_an_empty_pattern_yields_one_row_holding_zero() {
    let mut database = staff();
    // An ungrouped aggregate over no solutions is still one solution
    assert_eq!(
        query(
            &mut database,
            "SELECT (SUM(?x) AS ?s) WHERE { ?e <urn:nothing> ?x }",
        ),
        rows1(&["0"])
    );
}

#[test]
fn a_minimum_over_an_empty_pattern_is_unbound() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            "SELECT (MIN(?x) AS ?m) WHERE { ?e <urn:nothing> ?x }",
        ),
        rows1(&[""])
    );
}

#[test]
fn grouping_an_empty_pattern_yields_no_rows() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            "SELECT ?g (COUNT(?e) AS ?c) WHERE { ?e <urn:nothing> ?g } GROUP BY ?g",
        ),
        Vec::<Vec<String>>::new()
    );
}

// ---------------------------------------------------------------------------

#[test]
fn a_subquery_group_carries_only_its_key_and_aggregates() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!(
                r#"SELECT ?d ?total WHERE {{
                    {{ SELECT ?d (SUM(?salary) AS ?total) WHERE {{ {} }} GROUP BY ?d }}
                }}"#,
                STAFF_PATTERN
            ),
        ),
        rows2(&[("urn:eng", "500"), ("urn:sales", "400")])
    );
}

#[test]
fn a_subquery_does_not_carry_a_non_grouped_variable() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!(
                r#"SELECT ?name ?total WHERE {{
                    {{ SELECT ?name (SUM(?salary) AS ?total) WHERE {{ {} }} GROUP BY ?d }}
                }}"#,
                STAFF_PATTERN
            ),
        ),
        rows2(&[("", "400"), ("", "500")])
    );
}

#[test]
fn a_subquery_min_over_non_numeric_terms_returns_the_smallest_term() {
    let mut database = staff();
    assert_eq!(
        query(
            &mut database,
            &format!(
                r#"SELECT ?m WHERE {{
                    {{ SELECT (MIN(?name) AS ?m) WHERE {{ {} }} }}
                }}"#,
                STAFF_PATTERN
            ),
        ),
        rows1(&["Ann"])
    );
}
