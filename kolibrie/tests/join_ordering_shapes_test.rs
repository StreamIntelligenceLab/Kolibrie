/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Join planning must not change which solutions a basic graph pattern
//! produces. Every test here fixes a dataset, then asserts that writing the
//! same patterns in different source orders yields the same solution bag.

use kolibrie::execute_query::{execute_query_rayon_parallel2_volcano, execute_sparql_update};
use kolibrie::sparql_database::SparqlDatabase;

fn bag(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn query(database: &mut SparqlDatabase, sparql: &str) -> Vec<Vec<String>> {
    bag(execute_query_rayon_parallel2_volcano(sparql, database))
}

/// A path `n0 -> n1 -> ... -> n8` where only `n0` carries the anchor type,
/// plus decoy edges that make an unanchored scan far less selective.
fn chain_database() -> SparqlDatabase {
    let mut database = SparqlDatabase::new();
    let mut triples = String::new();
    for i in 0..8 {
        triples.push_str(&format!("<urn:n{}> <urn:next> <urn:n{}> .\n", i, i + 1));
    }
    for i in 0..8 {
        triples.push_str(&format!("<urn:d{}> <urn:next> <urn:d{}> .\n", i, i + 1));
        triples.push_str(&format!("<urn:e{}> <urn:next> <urn:e{}> .\n", i, i + 1));
    }
    triples.push_str("<urn:n0> <urn:type> <urn:Anchor> .\n");

    execute_sparql_update(
        &format!("INSERT DATA {{\n{}}}", triples),
        &mut database,
    )
    .unwrap();
    database
}

fn chain_patterns(links: usize) -> Vec<String> {
    (0..links)
        .map(|i| format!("?x{} <urn:next> ?x{} .", i, i + 1))
        .collect()
}

/// Builds a SELECT over the chain with the anchor pattern spliced in at
/// `anchor_at`, so the same query reaches the planner in different orders.
fn chain_query(links: usize, anchor_at: usize) -> String {
    let mut patterns = chain_patterns(links);
    patterns.insert(anchor_at, "?x0 <urn:type> <urn:Anchor> .".to_string());
    format!(
        "SELECT ?x0 ?x{} WHERE {{ {} }}",
        links,
        patterns.join(" ")
    )
}

#[test]
fn chain_results_are_independent_of_anchor_position() {
    for links in [2usize, 4, 8] {
        let mut database = chain_database();
        let expected = query(&mut database, &chain_query(links, 0));

        assert_eq!(
            expected,
            vec![vec![format!("urn:n0"), format!("urn:n{}", links)]],
            "chain of {} links should walk the anchored path only",
            links
        );

        for anchor_at in 1..=links {
            assert_eq!(
                query(&mut database, &chain_query(links, anchor_at)),
                expected,
                "chain of {} links changed results with the anchor at {}",
                links,
                anchor_at
            );
        }
    }
}

#[test]
fn star_results_are_independent_of_pattern_order() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:s1> <urn:a> "1" .
            <urn:s1> <urn:b> "2" .
            <urn:s1> <urn:c> "3" .
            <urn:s1> <urn:d> "4" .
            <urn:s2> <urn:a> "1" .
            <urn:s2> <urn:b> "2" .
            <urn:s2> <urn:c> "3" .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let forward = query(
        &mut database,
        r#"SELECT ?s WHERE {
            ?s <urn:a> ?a . ?s <urn:b> ?b . ?s <urn:c> ?c . ?s <urn:d> ?d .
        }"#,
    );
    let reversed = query(
        &mut database,
        r#"SELECT ?s WHERE {
            ?s <urn:d> ?d . ?s <urn:c> ?c . ?s <urn:b> ?b . ?s <urn:a> ?a .
        }"#,
    );

    assert_eq!(forward, vec![vec!["urn:s1".to_string()]]);
    assert_eq!(forward, reversed);
}

#[test]
fn cyclic_pattern_closes_the_loop() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:a> <urn:p> <urn:b> .
            <urn:b> <urn:p> <urn:c> .
            <urn:c> <urn:p> <urn:a> .
            <urn:x> <urn:p> <urn:y> .
            <urn:y> <urn:p> <urn:z> .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let forward = query(
        &mut database,
        r#"SELECT ?a ?b ?c WHERE {
            ?a <urn:p> ?b . ?b <urn:p> ?c . ?c <urn:p> ?a .
        }"#,
    );
    let rotated = query(
        &mut database,
        r#"SELECT ?a ?b ?c WHERE {
            ?c <urn:p> ?a . ?b <urn:p> ?c . ?a <urn:p> ?b .
        }"#,
    );

    assert_eq!(forward.len(), 3, "each rotation of the cycle is a solution");
    assert_eq!(forward, rotated);
}

#[test]
fn disconnected_patterns_produce_the_cross_product() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:s1> <urn:left> "l1" .
            <urn:s2> <urn:left> "l2" .
            <urn:t1> <urn:right> "r1" .
            <urn:t2> <urn:right> "r2" .
            <urn:t3> <urn:right> "r3" .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let forward = query(
        &mut database,
        r#"SELECT ?l ?r WHERE { ?s <urn:left> ?l . ?t <urn:right> ?r . }"#,
    );
    let swapped = query(
        &mut database,
        r#"SELECT ?l ?r WHERE { ?t <urn:right> ?r . ?s <urn:left> ?l . }"#,
    );

    assert_eq!(forward.len(), 6);
    assert_eq!(forward, swapped);
}

#[test]
fn repeated_variable_within_one_pattern_matches_only_self_loops() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:a> <urn:self> <urn:a> .
            <urn:b> <urn:self> <urn:c> .
            <urn:d> <urn:self> <urn:d> .
        }
        "#,
        &mut database,
    )
    .unwrap();

    assert_eq!(
        query(
            &mut database,
            r#"SELECT ?x WHERE { ?x <urn:self> ?x . }"#,
        ),
        vec![vec!["urn:a".to_string()], vec!["urn:d".to_string()]]
    );
}

#[test]
fn join_preserves_solution_multiplicity() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:s> <urn:p> <urn:m1> .
            <urn:s> <urn:p> <urn:m2> .
            <urn:s> <urn:q> "tag" .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let forward = query(
        &mut database,
        r#"SELECT ?tag WHERE { ?s <urn:p> ?m . ?s <urn:q> ?tag . }"#,
    );
    let swapped = query(
        &mut database,
        r#"SELECT ?tag WHERE { ?s <urn:q> ?tag . ?s <urn:p> ?m . }"#,
    );

    assert_eq!(
        forward,
        vec![vec!["tag".to_string()], vec!["tag".to_string()]],
        "both ?p matches must survive the join"
    );
    assert_eq!(forward, swapped);
}
