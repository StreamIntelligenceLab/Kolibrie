/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::streamertail_optimizer::PhysicalOperator;
use shared::dataset_index::{GraphTerm, QuadPattern};
use shared::terms::{Bindings, Term, TriplePattern};

mod common;

fn var(name: &str) -> Term {
    Term::Variable(name.to_string())
}

fn constant(database: &SparqlDatabase, value: &str) -> Term {
    Term::Constant(database.dictionary.write().unwrap().encode(value))
}

fn quad(pattern: TriplePattern, graph: GraphTerm) -> QuadPattern {
    QuadPattern {
        subject: pattern.0,
        predicate: pattern.1,
        object: pattern.2,
        graph,
    }
}

/// Normalizes a solution sequence into a comparable bag of sorted rows
fn bag(bindings: Bindings) -> Vec<Vec<(String, u32)>> {
    let mut rows: Vec<Vec<(String, u32)>> = bindings
        .into_iter()
        .map(|row| {
            let mut pairs: Vec<(String, u32)> = row.into_iter().collect();
            pairs.sort();
            pairs
        })
        .collect();
    rows.sort();
    rows
}

/// Asserts that all three general join operators agree on a pair of scans
fn assert_general_joins_agree(
    database: &mut SparqlDatabase,
    left: QuadPattern,
    right: QuadPattern,
    case: &str,
) -> Vec<Vec<(String, u32)>> {
    let scan_left = PhysicalOperator::quad_index_scan(left);
    let scan_right = PhysicalOperator::quad_index_scan(right);

    let nested = PhysicalOperator::nested_loop_join(scan_left.clone(), scan_right.clone());
    let hash = PhysicalOperator::hash_join(scan_left.clone(), scan_right.clone());
    let bind = PhysicalOperator::bind_join(scan_left, scan_right);

    let expected = bag(nested.execute_with_ids(database));
    let hash_rows = bag(hash.execute_with_ids(database));
    let bind_rows = bag(bind.execute_with_ids(database));

    assert_eq!(
        hash_rows, expected,
        "{case}: HashJoin disagreed with NestedLoopJoin"
    );
    assert_eq!(
        bind_rows, expected,
        "{case}: BindJoin disagreed with NestedLoopJoin"
    );

    expected
}

fn fixture() -> SparqlDatabase {
    common::database_with(
        r#"
        <http://ex/alice> <http://ex/knows> <http://ex/bob> .
        <http://ex/alice> <http://ex/knows> <http://ex/carol> .
        <http://ex/bob>   <http://ex/knows> <http://ex/carol> .
        <http://ex/carol> <http://ex/knows> <http://ex/alice> .
        <http://ex/alice> <http://ex/age>   "30" .
        <http://ex/bob>   <http://ex/age>   "40" .
        <http://ex/carol> <http://ex/age>   "50" .
        <http://ex/alice> <http://ex/city>  <http://ex/leuven> .
        <http://ex/bob>   <http://ex/city>  <http://ex/ghent> .
    "#,
    )
}

#[test]
fn join_variants_agree_on_a_shared_variable() {
    let mut database = fixture();
    let knows = constant(&database, "http://ex/knows");
    let age = constant(&database, "http://ex/age");

    let rows = assert_general_joins_agree(
        &mut database,
        quad((var("s"), knows, var("o")), GraphTerm::Default),
        quad((var("s"), age, var("a")), GraphTerm::Default),
        "shared subject variable",
    );

    // Every ?s that knows someone also has an age, so no solution is dropped
    assert_eq!(rows.len(), 4, "expected one row per knows triple");
}

#[test]
fn join_variants_agree_on_a_cartesian_product() {
    let mut database = fixture();
    let city = constant(&database, "http://ex/city");
    let age = constant(&database, "http://ex/age");

    // Disjoint variable sets on both sides: the join degenerates to a product
    let rows = assert_general_joins_agree(
        &mut database,
        quad((var("p"), city, var("c")), GraphTerm::Default),
        quad((var("q"), age, var("a")), GraphTerm::Default),
        "cartesian product",
    );

    assert_eq!(rows.len(), 2 * 3, "expected the full product");
}

#[test]
fn join_variants_agree_when_the_shared_variable_conflicts() {
    let mut database = fixture();
    let knows = constant(&database, "http://ex/knows");
    let leuven = constant(&database, "http://ex/leuven");
    let city = constant(&database, "http://ex/city");

    // ?s is a city on the right and a person on the left, so the sides never agree
    let rows = assert_general_joins_agree(
        &mut database,
        quad((var("s"), knows, var("o")), GraphTerm::Default),
        quad((leuven, city, var("s")), GraphTerm::Default),
        "conflicting shared variable",
    );

    assert!(rows.is_empty(), "conflicting bindings must join to nothing");
}

#[test]
fn join_variants_agree_on_a_repeated_variable_within_a_pattern() {
    let mut database = fixture();
    common::load(
        &mut database,
        "<http://ex/dave> <http://ex/knows> <http://ex/dave> .",
    );
    let knows = constant(&database, "http://ex/knows");
    let age = constant(&database, "http://ex/age");

    // ?self appears twice in one pattern, so the scan must self-constrain
    let rows = assert_general_joins_agree(
        &mut database,
        quad((var("self"), knows.clone(), var("self")), GraphTerm::Default),
        quad((var("other"), age, var("a")), GraphTerm::Default),
        "repeated variable in a pattern",
    );

    // Only dave knows himself, crossed with the three ages
    assert_eq!(rows.len(), 3, "expected the self-loop crossed with ages");
}

#[test]
fn join_variants_agree_when_one_side_is_empty() {
    let mut database = fixture();
    let knows = constant(&database, "http://ex/knows");
    let missing = constant(&database, "http://ex/no-such-predicate");

    let rows = assert_general_joins_agree(
        &mut database,
        quad((var("s"), knows, var("o")), GraphTerm::Default),
        quad((var("s"), missing, var("z")), GraphTerm::Default),
        "empty right side",
    );

    assert!(rows.is_empty(), "an empty side must produce no solutions");
}

#[test]
fn join_variants_agree_across_named_graphs() {
    let mut database = SparqlDatabase::new();
    kolibrie::execute_query::execute_sparql_update(
        r#"INSERT DATA {
            GRAPH <http://ex/g1> {
                <http://ex/alice> <http://ex/knows> <http://ex/bob> .
                <http://ex/alice> <http://ex/age> "30" .
            }
            GRAPH <http://ex/g2> {
                <http://ex/bob> <http://ex/knows> <http://ex/carol> .
                <http://ex/bob> <http://ex/age> "40" .
            }
        }"#,
        &mut database,
    )
    .expect("named graph fixture must load");

    let knows = constant(&database, "http://ex/knows");
    let age = constant(&database, "http://ex/age");
    let g1 = match constant(&database, "http://ex/g1") {
        Term::Constant(id) => id,
        _ => unreachable!(),
    };

    // Both sides pinned to the same named graph
    let rows = assert_general_joins_agree(
        &mut database,
        quad((var("s"), knows.clone(), var("o")), GraphTerm::Named(g1)),
        quad((var("s"), age.clone(), var("a")), GraphTerm::Named(g1)),
        "both sides in one named graph",
    );
    assert_eq!(rows.len(), 1, "only alice is in g1");

    // A graph variable on both sides must agree, restricting to one graph
    let rows = assert_general_joins_agree(
        &mut database,
        quad(
            (var("s"), knows, var("o")),
            GraphTerm::Variable("g".to_string()),
        ),
        quad(
            (var("s"), age, var("a")),
            GraphTerm::Variable("g".to_string()),
        ),
        "shared graph variable",
    );
    assert_eq!(rows.len(), 2, "one solution per named graph");
}

#[test]
fn star_join_agrees_with_a_bind_join_chain() {
    let mut database = fixture();
    let knows = constant(&database, "http://ex/knows");
    let age = constant(&database, "http://ex/age");
    let city = constant(&database, "http://ex/city");

    let patterns: Vec<TriplePattern> = vec![
        (var("s"), knows.clone(), var("o")),
        (var("s"), age.clone(), var("a")),
        (var("s"), city.clone(), var("c")),
    ];

    let star = PhysicalOperator::StarJoin {
        join_var: "s".to_string(),
        patterns: patterns.clone(),
    };

    // StarJoin hard-codes the default graph, so the equivalent chain does too
    let chain = PhysicalOperator::bind_join(
        PhysicalOperator::bind_join(
            PhysicalOperator::quad_index_scan(quad(patterns[0].clone(), GraphTerm::Default)),
            PhysicalOperator::quad_index_scan(quad(patterns[1].clone(), GraphTerm::Default)),
        ),
        PhysicalOperator::quad_index_scan(quad(patterns[2].clone(), GraphTerm::Default)),
    );

    let star_rows = bag(star.execute_with_ids(&mut database));
    let chain_rows = bag(chain.execute_with_ids(&mut database));

    assert_eq!(
        star_rows, chain_rows,
        "StarJoin disagreed with the equivalent BindJoin chain"
    );
    // alice and bob each have an age and a city; alice knows two people
    assert_eq!(star_rows.len(), 3, "expected the star to bind three rows");
}

#[test]
fn join_multiplicity_is_preserved_as_a_bag() {
    let mut database = SparqlDatabase::new();
    // Two triples give ?s two ways to reach each ?tag, so duplicates must survive
    common::load(
        &mut database,
        r#"
        <http://ex/a> <http://ex/p> <http://ex/x> .
        <http://ex/a> <http://ex/p> <http://ex/y> .
        <http://ex/a> <http://ex/tag> <http://ex/t1> .
        <http://ex/a> <http://ex/tag> <http://ex/t2> .
        "#,
    );
    let p = constant(&database, "http://ex/p");
    let tag = constant(&database, "http://ex/tag");

    let rows = assert_general_joins_agree(
        &mut database,
        quad((var("s"), p, var("o")), GraphTerm::Default),
        quad((var("s"), tag, var("t")), GraphTerm::Default),
        "multiplicity",
    );

    assert_eq!(rows.len(), 4, "2x2 solutions must not be deduplicated");
}
