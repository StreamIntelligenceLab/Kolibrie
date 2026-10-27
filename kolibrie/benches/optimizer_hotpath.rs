/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

extern crate criterion;
extern crate kolibrie;

use criterion::*;
use kolibrie::execute_query::{execute, execute_sparql_update};
use kolibrie::sparql_database::SparqlDatabase;

const SUBJECTS: usize = 20_000;
const NAMED_GRAPHS: usize = 4;
const DEPARTMENTS: usize = 50;
const REGIONS: usize = 10;

const EX: &str = "http://example.org/";

/// Builds the fixture: salary and department in the default graph, region
fn setup_database() -> SparqlDatabase {
    let mut database = SparqlDatabase::new();

    let mut default_graph = String::with_capacity(SUBJECTS * 160);
    for i in 0..SUBJECTS {
        let salary = 40_000 + (i % 60_000);
        default_graph.push_str(&format!(
            "<{EX}e{i}> <{EX}salary> \"{salary}\" .\n<{EX}e{i}> <{EX}dept> <{EX}d{}> .\n",
            i % DEPARTMENTS
        ));
    }
    execute_sparql_update(
        &format!("INSERT DATA {{\n{default_graph}\n}}"),
        &mut database,
    )
    .expect("default graph fixture must load");

    for graph in 0..NAMED_GRAPHS {
        let mut named = String::with_capacity((SUBJECTS / NAMED_GRAPHS) * 90);
        for i in (graph..SUBJECTS).step_by(NAMED_GRAPHS) {
            named.push_str(&format!(
                "<{EX}e{i}> <{EX}region> <{EX}r{}> .\n",
                i % REGIONS
            ));
        }
        execute_sparql_update(
            &format!("INSERT DATA {{ GRAPH <{EX}g{graph}> {{\n{named}\n}} }}"),
            &mut database,
        )
        .expect("named graph fixture must load");
    }

    database
}

/// Scans every named graph while binding the graph variable
const GRAPH_VARIABLE_SCAN: &str = r#"
SELECT ?s ?r
WHERE { GRAPH ?g { ?s <http://example.org/region> ?r } }"#;

/// Scans a single named graph with no graph variable to bind. The seed row
const GRAPH_FIXED_SCAN: &str = r#"
SELECT ?s ?r
WHERE { GRAPH <http://example.org/g0> { ?s <http://example.org/region> ?r } }"#;

/// Three UNION branches, each of which receives the incoming solution set
const UNION_BRANCHES: &str = r#"
SELECT ?s
WHERE {
    { ?s <http://example.org/salary> ?v }
    UNION
    { ?s <http://example.org/dept> ?d }
    UNION
    { GRAPH ?g { ?s <http://example.org/region> ?r } }
}"#;

/// A selective numeric FILTER over every solution
const FILTER_NUMERIC: &str = r#"
SELECT ?s ?v
WHERE {
    ?s <http://example.org/salary> ?v .
    FILTER(?v > 70000)
}"#;

/// A two-pattern join, driving one index probe per left-hand solution
const JOIN_TWO_PATTERNS: &str = r#"
SELECT ?s ?v ?d
WHERE {
    ?s <http://example.org/salary> ?v .
    ?s <http://example.org/dept> ?d
}"#;

/// A projection whose values repeat: 20,000 solutions over 50 distinct departments.
///
/// The counterpart to `join_two_patterns`, whose columns are a distinct IRI and a
/// near-distinct literal per row. Grouping columns, rdf:type and graph names all have
/// this shape, and it is the case where storing each value once can pay.
const LOW_CARDINALITY_PROJECTION: &str = r#"
SELECT ?d
WHERE {
    ?s <http://example.org/dept> ?d
}"#;

fn hot_paths(c: &mut Criterion) {
    let mut database = setup_database();

    let mut group = c.benchmark_group("execution_hot_paths");
    group.sample_size(20);

    for (name, sparql) in [
        ("graph_variable_scan", GRAPH_VARIABLE_SCAN),
        ("graph_fixed_scan", GRAPH_FIXED_SCAN),
        ("union_branches", UNION_BRANCHES),
        ("filter_numeric", FILTER_NUMERIC),
        ("join_two_patterns", JOIN_TWO_PATTERNS),
        ("low_cardinality_projection", LOW_CARDINALITY_PROJECTION),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                let results = execute(sparql, &mut database)
                    .expect("query should run")
                    .into_rows();
                black_box(results.len())
            })
        });
    }

    // The same queries read through the borrowing API instead of `into_rows`.
    //
    // `into_rows` materialises an owned String per cell whatever the result holds
    // internally, so it cannot show a difference between owning the values and
    // referring to them. `iter` and `get` return `&str`, so these arms are the ones
    // a representation change can move.
    for (name, sparql) in [
        ("borrowed_read/join_two_patterns", JOIN_TWO_PATTERNS),
        (
            "borrowed_read/low_cardinality_projection",
            LOW_CARDINALITY_PROJECTION,
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                let solutions = execute(sparql, &mut database).expect("query should run");
                let mut bytes = 0usize;
                for solution in solutions.iter() {
                    for variable in solutions.variables() {
                        if let Some(value) = solution.get(variable) {
                            bytes += value.len();
                        }
                    }
                }
                black_box(bytes)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, hot_paths);
criterion_main!(benches);
