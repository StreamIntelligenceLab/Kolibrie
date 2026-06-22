/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::execute_query::execute_query_rayon_parallel2_volcano;
use kolibrie::sparql_database::SparqlDatabase;

#[test]
fn plain_select_reads_default_graph_only() {
    let mut db = SparqlDatabase::new();
    db.add_triple_parts(
        "http://example.org/default-s",
        "http://example.org/p",
        "default-o",
    );

    execute_query_rayon_parallel2_volcano(
        r#"
        INSERT {
            GRAPH <http://example.org/g1> {
                <http://example.org/named-s> <http://example.org/p> "named-o" .
            }
        }
        WHERE { }
        "#,
        &mut db,
    );

    let rows = execute_query_rayon_parallel2_volcano(
        r#"
        SELECT ?s ?o
        WHERE {
            ?s <http://example.org/p> ?o .
        }
        "#,
        &mut db,
    );

    assert_eq!(
        rows,
        vec![vec![
            "http://example.org/default-s".to_string(),
            "default-o".to_string()
        ]]
    );
}

#[test]
fn graph_iri_select_reads_only_that_named_graph() {
    let mut db = SparqlDatabase::new();

    execute_query_rayon_parallel2_volcano(
        r#"
        INSERT {
            GRAPH <http://example.org/g1> {
                <http://example.org/s1> <http://example.org/p> "one" .
            }
            GRAPH <http://example.org/g2> {
                <http://example.org/s2> <http://example.org/p> "two" .
            }
        }
        WHERE { }
        "#,
        &mut db,
    );

    let rows = execute_query_rayon_parallel2_volcano(
        r#"
        SELECT ?s ?o
        WHERE {
            GRAPH <http://example.org/g1> {
                ?s <http://example.org/p> ?o .
            }
        }
        "#,
        &mut db,
    );

    assert_eq!(
        rows,
        vec![vec!["http://example.org/s1".to_string(), "one".to_string()]]
    );
}

#[test]
fn graph_variable_binds_named_graphs_only() {
    let mut db = SparqlDatabase::new();
    db.add_triple_parts(
        "http://example.org/default-s",
        "http://example.org/p",
        "default-o",
    );

    execute_query_rayon_parallel2_volcano(
        r#"
        INSERT {
            GRAPH <http://example.org/g1> {
                <http://example.org/s> <http://example.org/p> "one" .
            }
            GRAPH <http://example.org/g2> {
                <http://example.org/s> <http://example.org/p> "two" .
            }
        }
        WHERE { }
        "#,
        &mut db,
    );

    let mut rows = execute_query_rayon_parallel2_volcano(
        r#"
        SELECT ?g ?o
        WHERE {
            GRAPH ?g {
                <http://example.org/s> <http://example.org/p> ?o .
            }
        }
        "#,
        &mut db,
    );
    rows.sort();

    assert_eq!(
        rows,
        vec![
            vec!["http://example.org/g1".to_string(), "one".to_string()],
            vec!["http://example.org/g2".to_string(), "two".to_string()],
        ]
    );
}

#[test]
fn from_named_restricts_graph_variable_visibility() {
    let mut db = SparqlDatabase::new();

    execute_query_rayon_parallel2_volcano(
        r#"
        INSERT {
            GRAPH <http://example.org/g1> {
                <http://example.org/s> <http://example.org/p> "one" .
            }
            GRAPH <http://example.org/g2> {
                <http://example.org/s> <http://example.org/p> "two" .
            }
        }
        WHERE { }
        "#,
        &mut db,
    );

    let rows = execute_query_rayon_parallel2_volcano(
        r#"
        SELECT ?g ?o
        FROM NAMED <http://example.org/g2>
        WHERE {
            GRAPH ?g {
                <http://example.org/s> <http://example.org/p> ?o .
            }
        }
        "#,
        &mut db,
    );

    assert_eq!(
        rows,
        vec![vec!["http://example.org/g2".to_string(), "two".to_string()]]
    );
}

#[test]
fn nquads_roundtrip_preserves_named_graph() {
    let mut db = SparqlDatabase::new();
    db.parse_nquads_and_add(
        r#"<http://example.org/s> <http://example.org/p> "value" <http://example.org/g> ."#,
    );

    let rows = execute_query_rayon_parallel2_volcano(
        r#"
        SELECT ?g ?o
        WHERE {
            GRAPH ?g {
                <http://example.org/s> <http://example.org/p> ?o .
            }
        }
        "#,
        &mut db,
    );

    assert_eq!(
        rows,
        vec![vec![
            "http://example.org/g".to_string(),
            "value".to_string()
        ]]
    );
    assert!(db.generate_nquads().contains("<http://example.org/g>"));
}
