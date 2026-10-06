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

use kolibrie::execute_query::execute_sparql_update;
use kolibrie::sparql_database::SparqlDatabase;
use shared::dataset_index::GraphId;
use shared::triple::Triple;

fn seeded() -> SparqlDatabase {
    common::database_with(
        r#"<urn:a> <urn:p> "1" .
<urn:b> <urn:p> "2" .
<urn:c> <urn:p> "3" ."#,
    )
}

fn encode(database: &SparqlDatabase, term: &str) -> u32 {
    database.dictionary.write().unwrap().encode(term)
}

#[test]
fn statistics_are_cached_between_reads() {
    let mut database = seeded();
    let first = database.get_or_build_stats();
    let second = database.get_or_build_stats();
    assert!(
        std::sync::Arc::ptr_eq(&first, &second),
        "an unchanged database must not rebuild its statistics"
    );
}

#[test]
fn adding_a_triple_refreshes_the_statistics() {
    let mut database = seeded();
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    database.add_triple_parts("urn:d", "urn:p", "4");

    assert_eq!(database.get_or_build_stats().total_triples, 4);
}

#[test]
fn deleting_a_triple_refreshes_the_statistics() {
    let mut database = seeded();
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    assert!(database.delete_triple_parts("urn:a", "urn:p", "1"));

    assert_eq!(database.get_or_build_stats().total_triples, 2);
}

#[test]
fn adding_a_quad_refreshes_the_statistics() {
    let mut database = seeded();
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    assert!(database.add_quad_parts("urn:d", "urn:p", "4", "urn:g"));

    assert_eq!(database.get_or_build_stats().total_triples, 4);
}

#[test]
fn deleting_a_quad_refreshes_the_statistics() {
    let mut database = seeded();
    assert!(database.add_quad_parts("urn:d", "urn:p", "4", "urn:g"));
    assert_eq!(database.get_or_build_stats().total_triples, 4);

    let quad = shared::dataset_index::Quad {
        subject: encode(&database, "urn:d"),
        predicate: encode(&database, "urn:p"),
        object: encode(&database, "4"),
        graph: GraphId::Named(encode(&database, "urn:g")),
    };
    assert!(database.delete_quad(&quad));

    assert_eq!(database.get_or_build_stats().total_triples, 3);
}

#[test]
fn adding_a_raw_triple_refreshes_the_statistics() {
    let mut database = seeded();
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    let triple = Triple {
        subject: encode(&database, "urn:d"),
        predicate: encode(&database, "urn:p"),
        object: encode(&database, "4"),
    };
    database.add_triple(triple);

    assert_eq!(database.get_or_build_stats().total_triples, 4);
}

#[test]
fn loading_n_triples_refreshes_the_statistics() {
    let mut database = seeded();
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    database.parse_ntriples_and_add(
        "<urn:d> <urn:p> \"4\" .\n<urn:e> <urn:p> \"5\" .\n",
    );

    assert_eq!(database.get_or_build_stats().total_triples, 5);
}

#[test]
fn a_sparql_update_refreshes_the_statistics() {
    let mut database = seeded();
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    execute_sparql_update(
        r#"INSERT DATA { <urn:d> <urn:p> "4" . }"#,
        &mut database,
    )
    .unwrap();

    assert_eq!(database.get_or_build_stats().total_triples, 4);
}

#[test]
fn statistics_below_the_sampling_threshold_are_exact() {
    let mut triples = String::new();
    for index in 0..500 {
        triples.push_str(&format!("<urn:s{}> <urn:p> \"{}\" .\n", index, index));
    }
    let mut database = common::database_with(&triples);

    let stats = database.get_or_build_stats();
    assert_eq!(stats.total_triples, 500);
    // Every triple shares one predicate, so its cardinality is the whole set
    let predicate_total: u64 = stats.predicate_cardinalities.values().sum();
    assert_eq!(predicate_total, 500);
    assert_eq!(stats.distinct_subjects, 500);
}
