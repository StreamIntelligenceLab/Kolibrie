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

use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::streamertail_optimizer::{DatabaseStats, GraphMap, TermMap, TermSet};
use shared::dataset_index::GraphId;

/// Reference implementation over a materialized snapshot; exact only below the sampling threshold
fn reference_stats(database: &SparqlDatabase) -> DatabaseStats {
    let quads = database.dataset_index.all_quads();

    let mut predicate_cardinalities = TermMap::<u64>::default();
    let mut subject_cardinalities = TermMap::<u64>::default();
    let mut object_cardinalities = TermMap::<u64>::default();
    let mut predicate_subjects = TermMap::<TermSet>::default();
    let mut predicate_objects = TermMap::<TermSet>::default();
    let mut all_subjects = TermSet::default();
    let mut all_objects = TermSet::default();

    for quad in &quads {
        *predicate_cardinalities.entry(quad.predicate).or_insert(0) += 1;
        *subject_cardinalities.entry(quad.subject).or_insert(0) += 1;
        *object_cardinalities.entry(quad.object).or_insert(0) += 1;
        predicate_subjects
            .entry(quad.predicate)
            .or_default()
            .insert(quad.subject);
        predicate_objects
            .entry(quad.predicate)
            .or_default()
            .insert(quad.object);
        all_subjects.insert(quad.subject);
        all_objects.insert(quad.object);
    }

    let total_triples = quads.len() as u64;
    let cap = |observed: usize, cardinality: u64| (observed as u64).min(cardinality).max(1);

    let mut graph_cardinalities = GraphMap::default();
    for graph in database.dataset_index.graphs() {
        let size = quads.iter().filter(|quad| quad.graph == graph).count() as u64;
        graph_cardinalities.insert(graph, size);
    }

    DatabaseStats {
        total_triples,
        quoted_triple_count: database.quoted_triple_store.read().unwrap().len() as u64,
        named_graph_count: database.dataset_index.named_graphs().len() as u64,
        graph_cardinalities,
        predicate_distinct_subjects: predicate_subjects
            .iter()
            .map(|(predicate, subjects)| {
                let cardinality = predicate_cardinalities[predicate];
                (*predicate, cap(subjects.len(), cardinality))
            })
            .collect(),
        predicate_distinct_objects: predicate_objects
            .iter()
            .map(|(predicate, objects)| {
                let cardinality = predicate_cardinalities[predicate];
                (*predicate, cap(objects.len(), cardinality))
            })
            .collect(),
        distinct_subjects: cap(all_subjects.len(), total_triples),
        distinct_objects: cap(all_objects.len(), total_triples),
        predicate_cardinalities,
        subject_cardinalities,
        object_cardinalities,
    }
}

fn assert_matches_reference(database: &SparqlDatabase, case: &str) {
    let actual = DatabaseStats::gather_stats_fast(database);
    let expected = reference_stats(database);

    assert_eq!(actual.total_triples, expected.total_triples, "{case}: total");
    assert_eq!(
        actual.named_graph_count, expected.named_graph_count,
        "{case}: named graph count"
    );
    assert_eq!(
        actual.quoted_triple_count, expected.quoted_triple_count,
        "{case}: quoted triples"
    );
    assert_eq!(
        actual.graph_cardinalities, expected.graph_cardinalities,
        "{case}: graph cardinalities"
    );
    assert_eq!(
        actual.predicate_cardinalities, expected.predicate_cardinalities,
        "{case}: predicate cardinalities"
    );
    assert_eq!(
        actual.subject_cardinalities, expected.subject_cardinalities,
        "{case}: subject cardinalities"
    );
    assert_eq!(
        actual.object_cardinalities, expected.object_cardinalities,
        "{case}: object cardinalities"
    );
    assert_eq!(
        actual.predicate_distinct_subjects, expected.predicate_distinct_subjects,
        "{case}: distinct subjects per predicate"
    );
    assert_eq!(
        actual.predicate_distinct_objects, expected.predicate_distinct_objects,
        "{case}: distinct objects per predicate"
    );
    assert_eq!(
        actual.distinct_subjects, expected.distinct_subjects,
        "{case}: distinct subjects"
    );
    assert_eq!(
        actual.distinct_objects, expected.distinct_objects,
        "{case}: distinct objects"
    );
}

#[test]
fn an_empty_dataset_collects_the_same_statistics() {
    assert_matches_reference(&SparqlDatabase::new(), "empty");
}

#[test]
fn one_quad_and_its_duplicate_collect_the_same_statistics() {
    let mut database = common::database_with(r#"<urn:a> <urn:p> "1" ."#);
    assert_matches_reference(&database, "one quad");

    common::load(&mut database, r#"<urn:a> <urn:p> "1" ."#);
    assert_eq!(database.dataset_index.count_quads(), 1, "duplicate stored");
    assert_matches_reference(&database, "duplicate quad");
}

#[test]
fn the_default_graph_alone_collects_the_same_statistics() {
    let database = common::database_with(
        r#"<urn:a> <urn:p> "1" .
<urn:b> <urn:p> "2" .
<urn:c> <urn:q> "3" ."#,
    );
    assert_matches_reference(&database, "default graph only");
}

#[test]
fn named_graphs_including_empty_ones_collect_the_same_statistics() {
    let mut database = common::database_with(r#"<urn:a> <urn:p> "1" ."#);
    kolibrie::execute_query::execute_sparql_update(
        r#"INSERT DATA { GRAPH <urn:g1> { <urn:b> <urn:p> "2" . } }"#,
        &mut database,
    )
    .unwrap();
    kolibrie::execute_query::execute_sparql_update(
        r#"INSERT DATA { GRAPH <urn:g2> { <urn:c> <urn:q> "3" . } }"#,
        &mut database,
    )
    .unwrap();

    // Empty named graph
    let empty = database.dictionary.write().unwrap().encode("urn:g3");
    database.dataset_index.create_graph(GraphId::Named(empty));

    assert_matches_reference(&database, "named graphs");
    let stats = DatabaseStats::gather_stats_fast(&database);
    assert_eq!(
        stats.get_graph_cardinality(GraphId::Named(empty)),
        0,
        "an empty named graph is measured as empty, not as missing"
    );
}

#[test]
fn graphs_sharing_terms_collect_the_same_statistics() {
    let mut database = common::database_with(r#"<urn:a> <urn:p> "1" ."#);
    for graph in ["urn:g1", "urn:g2", "urn:g3"] {
        kolibrie::execute_query::execute_sparql_update(
            &format!(r#"INSERT DATA {{ GRAPH <{graph}> {{ <urn:a> <urn:p> "1" . }} }}"#),
            &mut database,
        )
        .unwrap();
    }
    assert_matches_reference(&database, "shared terms across graphs");
}

#[test]
fn a_skewed_predicate_distribution_collects_the_same_statistics() {
    let mut triples = String::new();
    for index in 0..900 {
        triples.push_str(&format!("<urn:s{index}> <urn:common> \"{index}\" .\n"));
    }
    triples.push_str("<urn:s0> <urn:rare> \"only\" .\n");
    let database = common::database_with(&triples);

    assert_matches_reference(&database, "skewed");
    let stats = DatabaseStats::gather_stats_fast(&database);
    let rare = database.dictionary.write().unwrap().encode("urn:rare");
    assert_eq!(
        stats.get_predicate_cardinality(rare),
        1,
        "a rare predicate is counted exactly below the threshold"
    );
}

#[test]
fn many_small_graphs_collect_the_same_statistics() {
    let mut database = SparqlDatabase::new();
    for graph in 0..60 {
        kolibrie::execute_query::execute_sparql_update(
            &format!(
                r#"INSERT DATA {{ GRAPH <urn:g{graph}> {{ <urn:s{graph}> <urn:p> "{graph}" . }} }}"#
            ),
            &mut database,
        )
        .unwrap();
    }
    assert_matches_reference(&database, "many small graphs");
}

/// Results are repeatable and independent of insertion order
#[test]
fn collection_is_deterministic_and_insertion_order_independent() {
    let forward: Vec<String> = (0..400)
        .map(|index| format!("<urn:s{index}> <urn:p{}> \"{index}\" .", index % 9))
        .collect();
    let mut reversed = forward.clone();
    reversed.reverse();

    let first = common::database_with(&forward.join("\n"));
    let second = common::database_with(&reversed.join("\n"));

    let a = DatabaseStats::gather_stats_fast(&first);
    let b = DatabaseStats::gather_stats_fast(&first);
    let c = DatabaseStats::gather_stats_fast(&second);

    assert_eq!(a.total_triples, b.total_triples);
    assert_eq!(a.predicate_cardinalities, b.predicate_cardinalities);
    assert_eq!(a.distinct_subjects, b.distinct_subjects);

    assert_eq!(a.total_triples, c.total_triples);
    assert_eq!(a.predicate_cardinalities, c.predicate_cardinalities);
    assert_eq!(a.subject_cardinalities, c.subject_cardinalities);
    assert_eq!(a.object_cardinalities, c.object_cardinalities);
    assert_eq!(a.distinct_subjects, c.distinct_subjects);
    assert_eq!(a.distinct_objects, c.distinct_objects);
}
