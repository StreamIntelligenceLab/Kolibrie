/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::sparql_database::SparqlDatabase;
use shared::dataset_index::GraphId;
use std::collections::BTreeSet;

fn create_empty_named_graph(database: &mut SparqlDatabase, name: &str) {
    let graph_id = database.dictionary.write().unwrap().encode(name);
    assert!(database
        .dataset_index
        .create_graph(GraphId::Named(graph_id)));
}

fn decoded_graph_names(database: &SparqlDatabase) -> BTreeSet<String> {
    database
        .dataset_index
        .named_graphs()
        .into_iter()
        .filter_map(|graph| match graph {
            GraphId::Default => None,
            GraphId::Named(graph_id) => database.decode_any(graph_id),
        })
        .collect()
}

#[test]
fn database_union_reencodes_and_preserves_the_complete_dataset() {
    let mut left = SparqlDatabase::new();
    left.add_triple_parts("urn:left-default", "urn:p", "urn:o");
    left.add_quad_parts(
        "<< <urn:left-s> <urn:left-p> <urn:left-o> >>",
        "urn:asserted-by",
        "urn:left-source",
        "urn:left-graph",
    );
    create_empty_named_graph(&mut left, "urn:left-empty");

    let mut right = SparqlDatabase::new();
    // Both independently-created databases start their dictionaries and
    // quoted-triple stores at the same IDs. Distinct lexical values therefore
    // exercise ID collision handling during the union.
    right.add_triple_parts("urn:right-default", "urn:p", "urn:o");
    right.add_quad_parts(
        "<< <urn:right-s> <urn:right-p> <urn:right-o> >>",
        "urn:asserted-by",
        "urn:right-source",
        "urn:right-graph",
    );
    create_empty_named_graph(&mut right, "urn:right-empty");

    let mut merged = left.union(&right);
    merged.build_all_indexes();

    let default_subjects: BTreeSet<_> = merged
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| merged.decode_any(triple.subject).unwrap())
        .collect();
    assert_eq!(
        default_subjects,
        BTreeSet::from([
            "urn:left-default".to_string(),
            "urn:right-default".to_string(),
        ])
    );

    assert_eq!(
        decoded_graph_names(&merged),
        BTreeSet::from([
            "urn:left-empty".to_string(),
            "urn:left-graph".to_string(),
            "urn:right-empty".to_string(),
            "urn:right-graph".to_string(),
        ])
    );

    let quoted_subjects: BTreeSet<_> = merged
        .dataset_index
        .named_graphs()
        .into_iter()
        .flat_map(|graph| merged.query_graph_quads(graph, None, None, None))
        .map(|quad| merged.decode_any(quad.subject).unwrap())
        .collect();
    assert_eq!(
        quoted_subjects,
        BTreeSet::from([
            "<< urn:left-s urn:left-p urn:left-o >>".to_string(),
            "<< urn:right-s urn:right-p urn:right-o >>".to_string(),
        ])
    );
}
