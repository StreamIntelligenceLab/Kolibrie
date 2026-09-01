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

use kolibrie::query_builder::QueryBuilder;
use kolibrie::sparql_database::SparqlDatabase;
use std::collections::HashMap;

/// Five triples whose rank under `<urn:rank>` reverses their natural triple order
fn ranked_database() -> SparqlDatabase {
    common::database_with(
        r#"<urn:a> <urn:rank> "5" .
<urn:b> <urn:rank> "4" .
<urn:c> <urn:rank> "3" .
<urn:d> <urn:rank> "2" .
<urn:e> <urn:rank> "1" ."#,
    )
}

/// Decodes every object once so the sort key needs no dictionary lock
fn object_labels(database: &SparqlDatabase) -> HashMap<u32, String> {
    let dictionary = database.dictionary.read().unwrap();
    database
        .query_default_triples(None, None, None)
        .iter()
        .map(|triple| {
            (
                triple.object,
                dictionary.decode(triple.object).unwrap_or("").to_string(),
            )
        })
        .collect()
}

/// Runs the builder with the given slice and returns the selected subjects
fn selected_subjects(
    database: &SparqlDatabase,
    descending: bool,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Vec<String> {
    let labels = object_labels(database);
    let mut builder =
        QueryBuilder::new(database).order_by(move |triple| labels[&triple.object].clone());
    if descending {
        builder = builder.desc();
    }
    if let Some(offset) = offset {
        builder = builder.offset(offset);
    }
    if let Some(limit) = limit {
        builder = builder.limit(limit);
    }

    let mut subjects: Vec<String> = builder
        .get_decoded_triples()
        .into_iter()
        .map(|(subject, _, _)| subject)
        .collect();
    subjects.sort();
    subjects
}

#[test]
fn ascending_limit_selects_the_lowest_ranked_triples() {
    let database = ranked_database();
    // Ranks 1 and 2 sit on <urn:e> and <urn:d>, the last two in natural triple order
    assert_eq!(
        selected_subjects(&database, false, None, Some(2)),
        vec!["urn:d".to_string(), "urn:e".to_string()]
    );
}

#[test]
fn descending_limit_selects_the_highest_ranked_triples() {
    let database = ranked_database();
    assert_eq!(
        selected_subjects(&database, true, None, Some(2)),
        vec!["urn:a".to_string(), "urn:b".to_string()]
    );
}

#[test]
fn ascending_offset_and_limit_select_the_middle_of_the_sorted_run() {
    let database = ranked_database();
    // Sorted ascending by rank: e(1) d(2) c(3) b(4) a(5); skip 1, take 2
    assert_eq!(
        selected_subjects(&database, false, Some(1), Some(2)),
        vec!["urn:c".to_string(), "urn:d".to_string()]
    );
}

#[test]
fn descending_offset_and_limit_select_the_middle_of_the_sorted_run() {
    let database = ranked_database();
    // Sorted descending by rank: a(5) b(4) c(3) d(2) e(1); skip 1, take 2
    assert_eq!(
        selected_subjects(&database, true, Some(1), Some(2)),
        vec!["urn:b".to_string(), "urn:c".to_string()]
    );
}

#[test]
fn offset_alone_drops_the_leading_triples_of_the_sorted_run() {
    let database = ranked_database();
    assert_eq!(
        selected_subjects(&database, false, Some(3), None),
        vec!["urn:a".to_string(), "urn:b".to_string()]
    );
}

#[test]
fn offset_equal_to_the_result_size_yields_nothing() {
    let database = ranked_database();
    assert!(selected_subjects(&database, false, Some(5), None).is_empty());
}

#[test]
fn offset_past_the_result_size_yields_nothing_instead_of_panicking() {
    let database = ranked_database();
    assert!(selected_subjects(&database, false, Some(9), None).is_empty());
    assert!(selected_subjects(&database, false, Some(9), Some(2)).is_empty());
}

#[test]
fn limit_larger_than_the_result_size_returns_everything() {
    let database = ranked_database();
    assert_eq!(selected_subjects(&database, false, None, Some(99)).len(), 5);
}

#[test]
fn limit_without_a_sort_key_still_selects_that_many_triples() {
    let database = ranked_database();
    let selected = QueryBuilder::new(&database).limit(3).get_decoded_triples();
    assert_eq!(selected.len(), 3);
}
