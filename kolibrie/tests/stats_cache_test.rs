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

use kolibrie::sparql_database::{PlanningStatsPolicy, SparqlDatabase};
use shared::dataset_index::{DatasetIndex, GraphId, Quad};

fn seeded() -> SparqlDatabase {
    common::database_with(
        r#"<urn:a> <urn:p> "1" .
<urn:b> <urn:p> "2" .
<urn:c> <urn:p> "3" ."#,
    )
}

fn bounded(minimum_batch: u64, refresh_fraction: f64) -> PlanningStatsPolicy {
    PlanningStatsPolicy::Bounded {
        minimum_batch,
        refresh_fraction,
    }
}

fn insert(database: &mut SparqlDatabase, index: u32) {
    database.add_triple_parts(&format!("urn:s{index}"), "urn:p", &format!("{index}"));
}

fn rebuilds(database: &SparqlDatabase) -> u64 {
    database.stats_rebuild_count
}

// Exact statistics

#[test]
fn exact_statistics_are_fresh_after_every_successful_mutation() {
    let mut database = seeded();
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    for index in 0..5 {
        insert(&mut database, index);
        assert_eq!(
            database.get_or_build_stats().total_triples,
            4 + index as u64,
            "exact statistics must never lag"
        );
    }
}

#[test]
fn exact_statistics_notice_a_write_that_bypassed_the_helpers() {
    let mut database = seeded();
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    // Insert directly into the index; a single lock guard avoids a self-deadlock
    let quad = {
        let mut dictionary = database.dictionary.write().unwrap();
        Quad {
            subject: dictionary.encode("urn:d"),
            predicate: dictionary.encode("urn:p"),
            object: dictionary.encode("4"),
            graph: GraphId::Default,
        }
    };
    assert!(database.dataset_index.insert_quad(&quad));

    assert_eq!(database.get_or_build_stats().total_triples, 4);
}

#[test]
fn a_mutation_that_changed_nothing_does_not_force_a_rebuild() {
    let mut database = seeded();
    database.get_or_build_stats();
    let before = rebuilds(&database);

    // No-op mutations: duplicate insert, missing delete, empty update
    database.add_triple_parts("urn:a", "urn:p", "1");
    assert!(!database.delete_triple_parts("urn:zz", "urn:p", "nope"));
    kolibrie::execute_query::execute_sparql_update(
        r#"DELETE { ?s <urn:p> "absent" } WHERE { ?s <urn:p> "absent" }"#,
        &mut database,
    )
    .unwrap();

    database.get_or_build_stats();
    assert_eq!(
        rebuilds(&database),
        before,
        "an idle write must leave a usable snapshot in place"
    );
}

// Planning statistics: AlwaysFresh

#[test]
fn the_default_policy_is_bounded() {
    assert_eq!(
        SparqlDatabase::new().planning_stats_policy,
        PlanningStatsPolicy::DEFAULT_BOUNDED
    );
}

#[test]
fn always_fresh_planning_statistics_match_the_exact_ones() {
    let mut database = seeded();
    database.planning_stats_policy = PlanningStatsPolicy::AlwaysFresh;

    for index in 0..5 {
        insert(&mut database, index);
        assert_eq!(
            database.get_or_build_planning_stats().total_triples,
            database.get_or_build_stats().total_triples,
        );
    }
}

// Planning statistics: Bounded

#[test]
fn bounded_planning_statistics_are_reused_below_the_boundary() {
    let mut database = seeded();
    database.planning_stats_policy = bounded(10, 0.10);
    let first = database.get_or_build_planning_stats();
    let after_first = rebuilds(&database);

    for index in 0..5 {
        insert(&mut database, index);
        let stats = database.get_or_build_planning_stats();
        assert!(
            std::sync::Arc::ptr_eq(&first, &stats),
            "five mutations is under a minimum batch of ten"
        );
    }
    assert_eq!(rebuilds(&database), after_first, "no rebuild happened");

    // Exact statistics are not affected by the policy
    assert_eq!(database.get_or_build_stats().total_triples, 8);
}

#[test]
fn bounded_planning_statistics_rebuild_at_the_boundary() {
    let mut database = seeded();
    database.planning_stats_policy = bounded(10, 0.10);
    let first = database.get_or_build_planning_stats();

    for index in 0..9 {
        insert(&mut database, index);
    }
    assert!(
        std::sync::Arc::ptr_eq(&first, &database.get_or_build_planning_stats()),
        "nine mutations is still under the boundary"
    );

    insert(&mut database, 9);
    let refreshed = database.get_or_build_planning_stats();
    assert!(!std::sync::Arc::ptr_eq(&first, &refreshed));
    assert_eq!(refreshed.total_triples, 13);
}

/// Above minimum_batch the threshold is proportional to dataset size
#[test]
fn the_boundary_is_proportional_once_it_exceeds_the_minimum_batch() {
    let mut database = SparqlDatabase::new();
    for index in 0..1_000 {
        insert(&mut database, index);
    }
    database.planning_stats_policy = bounded(10, 0.10);
    let first = database.get_or_build_planning_stats();
    assert_eq!(first.total_triples, 1_000);

    // Threshold: max(10, 0.1 * 1000) = 100
    for index in 1_000..1_090 {
        insert(&mut database, index);
    }
    assert!(
        std::sync::Arc::ptr_eq(&first, &database.get_or_build_planning_stats()),
        "ninety writes into a thousand-quad store is inside the boundary"
    );

    for index in 1_090..1_105 {
        insert(&mut database, index);
    }
    assert!(!std::sync::Arc::ptr_eq(
        &first,
        &database.get_or_build_planning_stats()
    ));
}

/// Delete/insert pairs count as mutations even when the size is unchanged
#[test]
fn a_constant_size_replacement_still_reaches_the_boundary() {
    let mut database = SparqlDatabase::new();
    for index in 0..100 {
        insert(&mut database, index);
    }
    database.planning_stats_policy = bounded(10, 0.10);
    let first = database.get_or_build_planning_stats();

    for index in 0..10 {
        assert!(database.delete_triple_parts(
            &format!("urn:s{index}"),
            "urn:p",
            &format!("{index}")
        ));
        insert(&mut database, index + 10_000);
    }

    assert_eq!(
        database.get_or_build_stats().total_triples,
        100,
        "the dataset is the same size it was"
    );
    assert!(
        !std::sync::Arc::ptr_eq(&first, &database.get_or_build_planning_stats()),
        "twenty successful mutations must still count as twenty"
    );
}

// Forced refresh

#[test]
fn an_empty_dataset_that_gains_data_refreshes_immediately() {
    let mut database = SparqlDatabase::new();
    database.planning_stats_policy = bounded(1_000, 0.90);
    let first = database.get_or_build_planning_stats();
    assert_eq!(first.total_triples, 0);

    insert(&mut database, 0);
    let refreshed = database.get_or_build_planning_stats();
    assert!(!std::sync::Arc::ptr_eq(&first, &refreshed));
    assert_eq!(refreshed.total_triples, 1);
}

#[test]
fn a_hard_invalidation_retires_every_snapshot_under_any_policy() {
    for policy in [PlanningStatsPolicy::AlwaysFresh, bounded(1_000, 0.90)] {
        let mut database = seeded();
        database.planning_stats_policy = policy;
        let first = database.get_or_build_planning_stats();
        let exact = database.get_or_build_stats();

        database.invalidate_stats_cache();

        assert!(
            !std::sync::Arc::ptr_eq(&first, &database.get_or_build_planning_stats()),
            "{policy:?}: planning snapshot survived a hard invalidation"
        );
        assert!(
            !std::sync::Arc::ptr_eq(&exact, &database.get_or_build_stats()),
            "{policy:?}: exact snapshot survived a hard invalidation"
        );
    }
}

#[test]
fn replacing_the_dataset_index_retires_every_snapshot() {
    let mut database = SparqlDatabase::new();
    for index in 0..200 {
        insert(&mut database, index);
    }
    database.planning_stats_policy = bounded(1_000, 0.90);
    let first = database.get_or_build_planning_stats();
    assert_eq!(first.total_triples, 200);

    database.replace_dataset_index(DatasetIndex::new());

    let refreshed = database.get_or_build_planning_stats();
    assert!(!std::sync::Arc::ptr_eq(&first, &refreshed));
    assert_eq!(refreshed.total_triples, 0);
}

/// A replacement index with a lower generation invalidates the snapshot
#[test]
fn a_replacement_index_with_a_lower_generation_is_not_mistaken_for_fresh() {
    let mut database = SparqlDatabase::new();
    for index in 0..200 {
        insert(&mut database, index);
    }
    database.planning_stats_policy = bounded(1_000, 0.90);
    let first = database.get_or_build_planning_stats();
    assert_eq!(first.total_triples, 200);

    // Generation 1, lower than the original 200
    let mut replacement = DatasetIndex::new();
    replacement.insert_quad(&Quad {
        subject: 1,
        predicate: 2,
        object: 3,
        graph: GraphId::Default,
    });
    // Direct assignment, bypassing replace_dataset_index
    database.dataset_index = replacement;

    let refreshed = database.get_or_build_planning_stats();
    assert!(!std::sync::Arc::ptr_eq(&first, &refreshed));
    assert_eq!(refreshed.total_triples, 1);
    assert_eq!(database.get_or_build_stats().total_triples, 1);
}

#[test]
fn clearing_the_dataset_refreshes_planning_statistics() {
    let mut database = SparqlDatabase::new();
    for index in 0..200 {
        insert(&mut database, index);
    }
    database.planning_stats_policy = bounded(10, 0.50);
    let first = database.get_or_build_planning_stats();
    assert_eq!(first.total_triples, 200);

    database.dataset_index.clear();

    let refreshed = database.get_or_build_planning_stats();
    assert!(!std::sync::Arc::ptr_eq(&first, &refreshed));
    assert_eq!(refreshed.total_triples, 0);
}

// Instrumentation

#[test]
fn rebuilds_are_counted_and_timed() {
    let mut database = seeded();
    let (count, duration) = database.stats_rebuild_metrics();
    assert_eq!(count, 0);
    assert_eq!(duration, std::time::Duration::ZERO);

    database.get_or_build_stats();
    database.get_or_build_stats();
    let (count, _) = database.stats_rebuild_metrics();
    assert_eq!(count, 1, "a cached read is not a rebuild");

    insert(&mut database, 0);
    database.get_or_build_stats();
    assert_eq!(database.stats_rebuild_metrics().0, 2);
}

/// Under Bounded, rebuilds are not proportional to query count
#[test]
fn interleaved_writes_and_queries_do_not_rebuild_per_query() {
    let mut database = SparqlDatabase::new();
    for index in 0..500 {
        insert(&mut database, index);
    }
    database.planning_stats_policy = bounded(25, 0.10);

    let queries = 100;
    for index in 500..500 + queries {
        insert(&mut database, index);
        common::query(
            &mut database,
            "SELECT ?s ?o WHERE { ?s <urn:p> ?o . ?s <urn:p> ?o2 . }",
        );
    }

    assert!(
        rebuilds(&database) < queries as u64 / 4,
        "{} rebuilds for {} queries is still proportional",
        rebuilds(&database),
        queries
    );
}
