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

/// Bounded policy with an effectively infinite threshold
fn frozen() -> PlanningStatsPolicy {
    PlanningStatsPolicy::Bounded {
        minimum_batch: u64::MAX / 2,
        refresh_fraction: f64::MAX,
    }
}

/// Builds a store with a frozen planning snapshot
fn with_frozen_statistics(triples: &str) -> SparqlDatabase {
    let mut database = common::database_with(triples);
    database.planning_stats_policy = frozen();
    database.get_or_build_planning_stats();
    database
}

#[test]
fn a_predicate_added_after_the_snapshot_is_still_queryable() {
    let mut database = with_frozen_statistics(r#"<urn:a> <urn:known> "1" ."#);

    common::load(&mut database, r#"<urn:b> <urn:brand-new> "2" ."#);

    assert_eq!(
        common::query(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:brand-new> ?o . }"
        ),
        common::rows1(&["urn:b"]),
        "a predicate the snapshot has never seen must still return its rows"
    );
}

#[test]
fn a_named_graph_added_after_the_snapshot_is_still_queryable() {
    let mut database = with_frozen_statistics(r#"<urn:a> <urn:p> "1" ."#);

    kolibrie::execute_query::execute_sparql_update(
        r#"INSERT DATA { GRAPH <urn:fresh> { <urn:b> <urn:p> "2" . } }"#,
        &mut database,
    )
    .unwrap();

    assert_eq!(
        common::query(
            &mut database,
            "SELECT ?s WHERE { GRAPH <urn:fresh> { ?s <urn:p> ?o . } }"
        ),
        common::rows1(&["urn:b"]),
        "a fixed graph created after the snapshot must not be planned away"
    );
    assert_eq!(
        common::query(
            &mut database,
            "SELECT ?s WHERE { GRAPH ?g { ?s <urn:p> ?o . } }"
        ),
        common::rows1(&["urn:b"]),
        "GRAPH ?g must range over graphs the snapshot has never seen"
    );
}

#[test]
fn a_rare_predicate_added_after_the_snapshot_is_still_queryable() {
    let mut triples = String::new();
    for index in 0..300 {
        triples.push_str(&format!("<urn:s{index}> <urn:common> \"{index}\" .\n"));
    }
    let mut database = with_frozen_statistics(&triples);

    common::load(&mut database, r#"<urn:rare-subject> <urn:rare> "once" ."#);

    assert_eq!(
        common::query(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:rare> ?o . ?s <urn:rare> ?o2 . }"
        ),
        common::rows1(&["urn:rare-subject"]),
    );
}

#[test]
fn deleting_everything_a_predicate_had_leaves_the_query_correct() {
    let mut triples = String::new();
    for index in 0..50 {
        triples.push_str(&format!("<urn:s{index}> <urn:doomed> \"{index}\" .\n"));
        triples.push_str(&format!("<urn:s{index}> <urn:kept> \"{index}\" .\n"));
    }
    let mut database = with_frozen_statistics(&triples);

    for index in 0..50 {
        assert!(database.delete_triple_parts(
            &format!("urn:s{index}"),
            "urn:doomed",
            &format!("{index}")
        ));
    }

    assert!(
        common::query(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:doomed> ?o . ?s <urn:kept> ?k . }"
        )
        .is_empty(),
        "the snapshot still believes in the predicate; the data does not"
    );
    assert_eq!(
        common::query(&mut database, "SELECT ?s WHERE { ?s <urn:kept> \"7\" . }"),
        common::rows1(&["urn:s7"]),
    );
}

/// Stale statistics must not change query results
#[test]
fn stale_statistics_never_change_a_result() {
    let mut triples = String::new();
    for index in 0..120 {
        triples.push_str(&format!("<urn:s{index}> <urn:p> <urn:o{}> .\n", index % 17));
        triples.push_str(&format!("<urn:o{}> <urn:q> \"{index}\" .\n", index % 17));
    }

    let queries = [
        "SELECT ?s ?v WHERE { ?s <urn:p> ?o . ?o <urn:q> ?v . }",
        "SELECT ?s WHERE { ?s <urn:p> ?o . }",
        "SELECT ?v WHERE { ?s <urn:p> ?o . ?o <urn:q> ?v . FILTER(?v > \"50\") }",
        "SELECT ?s ?o WHERE { ?s <urn:p> ?o . ?o <urn:q> ?v . ?s <urn:p> ?o2 . }",
    ];

    for query in queries {
        let mut fresh = common::database_with(&triples);
        let mut stale = with_frozen_statistics(&triples);

        // Apply identical mutations to both databases
        for database in [&mut fresh, &mut stale] {
            common::load(database, r#"<urn:s999> <urn:p> <urn:o3> ."#);
            database.delete_triple_parts("urn:s0", "urn:p", "urn:o0");
        }

        assert_eq!(
            common::query(&mut stale, query),
            common::query(&mut fresh, query),
            "stale statistics changed the answer to {query}"
        );
    }
}

// Trivial-plan shortcut

/// Returns the number of rebuilds triggered by one query
fn rebuilds_for(database: &mut SparqlDatabase, query: &str) -> u64 {
    let before = database.stats_rebuild_count;
    common::query(database, query);
    database.stats_rebuild_count - before
}

#[test]
fn a_plan_with_no_join_order_to_choose_builds_no_statistics() {
    let trivial = [
        "SELECT ?s WHERE { ?s <urn:p> ?o . }",
        "SELECT ?s WHERE { ?s <urn:p> \"1\" . }",
        "SELECT ?s WHERE { ?s <urn:p> ?o . FILTER(?o != \"2\") }",
        "SELECT ?s WHERE { GRAPH <urn:g> { ?s <urn:p> ?o . } }",
        "SELECT ?s WHERE { }",
    ];

    for query in trivial {
        let mut database = common::database_with(
            r#"<urn:a> <urn:p> "1" .
<urn:b> <urn:p> "2" ."#,
        );
        // Build exact statistics first so only the query's rebuilds are counted
        database.get_or_build_stats();

        assert_eq!(
            rebuilds_for(&mut database, query),
            0,
            "{query} has no ranking decision to make"
        );
    }
}

#[test]
fn a_plan_with_a_join_order_to_choose_still_builds_statistics() {
    let mut database = common::database_with(
        r#"<urn:a> <urn:p> "1" .
<urn:b> <urn:p> "2" ."#,
    );

    assert_eq!(
        rebuilds_for(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:p> ?o . ?s <urn:p> ?o2 . }"
        ),
        1,
        "two joinable patterns present a real choice"
    );
}

/// Skipping statistics must not change results
#[test]
fn the_shortcut_agrees_with_always_building_statistics() {
    let triples = r#"<urn:a> <urn:p> "1" .
<urn:b> <urn:p> "2" .
<urn:c> <urn:q> "3" ."#;

    for query in [
        "SELECT ?s WHERE { ?s <urn:p> ?o . }",
        "SELECT ?s ?o WHERE { ?s <urn:p> ?o . FILTER(?o != \"1\") }",
        "SELECT ?s WHERE { ?s <urn:p> ?o . ?s <urn:p> ?o2 . }",
        "SELECT ?o WHERE { <urn:a> <urn:p> ?o . }",
    ] {
        let mut shortcut = common::database_with(triples);
        let mut always = common::database_with(triples);
        always.planning_stats_policy = PlanningStatsPolicy::AlwaysFresh;

        assert_eq!(
            common::query(&mut shortcut, query),
            common::query(&mut always, query),
            "the shortcut changed the answer to {query}"
        );
    }
}
