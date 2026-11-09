/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Unit tests that reach into the store's crate-private state

use super::{KolibrieDB, PlanningStatsPolicy};
use shared::dataset_index::{DatasetIndex, GraphId, Quad};
use std::sync::Arc;

fn insert(database: &mut KolibrieDB, index: u32) {
    database.add_triple_parts(&format!("urn:s{index}"), "urn:p", &format!("{index}"));
}

#[test]
fn exact_statistics_notice_a_write_that_bypassed_the_helpers() {
    let mut database = KolibrieDB::new();
    for index in 0..3 {
        insert(&mut database, index);
    }
    assert_eq!(database.get_or_build_stats().total_triples, 3);

    let quad = {
        let mut dictionary = database.dictionary_mut();
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
fn a_replacement_index_with_a_lower_generation_is_not_mistaken_for_fresh() {
    let mut database = KolibrieDB::new();
    for index in 0..200 {
        insert(&mut database, index);
    }
    database.set_planning_stats_policy(PlanningStatsPolicy::Bounded {
        minimum_batch: 1_000,
        refresh_fraction: 0.90,
    });
    let first = database.get_or_build_planning_stats();
    assert_eq!(first.total_triples, 200);

    let mut replacement = DatasetIndex::new();
    replacement.insert_quad(&Quad {
        subject: 1,
        predicate: 2,
        object: 3,
        graph: GraphId::Default,
    });
    database.dataset_index = replacement;

    let refreshed = database.get_or_build_planning_stats();
    assert!(!Arc::ptr_eq(&first, &refreshed));
    assert_eq!(refreshed.total_triples, 1);
    assert_eq!(database.get_or_build_stats().total_triples, 1);
}
