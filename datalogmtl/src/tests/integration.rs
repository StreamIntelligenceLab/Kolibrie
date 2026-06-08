/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;
use crate::syntax::{DatalogMTLRule, TemporalAtom, Interval};
use crate::store::{TemporalSnapshotStore, IntervalFactStore, TemporalStore};
use crate::evaluator::DatalogMTLEvaluator;
use shared::triple::Triple;
use super::{make_dict, enc, triple, c, v};

fn make_diamond_rules(location: u32, was_near: u32) -> Vec<DatalogMTLRule> {
    vec![DatalogMTLRule {
        id:   "wasNear".into(),
        head: (v("x"), c(was_near), v("y")),
        body: vec![
            TemporalAtom::Base((v("x"), c(location), v("loc"))),
            TemporalAtom::Diamond {
                interval: Interval { start: 1000, end: 5000 },
                inner: Box::new(TemporalAtom::Base((v("y"), c(location), v("loc")))),
            },
        ],
    }]
}

/// Test 7: Phase 2 parity — derived triples must match Phase 1 at every tick.
#[test]
fn test_phase2_parity() {
    let dict1 = make_dict();
    let dict2 = make_dict();

    let a        = dict1.write().unwrap().encode(":a");
    let b        = dict1.write().unwrap().encode(":b");
    let location = dict1.write().unwrap().encode(":location");
    let was_near = dict1.write().unwrap().encode(":wasNear");
    let room1    = dict1.write().unwrap().encode(":room1");
    // Mirror in dict2
    let _ = dict2.write().unwrap().encode(":a");
    let _ = dict2.write().unwrap().encode(":b");
    let _ = dict2.write().unwrap().encode(":location");
    let _ = dict2.write().unwrap().encode(":wasNear");
    let _ = dict2.write().unwrap().encode(":room1");

    let rules1 = make_diamond_rules(location, was_near);
    let rules2 = make_diamond_rules(location, was_near);

    let mut eval1 = DatalogMTLEvaluator::new(
        rules1, TemporalSnapshotStore::new(10_000), dict1.clone()
    ).unwrap();
    let mut eval2 = DatalogMTLEvaluator::new(
        rules2, IntervalFactStore::new(10_000), dict2.clone()
    ).unwrap();

    let ticks: Vec<(u64, Vec<Triple>)> = vec![
        (0, vec![triple(b, location, room1)]),
        (1000, vec![]),
        (2000, vec![triple(a, location, room1)]),
        (3000, vec![triple(a, location, room1)]),
        (6000, vec![triple(a, location, room1)]),
    ];

    for (t, new_triples) in ticks {
        let (d1, _) = eval1.advance(t, new_triples.clone());
        let (d2, _) = eval2.advance(t, new_triples);
        let s1: HashSet<Triple> = d1.into_iter().collect();
        let s2: HashSet<Triple> = d2.into_iter().collect();
        assert_eq!(s1, s2,
            "Phase 1 and Phase 2 derived different results at t={}: p1={:?} p2={:?}",
            t, s1, s2);
    }

    // Phase 2 total_triple_count <= Phase 1 total_triple_count
    assert!(eval2.store.total_triple_count() <= eval1.store.total_triple_count(),
        "Phase 2 should use <= triples vs Phase 1");
}

/// Test 10: Eviction — after t=10000, no snapshot for t=0 (w_max=5000).
#[test]
fn test_eviction() {
    let dict = make_dict();
    let sensor_s  = enc(&dict, ":S");
    let sensor_p  = enc(&dict, ":sensor");
    let val42     = enc(&dict, ":v42");
    let stable    = enc(&dict, ":stableReading");

    // Rule with max interval 5000ms.
    let rule = DatalogMTLRule {
        id:   "stable".into(),
        head: (v("x"), c(stable), v("v")),
        body: vec![
            TemporalAtom::Box_ {
                interval: Interval { start: 0, end: 5000 },
                inner: Box::new(TemporalAtom::Base((v("x"), c(sensor_p), v("v")))),
            },
        ],
    };

    let store = TemporalSnapshotStore::new(6_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    for t in (0..=10_000u64).step_by(1000) {
        eval.advance(t, vec![triple(sensor_s, sensor_p, val42)]);
    }

    // After t=10000, cutoff = 10000 - 5000 = 5000.
    // Phase 1: no snapshot at t=0 (evicted).
    let ts_at_0 = eval.store.timestamps_in(0, 0);
    assert!(ts_at_0.is_empty(),
        "After eviction at t=10000 with w_max=5000, t=0 should be evicted");
}

/// Test 10b: Phase 2 eviction.
#[test]
fn test_eviction_phase2() {
    let dict = make_dict();
    let sensor_s  = enc(&dict, ":S");
    let sensor_p  = enc(&dict, ":sensor");
    let val42     = enc(&dict, ":v42");
    let stable    = enc(&dict, ":stableReading");

    let rule = DatalogMTLRule {
        id:   "stable".into(),
        head: (v("x"), c(stable), v("v")),
        body: vec![
            TemporalAtom::Box_ {
                interval: Interval { start: 0, end: 5000 },
                inner: Box::new(TemporalAtom::Base((v("x"), c(sensor_p), v("v")))),
            },
        ],
    };

    let store = IntervalFactStore::new(6_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    for t in (0..=10_000u64).step_by(1000) {
        eval.advance(t, vec![triple(sensor_s, sensor_p, val42)]);
    }

    // After t=10000, cutoff = 5000. No interval should start before t=5000.
    let ts_before_cutoff = eval.store.timestamps_in(0, 4999);
    assert!(ts_before_cutoff.is_empty(),
        "Phase 2: no intervals before cutoff=5000 after eviction; \
         found timestamps: {:?}", ts_before_cutoff);
}
