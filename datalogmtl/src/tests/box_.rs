/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::syntax::{DatalogMTLRule, TemporalAtom, Interval};
use crate::store::{TemporalSnapshotStore, TemporalStore};
use crate::evaluator::DatalogMTLEvaluator;
use super::{make_dict, enc, triple, c, v};

/// Test 4a: Box positive — holds at every timestamp.
/// Rule: (?x :stableReading ?v) :- Box[0, 10000] (?x :sensor ?v)
#[test]
fn test_box_positive() {
    let dict = make_dict();
    let sensor_s  = enc(&dict, ":S");
    let sensor_p  = enc(&dict, ":sensor");
    let stable    = enc(&dict, ":stableReading");
    let val42     = enc(&dict, ":v42");

    let rule = DatalogMTLRule {
        id:   "stableReading".into(),
        head: (v("x"), c(stable), v("val")),
        body: vec![
            TemporalAtom::Box_ {
                interval: Interval { start: 0, end: 10000 },
                inner: Box::new(TemporalAtom::Base((v("x"), c(sensor_p), v("val")))),
            },
        ],
    };

    let store = TemporalSnapshotStore::new(15_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    // Feed (S :sensor :v42) at every 1000ms tick from 0 to 10000.
    let mut last_derived = Vec::new();
    for t in (0..=10000u64).step_by(1000) {
        let (d, _) = eval.advance(t, vec![triple(sensor_s, sensor_p, val42)]);
        last_derived = d;
    }

    let expected = triple(sensor_s, stable, val42);

    let all_facts: Vec<_> = eval.store
        .query_at(&(v("x"), v("p"), v("o")), 10000)
        .into_iter()
        .map(|b| triple(
            *b.get("x").unwrap_or(&0),
            *b.get("p").unwrap_or(&0),
            *b.get("o").unwrap_or(&0),
        ))
        .collect();

    assert!(all_facts.contains(&expected) || last_derived.contains(&expected),
        "(S :stableReading :v42) should be derived at t=10000; \
         all_facts={:?}", all_facts);
}

/// Test 4b: Box gap — a different value at one timestamp breaks the invariant.
/// Under data-timestamp semantics, Box only checks ACTIVE timestamps.
/// A "gap" (missing event) is NOT an active timestamp, so Box still vacuously holds
/// over it. To make Box fail, we insert a DIFFERENT value at the gap timestamp.
#[test]
fn test_box_value_inconsistency() {
    let dict = make_dict();
    let sensor_s  = enc(&dict, ":S");
    let sensor_p  = enc(&dict, ":sensor");
    let stable    = enc(&dict, ":stableReading");
    let val42     = enc(&dict, ":v42");
    let val99     = enc(&dict, ":v99");

    let rule = DatalogMTLRule {
        id:   "stableReading".into(),
        head: (v("x"), c(stable), v("val")),
        body: vec![
            TemporalAtom::Box_ {
                interval: Interval { start: 0, end: 10000 },
                inner: Box::new(TemporalAtom::Base((v("x"), c(sensor_p), v("val")))),
            },
        ],
    };

    let store = TemporalSnapshotStore::new(15_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    // Feed val42 at every tick, but insert a DIFFERENT value at t=5000.
    // This means (S :sensor :v42) does NOT hold at t=5000 (only :v99 does).
    for t in (0..=10000u64).step_by(1000) {
        let val = if t == 5000 { val99 } else { val42 };
        eval.advance(t, vec![triple(sensor_s, sensor_p, val)]);
    }

    // At t=10000, Box[0,10000](?x :sensor ?v) for val=:v42 should fail
    // because (S :sensor :v42) doesn't hold at t=5000.
    let (derived, _) = eval.advance(10000, vec![]);
    let expected42 = triple(sensor_s, stable, val42);

    let derived_set: std::collections::HashSet<_> = derived.iter().cloned().collect();
    assert!(!derived_set.contains(&expected42),
        "(S :stableReading :v42) should NOT be derived at t=10000 \
         when val99 was present at t=5000; derived={:?}", derived);
}

/// Test 4c: Box fails when window is empty (no timestamps in range).
/// Box[1000, 5000] at t=0: t < interval.start (0 < 1000) → early exit, no derivation.
#[test]
fn test_box_vacuous() {
    let dict = make_dict();
    let sensor_s  = enc(&dict, ":S");
    let sensor_p  = enc(&dict, ":sensor");
    let stable    = enc(&dict, ":stableReading");
    let val42     = enc(&dict, ":v42");

    // Box[1000, 5000]: at t=0, t < interval.start → early exit.
    // Even if timestamps_in were called, an empty window must yield [] (no vacuous truth).
    let rule = DatalogMTLRule {
        id:   "stableReading".into(),
        head: (v("x"), c(stable), v("val")),
        body: vec![
            TemporalAtom::Box_ {
                interval: Interval { start: 1000, end: 5000 },
                inner: Box::new(TemporalAtom::Base((v("x"), c(sensor_p), v("val")))),
            },
        ],
    };

    let store = TemporalSnapshotStore::new(10_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    // At t=0, t < 1000 (interval.start) → guard fires, no derivation expected.
    let (derived, metrics) = eval.advance(0, vec![triple(sensor_s, sensor_p, val42)]);
    assert!(metrics.fixpoint_iterations >= 1);
    let expected = triple(sensor_s, stable, val42);
    let derived_set: std::collections::HashSet<_> = derived.iter().cloned().collect();
    assert!(!derived_set.contains(&expected),
        "Box should NOT fire when window is empty; derived={:?}", derived);
}
