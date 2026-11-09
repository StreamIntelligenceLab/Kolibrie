/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::{c, enc, make_dict, triple, v};
use crate::evaluator::DatalogMTLEvaluator;
use crate::store::TemporalSnapshotStore;
use crate::syntax::{DatalogMTLRule, Interval, TemporalAtom};

/// Test 4a: Box positive — inner holds at every integer point in the window
#[test]
fn test_box_positive() {
    let dict = make_dict();
    let sensor_s = enc(&dict, ":S");
    let sensor_p = enc(&dict, ":sensor");
    let stable = enc(&dict, ":stableReading");
    let val42 = enc(&dict, ":v42");

    let rule = DatalogMTLRule {
        id: "stableReading".into(),
        head: (v("x"), c(stable), v("val")),
        body: vec![TemporalAtom::Box_ {
            interval: Interval { start: 0, end: 5 },
            inner: Box::new(TemporalAtom::Base((v("x"), c(sensor_p), v("val")))),
        }],
    };

    let store = TemporalSnapshotStore::new(100);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    let mut last_derived = Vec::new();
    for t in 0..=5u64 {
        let (d, _) = eval.advance(t, vec![triple(sensor_s, sensor_p, val42)]);
        last_derived = d;
    }

    let expected = triple(sensor_s, stable, val42);
    assert!(
        last_derived.contains(&expected),
        "(S :stableReading :v42) should be derived at t=5; derived={:?}",
        last_derived
    );
}

/// Test 4b: Box negative — a different value at one integer point breaks it
#[test]
fn test_box_value_inconsistency() {
    let dict = make_dict();
    let sensor_s = enc(&dict, ":S");
    let sensor_p = enc(&dict, ":sensor");
    let stable = enc(&dict, ":stableReading");
    let val42 = enc(&dict, ":v42");
    let val99 = enc(&dict, ":v99");

    let rule = DatalogMTLRule {
        id: "stableReading".into(),
        head: (v("x"), c(stable), v("val")),
        body: vec![TemporalAtom::Box_ {
            interval: Interval { start: 0, end: 5 },
            inner: Box::new(TemporalAtom::Base((v("x"), c(sensor_p), v("val")))),
        }],
    };

    let store = TemporalSnapshotStore::new(100);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    for t in 0..=5u64 {
        let val = if t == 3 { val99 } else { val42 };
        eval.advance(t, vec![triple(sensor_s, sensor_p, val)]);
    }

    let (derived, _) = eval.advance(5, vec![]);
    let expected42 = triple(sensor_s, stable, val42);

    let derived_set: std::collections::HashSet<_> = derived.iter().cloned().collect();
    assert!(
        !derived_set.contains(&expected42),
        "(S :stableReading :v42) should NOT be derived at t=5 \
         when :v99 was present at t=3; derived={:?}",
        derived
    );
}

/// Test 4c: Box fails when window is empty (no timestamps in range)
#[test]
fn test_box_vacuous() {
    let dict = make_dict();
    let sensor_s = enc(&dict, ":S");
    let sensor_p = enc(&dict, ":sensor");
    let stable = enc(&dict, ":stableReading");
    let val42 = enc(&dict, ":v42");

    let rule = DatalogMTLRule {
        id: "stableReading".into(),
        head: (v("x"), c(stable), v("val")),
        body: vec![TemporalAtom::Box_ {
            interval: Interval {
                start: 1000,
                end: 5000,
            },
            inner: Box::new(TemporalAtom::Base((v("x"), c(sensor_p), v("val")))),
        }],
    };

    let store = TemporalSnapshotStore::new(10_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    let (derived, metrics) = eval.advance(0, vec![triple(sensor_s, sensor_p, val42)]);
    assert!(metrics.fixpoint_iterations >= 1);
    let expected = triple(sensor_s, stable, val42);
    let derived_set: std::collections::HashSet<_> = derived.iter().cloned().collect();
    assert!(
        !derived_set.contains(&expected),
        "Box should NOT fire when window is empty; derived={:?}",
        derived
    );
}
