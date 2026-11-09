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
use crate::store::{TemporalSnapshotStore, TemporalStore};
use crate::syntax::{DatalogMTLRule, Interval, TemporalAtom};

/// Test 5: Since operator
#[test]
fn test_since_basic() {
    let dict = make_dict();
    let w = enc(&dict, ":W");
    let active = enc(&dict, ":active");
    let started = enc(&dict, ":started");
    let cont_active = enc(&dict, ":continuouslyActive");
    let true_val = enc(&dict, ":true");

    let rule = DatalogMTLRule {
        id: "continuouslyActive".into(),
        head: (v("x"), c(cont_active), c(true_val)),
        body: vec![TemporalAtom::Since {
            interval: Interval {
                start: 0,
                end: 60_000,
            },
            phi: Box::new(TemporalAtom::Base((v("x"), c(active), c(true_val)))),
            psi: Box::new(TemporalAtom::Base((v("x"), c(started), c(true_val)))),
        }],
    };

    let store = TemporalSnapshotStore::new(1_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    eval.advance(0, vec![triple(w, started, true_val)]);

    let mut last_metrics = None;
    for t in 1..=6u64 {
        let (_, m) = eval.advance(t, vec![triple(w, active, true_val)]);
        last_metrics = Some(m);
    }

    let (derived6, metrics6) = eval.advance(6, vec![triple(w, active, true_val)]);
    let expected = triple(w, cont_active, true_val);

    let all6: Vec<_> = eval
        .store
        .query_at(&(v("x"), v("p"), v("o")), 6)
        .into_iter()
        .map(|b| {
            triple(
                *b.get("x").unwrap_or(&0),
                *b.get("p").unwrap_or(&0),
                *b.get("o").unwrap_or(&0),
            )
        })
        .collect();

    assert!(
        all6.contains(&expected) || derived6.contains(&expected),
        "(W :continuouslyActive) should be derived at t=6"
    );
    assert!(
        metrics6.since_scan_depth > 0 || last_metrics.map(|m| m.since_scan_depth).unwrap_or(0) > 0,
        "since_scan_depth should be > 0"
    );

    let (derived8, _) = eval.advance(8, vec![]);
    let all8: Vec<_> = eval
        .store
        .query_at(&(v("x"), v("p"), v("o")), 8)
        .into_iter()
        .map(|b| {
            triple(
                *b.get("x").unwrap_or(&0),
                *b.get("p").unwrap_or(&0),
                *b.get("o").unwrap_or(&0),
            )
        })
        .collect();

    assert!(
        !all8.contains(&expected) && !derived8.contains(&expected),
        "(W :continuouslyActive) should NOT hold at t=8 without :active"
    );
}
