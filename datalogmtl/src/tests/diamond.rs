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

/// Test 3: Diamond operator
#[test]
fn test_diamond_basic() {
    let dict = make_dict();
    let a = enc(&dict, ":a");
    let b = enc(&dict, ":b");
    let location = enc(&dict, ":location");
    let was_near = enc(&dict, ":wasNear");
    let room1 = enc(&dict, ":room1");

    let rule = DatalogMTLRule {
        id: "wasNear".into(),
        head: (v("x"), c(was_near), v("y")),
        body: vec![
            TemporalAtom::Base((v("x"), c(location), v("loc"))),
            TemporalAtom::Diamond {
                interval: Interval {
                    start: 1000,
                    end: 5000,
                },
                inner: Box::new(TemporalAtom::Base((v("y"), c(location), v("loc")))),
            },
        ],
    };

    let store = TemporalSnapshotStore::new(10_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    eval.advance(0, vec![triple(b, location, room1)]);

    let (derived, _) = eval.advance(3000, vec![triple(a, location, room1)]);

    let expected = triple(a, was_near, b);
    assert!(
        derived.contains(&expected),
        "Expected (A :wasNear B) at t=3000, got: {:?}",
        derived
    );

    let (derived2, _) = eval.advance(6000, vec![triple(a, location, room1)]);
    assert!(
        !derived2.contains(&expected),
        "(A :wasNear B) should NOT be derived at t=6000 (B too far back)"
    );
}
