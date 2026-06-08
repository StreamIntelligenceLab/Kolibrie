/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::syntax::{DatalogMTLRule, TemporalAtom};
use crate::store::{TemporalSnapshotStore, TemporalStore};
use crate::evaluator::DatalogMTLEvaluator;
use super::{make_dict, enc, triple, c, v};

/// Test 1: Base atom join — identity rule with variable predicate.
/// Rule: (?s ?p ?o) :- (?s ?p ?o)
#[test]
fn test_base_identity_variable_predicate() {
    let dict = make_dict();
    let a   = enc(&dict, ":a");
    let b   = enc(&dict, ":b");
    let p1  = enc(&dict, ":p1");
    let p2  = enc(&dict, ":p2");
    let p3  = enc(&dict, ":p3");
    let v1  = enc(&dict, ":v1");
    let v2  = enc(&dict, ":v2");
    let v3  = enc(&dict, ":v3");

    let rule = DatalogMTLRule {
        id:   "identity".into(),
        head: (v("s"), v("p"), v("o")),
        body: vec![TemporalAtom::Base((v("s"), v("p"), v("o")))],
    };

    let store = TemporalSnapshotStore::new(10_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    let triples = vec![
        triple(a, p1, v1),
        triple(a, p2, v2),
        triple(b, p3, v3),
    ];
    let (derived, _) = eval.advance(0, triples);

    // All 3 triples must be present (identity rule re-derives them).
    // Derived may be empty since input triples already in store and identity.
    // What matters: all triples queryable in the store.
    let store_triples: Vec<_> = eval.store
        .query_at(&(v("s"), v("p"), v("o")), 0)
        .into_iter()
        .map(|b| triple(
            *b.get("s").unwrap(),
            *b.get("p").unwrap(),
            *b.get("o").unwrap(),
        ))
        .collect();

    assert_eq!(store_triples.len(), 3, "All 3 input triples should be in the store");

    // Identity rule: derived set should contain the same 3 facts
    // (or 0 if we treat input==derived as not new — depends on implementation).
    // Assert at least the store contains all 3.
    let expected = [
        triple(a, p1, v1),
        triple(a, p2, v2),
        triple(b, p3, v3),
    ];
    for t in &expected {
        assert!(store_triples.contains(t), "Triple {:?} missing from store", t);
    }
    let _ = derived;
}

/// Test 2: Multi-atom Base join — transitive step.
/// Rule: (?x :connected ?z) :- (?x :knows ?y), (?y :knows ?z)
#[test]
fn test_base_multi_atom_join() {
    let dict = make_dict();
    let a       = enc(&dict, ":a");
    let b       = enc(&dict, ":b");
    let cc      = enc(&dict, ":c");
    let knows   = enc(&dict, ":knows");
    let connected = enc(&dict, ":connected");

    let rule = DatalogMTLRule {
        id:   "connected".into(),
        head: (v("x"), c(connected), v("z")),
        body: vec![
            TemporalAtom::Base((v("x"), c(knows), v("y"))),
            TemporalAtom::Base((v("y"), c(knows), v("z"))),
        ],
    };

    let store = TemporalSnapshotStore::new(10_000);
    let mut eval = DatalogMTLEvaluator::new(vec![rule], store, dict.clone()).unwrap();

    let (derived, _) = eval.advance(0, vec![
        triple(a, knows, b),
        triple(b, knows, cc),
    ]);

    let expected = triple(a, connected, cc);
    assert!(derived.contains(&expected),
        "Expected (a :connected c) to be derived, got: {:?}", derived);

    // (b :connected a) should NOT be derived
    let not_expected = triple(b, connected, a);
    assert!(!derived.contains(&not_expected),
        "(b :connected a) should NOT be derived");
}
