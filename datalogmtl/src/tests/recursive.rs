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

/// Test 6: Recursive rules — transitive ancestor.
/// Rules:
///   (?x :ancestor ?z) :- (?x :parent ?z)
///   (?x :ancestor ?z) :- (?x :parent ?y), (?y :ancestor ?z)
///
/// ABox at t=0: (a :parent b), (b :parent c), (c :parent d)
/// Expected: 6 ancestor triples: a->b, a->c, a->d, b->c, b->d, c->d
#[test]
fn test_recursive_ancestor() {
    let dict = make_dict();
    let a        = enc(&dict, ":a");
    let b        = enc(&dict, ":b");
    let cc       = enc(&dict, ":c");
    let d        = enc(&dict, ":d");
    let parent   = enc(&dict, ":parent");
    let ancestor = enc(&dict, ":ancestor");

    let rule1 = DatalogMTLRule {
        id:   "ancestor_base".into(),
        head: (v("x"), c(ancestor), v("z")),
        body: vec![TemporalAtom::Base((v("x"), c(parent), v("z")))],
    };

    let rule2 = DatalogMTLRule {
        id:   "ancestor_recursive".into(),
        head: (v("x"), c(ancestor), v("z")),
        body: vec![
            TemporalAtom::Base((v("x"), c(parent), v("y"))),
            TemporalAtom::Base((v("y"), c(ancestor), v("z"))),
        ],
    };

    let store = TemporalSnapshotStore::new(10_000);
    let mut eval = DatalogMTLEvaluator::new(
        vec![rule1, rule2], store, dict.clone()
    ).unwrap();

    let (_, metrics) = eval.advance(0, vec![
        triple(a, parent, b),
        triple(b, parent, cc),
        triple(cc, parent, d),
    ]);

    // Expected 6 ancestor triples
    let expected = vec![
        triple(a, ancestor, b),
        triple(a, ancestor, cc),
        triple(a, ancestor, d),
        triple(b, ancestor, cc),
        triple(b, ancestor, d),
        triple(cc, ancestor, d),
    ];

    let store_triples: Vec<_> = eval.store
        .query_at(&(v("x"), c(ancestor), v("z")), 0)
        .into_iter()
        .map(|b| triple(
            *b.get("x").unwrap(),
            ancestor,
            *b.get("z").unwrap(),
        ))
        .collect();

    for t in &expected {
        assert!(store_triples.contains(t),
            "Expected ancestor triple {:?} missing. Store has: {:?}", t, store_triples);
    }
    assert_eq!(store_triples.len(), 6,
        "Expected exactly 6 ancestor triples, got {}: {:?}", store_triples.len(), store_triples);

    assert!(metrics.fixpoint_iterations >= 2,
        "Expected >= 2 fixpoint iterations for recursive rules, got {}", metrics.fixpoint_iterations);
}
