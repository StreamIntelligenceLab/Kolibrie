/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::triple::Triple;
use crate::syntax::{DatalogMTLRule, Interval, TemporalAtom};
use crate::automata::interval::{coalesce, TInterval, POS_INF};
use crate::automata::{entails, materialize, materialize_omega, transducer};
use super::{make_dict, enc, c, v};

fn iv(s: i64, e: i64) -> TInterval { TInterval::closed(s, e) }
fn op(s: u64, e: u64) -> Interval { Interval { start: s, end: e } }

/// Diamond dilates: [2,17] under Diamondminus[0,2] -> [2,19].
#[test]
fn test_diamond_transducer() {
    assert_eq!(transducer::diamond(&[iv(2, 17)], &op(0, 2)), vec![iv(2, 19)]);
}

/// Box erodes: [2,19] under Boxminus[0,5] -> [7,19]; a too-short interval drops.
#[test]
fn test_box_transducer() {
    assert_eq!(transducer::box_(&[iv(2, 19)], &op(0, 5)), vec![iv(7, 19)]);
    assert_eq!(transducer::box_(&[iv(20, 22)], &op(0, 5)), Vec::<TInterval>::new());
}

/// Future ops: Diamondplus shifts earlier; Boxplus erodes from the right.
#[test]
fn test_future_transducers() {
    // Diamondplus[0,1]: [5,10] -> [5-1, 10-0] = [4,10].
    assert_eq!(transducer::diamond_plus(&[iv(5, 10)], &op(0, 1)), vec![iv(4, 10)]);
    // Boxplus[0,2]: [5,10] -> [5-0, 10-2] = [5,8]; a too-short interval drops.
    assert_eq!(transducer::box_plus(&[iv(5, 10)], &op(0, 2)), vec![iv(5, 8)]);
    assert_eq!(transducer::box_plus(&[iv(5, 6)], &op(0, 2)), Vec::<TInterval>::new());
    // A(a)@[0,20] Until[1,2] B(a)@[5,5]  ->  [3,4].
    assert_eq!(transducer::until(&[iv(0, 20)], &[iv(5, 5)], &op(1, 2)), vec![iv(3, 4)]);
}

/// End-to-end future op in the interval evaluator (static).
#[test]
fn test_future_materialize() {
    let dict = make_dict();
    let rdf_type = enc(&dict, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let a_pred = enc(&dict, "A");
    let b_pred = enc(&dict, "B");
    let a = enc(&dict, "a");
    let rule = DatalogMTLRule {
        id: "b".into(),
        head: (v("X"), c(rdf_type), c(b_pred)),
        body: vec![TemporalAtom::DiamondPlus {
            interval: op(0, 1),
            inner: Box::new(TemporalAtom::Base((v("X"), c(rdf_type), c(a_pred)))),
        }],
    };
    let facts = vec![(Triple { subject: a, predicate: rdf_type, object: a_pred }, iv(5, 10))];
    let db = materialize(&[rule], facts, 100);
    let b_a = Triple { subject: a, predicate: rdf_type, object: b_pred };
    assert_eq!(db.facts.get(&b_a), Some(&vec![iv(4, 10)]));
}

/// Real-line coalescing: touching closed intervals merge; integer-adjacent
/// (real-separated) ones do NOT — the ℤ-vs-ℝ fix.
#[test]
fn test_coalesce_real_line() {
    assert_eq!(coalesce(vec![iv(1, 5), iv(5, 8)]), vec![iv(1, 8)]);           // touch
    assert_eq!(coalesce(vec![iv(1, 5), iv(6, 8)]), vec![iv(1, 5), iv(6, 8)]); // gap (1,6)
    assert_eq!(coalesce(vec![iv(2, 19), iv(20, 22)]), vec![iv(2, 19), iv(20, 22)]);
}

/// End-to-end: the g28 chained-operator case that the tick engine got wrong.
/// RAC(X) :- Diamondminus[0,2] GraduateStudent(X)
/// RA(X)  :- Boxminus[0,5] RAC(X)
/// GS(g)@[2,17], GS(g)@[20,20]  =>  RAC(g)=[2,19]∪[20,22],  RA(g)=[7,19]  (MeTeoR).
#[test]
fn test_chained_operators_zvsr() {
    let dict = make_dict();
    let rdf_type = enc(&dict, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let gs = enc(&dict, "GraduateStudent");
    let rac = enc(&dict, "ResearchAssistantCandidate");
    let ra = enc(&dict, "ResearchAssistant");
    let g = enc(&dict, "g");

    let base_gs = TemporalAtom::Base((v("X"), c(rdf_type), c(gs)));
    let rule_rac = DatalogMTLRule {
        id: "rac".into(),
        head: (v("X"), c(rdf_type), c(rac)),
        body: vec![TemporalAtom::Diamond { interval: op(0, 2), inner: Box::new(base_gs) }],
    };
    let rule_ra = DatalogMTLRule {
        id: "ra".into(),
        head: (v("X"), c(rdf_type), c(ra)),
        body: vec![TemporalAtom::Box_ {
            interval: op(0, 5),
            inner: Box::new(TemporalAtom::Base((v("X"), c(rdf_type), c(rac)))),
        }],
    };

    let facts = vec![
        (Triple { subject: g, predicate: rdf_type, object: gs }, iv(2, 17)),
        (Triple { subject: g, predicate: rdf_type, object: gs }, iv(20, 20)),
    ];
    let db = materialize(&[rule_rac, rule_ra], facts, 100);

    let rac_fact = Triple { subject: g, predicate: rdf_type, object: rac };
    let ra_fact = Triple { subject: g, predicate: rdf_type, object: ra };
    assert_eq!(db.facts.get(&rac_fact), Some(&vec![iv(2, 19), iv(20, 22)]));
    assert_eq!(db.facts.get(&ra_fact), Some(&vec![iv(7, 19)]),
        "RA must be [7,19] (real-time), not [7,22] (integer-adjacency)");
}

/// A binary temporal join: Near(X,Y) :- Loc(X,L), Diamondminus[1,5] Loc(Y,L).
#[test]
fn test_binary_temporal_join() {
    let dict = make_dict();
    let loc = enc(&dict, "Loc");
    let near = enc(&dict, "Near");
    let a = enc(&dict, "a");
    let b = enc(&dict, "b");
    let r1 = enc(&dict, "r1");

    let rule = DatalogMTLRule {
        id: "near".into(),
        head: (v("X"), c(near), v("Y")),
        body: vec![
            TemporalAtom::Base((v("X"), c(loc), v("L"))),
            TemporalAtom::Diamond {
                interval: op(1, 5),
                inner: Box::new(TemporalAtom::Base((v("Y"), c(loc), v("L")))),
            },
        ],
    };
    // a at r1 over [3,3]; b at r1 over [0,0]. Near(a,b) holds where Loc(a,r1) AND
    // Diamond[1,5]Loc(b,r1): b's [0,0] dilates to [1,5]; intersect [3,3] -> [3,3].
    let facts = vec![
        (Triple { subject: a, predicate: loc, object: r1 }, iv(3, 3)),
        (Triple { subject: b, predicate: loc, object: r1 }, iv(0, 0)),
    ];
    let db = materialize(&[rule], facts, 100);
    let near_ab = Triple { subject: a, predicate: near, object: b };
    assert_eq!(db.facts.get(&near_ab), Some(&vec![iv(3, 3)]));
}

/// ω: an eventually-always recursion saturates to `[l, +∞)` and answers
/// far-future entailment without materializing to that time.
/// Alarm(X) :- Trigger(X)
/// Alarm(X) :- Diamondminus[0,1] Alarm(X)   (overlapping copies -> continuous)
#[test]
fn test_omega_eventually_always() {
    let dict = make_dict();
    let rdf_type = enc(&dict, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let alarm = enc(&dict, "Alarm");
    let trigger = enc(&dict, "Trigger");
    let a = enc(&dict, "a");

    let seed = DatalogMTLRule {
        id: "seed".into(),
        head: (v("X"), c(rdf_type), c(alarm)),
        body: vec![TemporalAtom::Base((v("X"), c(rdf_type), c(trigger)))],
    };
    let propagate = DatalogMTLRule {
        id: "propagate".into(),
        head: (v("X"), c(rdf_type), c(alarm)),
        body: vec![TemporalAtom::Diamond {
            interval: op(0, 1),
            inner: Box::new(TemporalAtom::Base((v("X"), c(rdf_type), c(alarm)))),
        }],
    };

    let facts = vec![(Triple { subject: a, predicate: rdf_type, object: trigger }, iv(5, 5))];
    let model = materialize_omega(&[seed, propagate], facts).expect("eventually-always is supported");

    let alarm_a = Triple { subject: a, predicate: rdf_type, object: alarm };
    assert_eq!(
        model.db.facts.get(&alarm_a),
        Some(&vec![TInterval::from_to_inf(5, false)]),
        "Alarm(a) must saturate to [5, +∞)",
    );
    // Far-future entailment, answered from the lasso representation.
    assert!(entails(&model, &alarm_a, iv(1_000_000, 1_000_000)));
    assert!(entails(&model, &alarm_a, iv(5, 1_000_000)));   // whole span (continuous)
    assert!(entails(&model, &alarm_a, iv(5, 5)));
    assert!(!entails(&model, &alarm_a, iv(4, 4)));
    assert_eq!(model.db.facts[&alarm_a][0].end, POS_INF);
}

/// ω: a genuinely periodic (gappy) program is *extracted* (prefix + period) and
/// answers far-future entailment by extrapolation.
/// P(X) :- Seed(X);  P(X) :- Diamondminus[2,2] P(X)   (holds at even ticks, forever)
#[test]
fn test_omega_periodic_extracted() {
    let dict = make_dict();
    let rdf_type = enc(&dict, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let p = enc(&dict, "P");
    let seed_pred = enc(&dict, "Seed");
    let a = enc(&dict, "a");

    let seed = DatalogMTLRule {
        id: "seed".into(),
        head: (v("X"), c(rdf_type), c(p)),
        body: vec![TemporalAtom::Base((v("X"), c(rdf_type), c(seed_pred)))],
    };
    let step2 = DatalogMTLRule {
        id: "step2".into(),
        head: (v("X"), c(rdf_type), c(p)),
        body: vec![TemporalAtom::Diamond {
            interval: op(2, 2),
            inner: Box::new(TemporalAtom::Base((v("X"), c(rdf_type), c(p)))),
        }],
    };

    let facts = vec![(Triple { subject: a, predicate: rdf_type, object: seed_pred }, iv(0, 0))];
    let model = materialize_omega(&[seed, step2], facts).expect("periodic extraction");
    assert!(model.right_periodic && model.period == 2);

    let p_a = Triple { subject: a, predicate: rdf_type, object: p };
    // Holds at even ticks forever, not odd ones, and never over a 2-wide span.
    assert!(entails(&model, &p_a, iv(1000, 1000)));
    assert!(entails(&model, &p_a, iv(1_000_000, 1_000_000)));
    assert!(!entails(&model, &p_a, iv(1001, 1001)));
    assert!(!entails(&model, &p_a, iv(999, 999)));
    assert!(!entails(&model, &p_a, iv(1000, 1001)), "no continuous coverage across the gap");
}

/// ω + future: backward recursion gives a LEFT (past) period.
/// P(X) :- Seed(X);  P(X) :- Diamondplus[2,2] P(X)   (holds at even ticks toward −∞)
#[test]
fn test_omega_left_periodic() {
    let dict = make_dict();
    let rdf_type = enc(&dict, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let p = enc(&dict, "P");
    let seed_pred = enc(&dict, "Seed");
    let a = enc(&dict, "a");

    let seed = DatalogMTLRule {
        id: "seed".into(),
        head: (v("X"), c(rdf_type), c(p)),
        body: vec![TemporalAtom::Base((v("X"), c(rdf_type), c(seed_pred)))],
    };
    let back2 = DatalogMTLRule {
        id: "back2".into(),
        head: (v("X"), c(rdf_type), c(p)),
        body: vec![TemporalAtom::DiamondPlus {
            interval: op(2, 2),
            inner: Box::new(TemporalAtom::Base((v("X"), c(rdf_type), c(p)))),
        }],
    };

    let facts = vec![(Triple { subject: a, predicate: rdf_type, object: seed_pred }, iv(0, 0))];
    let model = materialize_omega(&[seed, back2], facts).expect("left periodic extraction");
    assert!(model.left_periodic && model.period == 2);

    let p_a = Triple { subject: a, predicate: rdf_type, object: p };
    assert!(entails(&model, &p_a, iv(-1000, -1000)));      // even → holds forever into the past
    assert!(entails(&model, &p_a, iv(-1_000_000, -1_000_000)));
    assert!(!entails(&model, &p_a, iv(-1001, -1001)));      // odd → never
    assert!(!entails(&model, &p_a, iv(-1000, -999)), "no continuous coverage across the gap");
}

/// ω + future: overlapping backward recursion saturates to `(−∞, e]`.
/// Q(X) :- Seed(X);  Q(X) :- Diamondplus[0,1] Q(X)
#[test]
fn test_omega_past_always() {
    let dict = make_dict();
    let rdf_type = enc(&dict, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let q = enc(&dict, "Q");
    let seed_pred = enc(&dict, "Seed");
    let a = enc(&dict, "a");

    let seed = DatalogMTLRule {
        id: "seed".into(),
        head: (v("X"), c(rdf_type), c(q)),
        body: vec![TemporalAtom::Base((v("X"), c(rdf_type), c(seed_pred)))],
    };
    let back = DatalogMTLRule {
        id: "back".into(),
        head: (v("X"), c(rdf_type), c(q)),
        body: vec![TemporalAtom::DiamondPlus {
            interval: op(0, 1),
            inner: Box::new(TemporalAtom::Base((v("X"), c(rdf_type), c(q)))),
        }],
    };

    let facts = vec![(Triple { subject: a, predicate: rdf_type, object: seed_pred }, iv(0, 0))];
    let model = materialize_omega(&[seed, back], facts).expect("past-always");
    let q_a = Triple { subject: a, predicate: rdf_type, object: q };
    assert_eq!(model.db.facts.get(&q_a), Some(&vec![TInterval::from_neg_inf(0, false)]),
        "Q must saturate to (−∞, 0]");
    assert!(entails(&model, &q_a, iv(-1_000_000, 0)));   // whole past span
    assert!(entails(&model, &q_a, iv(-1_000_000, -1_000_000)));
    assert!(!entails(&model, &q_a, iv(1, 1)));            // nothing in the future
}

/// ω over a finite program agrees with the finite evaluator (generalization check).
#[test]
fn test_omega_finite_agrees() {
    let dict = make_dict();
    let rdf_type = enc(&dict, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let b_pred = enc(&dict, "B");
    let a_pred = enc(&dict, "A");
    let a = enc(&dict, "a");

    let rule = DatalogMTLRule {
        id: "b".into(),
        head: (v("X"), c(rdf_type), c(b_pred)),
        body: vec![TemporalAtom::Diamond {
            interval: op(1, 5),
            inner: Box::new(TemporalAtom::Base((v("X"), c(rdf_type), c(a_pred)))),
        }],
    };
    let facts = vec![(Triple { subject: a, predicate: rdf_type, object: a_pred }, iv(0, 0))];
    let model = materialize_omega(&[rule], facts).expect("finite program");
    let b_a = Triple { subject: a, predicate: rdf_type, object: b_pred };
    assert_eq!(model.db.facts.get(&b_a), Some(&vec![iv(1, 5)]));
}
