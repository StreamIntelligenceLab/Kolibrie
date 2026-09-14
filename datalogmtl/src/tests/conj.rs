/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Conjunction scoped under a temporal operator (`TemporalAtom::Conj`).

use crate::evaluator::DatalogMTLEvaluator;
use crate::rdf_parser::parse_rules;
use crate::store::TemporalSnapshotStore;
use crate::syntax::Mode;
use shared::dictionary::Dictionary;
use std::sync::{Arc, RwLock};

use super::{enc, make_dict, triple};

/// The zone-transition rule: in one restricted zone for 10 consecutive ticks,
/// having been in a *differently* classified zone 10–20 ticks before that.
const TRANSITION_RULE: &str = r#"
PREFIX d: <http://x/>

[test]
(?dr, d:test, ?z) :-
    Box[0,10]((?dr, d:inZone, ?z), (?z, d:status, d:Restricted2)),
    Diamond[10,20]((?dr, d:inZone, ?z2), (?z2, d:status, d:Restricted1)).
"#;

struct Fixture {
    dict: Arc<RwLock<Dictionary>>,
    zone_a: u32,
    zone_b: u32,
    in_zone: u32,
    status: u32,
    r1: u32,
    r2: u32,
    drone: u32,
    test_pred: u32,
}

fn fixture() -> Fixture {
    let dict = make_dict();
    Fixture {
        zone_a: enc(&dict, "http://x/zoneA"),
        zone_b: enc(&dict, "http://x/zoneB"),
        in_zone: enc(&dict, "http://x/inZone"),
        status: enc(&dict, "http://x/status"),
        r1: enc(&dict, "http://x/Restricted1"),
        r2: enc(&dict, "http://x/Restricted2"),
        drone: enc(&dict, "http://x/droneX"),
        test_pred: enc(&dict, "http://x/test"),
        dict,
    }
}

/// Drive the rule over a tick timeline: zoneA (Restricted1) for ticks 0..=12,
/// then zoneB (Restricted2) for ticks 13..=25. Returns the ticks at which the
/// head was derived.
fn run(rule_text: &str, f: &Fixture) -> Vec<u64> {
    let rules = {
        let mut d = f.dict.write().unwrap();
        parse_rules(rule_text, &mut d, Mode::Streaming).expect("rule should parse")
    };
    let store = TemporalSnapshotStore::new(200);
    let mut eval = DatalogMTLEvaluator::new(rules, store, f.dict.clone()).unwrap();

    let mut fired = Vec::new();
    for t in 0..=25u64 {
        // Zone classification is background data, asserted at every tick.
        let mut facts = vec![
            triple(f.zone_a, f.status, f.r1),
            triple(f.zone_b, f.status, f.r2),
        ];
        facts.push(if t <= 12 {
            triple(f.drone, f.in_zone, f.zone_a)
        } else {
            triple(f.drone, f.in_zone, f.zone_b)
        });

        let (derived, _) = eval.advance(t, facts);
        if derived.iter().any(|d| d.predicate == f.test_pred && d.object == f.zone_b) {
            fired.push(t);
        }
    }
    fired
}

/// The rule fires only once both halves are satisfied: 10 full ticks in zoneB
/// (so t >= 23) with zoneA seen 10–20 ticks earlier.
#[test]
fn test_zone_transition_rule() {
    let f = fixture();
    let fired = run(TRANSITION_RULE, &f);

    assert!(!fired.is_empty(), "the transition rule should fire");
    assert_eq!(
        *fired.first().unwrap(),
        23,
        "needs ticks 13..=23 in zoneB, and zoneA within [t-20, t-10]"
    );
    // By t=33 zoneA would fall out of the Diamond window, but the run stops at 25.
    assert!(fired.iter().all(|t| *t >= 23));
}

/// The point of scoping a conjunction under the operator: `Box[0,10](A, B)`
/// requires B at every point of the window, whereas hoisting B out only requires
/// it at the current time. Here B is `(?z, status, Restricted2)` — a fact that
/// only starts holding at tick 13 — so the two forms disagree.
#[test]
fn test_conjunction_under_box_is_stronger_than_hoisting() {
    // Classification for zoneB is withheld until tick 20, well after the drone
    // enters it at tick 13.
    let f = fixture();
    let rules = {
        let mut d = f.dict.write().unwrap();
        parse_rules(
            "PREFIX d: <http://x/>\n\
             [scoped] (?dr, d:test, ?z) :- Box[0,5]((?dr, d:inZone, ?z), (?z, d:status, d:Restricted2)).\n\
             [hoisted] (?dr, d:test2, ?z) :- Box[0,5](?dr, d:inZone, ?z), (?z, d:status, d:Restricted2).",
            &mut d,
            Mode::Streaming,
        )
        .unwrap()
    };
    let test2 = enc(&f.dict, "http://x/test2");

    let store = TemporalSnapshotStore::new(200);
    let mut eval = DatalogMTLEvaluator::new(rules, store, f.dict.clone()).unwrap();

    let mut scoped_first = None;
    let mut hoisted_first = None;
    for t in 0..=30u64 {
        let mut facts = vec![triple(f.drone, f.in_zone, f.zone_b)];
        if t >= 20 {
            facts.push(triple(f.zone_b, f.status, f.r2));
        }
        let (derived, _) = eval.advance(t, facts);
        for d in &derived {
            if d.predicate == f.test_pred && scoped_first.is_none() {
                scoped_first = Some(t);
            }
            if d.predicate == test2 && hoisted_first.is_none() {
                hoisted_first = Some(t);
            }
        }
    }

    // Hoisted: needs inZone over [t-5,t] and the class at t only -> fires at 20.
    assert_eq!(hoisted_first, Some(20), "hoisted form only needs the class now");
    // Scoped: needs the class at every point of [t-5,t] too -> fires 5 ticks later.
    assert_eq!(scoped_first, Some(25), "scoped form needs the class all window");
}

/// Nested operators read additively, and `compute_w_max` must say so — it drives
/// store eviction, and under-reporting it silently starves the rule of history.
#[test]
fn test_w_max_is_additive_for_nested_operators() {
    use crate::evaluator::compute_w_max;

    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "PREFIX d: <http://x/>\n\
         # reads back 20 + 20 + 5 = 45\n\
         (?a, d:out, ?z) :- Diamond[6,20](Box[0,5]((?a, d:in, ?z), (?z, d:st, d:R1)),\n\
                                          Diamond[6,20](Box[0,5](?a, d:in, ?z2))).\n\
         # a flat rule is unaffected: still 30\n\
         (?a, d:flat, ?z) :- Box[0,30](?a, d:in, ?z).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();

    assert_eq!(compute_w_max(&rules[..1]), 45, "nested reach must sum");
    assert_eq!(compute_w_max(&rules[1..]), 30, "flat rules are unchanged");
}
