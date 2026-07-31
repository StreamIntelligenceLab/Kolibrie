/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! ω-materialization: unbounded-time reasoning via a finite **lasso**
//! (prefix + repeating period), the automata-theoretic canonical model.
//!
//! DatalogMTL minimal models are ultimately periodic. In the past-only fragment
//! recursion propagates **forward**, so beyond `R = max_data_endpoint + max_reach`
//! the model repeats with a period dividing `P = lcm` of the metric constants.
//! We therefore:
//!   1. materialize a finite prefix clipping interval ends to `H = R + 3P`;
//!   2. widen any fact holding **contiguously** over `[R, R+P]` to `[l, +∞)`
//!      (eventually-always — represented exactly, not as a period);
//!   3. **verify** the remaining pattern is `P`-periodic (two consecutive period
//!      windows agree) and keep it as a `PeriodicModel`.
//!
//! Entailment for arbitrarily far times reduces the query modulo `P` into the
//! materialized period window and checks real-interval inclusion — O(1) in the
//! query time.

use shared::triple::Triple;
use crate::syntax::{DatalogMTLRule, Interval, TemporalAtom};
use crate::evaluator::compute_w_max;
use super::interval::{coalesce, TInterval, POS_INF};
use super::eval::eval_rule;
use super::relation::Database;

/// A finitely-represented (possibly infinite, ultimately periodic) model.
pub struct PeriodicModel {
    /// Facts materialized over the prefix `[0, H]`, plus `[l, +∞)` for
    /// eventually-always facts.
    pub db: Database,
    /// Start of the periodic regime `R`.
    pub period_start: i64,
    /// Period `P` (the pattern repeats every `P` beyond `period_start`).
    pub period: i64,
    /// Materialization bound `H = R + 3P` (contains ≥2 full period windows).
    pub horizon: i64,
    /// True if a genuinely gappy period was observed (vs purely finite / always).
    pub periodic: bool,
}

/// Build the ω-model of `rules` over `facts`.
pub fn materialize_omega(
    rules: &[DatalogMTLRule],
    facts: Vec<(Triple, TInterval)>,
) -> Result<PeriodicModel, String> {
    let max_reach = compute_w_max(rules) as i64;
    let period = op_ends(rules).into_iter().filter(|&e| e > 0).fold(1i64, lcm).max(1);
    let max_x = facts.iter()
        .map(|(_, iv)| iv.end)
        .filter(|e| *e != POS_INF)
        .max()
        .unwrap_or(0);
    let r = max_x + max_reach;
    let h = r + 3 * period;
    let base = r + period; // first period window we compare from

    // 1. Finite fixpoint, clipping interval ends to `H`.
    let mut db = Database::new();
    for (t, iv) in facts {
        db.add_raw(t, iv);
    }
    db.coalesce_all();
    fixpoint(&mut db, rules, h);

    // 2. Widen eventually-always facts (contiguous over a full period window) to +∞.
    let window = TInterval::closed(r, r + period);
    let mut widened = false;
    for t in db.facts.keys().cloned().collect::<Vec<_>>() {
        let ivs = db.facts[&t].clone();
        let mut out = Vec::with_capacity(ivs.len());
        let mut changed = false;
        for iv in ivs {
            if TInterval::inclusion(window, iv) {
                out.push(TInterval::from_to_inf(iv.start, iv.start_open));
                changed = true;
            } else {
                out.push(iv);
            }
        }
        if changed {
            *db.facts.get_mut(&t).unwrap() = coalesce(out);
            widened = true;
        }
    }
    if widened {
        fixpoint(&mut db, rules, h); // propagate +∞ (still clipped, ∞ is absorbing)
    }

    // 3. Verify P-periodicity of the tail: window [base, base+P) == [base+P, base+2P).
    let mut periodic = false;
    for ivs in db.facts.values() {
        let has_tail = ivs.iter().any(|iv| iv.end != POS_INF && iv.end >= base);
        if has_tail { periodic = true; }
        for k in 0..period {
            if holds_at(ivs, base + k) != holds_at(ivs, base + period + k) {
                return Err(format!(
                    "period {} did not stabilize by {}; unexpected periodicity", period, base));
            }
        }
    }

    Ok(PeriodicModel { db, period_start: r, period, horizon: h, periodic })
}

/// Does `triple` hold over all of `query` in the ω-model (any time, incl. far future)?
pub fn entails(model: &PeriodicModel, triple: &Triple, query: TInterval) -> bool {
    let Some(ivs) = model.db.facts.get(triple) else { return false };

    // Covered directly (prefix, or an eventually-always `[l, +∞)` interval).
    if ivs.iter().any(|iv| TInterval::inclusion(query, *iv)) {
        return true;
    }
    if !model.periodic || query.end == POS_INF {
        return false;
    }

    // Far-future: reduce the query modulo P into the materialized period window.
    let base = model.period_start;
    let p = model.period;
    if query.start < base {
        return false; // straddles prefix and far tail — conservative
    }
    if query.end - query.start >= p {
        return false; // a gappy period cannot cover a full-period-wide span
    }
    let k = (query.start - base) / p;
    let reduced = query.shifted(-k * p);
    ivs.iter().any(|iv| TInterval::inclusion(reduced, *iv))
}

fn holds_at(ivs: &[TInterval], t: i64) -> bool {
    ivs.iter().any(|iv| iv.contains_point(t))
}

/// Naive interval fixpoint with derived interval ends clamped to `clip`.
fn fixpoint(db: &mut Database, rules: &[DatalogMTLRule], clip: i64) {
    const MAX_ITERS: usize = 1_000_000;
    for _ in 0..MAX_ITERS {
        let mut derived = Vec::new();
        for rule in rules {
            derived.extend(eval_rule(rule, db));
        }
        let mut changed = false;
        for (t, ints) in derived {
            let ints = clip_intervals(ints, clip);
            if !ints.is_empty() && db.add_intervals(t, &ints) {
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
}

fn clip_intervals(ints: Vec<TInterval>, h: i64) -> Vec<TInterval> {
    ints.into_iter().filter_map(|iv| {
        if iv.start > h {
            None
        } else if iv.end != POS_INF && iv.end > h {
            Some(TInterval { start: iv.start, end: h, start_open: iv.start_open, end_open: false })
        } else {
            Some(iv)
        }
    }).collect()
}

/// All operator interval end-values appearing in rule bodies.
fn op_ends(rules: &[DatalogMTLRule]) -> Vec<i64> {
    fn walk(atom: &TemporalAtom, out: &mut Vec<i64>) {
        match atom {
            TemporalAtom::Base(_) => {}
            TemporalAtom::Diamond { interval, inner }
            | TemporalAtom::Box_ { interval, inner }
            | TemporalAtom::Prev { interval, inner } => {
                out.push(interval.end as i64);
                walk(inner, out);
            }
            TemporalAtom::Since { interval, phi, psi } => {
                out.push(interval.end as i64);
                walk(phi, out);
                walk(psi, out);
            }
        }
    }
    let mut ends = Vec::new();
    for r in rules {
        for a in &r.body {
            walk(a, &mut ends);
        }
    }
    ends
}

fn gcd(a: i64, b: i64) -> i64 {
    let (mut a, mut b) = (a.abs(), b.abs());
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

fn lcm(a: i64, b: i64) -> i64 {
    if a == 0 || b == 0 { 0 } else { (a / gcd(a, b)) * b }
}
