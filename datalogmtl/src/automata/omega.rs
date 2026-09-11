/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! ω-materialization: unbounded-time reasoning via a finite **lasso** (prefix +
//! repeating period), the automata-theoretic canonical model.
//!
//! DatalogMTL minimal models are ultimately periodic in **both** directions.
//! Past operators propagate forward (a *right* period toward +∞); future
//! operators propagate backward (a *left* period toward −∞). Beyond
//! `R = max_data_end + reach` (right) and before `L = min_data_start − reach`
//! (left) the model repeats with a period dividing `P = lcm` of the metric
//! constants. We:
//!   1. materialize a finite prefix clipping intervals to `[Lb, H]`
//!      (`H = R+3P`, `Lb = L−3P`);
//!   2. widen contiguous saturation to `[l, +∞)` (right) or `(−∞, e]` (left);
//!   3. verify each side is `P`-periodic and keep it.
//!
//! Entailment for arbitrarily far times reduces the query modulo `P` into the
//! materialized period window (right or left) and checks real-interval inclusion.

use shared::triple::Triple;
use crate::syntax::{DatalogMTLRule, TemporalAtom};
use crate::evaluator::compute_w_max;
use super::interval::{coalesce, TInterval, NEG_INF, POS_INF};
use super::eval::eval_rule;
use super::relation::Database;

/// A finitely-represented (possibly infinite, ultimately periodic) model.
pub struct PeriodicModel {
    /// Facts materialized over the prefix `[Lb, H]`, plus `[l,+∞)` / `(−∞,e]`.
    pub db: Database,
    /// Period `P` — the pattern repeats every `P` beyond each tail.
    pub period: i64,
    /// Right (future) regime start `R` and materialization bound `H = R+3P`.
    pub right_start: i64,
    pub right_horizon: i64,
    /// A genuinely gappy right period was observed.
    pub right_periodic: bool,
    /// Left (past) regime start `L` and materialization bound `Lb = L−3P`.
    pub left_start: i64,
    pub left_horizon: i64,
    /// A genuinely gappy left period was observed.
    pub left_periodic: bool,
}

/// Build the ω-model of `rules` over `facts`.
pub fn materialize_omega(
    rules: &[DatalogMTLRule],
    facts: Vec<(Triple, TInterval)>,
) -> Result<PeriodicModel, String> {
    let reach = compute_w_max(rules) as i64;
    let period = op_ends(rules).into_iter().filter(|&e| e > 0).fold(1i64, lcm).max(1);
    let max_x = facts.iter().map(|(_, iv)| iv.end).filter(|e| *e != POS_INF).max().unwrap_or(0);
    let min_x = facts.iter().map(|(_, iv)| iv.start).filter(|s| *s != NEG_INF).min().unwrap_or(0);

    let r = max_x + reach;
    let h = r + 3 * period;   // right materialization bound
    let l = min_x - reach;
    let lb = l - 3 * period;  // left materialization bound

    // 1. Finite fixpoint, clipping interval ends/starts to `[lb, h]`.
    let mut db = Database::new();
    for (t, iv) in facts {
        db.add_raw(t, iv);
    }
    db.coalesce_all();
    fixpoint(&mut db, rules, lb, h);

    // 2. Widen contiguous saturation on each side.
    let right_win = TInterval::closed(r, r + period);
    let left_win = TInterval::closed(l - period, l);
    let mut widened = false;
    for t in db.facts.keys().cloned().collect::<Vec<_>>() {
        let ivs = db.facts[&t].clone();
        let mut out = Vec::with_capacity(ivs.len());
        let mut changed = false;
        for iv in ivs {
            if TInterval::inclusion(right_win, iv) {
                out.push(TInterval::from_to_inf(iv.start, iv.start_open));
                changed = true;
            } else if TInterval::inclusion(left_win, iv) {
                out.push(TInterval::from_neg_inf(iv.end, iv.end_open));
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
        fixpoint(&mut db, rules, lb, h); // propagate ±∞ (still clipped; ∞ is absorbing)
    }

    // 3. Verify P-periodicity of each tail.
    let right_periodic = verify_side(&db, r + period, period, Dir::Right)?;
    let left_periodic = verify_side(&db, l - period, period, Dir::Left)?;

    Ok(PeriodicModel {
        db,
        period,
        right_start: r,
        right_horizon: h,
        right_periodic,
        left_start: l,
        left_horizon: lb,
        left_periodic,
    })
}

/// Does `triple` hold over all of `query` in the ω-model (any time)?
pub fn entails(model: &PeriodicModel, triple: &Triple, query: TInterval) -> bool {
    let Some(ivs) = model.db.facts.get(triple) else { return false };

    // Covered directly (prefix, or an unbounded `[l,+∞)` / `(−∞,e]` interval).
    if ivs.iter().any(|iv| TInterval::inclusion(query, *iv)) {
        return true;
    }
    let width = if query.is_finite() { query.end - query.start } else { i64::MAX };

    // Far future: reduce modulo P into the materialized right period window.
    if model.right_periodic && query.is_finite() && query.start > model.right_horizon {
        if width >= model.period { return false; }
        let base = model.right_start + model.period;
        let k = (query.start - base) / model.period; // >= 1
        let reduced = query.shifted(-k * model.period);
        return ivs.iter().any(|iv| TInterval::inclusion(reduced, *iv));
    }
    // Far past: reduce modulo P into the materialized left period window.
    if model.left_periodic && query.is_finite() && query.end < model.left_horizon {
        if width >= model.period { return false; }
        let base = model.left_start - model.period;
        let k = (base - query.end) / model.period; // >= 1
        let reduced = query.shifted(k * model.period);
        return ivs.iter().any(|iv| TInterval::inclusion(reduced, *iv));
    }
    false
}

#[derive(Clone, Copy)]
enum Dir { Left, Right }

/// Confirm a tail is `P`-periodic (two consecutive period windows agree) and
/// report whether a genuinely gappy (finite) tail exists on that side.
fn verify_side(db: &Database, base: i64, p: i64, dir: Dir) -> Result<bool, String> {
    let mut has_tail = false;
    for ivs in db.facts.values() {
        let tail = ivs.iter().any(|iv| match dir {
            Dir::Right => iv.end != POS_INF && iv.end >= base,
            Dir::Left => iv.start != NEG_INF && iv.start <= base,
        });
        if tail { has_tail = true; }
        for k in 0..p {
            let (a, b) = match dir {
                Dir::Right => (base + k, base + p + k),
                Dir::Left => (base - k, base - p - k),
            };
            if holds_at(ivs, a) != holds_at(ivs, b) {
                return Err(format!(
                    "{} period {} did not stabilize",
                    match dir { Dir::Right => "right", Dir::Left => "left" }, p));
            }
        }
    }
    Ok(has_tail)
}

fn holds_at(ivs: &[TInterval], t: i64) -> bool {
    ivs.iter().any(|iv| iv.contains_point(t))
}

/// Naive interval fixpoint with derived interval endpoints clamped to `[lb, h]`.
fn fixpoint(db: &mut Database, rules: &[DatalogMTLRule], lb: i64, h: i64) {
    const MAX_ITERS: usize = 1_000_000;
    for _ in 0..MAX_ITERS {
        let mut derived = Vec::new();
        for rule in rules {
            derived.extend(eval_rule(rule, db));
        }
        let mut changed = false;
        for (t, ints) in derived {
            let ints = clip_intervals(ints, lb, h);
            if !ints.is_empty() && db.add_intervals(t, &ints) {
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
}

fn clip_intervals(ints: Vec<TInterval>, lb: i64, h: i64) -> Vec<TInterval> {
    ints.into_iter().filter_map(|mut iv| {
        // Drop intervals entirely outside the materialized window.
        if (iv.start != NEG_INF && iv.start > h) || (iv.end != POS_INF && iv.end < lb) {
            return None;
        }
        if iv.end != POS_INF && iv.end > h { iv.end = h; iv.end_open = false; }
        if iv.start != NEG_INF && iv.start < lb { iv.start = lb; iv.start_open = false; }
        Some(iv)
    }).collect()
}

/// All operator interval end-values appearing in rule bodies.
fn op_ends(rules: &[DatalogMTLRule]) -> Vec<i64> {
    fn walk(atom: &TemporalAtom, out: &mut Vec<i64>) {
        match atom {
            TemporalAtom::Base(_) => {}
            TemporalAtom::Diamond { interval, inner }
            | TemporalAtom::Box_ { interval, inner }
            | TemporalAtom::Prev { interval, inner }
            | TemporalAtom::DiamondPlus { interval, inner }
            | TemporalAtom::BoxPlus { interval, inner } => {
                out.push(interval.end as i64);
                walk(inner, out);
            }
            TemporalAtom::Since { interval, phi, psi }
            | TemporalAtom::Until { interval, phi, psi } => {
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
