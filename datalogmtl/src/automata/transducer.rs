/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Per-operator interval-list transducers — the batch I/O behaviour of each
//! temporal operator's automaton. Faithful port of MeTeoR
//! `meteor_reasoner/materialization/apply.py`. Each takes the inner atom's
//! interval list and returns the operator's output interval list (coalesced).

use crate::syntax::Interval as OpInterval;
use super::interval::{coalesce, TInterval};

/// The metric bound `[a,b]` of an operator as a closed rational interval.
fn bound(op: &OpInterval) -> TInterval {
    TInterval::closed(op.start as i64, op.end as i64)
}

/// Diamondminus[a,b]: dilate each interval `[s,e] -> [s+a, e+b]`.
pub fn diamond(inner: &[TInterval], op: &OpInterval) -> Vec<TInterval> {
    let b = bound(op);
    coalesce(inner.iter().map(|&iv| TInterval::add(iv, b)).collect())
}

/// Boxminus[a,b]: erode each interval `[s,e] -> [s+b, e+a]`, dropping invalid.
pub fn box_(inner: &[TInterval], op: &OpInterval) -> Vec<TInterval> {
    let b = bound(op);
    coalesce(inner.iter().filter_map(|&iv| TInterval::circle_add(iv, b)).collect())
}

/// phi Since[a,b] psi (MeTeoR `since_deduce`), combined over all interval pairs.
pub fn since(phi: &[TInterval], psi: &[TInterval], op: &OpInterval) -> Vec<TInterval> {
    let b = bound(op);
    let mut out = Vec::new();
    for &p in phi {
        for &q in psi {
            if let Some(r) = since_deduce(&b, p, q) {
                out.push(r);
            }
        }
    }
    coalesce(out)
}

/// One (phi_interval, psi_interval) pair of the Since operator.
fn since_deduce(op: &TInterval, phi: TInterval, psi: TInterval) -> Option<TInterval> {
    // Special case S_[0,b]: MeTeoR returns psi shifted by the operator bound.
    if !op.start_open && op.start == 0 {
        return Some(TInterval::add(psi, *op));
    }
    let closed_phi = TInterval::closed(phi.start, phi.end);
    let s = TInterval::intersection(closed_phi, psi)?;
    let t = TInterval::add(s, *op);
    TInterval::intersection(t, closed_phi)
}

// ── Future operators (static data only) ──

/// Diamondplus[a,b]: `[s,e] -> [s-b, e-a]`.
pub fn diamond_plus(inner: &[TInterval], op: &OpInterval) -> Vec<TInterval> {
    let b = bound(op);
    coalesce(inner.iter().map(|&iv| TInterval::sub(iv, b)).collect())
}

/// Boxplus[a,b]: `[s,e] -> [s-a, e-b]`, dropping invalid (future erosion).
pub fn box_plus(inner: &[TInterval], op: &OpInterval) -> Vec<TInterval> {
    let b = bound(op);
    coalesce(inner.iter().filter_map(|&iv| TInterval::circle_sub(iv, b)).collect())
}

/// phi Until[a,b] psi (MeTeoR `until_deduce`), combined over all interval pairs.
pub fn until(phi: &[TInterval], psi: &[TInterval], op: &OpInterval) -> Vec<TInterval> {
    let b = bound(op);
    let mut out = Vec::new();
    for &p in phi {
        for &q in psi {
            if let Some(r) = until_deduce(&b, p, q) {
                out.push(r);
            }
        }
    }
    coalesce(out)
}

/// One (phi_interval, psi_interval) pair of the Until operator (mirror of Since with `sub`).
fn until_deduce(op: &TInterval, phi: TInterval, psi: TInterval) -> Option<TInterval> {
    if !op.start_open && op.start == 0 {
        return Some(TInterval::sub(psi, *op));
    }
    let closed_phi = TInterval::closed(phi.start, phi.end);
    let s = TInterval::intersection(closed_phi, psi)?;
    let t = TInterval::sub(s, *op);
    TInterval::intersection(t, closed_phi)
}
