/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Rational/real-line intervals with open/closed bounds — a faithful Rust port
//! of MeTeoR's `meteor_reasoner/classes/interval.py`. Endpoints are `i64` (data
//! and operator bounds are integer); ±infinity is out of scope for the finite
//! materialization milestone.
//!
//! The key difference from the tick engine is **real-line** merging: two
//! integer-adjacent-but-separated intervals like `[2,19]` and `[20,22]` are NOT
//! coalesced (there is a real gap `(19,20)`), which is exactly what makes this
//! evaluator agree with MeTeoR on chained operators.

/// Negative infinity sentinel (used only as a left endpoint, always open).
pub const NEG_INF: i64 = i64::MIN;
/// Positive infinity sentinel (used only as a right endpoint, always open).
pub const POS_INF: i64 = i64::MAX;

fn is_inf(v: i64) -> bool { v == NEG_INF || v == POS_INF }

/// A closed/open interval `[start, end]` over the rationals (integer endpoints),
/// with `NEG_INF`/`POS_INF` sentinels for unbounded tails (ω-materialization).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TInterval {
    pub start: i64,
    pub end: i64,
    pub start_open: bool,
    pub end_open: bool,
}

impl TInterval {
    /// A closed interval `[start, end]`.
    pub fn closed(start: i64, end: i64) -> Self {
        TInterval { start, end, start_open: false, end_open: false }
    }

    /// The unbounded-future interval `[start, +∞)`.
    pub fn from_to_inf(start: i64, start_open: bool) -> Self {
        TInterval { start, end: POS_INF, start_open, end_open: true }
    }

    /// The unbounded-past interval `(-∞, end]`.
    pub fn from_neg_inf(end: i64, end_open: bool) -> Self {
        TInterval { start: NEG_INF, end, start_open: true, end_open }
    }

    pub fn is_finite(&self) -> bool {
        !is_inf(self.start) && !is_inf(self.end)
    }

    /// Whether integer point `t` lies inside this interval (respecting open bounds).
    pub fn contains_point(&self, t: i64) -> bool {
        let after_start = if self.start_open { t > self.start } else { t >= self.start };
        let before_end = if self.end_open { t < self.end } else { t <= self.end };
        after_start && before_end
    }

    /// This interval shifted along the timeline by `delta` (finite endpoints only).
    pub fn shifted(&self, delta: i64) -> TInterval {
        TInterval {
            start: if is_inf(self.start) { self.start } else { self.start + delta },
            end: if is_inf(self.end) { self.end } else { self.end + delta },
            start_open: self.start_open,
            end_open: self.end_open,
        }
    }

    /// Whether the four fields form a valid non-empty interval (MeTeoR `is_valid_interval`).
    pub fn is_valid(start: i64, end: i64, start_open: bool, end_open: bool) -> bool {
        if start == end && start_open && end_open { return false; }
        if start > end { return false; }
        // Infinite endpoints must be open.
        if is_inf(start) && !start_open { return false; }
        if is_inf(end) && !end_open { return false; }
        if start == end && start_open != end_open { return false; }
        true
    }

    fn make(start: i64, end: i64, start_open: bool, end_open: bool) -> Option<TInterval> {
        if Self::is_valid(start, end, start_open, end_open) {
            Some(TInterval { start, end, start_open, end_open })
        } else {
            None
        }
    }

    /// The finite integer points covered by this interval, clamped to
    /// `[0, cap]` (an unbounded `+∞` end is treated as `cap`).
    pub fn integer_points_upto(&self, cap: u64) -> impl Iterator<Item = u64> {
        let lo = if is_inf(self.start) { 0 } else if self.start_open { self.start + 1 } else { self.start };
        let hi_raw = if self.end == POS_INF { cap as i64 } else if self.end_open { self.end - 1 } else { self.end };
        let lo = lo.max(0);
        let hi = hi_raw.min(cap as i64);
        (lo..=hi).filter(|v| *v >= 0).map(|v| v as u64)
    }

    // ── Metric shifts (MeTeoR Interval.add / sub / circle_add / circle_sub) ──
    // `v2` is always the finite operator bound; only `v1`'s endpoints may be ±∞.

    /// `v1- + v2-`, `v1+ + v2+` — Diamondminus dilation.
    pub fn add(v1: TInterval, v2: TInterval) -> TInterval {
        let start = if is_inf(v1.start) { v1.start } else { v1.start + v2.start };
        let end = if is_inf(v1.end) { v1.end } else { v1.end + v2.end };
        TInterval {
            start,
            end,
            start_open: is_inf(start) || v1.start_open || v2.start_open,
            end_open: is_inf(end) || v1.end_open || v2.end_open,
        }
    }

    /// `v1- - v2+`, `v1+ - v2-` — Diamondplus.
    pub fn sub(v1: TInterval, v2: TInterval) -> TInterval {
        let start = if is_inf(v1.start) { v1.start } else { v1.start - v2.end };
        let end = if is_inf(v1.end) { v1.end } else { v1.end - v2.start };
        TInterval {
            start,
            end,
            start_open: is_inf(start) || v1.start_open || v2.end_open,
            end_open: is_inf(end) || v1.end_open || v2.start_open,
        }
    }

    /// `v1- + v2+`, `v1+ + v2-` — Boxminus erosion (may be invalid → `None`).
    pub fn circle_add(v1: TInterval, v2: TInterval) -> Option<TInterval> {
        let start = if is_inf(v1.start) { v1.start } else { v1.start + v2.end };
        let end = if is_inf(v1.end) { v1.end } else { v1.end + v2.start };
        Self::make(
            start,
            end,
            is_inf(start) || (v1.start_open && !v2.end_open),
            is_inf(end) || (v1.end_open && !v2.start_open),
        )
    }

    /// `v1- - v2-`, `v1+ - v2+` — Boxplus.
    pub fn circle_sub(v1: TInterval, v2: TInterval) -> Option<TInterval> {
        let start = if is_inf(v1.start) { v1.start } else { v1.start - v2.start };
        let end = if is_inf(v1.end) { v1.end } else { v1.end - v2.end };
        Self::make(
            start,
            end,
            is_inf(start) || (v1.start_open && !v2.start_open),
            is_inf(end) || (v1.end_open && !v2.end_open),
        )
    }

    // ── Set operations (MeTeoR intersection / union / inclusion) ──

    /// Intersection, or `None` if disjoint.
    pub fn intersection(v1: TInterval, v2: TInterval) -> Option<TInterval> {
        let (start, start_open) = if v1.start > v2.start {
            (v1.start, v1.start_open)
        } else if v1.start < v2.start {
            (v2.start, v2.start_open)
        } else {
            (v2.start, if v1.start_open != v2.start_open { true } else { v2.start_open })
        };
        let (end, end_open) = if v1.end < v2.end {
            (v1.end, v1.end_open)
        } else if v1.end > v2.end {
            (v2.end, v2.end_open)
        } else {
            (v2.end, if v1.end_open != v2.end_open { true } else { v2.end_open })
        };
        Self::make(start, end, start_open, end_open)
    }

    /// Union of overlapping/touching intervals, or `None` if there is a real gap.
    pub fn union(v1: TInterval, v2: TInterval) -> Option<TInterval> {
        let (v1, v2) = if v1.start > v2.start { (v2, v1) } else { (v1, v2) };

        if v1.start == v2.start {
            let start_open = if v1.start_open != v2.start_open { false } else { v1.start_open };
            let (end, end_open) = Self::merge_right(v1, v2);
            return Some(TInterval { start: v1.start, end, start_open, end_open });
        }
        if v2.start == v1.end && v2.start_open && v1.end_open {
            return None; // touch only at an excluded point
        }
        if v2.start <= v1.end {
            let (end, end_open) = Self::merge_right(v1, v2);
            Some(TInterval { start: v1.start, end, start_open: v1.start_open, end_open })
        } else {
            None
        }
    }

    /// Right endpoint of a union (v1.start <= v2.start already established).
    fn merge_right(v1: TInterval, v2: TInterval) -> (i64, bool) {
        let end = v1.end.max(v2.end);
        let end_open = if v1.end == v2.end && v1.end_open != v2.end_open {
            false
        } else if v1.end == end {
            v1.end_open
        } else {
            v2.end_open
        };
        (end, end_open)
    }

    /// Is `v1` included in `v2`?
    pub fn inclusion(v1: TInterval, v2: TInterval) -> bool {
        if v1.start < v2.start || v1.end > v2.end {
            return false;
        }
        if v1.start == v2.start && v2.start_open && !v1.start_open {
            return false;
        }
        if v1.end == v2.end && v2.end_open && !v1.end_open {
            return false;
        }
        true
    }
}

/// Merge a list of intervals into sorted, disjoint canonical form (MeTeoR
/// `coalescing`): sort by `(start, start_open)`, then fold with `union`.
pub fn coalesce(mut intervals: Vec<TInterval>) -> Vec<TInterval> {
    if intervals.is_empty() {
        return intervals;
    }
    intervals.sort_by(|a, b| {
        a.start.cmp(&b.start).then(a.start_open.cmp(&b.start_open))
    });
    let mut out = Vec::with_capacity(intervals.len());
    let mut mover = intervals[0];
    for &iv in &intervals[1..] {
        match TInterval::union(mover, iv) {
            Some(u) => mover = u,
            None => { out.push(mover); mover = iv; }
        }
    }
    out.push(mover);
    out
}

/// Intersect two interval lists: all pairwise intersections, coalesced.
pub fn intersect_lists(a: &[TInterval], b: &[TInterval]) -> Vec<TInterval> {
    let mut out = Vec::new();
    for &x in a {
        for &y in b {
            if let Some(i) = TInterval::intersection(x, y) {
                out.push(i);
            }
        }
    }
    coalesce(out)
}
