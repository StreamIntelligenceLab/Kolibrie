/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Where temporal facts live, and the trait the evaluator reads them through.
//!
//! Two implementations of [`TemporalStore`], differing in representation rather
//! than in what they answer:
//!
//! - [`TemporalSnapshotStore`] keeps one index per timestamp, so memory grows with
//!   the number of distinct timestamps held.
//! - [`IntervalFactStore`] keeps a validity-interval list per distinct fact and
//!   extends the last interval when the same fact arrives at the next millisecond,
//!   so memory grows with the number of contiguous segments. On a sampled stream,
//!   where a reading repeats until it changes, that is far fewer.
//!
//! Both are bounded: facts older than the `horizon` are dropped by
//! [`evict`](TemporalStore::evict), which the evaluator calls each tick. A query
//! about an evicted time does not fail, it simply finds nothing, so the horizon
//! has to exceed the furthest any rule reaches back.

use std::collections::{BTreeMap, HashMap, HashSet};
use shared::dataset_index::DatasetIndex;
use shared::triple::Triple;
use shared::terms::TriplePattern;
use shared::pattern::matches_rule_pattern;

/// The stable store interface. Both Phase 1 and Phase 2 implement this.
/// Define as a trait so the evaluator is generic over the store.
pub trait TemporalStore {
    /// Records that `triple` holds at `t`. Inserting the same fact twice at the
    /// same time is not an error.
    fn insert(&mut self, triple: &Triple, t: u64);
    /// Bindings for `pattern` from the facts holding at exactly `t`.
    ///
    /// A time with no facts and a time already evicted are indistinguishable:
    /// both answer with nothing.
    fn query_at(&self, pattern: &TriplePattern, t: u64) -> Vec<HashMap<String, u32>>;
    /// Timestamps in `lo..=hi` that hold at least one fact, ascending. The
    /// bounds are inclusive, and the result skips times where nothing happened,
    /// which is what lets an existential operator scan only occupied points.
    fn timestamps_in(&self, lo: u64, hi: u64) -> Vec<u64>;
    /// Discards everything strictly before `cutoff`. Irreversible: a later query
    /// about a dropped time simply finds nothing.
    fn evict(&mut self, cutoff: u64);
    /// Distinct timestamps currently held. Diagnostic only.
    fn snapshot_count(&self) -> usize;
    /// Facts currently held, summed over all timestamps. Diagnostic only, and
    /// not necessarily cheap.
    fn total_triple_count(&self) -> usize;
}

// ────────────────────────────────────────────────────────────────
// Phase 1: TemporalSnapshotStore
// ────────────────────────────────────────────────────────────────

/// Phase 1: one DatasetIndex per timestamp. O(w * |facts|) memory.
pub struct TemporalSnapshotStore {
    snapshots: BTreeMap<u64, DatasetIndex>,
    /// How far back to keep facts. Advisory: this store does not evict on its
    /// own, so the evaluator decides the cutoff and calls
    /// [`evict`](TemporalStore::evict).
    pub horizon: u64,
}


// Summarised rather than derived: one index per timestamp, so the derived form would
// print every fact at every time point. Counting snapshots is O(1); counting the
// facts in them is not, and a `Debug` impl should not be the expensive call in a log
// line.
impl std::fmt::Debug for TemporalSnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporalSnapshotStore")
            .field("snapshots", &self.snapshots.len())
            .field("horizon", &self.horizon)
            .finish_non_exhaustive()
    }
}

impl TemporalSnapshotStore {
    /// An empty store keeping facts for `horizon` time units.
    pub fn new(horizon: u64) -> Self {
        Self { snapshots: BTreeMap::new(), horizon }
    }
}

impl TemporalStore for TemporalSnapshotStore {
    fn insert(&mut self, triple: &Triple, t: u64) {
        self.snapshots.entry(t).or_insert_with(DatasetIndex::new).insert(triple);
    }

    fn query_at(&self, pattern: &TriplePattern, t: u64) -> Vec<HashMap<String, u32>> {
        // Use the index to fetch only triples whose constant positions match the
        // pattern, instead of scanning every fact at t.
        let facts = self.snapshots.get(&t)
            .map(|idx| idx.get_matching_triples(pattern))
            .unwrap_or_default();
        let mut results = Vec::new();
        for fact in &facts {
            let mut bindings = HashMap::new();
            if matches_rule_pattern(pattern, fact, &mut bindings) {
                results.push(bindings);
            }
        }
        results
    }

    fn timestamps_in(&self, lo: u64, hi: u64) -> Vec<u64> {
        self.snapshots.range(lo..=hi).map(|(t, _)| *t).collect()
    }

    fn evict(&mut self, cutoff: u64) {
        self.snapshots.retain(|t, _| *t >= cutoff);
    }

    fn snapshot_count(&self) -> usize { self.snapshots.len() }

    fn total_triple_count(&self) -> usize {
        self.snapshots.values()
            .map(|idx| idx.query(None, None, None).len())
            .sum()
    }
}

// ────────────────────────────────────────────────────────────────
// Phase 2: IntervalFactStore
// ────────────────────────────────────────────────────────────────

/// A closed validity interval [start, end] in absolute milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidityInterval {
    /// First time the fact holds, inclusive.
    pub start: u64,
    /// Last time it holds, inclusive. A single-instant fact has `end == start`.
    pub end: u64,
}

impl ValidityInterval {
    /// Whether the fact holds at `t`. Both bounds count.
    pub fn contains(&self, t: u64) -> bool {
        t >= self.start && t <= self.end
    }

    /// Whether the fact holds anywhere in the closed range `lo..=hi`. Touching
    /// at a single endpoint counts as overlapping.
    pub fn overlaps(&self, lo: u64, hi: u64) -> bool {
        self.start <= hi && self.end >= lo
    }
}

/// Phase 2: interval-valued fact store. O(k * |distinct facts|) memory,
/// where k = number of contiguous interval segments (k << w on persistent streams).
pub struct IntervalFactStore {
    /// For each distinct triple, a sorted list of non-overlapping validity intervals.
    facts: HashMap<Triple, Vec<ValidityInterval>>,
    /// Current tick's index, rebuilt on each advance() for Base atom joins.
    current_index: DatasetIndex,
    current_tick: u64,
    /// How far back to keep facts, with the same advisory meaning as on
    /// [`TemporalSnapshotStore::horizon`].
    pub horizon: u64,
}


// Summarised rather than derived, for the same reason as
// [`TemporalSnapshotStore`]. The segment count is the interesting number here, since
// it is what this representation exists to keep small.
impl std::fmt::Debug for IntervalFactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let segments: usize = self.facts.values().map(Vec::len).sum();
        f.debug_struct("IntervalFactStore")
            .field("facts", &self.facts.len())
            .field("segments", &segments)
            .field("current_tick", &self.current_tick)
            .field("horizon", &self.horizon)
            .finish_non_exhaustive()
    }
}

impl IntervalFactStore {
    /// An empty store keeping facts for `horizon` time units.
    pub fn new(horizon: u64) -> Self {
        Self {
            facts: HashMap::new(),
            current_index: DatasetIndex::new(),
            current_tick: 0,
            horizon,
        }
    }

    /// Insert triple at time t. Extends the last interval by 1ms if contiguous
    /// (common case for step function streams), otherwise appends a new interval.
    fn insert_interval(&mut self, triple: Triple, t: u64) {
        let intervals = self.facts.entry(triple).or_insert_with(Vec::new);
        if let Some(last) = intervals.last_mut() {
            if last.end + 1 == t || last.end == t {
                last.end = t;
                return;
            }
        }
        intervals.push(ValidityInterval { start: t, end: t });
    }

}

impl TemporalStore for IntervalFactStore {
    fn insert(&mut self, triple: &Triple, t: u64) {
        self.current_tick = t;
        self.current_index.insert(triple);
        self.insert_interval(triple.clone(), t);
    }

    fn query_at(&self, pattern: &TriplePattern, t: u64) -> Vec<HashMap<String, u32>> {
        // Collect all triples valid at t, then match pattern against them.
        let candidates: Vec<Triple> = self.facts.iter()
            .filter(|(_, ivs)| ivs.iter().any(|iv| iv.contains(t)))
            .map(|(triple, _)| triple.clone())
            .collect();
        let mut results = Vec::new();
        for fact in &candidates {
            let mut bindings = HashMap::new();
            if matches_rule_pattern(pattern, fact, &mut bindings) {
                results.push(bindings);
            }
        }
        results
    }

    fn timestamps_in(&self, lo: u64, hi: u64) -> Vec<u64> {
        // Collect all distinct timestamps in [lo, hi] that have at least one active fact.
        // For Phase 2 this is derived from interval endpoints, not stored directly.
        // Collect all interval boundaries within [lo, hi] and the endpoints themselves.
        let mut ts: HashSet<u64> = HashSet::new();
        for intervals in self.facts.values() {
            for iv in intervals {
                if iv.overlaps(lo, hi) {
                    // Add boundary points clamped to [lo, hi]
                    ts.insert(iv.start.max(lo));
                    ts.insert(iv.end.min(hi));
                }
            }
        }
        let mut result: Vec<u64> = ts.into_iter().filter(|t| *t >= lo && *t <= hi).collect();
        result.sort();
        result
    }

    fn evict(&mut self, cutoff: u64) {
        for intervals in self.facts.values_mut() {
            // Clip interval starts to cutoff, drop fully expired intervals.
            intervals.retain_mut(|iv| {
                if iv.end < cutoff { return false; }
                if iv.start < cutoff { iv.start = cutoff; }
                true
            });
        }
        self.facts.retain(|_, ivs| !ivs.is_empty());
        // Also clear current_index entries — we don't need to evict it separately
        // since it's rebuilt on each tick.
    }

    fn snapshot_count(&self) -> usize {
        // For Phase 2, "snapshot count" is the number of distinct interval segments.
        self.facts.values().map(|ivs| ivs.len()).sum()
    }

    fn total_triple_count(&self) -> usize {
        self.facts.len()
    }
}

