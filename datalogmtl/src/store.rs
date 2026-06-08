/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap, HashSet};
use shared::index_manager::UnifiedIndex;
use shared::triple::Triple;
use shared::terms::TriplePattern;
use datalog::reasoning::matches_rule_pattern;

/// The stable store interface. Both Phase 1 and Phase 2 implement this.
/// Define as a trait so the evaluator is generic over the store.
pub trait TemporalStore {
    fn insert(&mut self, triple: &Triple, t: u64);
    fn query_at(&self, pattern: &TriplePattern, t: u64) -> Vec<HashMap<String, u32>>;
    fn timestamps_in(&self, lo: u64, hi: u64) -> Vec<u64>;
    fn evict(&mut self, cutoff: u64);
    fn snapshot_count(&self) -> usize;
    fn total_triple_count(&self) -> usize;
}

// ────────────────────────────────────────────────────────────────
// Phase 1: TemporalSnapshotStore
// ────────────────────────────────────────────────────────────────

/// Phase 1: one UnifiedIndex per timestamp. O(w * |facts|) memory.
pub struct TemporalSnapshotStore {
    snapshots: BTreeMap<u64, UnifiedIndex>,
    pub horizon: u64,
}

impl TemporalSnapshotStore {
    pub fn new(horizon: u64) -> Self {
        Self { snapshots: BTreeMap::new(), horizon }
    }
}

impl TemporalStore for TemporalSnapshotStore {
    fn insert(&mut self, triple: &Triple, t: u64) {
        self.snapshots.entry(t).or_insert_with(UnifiedIndex::new).insert(triple);
    }

    fn query_at(&self, pattern: &TriplePattern, t: u64) -> Vec<HashMap<String, u32>> {
        let facts = self.snapshots.get(&t)
            .map(|idx| idx.query(None, None, None))
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
    pub start: u64,
    pub end: u64,
}

impl ValidityInterval {
    pub fn contains(&self, t: u64) -> bool {
        t >= self.start && t <= self.end
    }

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
    current_index: UnifiedIndex,
    current_tick: u64,
    pub horizon: u64,
}

impl IntervalFactStore {
    pub fn new(horizon: u64) -> Self {
        Self {
            facts: HashMap::new(),
            current_index: UnifiedIndex::new(),
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

    /// Merge overlapping or adjacent intervals in a sorted list.
    #[allow(dead_code)]
    fn coalesce(intervals: &mut Vec<ValidityInterval>) {
        if intervals.len() < 2 { return; }
        intervals.sort_by_key(|iv| iv.start);
        let mut merged: Vec<ValidityInterval> = Vec::new();
        for iv in intervals.drain(..) {
            if let Some(last) = merged.last_mut() {
                if iv.start <= last.end + 1 {
                    last.end = last.end.max(iv.end);
                    continue;
                }
            }
            merged.push(iv);
        }
        *intervals = merged;
    }

    /// Returns true if triple holds at every timestamp in timestamps.
    /// Used for Box evaluation.
    #[allow(dead_code)]
    fn holds_at_all(&self, triple: &Triple, timestamps: &[u64]) -> bool {
        if timestamps.is_empty() { return true; } // vacuously true
        let Some(intervals) = self.facts.get(triple) else { return false; };
        timestamps.iter().all(|t| intervals.iter().any(|iv| iv.contains(*t)))
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

