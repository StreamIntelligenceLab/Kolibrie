/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Where temporal facts live, and the trait the evaluator reads them through

use shared::dataset_index::DatasetIndex;
use shared::pattern::matches_rule_pattern;
use shared::terms::{Bindings, TriplePattern};
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap, HashSet};

/// The stable store interface
pub trait TemporalStore {
    /// Records that `triple` holds at `t`
    fn insert(&mut self, triple: &Triple, t: u64);
    /// Bindings for `pattern` from the facts holding at exactly `t`
    fn query_at(&self, pattern: &TriplePattern, t: u64) -> Bindings;
    /// Timestamps in `lo..=hi` that hold at least one fact, ascending
    fn timestamps_in(&self, lo: u64, hi: u64) -> Vec<u64>;
    /// Discards everything strictly before `cutoff`
    fn evict(&mut self, cutoff: u64);
    /// Distinct timestamps currently held
    fn snapshot_count(&self) -> usize;
    /// Facts currently held, summed over all timestamps
    fn total_triple_count(&self) -> usize;
}

/// Phase 1: one DatasetIndex per timestamp
pub struct TemporalSnapshotStore {
    snapshots: BTreeMap<u64, DatasetIndex>,
    /// How far back to keep facts
    pub horizon: u64,
}

impl std::fmt::Debug for TemporalSnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporalSnapshotStore")
            .field("snapshots", &self.snapshots.len())
            .field("horizon", &self.horizon)
            .finish_non_exhaustive()
    }
}

impl TemporalSnapshotStore {
    /// An empty store keeping facts for `horizon` time units
    pub fn new(horizon: u64) -> Self {
        Self {
            snapshots: BTreeMap::new(),
            horizon,
        }
    }
}

impl TemporalStore for TemporalSnapshotStore {
    fn insert(&mut self, triple: &Triple, t: u64) {
        self.snapshots
            .entry(t)
            .or_insert_with(DatasetIndex::new)
            .insert(triple);
    }

    fn query_at(&self, pattern: &TriplePattern, t: u64) -> Bindings {
        let facts = self
            .snapshots
            .get(&t)
            .map(|idx| idx.get_matching_triples(pattern))
            .unwrap_or_default();
        let mut results = Bindings::empty();
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

    fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }

    fn total_triple_count(&self) -> usize {
        self.snapshots
            .values()
            .map(|idx| idx.query(None, None, None).len())
            .sum()
    }
}

/// A closed validity interval [start, end] in absolute milliseconds
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidityInterval {
    /// First time the fact holds, inclusive
    pub start: u64,
    /// Last time it holds, inclusive
    pub end: u64,
}

impl ValidityInterval {
    /// Whether the fact holds at `t`
    pub fn contains(&self, t: u64) -> bool {
        t >= self.start && t <= self.end
    }

    /// Whether the fact holds anywhere in the closed range `lo..=hi`
    pub fn overlaps(&self, lo: u64, hi: u64) -> bool {
        self.start <= hi && self.end >= lo
    }
}

/// Phase 2: interval-valued fact store
pub struct IntervalFactStore {
    facts: HashMap<Triple, Vec<ValidityInterval>>,
    current_index: DatasetIndex,
    current_tick: u64,
    /// How far back to keep facts, with the same advisory meaning as on
    /// [`TemporalSnapshotStore::horizon`]
    pub horizon: u64,
}

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
    /// An empty store keeping facts for `horizon` time units
    pub fn new(horizon: u64) -> Self {
        Self {
            facts: HashMap::new(),
            current_index: DatasetIndex::new(),
            current_tick: 0,
            horizon,
        }
    }

    /// Insert triple at time t
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

    fn query_at(&self, pattern: &TriplePattern, t: u64) -> Bindings {
        let candidates: Vec<Triple> = self
            .facts
            .iter()
            .filter(|(_, ivs)| ivs.iter().any(|iv| iv.contains(t)))
            .map(|(triple, _)| triple.clone())
            .collect();
        let mut results = Bindings::empty();
        for fact in &candidates {
            let mut bindings = HashMap::new();
            if matches_rule_pattern(pattern, fact, &mut bindings) {
                results.push(bindings);
            }
        }
        results
    }

    fn timestamps_in(&self, lo: u64, hi: u64) -> Vec<u64> {
        let mut ts: HashSet<u64> = HashSet::new();
        for intervals in self.facts.values() {
            for iv in intervals {
                if iv.overlaps(lo, hi) {
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
            intervals.retain_mut(|iv| {
                if iv.end < cutoff {
                    return false;
                }
                if iv.start < cutoff {
                    iv.start = cutoff;
                }
                true
            });
        }
        self.facts.retain(|_, ivs| !ivs.is_empty());
    }

    fn snapshot_count(&self) -> usize {
        self.facts.values().map(|ivs| ivs.len()).sum()
    }

    fn total_triple_count(&self) -> usize {
        self.facts.len()
    }
}
