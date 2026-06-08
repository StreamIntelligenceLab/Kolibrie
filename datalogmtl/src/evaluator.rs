/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use shared::dictionary::Dictionary;
use shared::triple::Triple;
use shared::terms::{Term, TriplePattern};
use datalog::reasoning::{matches_rule_pattern, construct_triple};
use crate::syntax::{DatalogMTLRule, TemporalAtom, Interval};
use crate::store::TemporalStore;
use crate::metrics::TickMetrics;
use crate::validate::validate_rules;

pub struct DatalogMTLEvaluator<S: TemporalStore> {
    pub rules:      Vec<DatalogMTLRule>,
    pub store:      S,
    pub dictionary: Arc<RwLock<Dictionary>>,
    /// Maximum interval width across all rules. Used for eviction cutoff.
    w_max:          u64,
}

impl<S: TemporalStore> DatalogMTLEvaluator<S> {
    pub fn new(
        rules: Vec<DatalogMTLRule>,
        store: S,
        dictionary: Arc<RwLock<Dictionary>>,
    ) -> Result<Self, String> {
        validate_rules(&rules)?;
        let w_max = compute_w_max(&rules);
        Ok(Self { rules, store, dictionary, w_max })
    }

    /// Advance to time t, ingesting new_triples into the store.
    /// Returns all newly derived triples and per-tick metrics.
    pub fn advance(
        &mut self,
        t: u64,
        new_triples: Vec<Triple>,
    ) -> (Vec<Triple>, TickMetrics) {
        let start = Instant::now();
        let mut metrics = TickMetrics::new(t);

        // 1. Ingest stream triples at t.
        for triple in &new_triples {
            self.store.insert(triple, t);
        }

        // 2. Bottom-up fixpoint: derive new facts at t.
        let mut all_derived: HashSet<Triple> = HashSet::new();
        loop {
            let mut new_this_round = Vec::new();
            for rule in &self.rules.clone() {
                let bindings = self.evaluate_rule(rule, t, &mut metrics);
                metrics.rules_fired += bindings.len();
                for binding in bindings {
                    let mut dict = self.dictionary.write().unwrap();
                    let derived = construct_triple(&rule.head, &binding, &mut dict);
                    drop(dict);
                    if !all_derived.contains(&derived) {
                        all_derived.insert(derived.clone());
                        new_this_round.push(derived.clone());
                        self.store.insert(&derived, t);
                    }
                }
            }
            metrics.fixpoint_iterations += 1;
            if new_this_round.is_empty() { break; }
            metrics.new_triples += new_this_round.len();
        }

        // 3. Evict snapshots older than w_max.
        let cutoff = t.saturating_sub(self.w_max);
        self.store.evict(cutoff);

        metrics.snapshot_count = self.store.snapshot_count();
        metrics.total_triples_in_store = self.store.total_triple_count();
        metrics.eval_time_us = start.elapsed().as_micros() as u64;

        (all_derived.into_iter().collect(), metrics)
    }

    /// Evaluate one rule at time t. Returns all satisfying variable bindings.
    fn evaluate_rule(
        &self,
        rule: &DatalogMTLRule,
        t: u64,
        metrics: &mut TickMetrics,
    ) -> Vec<HashMap<String, u32>> {
        // Step 1: seed bindings from all Base atoms at current snapshot.
        let base_atoms: Vec<_> = rule.body.iter()
            .filter_map(|a| if let TemporalAtom::Base(p) = a { Some(p) } else { None })
            .collect();

        let wildcard: TriplePattern = (
            Term::Variable("_s".into()),
            Term::Variable("_p".into()),
            Term::Variable("_o".into()),
        );
        let current_facts: HashSet<Triple> = self.store
            .query_at(&wildcard, t)
            .into_iter()
            .filter_map(|b| {
                let s = b.get("_s").copied()?;
                let p = b.get("_p").copied()?;
                let o = b.get("_o").copied()?;
                Some(Triple { subject: s, predicate: p, object: o })
            })
            .collect();

        let mut bindings: Vec<HashMap<String, u32>> = if base_atoms.is_empty() {
            vec![HashMap::new()]
        } else {
            let mut results = Vec::new();
            for fact in &current_facts {
                let mut b = HashMap::new();
                if matches_rule_pattern(base_atoms[0], fact, &mut b) {
                    if base_atoms.len() == 1 {
                        results.push(b);
                    } else {
                        let joined = join_base_atoms(&base_atoms[1..], &current_facts, b);
                        results.extend(joined);
                    }
                }
            }
            results
        };

        // Step 2: for each temporal atom, filter/extend bindings.
        for atom in &rule.body {
            match atom {
                TemporalAtom::Base(_) => {} // already handled above
                TemporalAtom::Diamond { interval, inner } => {
                    metrics.diamond_evals += 1;
                    bindings = self.eval_diamond(interval, inner, t, &bindings);
                }
                TemporalAtom::Box_ { interval, inner } => {
                    metrics.box_evals += 1;
                    bindings = self.eval_box(interval, inner, t, &bindings);
                }
                TemporalAtom::Prev { interval, inner } => {
                    bindings = self.eval_prev(interval, inner, t, &bindings);
                }
                TemporalAtom::Since { interval, phi, psi } => {
                    metrics.since_evals += 1;
                    let (new_bindings, depth) =
                        self.eval_since(interval, phi, psi, t, &bindings);
                    bindings = new_bindings;
                    metrics.since_scan_depth += depth;
                }
            }
            if bindings.is_empty() { break; }
        }
        bindings
    }

    // --- Temporal operator implementations ---

    /// Diamond[a,b]: phi must hold at SOME t' in [t-b, t-a].
    fn eval_diamond(
        &self,
        interval: &Interval,
        inner: &TemporalAtom,
        t: u64,
        bindings: &[HashMap<String, u32>],
    ) -> Vec<HashMap<String, u32>> {
        if t < interval.start { return vec![]; }
        let (lo, hi) = interval.absolute_range(t);
        let timestamps = self.store.timestamps_in(lo, hi);
        let mut results = Vec::new();
        for binding in bindings {
            for &t_prime in &timestamps {
                let inner_bindings =
                    self.eval_atom_at(inner, t_prime, &[binding.clone()]);
                results.extend(inner_bindings);
            }
        }
        results
    }

    /// Box[a,b]: phi must hold at EVERY t' in [t-b, t-a].
    /// Vacuously true if no timestamps exist in the range.
    /// Collects candidate bindings from the first timestamp, then filters
    /// them against every subsequent timestamp — so variables introduced
    /// solely inside Box (e.g. Box[0,10000](?x :sensor ?v)) are still grounded.
    fn eval_box(
        &self,
        interval: &Interval,
        inner: &TemporalAtom,
        t: u64,
        bindings: &[HashMap<String, u32>],
    ) -> Vec<HashMap<String, u32>> {
        if t < interval.start { return vec![]; }
        let (lo, hi) = interval.absolute_range(t);
        let timestamps = self.store.timestamps_in(lo, hi);
        if timestamps.is_empty() {
            return vec![];
        }
        let mut results = Vec::new();
        'outer: for binding in bindings {
            // Seed candidates from the first timestamp.
            let mut candidates =
                self.eval_atom_at(inner, timestamps[0], &[binding.clone()]);
            if candidates.is_empty() { continue 'outer; }

            // Filter candidates against every subsequent timestamp.
            for &t_prime in &timestamps[1..] {
                let mut surviving = Vec::new();
                for candidate in candidates {
                    let check = self.eval_atom_at(inner, t_prime, &[candidate.clone()]);
                    if !check.is_empty() { surviving.push(candidate); }
                }
                candidates = surviving;
                if candidates.is_empty() { continue 'outer; }
            }
            results.extend(candidates);
        }
        results
    }

    /// Prev[a,b]: phi holds at the MOST RECENT t' in [t-b, t-a].
    fn eval_prev(
        &self,
        interval: &Interval,
        inner: &TemporalAtom,
        t: u64,
        bindings: &[HashMap<String, u32>],
    ) -> Vec<HashMap<String, u32>> {
        if t < interval.start { return vec![]; }
        let (lo, hi) = interval.absolute_range(t);
        let timestamps = self.store.timestamps_in(lo, hi);
        let Some(&t_prev) = timestamps.last() else {
            return Vec::new();
        };
        let mut results = Vec::new();
        for binding in bindings {
            let inner_results =
                self.eval_atom_at(inner, t_prev, &[binding.clone()]);
            results.extend(inner_results);
        }
        results
    }

    /// phi Since[a,b] psi:
    ///   EXISTS t' in [t-b, t-a]: psi holds at t'
    ///   AND FORALL t'' in (t', t]: phi holds at t''
    /// Returns (bindings, total_scan_depth).
    fn eval_since(
        &self,
        interval: &Interval,
        phi: &TemporalAtom,
        psi: &TemporalAtom,
        t: u64,
        bindings: &[HashMap<String, u32>],
    ) -> (Vec<HashMap<String, u32>>, usize) {
        let (lo, hi) = interval.absolute_range(t);
        let since_timestamps = self.store.timestamps_in(lo, hi);
        // Guard: if hi >= t, there are no timestamps strictly between hi and t.
        let cont_timestamps = if hi < t {
            self.store.timestamps_in(hi + 1, t)
        } else {
            Vec::new()
        };
        let mut results = Vec::new();
        let mut scan_depth = 0;

        for binding in bindings {
            'reset: for &t_prime in since_timestamps.iter().rev() {
                scan_depth += 1;
                let psi_results =
                    self.eval_atom_at(psi, t_prime, &[binding.clone()]);
                if psi_results.is_empty() { continue; }

                // FORALL t'' in (t', t]: use active timestamps PLUS the current
                // evaluation time t itself (closed-world: no event at t means phi
                // must explicitly hold there, or it fails).
                let mut after_set: std::collections::HashSet<u64> = cont_timestamps.iter()
                    .chain(since_timestamps.iter())
                    .filter(|&&ts| ts > t_prime && ts <= t)
                    .copied()
                    .collect();
                // Always include t in the continuation check.
                if t > t_prime { after_set.insert(t); }
                let mut after_ts: Vec<u64> = after_set.into_iter().collect();
                after_ts.sort();

                for &t_pp in &after_ts {
                    scan_depth += 1;
                    let phi_results =
                        self.eval_atom_at(phi, t_pp, &[binding.clone()]);
                    if phi_results.is_empty() { continue 'reset; }
                }
                results.extend(psi_results);
                break 'reset; // existential: first valid reset point is enough
            }
        }
        (results, scan_depth)
    }

    /// Evaluate a single TemporalAtom at a fixed time point t_prime,
    /// starting from the given partial bindings.
    fn eval_atom_at(
        &self,
        atom: &TemporalAtom,
        t_prime: u64,
        bindings: &[HashMap<String, u32>],
    ) -> Vec<HashMap<String, u32>> {
        match atom {
            TemporalAtom::Base(pattern) => {
                let mut results = Vec::new();
                for binding in bindings {
                    let candidates = self.store.query_at(pattern, t_prime);
                    for candidate in candidates {
                        let mut merged = binding.clone();
                        let mut consistent = true;
                        for (var, val) in &candidate {
                            if let Some(&existing) = merged.get(var) {
                                if existing != *val { consistent = false; break; }
                            } else {
                                merged.insert(var.clone(), *val);
                            }
                        }
                        if consistent { results.push(merged); }
                    }
                }
                results
            }
            TemporalAtom::Diamond { interval, inner } =>
                self.eval_diamond(interval, inner, t_prime, bindings),
            TemporalAtom::Box_ { interval, inner } =>
                self.eval_box(interval, inner, t_prime, bindings),
            TemporalAtom::Prev { interval, inner } =>
                self.eval_prev(interval, inner, t_prime, bindings),
            TemporalAtom::Since { interval, phi, psi } => {
                let (b, _) = self.eval_since(interval, phi, psi, t_prime, bindings);
                b
            }
        }
    }
}

/// Join a list of TriplePatterns against a HashSet<Triple> starting from a seed binding.
fn join_base_atoms(
    patterns: &[&TriplePattern],
    facts: &HashSet<Triple>,
    seed: HashMap<String, u32>,
) -> Vec<HashMap<String, u32>> {
    let mut results = vec![seed];
    for pattern in patterns {
        let mut new_results = Vec::new();
        for binding in results {
            for fact in facts {
                let mut b = binding.clone();
                if matches_rule_pattern(pattern, fact, &mut b) {
                    new_results.push(b);
                }
            }
        }
        results = new_results;
        if results.is_empty() { break; }
    }
    results
}

/// Compute the maximum interval width across all rules.
pub fn compute_w_max(rules: &[DatalogMTLRule]) -> u64 {
    fn atom_max(atom: &TemporalAtom) -> u64 {
        match atom {
            TemporalAtom::Base(_) => 0,
            TemporalAtom::Diamond { interval, inner } =>
                interval.end.max(atom_max(inner)),
            TemporalAtom::Box_ { interval, inner } =>
                interval.end.max(atom_max(inner)),
            TemporalAtom::Prev { interval, inner } =>
                interval.end.max(atom_max(inner)),
            TemporalAtom::Since { interval, phi, psi } =>
                interval.end.max(atom_max(phi)).max(atom_max(psi)),
        }
    }
    rules.iter()
        .flat_map(|r| r.body.iter().map(atom_max))
        .max()
        .unwrap_or(0)
}
