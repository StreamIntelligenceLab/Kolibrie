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
use datalog::reasoning::construct_triple;
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
        // Step 1: seed bindings from all Base atoms at the current snapshot,
        // using indexed store lookups and indexed joins (no full-fact scan).
        let base_atoms: Vec<&TriplePattern> = rule.body.iter()
            .filter_map(|a| if let TemporalAtom::Base(p) = a { Some(p) } else { None })
            .collect();

        let mut bindings = self.seed_base_atoms(&base_atoms, t);

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
                // Future operators require static data; the streaming tick engine
                // cannot evaluate them. Unreachable — the parser blocks future ops
                // in Streaming mode, and Static mode uses the interval engine.
                TemporalAtom::DiamondPlus { .. }
                | TemporalAtom::BoxPlus { .. }
                | TemporalAtom::Until { .. } => {
                    bindings = Vec::new();
                }
            }
            if bindings.is_empty() { break; }
        }
        bindings
    }

    /// Seed bindings by joining the rule's Base atoms at time `t` using indexed
    /// store lookups. Each subsequent atom is queried with the running binding
    /// substituted in, turning the join into indexed probes rather than an
    /// O(facts^k) nested-loop scan.
    fn seed_base_atoms(
        &self,
        base_atoms: &[&TriplePattern],
        t: u64,
    ) -> Vec<HashMap<String, u32>> {
        let Some((first, rest)) = base_atoms.split_first() else {
            return vec![HashMap::new()];
        };
        let mut bindings = self.store.query_at(first, t);
        for pattern in rest {
            if bindings.is_empty() { break; }
            let mut next = Vec::with_capacity(bindings.len());
            for binding in &bindings {
                let spec = substitute_pattern(pattern, binding);
                for candidate in self.store.query_at(&spec, t) {
                    let mut merged = binding.clone();
                    let mut consistent = true;
                    for (var, val) in &candidate {
                        if let Some(&existing) = merged.get(var) {
                            if existing != *val { consistent = false; break; }
                        } else {
                            merged.insert(var.clone(), *val);
                        }
                    }
                    if consistent { next.push(merged); }
                }
            }
            bindings = next;
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

    /// Box[a,b]: phi must hold at EVERY integer point t' in [t-b, t-a]
    /// (dense semantics, matching the DatalogMTL/MeTeoR reference: an integer in
    /// the window with no supporting fact makes Box fail — it is not skipped).
    /// Collects candidate bindings from the first point, then filters them
    /// against every subsequent point — so variables introduced solely inside
    /// Box (e.g. Box[0,5](?x :sensor ?v)) are still grounded.
    fn eval_box(
        &self,
        interval: &Interval,
        inner: &TemporalAtom,
        t: u64,
        bindings: &[HashMap<String, u32>],
    ) -> Vec<HashMap<String, u32>> {
        // Universal operator: the whole window [t-end, t-start] must lie within
        // observable time [0, t]. If t < end the window extends before time 0,
        // where nothing holds, so Box fails. (Guarding on interval.start would
        // let absolute_range's saturating_sub clamp the lower bound to 0 and
        // spuriously satisfy Box at the leading boundary.)
        if t < interval.end { return vec![]; }
        let (lo, hi) = interval.absolute_range(t); // lo <= hi since end >= start
        let mut results = Vec::new();
        'outer: for binding in bindings {
            // Seed candidates from the first integer point.
            let mut candidates =
                self.eval_atom_at(inner, lo, &[binding.clone()]);
            if candidates.is_empty() { continue 'outer; }

            // Require every subsequent integer point in the window to satisfy inner.
            for t_prime in (lo + 1)..=hi {
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
        // Reset candidates: fact-bearing points in the window where psi may hold
        // (psi only holds where a fact exists, so scanning timestamps is complete).
        let since_timestamps = self.store.timestamps_in(lo, hi);
        let mut results = Vec::new();
        let mut scan_depth = 0;

        for binding in bindings {
            'reset: for &t_prime in since_timestamps.iter().rev() {
                scan_depth += 1;
                let psi_results =
                    self.eval_atom_at(psi, t_prime, &[binding.clone()]);
                if psi_results.is_empty() { continue; }

                // FORALL integer t'' in (t', t]: phi must hold (dense semantics —
                // an uncovered integer point makes the continuation fail).
                for t_pp in (t_prime + 1)..=t {
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
                    // Specialize the pattern with already-bound variables so the
                    // store query is as constrained (and indexed) as possible.
                    let spec = substitute_pattern(pattern, binding);
                    for candidate in self.store.query_at(&spec, t_prime) {
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
            // Future operators are unsupported by the streaming tick engine
            // (blocked upstream by the parser/mode); yield no bindings.
            TemporalAtom::DiamondPlus { .. }
            | TemporalAtom::BoxPlus { .. }
            | TemporalAtom::Until { .. } => Vec::new(),
        }
    }
}

/// Substitute a pattern's variables that are already bound in `binding` with
/// their constant values, so the resulting query is maximally constrained.
fn substitute_pattern(pattern: &TriplePattern, binding: &HashMap<String, u32>) -> TriplePattern {
    let resolve = |term: &Term| -> Term {
        match term {
            Term::Variable(v) => match binding.get(v) {
                Some(&id) => Term::Constant(id),
                None => term.clone(),
            },
            other => other.clone(),
        }
    };
    (resolve(&pattern.0), resolve(&pattern.1), resolve(&pattern.2))
}

/// Compute the maximum interval width across all rules.
pub fn compute_w_max(rules: &[DatalogMTLRule]) -> u64 {
    fn atom_max(atom: &TemporalAtom) -> u64 {
        match atom {
            TemporalAtom::Base(_) => 0,
            TemporalAtom::Diamond { interval, inner }
            | TemporalAtom::Box_ { interval, inner }
            | TemporalAtom::Prev { interval, inner }
            | TemporalAtom::DiamondPlus { interval, inner }
            | TemporalAtom::BoxPlus { interval, inner } =>
                interval.end.max(atom_max(inner)),
            TemporalAtom::Since { interval, phi, psi }
            | TemporalAtom::Until { interval, phi, psi } =>
                interval.end.max(atom_max(phi)).max(atom_max(psi)),
        }
    }
    rules.iter()
        .flat_map(|r| r.body.iter().map(atom_max))
        .max()
        .unwrap_or(0)
}
