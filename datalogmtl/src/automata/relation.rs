/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Timed binding-relations and the relational join that carries interval
//! payloads. An atom evaluates to a `BindingRelation` (variable tuple ->
//! interval list); conjunction is a natural join with per-binding interval
//! **intersection**; temporal operators transduce the interval lists.

use std::collections::HashMap;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use datalog::reasoning::matches_rule_pattern;
use crate::syntax::TemporalAtom;
use super::interval::{coalesce, intersect_lists, TInterval};
use super::transducer;

/// The interval-valued fact base, indexed for pattern matching.
pub struct Database {
    pub facts: HashMap<Triple, Vec<TInterval>>,
    by_pred: HashMap<u32, Vec<Triple>>,
    by_pred_obj: HashMap<(u32, u32), Vec<Triple>>,
}

impl Database {
    pub fn new() -> Self {
        Database { facts: HashMap::new(), by_pred: HashMap::new(), by_pred_obj: HashMap::new() }
    }

    fn index_triple(&mut self, t: &Triple) {
        self.by_pred.entry(t.predicate).or_default().push(t.clone());
        self.by_pred_obj.entry((t.predicate, t.object)).or_default().push(t.clone());
    }

    /// Append one interval to a fact (indexes the triple if new). Does not coalesce.
    pub fn add_raw(&mut self, t: Triple, iv: TInterval) {
        if !self.facts.contains_key(&t) {
            self.index_triple(&t);
        }
        self.facts.entry(t).or_default().push(iv);
    }

    /// Coalesce every fact's interval list into canonical form.
    pub fn coalesce_all(&mut self) {
        for ivs in self.facts.values_mut() {
            *ivs = coalesce(std::mem::take(ivs));
        }
    }

    /// Union `new_ivs` into fact `t`, coalescing. Returns true if it changed.
    pub fn add_intervals(&mut self, t: Triple, new_ivs: &[TInterval]) -> bool {
        let is_new = !self.facts.contains_key(&t);
        if is_new {
            self.index_triple(&t);
        }
        let entry = self.facts.entry(t).or_default();
        let old = entry.clone();
        entry.extend_from_slice(new_ivs);
        let merged = coalesce(std::mem::take(entry));
        *entry = merged;
        *entry != old
    }

    /// Candidate ground triples that could match a pattern, using the indexes.
    fn candidates(&self, pattern: &TriplePattern) -> Vec<Triple> {
        let pred = as_const(&pattern.1);
        let obj = as_const(&pattern.2);
        match (pred, obj) {
            (Some(p), Some(o)) => self.by_pred_obj.get(&(p, o)).cloned().unwrap_or_default(),
            (Some(p), None) => self.by_pred.get(&p).cloned().unwrap_or_default(),
            _ => self.facts.keys().cloned().collect(),
        }
    }
}

fn as_const(term: &Term) -> Option<u32> {
    match term { Term::Constant(c) => Some(*c), _ => None }
}

/// A relation over some variables with an interval list per binding tuple.
pub struct BindingRelation {
    pub vars: Vec<String>,                        // sorted, distinct
    pub rows: HashMap<Vec<u32>, Vec<TInterval>>,  // values aligned to `vars`
}

impl BindingRelation {
    fn empty(vars: Vec<String>) -> Self {
        BindingRelation { vars, rows: HashMap::new() }
    }
}

/// Distinct variable names appearing in a pattern, sorted.
fn pattern_vars(pattern: &TriplePattern) -> Vec<String> {
    let mut vs = Vec::new();
    for term in [&pattern.0, &pattern.1, &pattern.2] {
        if let Term::Variable(v) = term {
            if !vs.contains(v) { vs.push(v.clone()); }
        }
    }
    vs.sort();
    vs
}

/// Evaluate a (possibly nested) temporal atom to a timed binding-relation.
pub fn eval_atom(atom: &TemporalAtom, db: &Database) -> BindingRelation {
    match atom {
        TemporalAtom::Base(pattern) => eval_base(pattern, db),
        TemporalAtom::Diamond { interval, inner } => {
            let r = eval_atom(inner, db);
            map_transduce(r, |ints| transducer::diamond(ints, interval))
        }
        TemporalAtom::Box_ { interval, inner } => {
            let r = eval_atom(inner, db);
            map_transduce(r, |ints| transducer::box_(ints, interval))
        }
        TemporalAtom::Prev { .. } => {
            panic!("Prev has no MeTeoR analogue and is unsupported by the interval strategy");
        }
        TemporalAtom::Since { interval, phi, psi } => {
            let a = eval_atom(phi, db);
            let b = eval_atom(psi, db);
            join_with(&a, &b, |pints, qints| transducer::since(pints, qints, interval))
        }
        TemporalAtom::DiamondPlus { interval, inner } => {
            let r = eval_atom(inner, db);
            map_transduce(r, |ints| transducer::diamond_plus(ints, interval))
        }
        TemporalAtom::BoxPlus { interval, inner } => {
            let r = eval_atom(inner, db);
            map_transduce(r, |ints| transducer::box_plus(ints, interval))
        }
        TemporalAtom::Until { interval, phi, psi } => {
            let a = eval_atom(phi, db);
            let b = eval_atom(psi, db);
            join_with(&a, &b, |pints, qints| transducer::until(pints, qints, interval))
        }
    }
}

fn eval_base(pattern: &TriplePattern, db: &Database) -> BindingRelation {
    let vars = pattern_vars(pattern);
    let mut rows: HashMap<Vec<u32>, Vec<TInterval>> = HashMap::new();
    for t in db.candidates(pattern) {
        let mut binding = HashMap::new();
        if matches_rule_pattern(pattern, &t, &mut binding) {
            let key: Vec<u32> = vars.iter().map(|v| binding[v]).collect();
            if let Some(ivs) = db.facts.get(&t) {
                rows.entry(key).or_default().extend_from_slice(ivs);
            }
        }
    }
    for v in rows.values_mut() { *v = coalesce(std::mem::take(v)); }
    BindingRelation { vars, rows }
}

/// Apply an interval-list transform to every row, dropping rows that become empty.
fn map_transduce(
    r: BindingRelation,
    f: impl Fn(&[TInterval]) -> Vec<TInterval>,
) -> BindingRelation {
    let mut rows = HashMap::new();
    for (k, ivs) in r.rows {
        let out = f(&ivs);
        if !out.is_empty() { rows.insert(k, out); }
    }
    BindingRelation { vars: r.vars, rows }
}

/// Natural join of two relations on shared variables, combining the interval
/// lists per matched binding with `combine` (intersection for conjunction,
/// `since` for the Since operator).
pub fn join_with(
    a: &BindingRelation,
    b: &BindingRelation,
    combine: impl Fn(&[TInterval], &[TInterval]) -> Vec<TInterval>,
) -> BindingRelation {
    let shared: Vec<String> = a.vars.iter().filter(|v| b.vars.contains(v)).cloned().collect();
    let mut out_vars = a.vars.clone();
    for v in &b.vars {
        if !out_vars.contains(v) { out_vars.push(v.clone()); }
    }
    out_vars.sort();
    if a.rows.is_empty() || b.rows.is_empty() {
        return BindingRelation::empty(out_vars);
    }

    let a_shared: Vec<usize> = shared.iter()
        .map(|v| a.vars.iter().position(|x| x == v).unwrap()).collect();
    let b_shared: Vec<usize> = shared.iter()
        .map(|v| b.vars.iter().position(|x| x == v).unwrap()).collect();

    // Hash b by its shared-variable values.
    let mut b_index: HashMap<Vec<u32>, Vec<(&Vec<u32>, &Vec<TInterval>)>> = HashMap::new();
    for (bk, bints) in &b.rows {
        let key: Vec<u32> = b_shared.iter().map(|&i| bk[i]).collect();
        b_index.entry(key).or_default().push((bk, bints));
    }

    let mut rows: HashMap<Vec<u32>, Vec<TInterval>> = HashMap::new();
    for (ak, aints) in &a.rows {
        let key: Vec<u32> = a_shared.iter().map(|&i| ak[i]).collect();
        let Some(matches) = b_index.get(&key) else { continue };
        for (bk, bints) in matches {
            let combined = combine(aints, bints);
            if combined.is_empty() { continue; }
            let mut binding: HashMap<&str, u32> = HashMap::new();
            for (i, v) in a.vars.iter().enumerate() { binding.insert(v, ak[i]); }
            for (i, v) in b.vars.iter().enumerate() { binding.insert(v, bk[i]); }
            let outkey: Vec<u32> = out_vars.iter().map(|v| binding[v.as_str()]).collect();
            rows.entry(outkey).or_default().extend(combined);
        }
    }
    for v in rows.values_mut() { *v = coalesce(std::mem::take(v)); }
    BindingRelation { vars: out_vars, rows }
}

/// Conjunctive join (interval intersection) — used to fold a rule body.
pub fn conjoin(a: &BindingRelation, b: &BindingRelation) -> BindingRelation {
    join_with(a, b, |x, y| intersect_lists(x, y))
}
