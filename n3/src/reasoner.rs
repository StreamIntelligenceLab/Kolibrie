/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Forward N3 reasoning: semi-naive saturation over a Kolibrie [`DatasetIndex`]

use crate::error::N3Error;
use crate::rules::{compile_rule, formula_node, Atom, N3Rule, PTerm};
use crate::syntax::{Document, Node, LOG_IMPLIED_BY, LOG_IMPLIES, LOG_QUERY};
use crate::terms::{Compound, TermStore};
use shared::dataset_index::DatasetIndex;
use shared::pattern::matches_rule_pattern;
use shared::terms::Binding;
use shared::triple::Triple;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Resource budgets that stop a run which would not reach a fixpoint
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Most facts the store may hold, input included
    pub max_facts: usize,
    /// Most saturation rounds
    pub max_rounds: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_facts: 5_000_000,
            max_rounds: 100_000,
        }
    }
}

/// Facts, rules, and the terms both are encoded against
#[derive(Debug, Default)]
pub struct N3Reasoner {
    /// The term dictionary and compound-term structure
    pub store: TermStore,
    facts: DatasetIndex,
    known: HashSet<Triple>,
    rules: Vec<N3Rule>,
    rule_sources: HashSet<(u32, u32)>,
    rule_facts: DatasetIndex,
    derived: Vec<Triple>,
    fresh: usize,
    /// Budgets for [`N3Reasoner::run`]
    pub limits: Limits,
}

/// What a pattern position says about the id it must match
enum Key {
    Bound(u32),
    Free,
    Impossible,
}

impl N3Reasoner {
    /// An empty reasoner with default limits
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a document's facts and rules
    ///
    /// # Errors
    /// Returns [`N3Error::Unsupported`] for backward rules, queries, and rules this engine
    /// cannot evaluate yet
    pub fn load(&mut self, document: &Document) -> Result<(), N3Error> {
        for statement in &document.statements {
            if let Node::Iri(predicate) = &statement.predicate {
                if predicate == LOG_IMPLIED_BY {
                    return Err(N3Error::unsupported("backward rules (<=)"));
                }
                if predicate == LOG_QUERY {
                    return Err(N3Error::unsupported("log:query"));
                }
                if predicate == LOG_IMPLIES {
                    let rule =
                        compile_rule(&mut self.store, &statement.subject, &statement.object)?;
                    let triple = self.store.encode_statement(statement);
                    self.rule_sources.insert((triple.subject, triple.object));
                    self.rule_facts.insert(&triple);
                    self.rules.push(rule);
                    continue;
                }
            }
            let triple = self.store.encode_statement(statement);
            self.insert(triple);
        }
        Ok(())
    }

    fn insert(&mut self, triple: Triple) -> bool {
        if self.known.insert(triple.clone()) {
            self.facts.insert(&triple);
            true
        } else {
            false
        }
    }

    /// Every fact the store holds, input and derived
    pub fn facts(&self) -> Vec<Triple> {
        self.facts.query(None, None, None)
    }

    /// The facts derived so far, in derivation order
    pub fn derived(&self) -> &[Triple] {
        &self.derived
    }

    /// Saturates the store under the forward rules and returns the derived facts
    ///
    /// Rules that introduce existentials are deferred: ordinary rules saturate first, then one
    /// existential rule fires, and ordinary reasoning resumes on what it added. An existential
    /// rule whose conclusion could satisfy another's goes before it.
    ///
    /// # Errors
    /// Returns [`N3Error::Limit`] when a budget in [`N3Reasoner::limits`] runs out, and
    /// [`N3Error::Unsupported`] when a derived rule cannot be evaluated
    pub fn run(&mut self) -> Result<&[Triple], N3Error> {
        let mut pending: Vec<Triple> = self.facts();
        let mut fresh_rules: Vec<usize> = (0..self.rules.len()).collect();
        loop {
            self.saturate(&mut pending, &mut fresh_rules)?;
            let mut progressed = false;
            for rule_index in self.existential_order() {
                let rule = self.rules[rule_index].clone();
                let before = pending.len();
                for binding in self.solve(&rule.body, None, Binding::new()) {
                    let conclusion = self.fire(&rule, &binding)?;
                    self.commit(conclusion, &mut pending, &mut fresh_rules)?;
                }
                if pending.len() > before {
                    progressed = true;
                    break;
                }
            }
            if !progressed {
                return Ok(&self.derived);
            }
        }
    }

    /// Runs the rules without existentials to a fixpoint, semi-naively from `pending`
    fn saturate(
        &mut self,
        pending: &mut Vec<Triple>,
        fresh_rules: &mut Vec<usize>,
    ) -> Result<(), N3Error> {
        let mut rounds = 0;
        loop {
            let ordinary_fresh: Vec<usize> = fresh_rules
                .iter()
                .copied()
                .filter(|&i| !self.rules[i].has_existentials)
                .collect();
            if pending.is_empty() && ordinary_fresh.is_empty() {
                return Ok(());
            }
            rounds += 1;
            if rounds > self.limits.max_rounds {
                return Err(N3Error::Limit(format!("{} rounds", self.limits.max_rounds)));
            }
            fresh_rules.retain(|i| self.rules[*i].has_existentials);
            let mut delta_index = DatasetIndex::new();
            for triple in pending.drain(..) {
                delta_index.insert(&triple);
            }
            for rule_index in 0..self.rules.len() {
                let rule = self.rules[rule_index].clone();
                if rule.has_existentials {
                    continue;
                }
                let full = ordinary_fresh.contains(&rule_index);
                for binding in self.rule_solutions(&rule, &delta_index, full) {
                    let conclusion = self.fire(&rule, &binding)?;
                    self.commit(conclusion, pending, fresh_rules)?;
                }
            }
        }
    }

    /// Stores new conclusion triples, queues them for the next round, and adopts derived rules
    fn commit(
        &mut self,
        conclusion: Vec<Triple>,
        pending: &mut Vec<Triple>,
        fresh_rules: &mut Vec<usize>,
    ) -> Result<(), N3Error> {
        for triple in conclusion {
            if !self.insert(triple.clone()) {
                continue;
            }
            self.derived.push(triple.clone());
            if self.adopt_derived_rule(&triple)? {
                fresh_rules.push(self.rules.len() - 1);
            }
            pending.push(triple);
        }
        if self.known.len() > self.limits.max_facts {
            return Err(N3Error::Limit(format!("{} facts", self.limits.max_facts)));
        }
        Ok(())
    }

    /// The existential rules, each after every rule whose conclusion could satisfy its own
    fn existential_order(&self) -> Vec<usize> {
        let existential: Vec<usize> = (0..self.rules.len())
            .filter(|&i| self.rules[i].has_existentials)
            .collect();
        let mut placed: Vec<usize> = Vec::with_capacity(existential.len());
        let mut waiting = existential.clone();
        while !waiting.is_empty() {
            let ready = waiting
                .iter()
                .position(|&candidate| {
                    !waiting.iter().any(|&other| {
                        other != candidate
                            && covers(&self.rules[other].head, &self.rules[candidate].head)
                            && !covers(&self.rules[candidate].head, &self.rules[other].head)
                    })
                })
                .unwrap_or(0);
            placed.push(waiting.remove(ready));
        }
        placed
    }

    /// Turns a derived `{…} log:implies {…}` triple into a rule; true if it added one
    fn adopt_derived_rule(&mut self, triple: &Triple) -> Result<bool, N3Error> {
        if self
            .store
            .lookup(LOG_IMPLIED_BY)
            .is_some_and(|id| id == triple.predicate)
        {
            return Err(N3Error::unsupported("backward rules (<=), derived"));
        }
        let Some(implies) = self.store.lookup(LOG_IMPLIES) else {
            return Ok(false);
        };
        if triple.predicate != implies || !self.rule_sources.insert((triple.subject, triple.object))
        {
            return Ok(false);
        }
        let (Some(body), Some(head)) = (
            formula_node(&self.store, triple.subject),
            formula_node(&self.store, triple.object),
        ) else {
            return Ok(false);
        };
        let rule = compile_rule(&mut self.store, &body, &head)?;
        self.rules.push(rule);
        Ok(true)
    }

    /// Every binding of the rule body that uses at least one delta fact, or all bindings when
    /// `full`
    fn rule_solutions(&self, rule: &N3Rule, delta: &DatasetIndex, full: bool) -> Vec<Binding> {
        if rule.body.is_empty() {
            return if full {
                vec![Binding::new()]
            } else {
                Vec::new()
            };
        }
        let mut seen: HashSet<Vec<(String, u32)>> = HashSet::new();
        let mut out = Vec::new();
        let starts: Vec<Option<usize>> = if full {
            vec![None]
        } else {
            (0..rule.body.len()).map(Some).collect()
        };
        for start in starts {
            let solutions = self.solve(&rule.body, start.map(|i| (i, delta)), Binding::new());
            for binding in solutions {
                let mut key: Vec<(String, u32)> =
                    binding.iter().map(|(k, v)| (k.clone(), *v)).collect();
                key.sort();
                if seen.insert(key) {
                    out.push(binding);
                }
            }
        }
        out
    }

    /// Joins `atoms`, matching the atom `seed.0` against `seed.1` and the rest against the store
    fn solve(
        &self,
        atoms: &[Atom],
        seed: Option<(usize, &DatasetIndex)>,
        initial: Binding,
    ) -> Vec<Binding> {
        let mut remaining: Vec<usize> = (0..atoms.len()).collect();
        let mut bindings = vec![initial];
        let mut bound: BTreeSet<String> = bindings[0].keys().cloned().collect();
        if let Some((first, index)) = seed {
            remaining.retain(|&i| i != first);
            bindings = self.extend(&atoms[first], &[index], bindings);
            atoms[first].variables(&mut bound);
        }
        while !remaining.is_empty() && !bindings.is_empty() {
            let pick = remaining
                .iter()
                .enumerate()
                .max_by_key(|(_, &i)| boundness(&atoms[i], &bound))
                .map(|(position, _)| position)
                .unwrap_or(0);
            let next = remaining.remove(pick);
            let sources = self.sources(&atoms[next]);
            bindings = self.extend(&atoms[next], &sources, bindings);
            atoms[next].variables(&mut bound);
        }
        bindings
    }

    /// The indexes an atom matches against: the facts, plus the input rules when it asks for
    /// `log:implies` by name
    fn sources(&self, atom: &Atom) -> Vec<&DatasetIndex> {
        let names_implies = matches!(atom.predicate, PTerm::Const(id)
            if self.store.lookup(LOG_IMPLIES) == Some(id));
        if names_implies {
            vec![&self.facts, &self.rule_facts]
        } else {
            vec![&self.facts]
        }
    }

    /// Extends each binding with every way `atom` matches a fact in one of `indexes`
    fn extend(
        &self,
        atom: &Atom,
        indexes: &[&DatasetIndex],
        bindings: Vec<Binding>,
    ) -> Vec<Binding> {
        let mut out = Vec::new();
        for binding in bindings {
            let mut keys = [None, None, None];
            let mut impossible = false;
            for (slot, position) in atom.positions().into_iter().enumerate() {
                match self.key(position, &binding) {
                    Key::Bound(id) => keys[slot] = Some(id),
                    Key::Free => {}
                    Key::Impossible => impossible = true,
                }
            }
            if impossible {
                continue;
            }
            let candidates = indexes
                .iter()
                .flat_map(|index| index.query(keys[0], keys[1], keys[2]));
            for fact in candidates {
                if let Some(flat) = &atom.flat {
                    let mut extended = binding.clone();
                    if matches_rule_pattern(flat, &fact, &mut extended) {
                        out.push(extended);
                    }
                    continue;
                }
                let mut partial = vec![binding.clone()];
                for (position, id) in
                    atom.positions()
                        .into_iter()
                        .zip([fact.subject, fact.predicate, fact.object])
                {
                    partial = partial
                        .into_iter()
                        .flat_map(|b| self.unify(position, id, b))
                        .collect();
                    if partial.is_empty() {
                        break;
                    }
                }
                out.extend(partial);
            }
        }
        out
    }

    /// The id a position must equal under `binding`, if it can be known without a lookup scan
    fn key(&self, position: &PTerm, binding: &Binding) -> Key {
        match position {
            PTerm::Const(id) => Key::Bound(*id),
            PTerm::Var(name) => match binding.get(name) {
                Some(&id) => Key::Bound(id),
                None => Key::Free,
            },
            PTerm::List(items) => {
                let mut ids = Vec::with_capacity(items.len());
                for item in items {
                    match self.key(item, binding) {
                        Key::Bound(id) => ids.push(id),
                        Key::Free => return Key::Free,
                        Key::Impossible => return Key::Impossible,
                    }
                }
                match self.store.find_list(&ids) {
                    Some(id) => Key::Bound(id),
                    None => Key::Impossible,
                }
            }
            PTerm::Formula(_) | PTerm::Exists(_) => Key::Free,
        }
    }

    /// Every extension of `binding` under which `position` denotes `id`
    fn unify(&self, position: &PTerm, id: u32, mut binding: Binding) -> Vec<Binding> {
        match position {
            PTerm::Const(c) => {
                if *c == id {
                    vec![binding]
                } else {
                    Vec::new()
                }
            }
            PTerm::Var(name) => match binding.get(name) {
                Some(&bound) if bound != id => Vec::new(),
                Some(_) => vec![binding],
                None => {
                    binding.insert(name.clone(), id);
                    vec![binding]
                }
            },
            PTerm::List(items) => {
                let Some(Compound::List(members)) = self.store.compound(id) else {
                    return Vec::new();
                };
                if members.len() != items.len() {
                    return Vec::new();
                }
                let mut partial = vec![binding];
                for (item, &member) in items.iter().zip(members) {
                    partial = partial
                        .into_iter()
                        .flat_map(|b| self.unify(item, member, b))
                        .collect();
                    if partial.is_empty() {
                        break;
                    }
                }
                partial
            }
            PTerm::Formula(atoms) => {
                let Some(Compound::Formula(triples)) = self.store.compound(id) else {
                    return Vec::new();
                };
                if triples.len() != atoms.len() {
                    return Vec::new();
                }
                let mut used = vec![false; triples.len()];
                let mut out = Vec::new();
                self.unify_formula(atoms, triples, &mut used, binding, &mut out);
                out
            }
            PTerm::Exists(_) => Vec::new(),
        }
    }

    /// Matches each atom to a distinct triple, collecting every consistent binding
    fn unify_formula(
        &self,
        atoms: &[Atom],
        triples: &[Triple],
        used: &mut [bool],
        binding: Binding,
        out: &mut Vec<Binding>,
    ) {
        let Some((atom, rest)) = atoms.split_first() else {
            out.push(binding);
            return;
        };
        for (index, triple) in triples.iter().enumerate() {
            if used[index] {
                continue;
            }
            let mut partial = vec![binding.clone()];
            for (position, id) in
                atom.positions()
                    .into_iter()
                    .zip([triple.subject, triple.predicate, triple.object])
            {
                partial = partial
                    .into_iter()
                    .flat_map(|b| self.unify(position, id, b))
                    .collect();
            }
            used[index] = true;
            for b in partial {
                self.unify_formula(rest, triples, used, b, out);
            }
            used[index] = false;
        }
    }

    /// The conclusion triples of one firing; empty when its existential pieces already hold
    fn fire(&mut self, rule: &N3Rule, binding: &Binding) -> Result<Vec<Triple>, N3Error> {
        if rule.has_existentials && self.conclusion_holds(rule, binding) {
            return Ok(Vec::new());
        }
        let mut witnesses = HashMap::new();
        let mut out = Vec::with_capacity(rule.head.len());
        for atom in &rule.head {
            out.push(Triple {
                subject: self.instantiate(&atom.subject, binding, &mut witnesses)?,
                predicate: self.instantiate(&atom.predicate, binding, &mut witnesses)?,
                object: self.instantiate(&atom.object, binding, &mut witnesses)?,
            });
        }
        Ok(out)
    }

    /// Whether some facts already satisfy the conclusion, existentials read as variables
    fn conclusion_holds(&self, rule: &N3Rule, binding: &Binding) -> bool {
        let as_pattern: Vec<Atom> = rule
            .head
            .iter()
            .map(|atom| {
                Atom::new(
                    exists_as_var(&atom.subject),
                    exists_as_var(&atom.predicate),
                    exists_as_var(&atom.object),
                )
            })
            .collect();
        !self.solve(&as_pattern, None, binding.clone()).is_empty()
    }

    fn instantiate(
        &mut self,
        position: &PTerm,
        binding: &Binding,
        witnesses: &mut HashMap<String, u32>,
    ) -> Result<u32, N3Error> {
        Ok(match position {
            PTerm::Const(id) => *id,
            PTerm::Var(name) => match binding.get(name) {
                Some(&id) => id,
                None => {
                    return Err(N3Error::unsupported(format!(
                        "conclusion variable ?{name} left unbound"
                    )))
                }
            },
            PTerm::List(items) => {
                let mut ids = Vec::with_capacity(items.len());
                for item in items {
                    ids.push(self.instantiate(item, binding, witnesses)?);
                }
                self.store.list(ids)
            }
            PTerm::Formula(atoms) => {
                let mut triples = Vec::with_capacity(atoms.len());
                for atom in atoms {
                    triples.push(Triple {
                        subject: self.instantiate(&atom.subject, binding, witnesses)?,
                        predicate: self.instantiate(&atom.predicate, binding, witnesses)?,
                        object: self.instantiate(&atom.object, binding, witnesses)?,
                    });
                }
                self.store.formula(triples)
            }
            PTerm::Exists(label) => {
                if let Some(&id) = witnesses.get(label) {
                    id
                } else {
                    self.fresh += 1;
                    let id = self
                        .store
                        .dictionary
                        .encode(&format!("_:kolibrie-n3-.e{}", self.fresh));
                    witnesses.insert(label.clone(), id);
                    id
                }
            }
        })
    }

    /// The derived facts as N3 text, one statement per line
    pub fn derived_n3(&self) -> String {
        let mut out = String::new();
        for triple in &self.derived {
            out.push_str(&self.store.n3_triple(triple));
            out.push('\n');
        }
        out
    }
}

fn exists_as_var(position: &PTerm) -> PTerm {
    match position {
        PTerm::Exists(label) => PTerm::Var(format!("?exists {label}")),
        PTerm::List(items) => PTerm::List(items.iter().map(exists_as_var).collect()),
        PTerm::Formula(atoms) => PTerm::Formula(
            atoms
                .iter()
                .map(|a| {
                    Atom::new(
                        exists_as_var(&a.subject),
                        exists_as_var(&a.predicate),
                        exists_as_var(&a.object),
                    )
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Whether two pattern positions could denote the same term
fn compatible(left: &PTerm, right: &PTerm) -> bool {
    match (left, right) {
        (PTerm::Const(a), PTerm::Const(b)) => a == b,
        (PTerm::List(a), PTerm::List(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| compatible(x, y))
        }
        _ => true,
    }
}

/// Whether every existential atom of `piece` could be produced by some atom of `head`
fn covers(head: &[Atom], piece: &[Atom]) -> bool {
    piece.iter().filter(|atom| atom.has_exists()).all(|atom| {
        head.iter().any(|candidate| {
            atom.positions()
                .into_iter()
                .zip(candidate.positions())
                .all(|(a, b)| compatible(a, b))
        })
    })
}

/// How many of an atom's positions are already fixed, for picking the next atom to join
fn boundness(atom: &Atom, bound: &BTreeSet<String>) -> usize {
    atom.positions()
        .into_iter()
        .filter(|position| {
            let mut vars = BTreeSet::new();
            position.variables(&mut vars);
            vars.is_subset(bound) && !matches!(position, PTerm::Exists(_) | PTerm::Formula(_))
        })
        .count()
}
