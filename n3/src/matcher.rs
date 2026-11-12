/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Joins rule atoms against a [`DatasetIndex`], evaluating built-ins as their inputs bind

use crate::numeric::{numbers_equal, numeric};
use crate::rules::{Atom, PTerm};
use crate::syntax::Node;
use crate::terms::{Compound, TermStore};
use shared::dataset_index::DatasetIndex;
use shared::pattern::matches_rule_pattern;
use shared::terms::Binding;
use shared::triple::Triple;
use std::collections::BTreeSet;

/// What a pattern position says about the id it must match
enum Key {
    Bound(u32),
    Free,
    Impossible,
}

/// The fact base and term store one join reads, borrowed for the join's duration
#[derive(Debug)]
pub(crate) struct Matcher<'a> {
    pub(crate) store: &'a mut TermStore,
    pub(crate) facts: &'a DatasetIndex,
    pub(crate) rule_facts: &'a DatasetIndex,
    pub(crate) implies: Option<u32>,
}

impl Matcher<'_> {
    /// Joins `atoms`, matching the atom `seed.0` against `seed.1` and the rest against the store
    pub(crate) fn solve(
        &mut self,
        atoms: &[Atom],
        seed: Option<(usize, &DatasetIndex)>,
        initial: Binding,
    ) -> Vec<Binding> {
        let mut remaining: Vec<usize> = (0..atoms.len()).collect();
        let mut bound: BTreeSet<String> = initial.keys().cloned().collect();
        let mut bindings = vec![initial];
        if let Some((first, index)) = seed {
            remaining.retain(|&i| i != first);
            bindings = self.extend(&atoms[first], &[index], bindings);
            atoms[first].variables(&mut bound);
        }
        while !remaining.is_empty() && !bindings.is_empty() {
            let is_ground = |position: &PTerm| {
                let mut vars = BTreeSet::new();
                position.variables(&mut vars);
                vars.is_subset(&bound) && !position.has_exists()
            };
            let ready_builtin = remaining.iter().position(|&i| {
                atoms[i]
                    .builtin
                    .is_some_and(|b| b.ready(&atoms[i].subject, &atoms[i].object, &is_ground))
            });
            let pick = match ready_builtin {
                Some(position) => position,
                None => {
                    let data = remaining
                        .iter()
                        .enumerate()
                        .filter(|(_, &i)| atoms[i].builtin.is_none())
                        .max_by_key(|(_, &i)| boundness(&atoms[i], &bound));
                    match data {
                        Some((position, _)) => position,
                        None => return Vec::new(),
                    }
                }
            };
            let next = remaining.remove(pick);
            let atom = &atoms[next];
            bindings = match atom.builtin {
                Some(builtin) => bindings
                    .into_iter()
                    .flat_map(|b| builtin.evaluate(self, &atom.subject, &atom.object, b))
                    .collect(),
                None => {
                    let rule_facts = self.rule_facts;
                    let facts = self.facts;
                    let names_implies =
                        matches!(atom.predicate, PTerm::Const(id) if Some(id) == self.implies);
                    let sources: &[&DatasetIndex] = if names_implies {
                        &[facts, rule_facts]
                    } else {
                        &[facts]
                    };
                    self.extend(atom, sources, bindings)
                }
            };
            atom.variables(&mut bound);
        }
        bindings
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
                out.extend(self.unify_triple(atom, &fact, binding.clone()));
            }
        }
        out
    }

    fn unify_triple(&self, atom: &Atom, triple: &Triple, binding: Binding) -> Vec<Binding> {
        let mut partial = vec![binding];
        for (position, id) in
            atom.positions()
                .into_iter()
                .zip([triple.subject, triple.predicate, triple.object])
        {
            partial = partial
                .into_iter()
                .flat_map(|b| self.unify(position, id, b))
                .collect();
            if partial.is_empty() {
                break;
            }
        }
        partial
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
    pub(crate) fn unify(&self, position: &PTerm, id: u32, mut binding: Binding) -> Vec<Binding> {
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
            let partial = self.unify_triple(atom, triple, binding.clone());
            used[index] = true;
            for b in partial {
                self.unify_formula(rest, triples, used, b, out);
            }
            used[index] = false;
        }
    }

    /// The id `position` denotes under `binding`, interning lists and formulas it builds
    pub(crate) fn ground(&mut self, position: &PTerm, binding: &Binding) -> Option<u32> {
        match position {
            PTerm::Const(id) => Some(*id),
            PTerm::Var(name) => binding.get(name).copied(),
            PTerm::List(items) => {
                let ids = items
                    .iter()
                    .map(|item| self.ground(item, binding))
                    .collect::<Option<Vec<u32>>>()?;
                Some(self.store.list(ids))
            }
            PTerm::Formula(atoms) => {
                let mut triples = Vec::with_capacity(atoms.len());
                for atom in atoms {
                    triples.push(Triple {
                        subject: self.ground(&atom.subject, binding)?,
                        predicate: self.ground(&atom.predicate, binding)?,
                        object: self.ground(&atom.object, binding)?,
                    });
                }
                Some(self.store.formula(triples))
            }
            PTerm::Exists(_) => None,
        }
    }

    /// The node behind an id
    pub(crate) fn node(&self, id: u32) -> Node {
        self.store.node(id)
    }

    /// The member ids of a list
    pub(crate) fn members(&self, id: u32) -> Option<Vec<u32>> {
        match self.store.compound(id) {
            Some(Compound::List(items)) => Some(items.clone()),
            _ => None,
        }
    }

    /// The id of a node, interning it
    pub(crate) fn intern(&mut self, node: &Node) -> u32 {
        self.store.encode(node)
    }

    /// Whether two ids denote the same term: numbers of one datatype by value, plain and
    /// `xsd:string` text alike, lists member by member
    pub(crate) fn same_term(&self, left: u32, right: u32) -> bool {
        if left == right {
            return true;
        }
        if let (Some(a), Some(b)) = (self.members(left), self.members(right)) {
            return a.len() == b.len() && a.iter().zip(&b).all(|(x, y)| self.same_term(*x, *y));
        }
        let (a, b) = (self.node(left), self.node(right));
        match (&a, &b, numeric(&a), numeric(&b)) {
            (
                Node::Literal { datatype: da, .. },
                Node::Literal { datatype: db, .. },
                Some(x),
                Some(y),
            ) => da == db && numbers_equal(&x, &y),
            _ => false,
        }
    }
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
