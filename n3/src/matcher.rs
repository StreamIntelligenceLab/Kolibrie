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

use crate::builtins::Builtin;
use crate::numeric::{numbers_equal, numeric};
use crate::rules::{Atom, PTerm};
use crate::syntax::Node;
use crate::terms::{Compound, TermStore};
use shared::dataset_index::DatasetIndex;
use shared::pattern::matches_rule_pattern;
use shared::terms::Binding;
use shared::triple::Triple;
use std::collections::{BTreeSet, HashMap, HashSet};

/// What a pattern position says about the id it must match
enum Key {
    Bound(u32),
    Free,
    Impossible,
}

/// The ids of the vocabulary the matcher treats specially, where interned
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Vocabulary {
    pub(crate) implies: Option<u32>,
    pub(crate) first: Option<u32>,
    pub(crate) rest: Option<u32>,
    pub(crate) nil: Option<u32>,
}

impl Vocabulary {
    /// Looks the vocabulary up in a store
    pub(crate) fn of(store: &TermStore) -> Self {
        Vocabulary {
            implies: store.lookup(crate::syntax::LOG_IMPLIES),
            first: store.lookup(RDF_FIRST),
            rest: store.lookup(RDF_REST),
            nil: store.lookup(RDF_NIL),
        }
    }
}

/// `rdf:first`
pub(crate) const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
/// `rdf:rest`
pub(crate) const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
/// `rdf:nil`
pub(crate) const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

/// The fact base and term store one join reads, borrowed for the join's duration
#[derive(Debug)]
pub(crate) struct Matcher<'a> {
    pub(crate) store: &'a mut TermStore,
    pub(crate) facts: &'a DatasetIndex,
    pub(crate) rule_facts: &'a DatasetIndex,
    /// The non-empty native lists occurring in facts, which `rdf:first`/`rdf:rest` also match
    pub(crate) list_terms: &'a BTreeSet<u32>,
    /// The order facts were added in, which lookups keep so joins are deterministic
    pub(crate) sequence: &'a HashMap<Triple, usize>,
    pub(crate) vocabulary: Vocabulary,
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
            bindings = self.extend(&atoms[first], &[index], false, bindings);
            atoms[first].variables(&mut bound);
        }
        while !remaining.is_empty() && !bindings.is_empty() {
            let is_ground = |position: &PTerm| {
                let mut vars = BTreeSet::new();
                position.variables(&mut vars);
                vars.is_subset(&bound) && !position.has_exists()
            };
            let builtin_where = |test: &dyn Fn(Builtin, &Atom) -> bool| {
                remaining
                    .iter()
                    .position(|&i| atoms[i].builtin.is_some_and(|b| test(b, &atoms[i])))
            };
            let forward = builtin_where(&|b, a| b.forward(&a.subject, &a.object, &is_ground));
            let data = || {
                remaining
                    .iter()
                    .enumerate()
                    .filter(|(_, &i)| atoms[i].builtin.is_none())
                    .max_by_key(|(_, &i)| boundness(&atoms[i], &bound))
                    .map(|(position, _)| position)
            };
            let reverse = || builtin_where(&|b, a| b.ready(&a.subject, &a.object, &is_ground));
            let Some(pick) = forward.or_else(data).or_else(reverse) else {
                return Vec::new();
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
                    let names_implies = matches!(atom.predicate,
                        PTerm::Const(id) if Some(id) == self.vocabulary.implies);
                    let sources: &[&DatasetIndex] = if names_implies {
                        &[facts, rule_facts]
                    } else {
                        &[facts]
                    };
                    self.extend(atom, sources, true, bindings)
                }
            };
            atom.variables(&mut bound);
        }
        bindings
    }

    /// Extends each binding with every way `atom` matches a fact in one of `indexes`, and with
    /// `virtual_lists` also how `rdf:first`/`rdf:rest` hold of a native list
    fn extend(
        &mut self,
        atom: &Atom,
        indexes: &[&DatasetIndex],
        virtual_lists: bool,
        bindings: Vec<Binding>,
    ) -> Vec<Binding> {
        let list_step = match atom.predicate {
            PTerm::Const(id) if virtual_lists && Some(id) == self.vocabulary.first => Some(true),
            PTerm::Const(id) if virtual_lists && Some(id) == self.vocabulary.rest => Some(false),
            _ => None,
        };
        let mut out = Vec::new();
        for binding in bindings {
            if let Some(first) = list_step {
                out.extend(self.list_step(atom, first, binding.clone()));
            }
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
            let mut candidates: Vec<Triple> = indexes
                .iter()
                .flat_map(|index| index.query(keys[0], keys[1], keys[2]))
                .collect();
            self.in_order(&mut candidates);
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

    /// Sorts facts into the order they were added, unknown ones last in a fixed order
    pub(crate) fn in_order(&self, facts: &mut [Triple]) {
        facts.sort_by(|a, b| {
            let rank = |t: &Triple| self.sequence.get(t).copied().unwrap_or(usize::MAX);
            rank(a).cmp(&rank(b)).then_with(|| a.cmp(b))
        });
    }

    /// How `rdf:first` (or, when `first` is false, `rdf:rest`) holds of a native list
    fn list_step(&mut self, atom: &Atom, first: bool, binding: Binding) -> Vec<Binding> {
        let subjects: Vec<u32> = match self.key(&atom.subject, &binding) {
            Key::Bound(id) => vec![id],
            Key::Free => self.list_terms.iter().copied().collect(),
            Key::Impossible => return Vec::new(),
        };
        let mut out = Vec::new();
        for subject in subjects {
            let Some(Compound::List(items)) = self.store.compound(subject) else {
                continue;
            };
            let Some((&head, tail)) = items.split_first() else {
                continue;
            };
            let value = if first {
                head
            } else {
                let tail = tail.to_vec();
                self.store.list(tail)
            };
            for b in self.unify(&atom.subject, subject, binding.clone()) {
                out.extend(self.unify_value(&atom.object, value, b));
            }
        }
        out
    }

    /// The members of a list: a native list, `rdf:nil`, or an `rdf:first`/`rdf:rest` chain
    pub(crate) fn list_items(&self, id: u32) -> Option<Vec<u32>> {
        if let Some(Compound::List(items)) = self.store.compound(id) {
            return Some(items.clone());
        }
        let vocabulary = self.vocabulary;
        if Some(id) == vocabulary.nil {
            return Some(Vec::new());
        }
        let (first, rest) = (vocabulary.first?, vocabulary.rest?);
        let mut items = Vec::new();
        let mut seen = HashSet::new();
        let mut node = id;
        loop {
            if Some(node) == vocabulary.nil {
                return Some(items);
            }
            if let Some(Compound::List(tail)) = self.store.compound(node) {
                items.extend(tail);
                return Some(items);
            }
            if !seen.insert(node) {
                return None;
            }
            let head = self.facts.query(Some(node), Some(first), None);
            let next = self.facts.query(Some(node), Some(rest), None);
            let ([head], [next]) = (head.as_slice(), next.as_slice()) else {
                return None;
            };
            items.push(head.object);
            node = next.object;
        }
    }

    /// Whether two ids denote the same value for a list built-in: the same term, or lists of
    /// pairwise equivalent members however each is written
    pub(crate) fn equivalent(&self, left: u32, right: u32) -> bool {
        if self.same_term(left, right) {
            return true;
        }
        match (self.list_items(left), self.list_items(right)) {
            (Some(a), Some(b)) => {
                a.len() == b.len() && a.iter().zip(&b).all(|(x, y)| self.equivalent(*x, *y))
            }
            _ => false,
        }
    }

    /// Unifies a pattern with a value a built-in computed, reading lists however they are written
    pub(crate) fn unify_value(
        &mut self,
        position: &PTerm,
        id: u32,
        binding: Binding,
    ) -> Vec<Binding> {
        if let Some(known) = self.ground(position, &binding) {
            return if self.equivalent(known, id) {
                vec![binding]
            } else {
                Vec::new()
            };
        }
        let PTerm::List(patterns) = position else {
            return self.unify(position, id, binding);
        };
        let Some(items) = self.list_items(id) else {
            return Vec::new();
        };
        if items.len() != patterns.len() {
            return Vec::new();
        }
        let mut partial = vec![binding];
        for (pattern, item) in patterns.iter().zip(items) {
            partial = partial
                .into_iter()
                .flat_map(|b| self.unify_value(pattern, item, b))
                .collect();
            if partial.is_empty() {
                break;
            }
        }
        partial
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

    /// The id `position` denotes as data: like [`Matcher::ground`], except that a variable still
    /// unbound inside a formula stays in it as a quoted variable
    pub(crate) fn quote(&mut self, position: &PTerm, binding: &Binding) -> Option<u32> {
        self.quote_in(position, binding, false)
    }

    fn quote_in(&mut self, position: &PTerm, binding: &Binding, inside: bool) -> Option<u32> {
        match position {
            PTerm::Var(name) => match binding.get(name) {
                Some(&id) => Some(id),
                None if inside => Some(self.store.quoted_var(name)),
                None => None,
            },
            PTerm::List(items) => {
                let ids = items
                    .iter()
                    .map(|item| self.quote_in(item, binding, inside))
                    .collect::<Option<Vec<u32>>>()?;
                Some(self.store.list(ids))
            }
            PTerm::Formula(atoms) => {
                let mut triples = Vec::with_capacity(atoms.len());
                for atom in atoms {
                    triples.push(Triple {
                        subject: self.quote_in(&atom.subject, binding, true)?,
                        predicate: self.quote_in(&atom.predicate, binding, true)?,
                        object: self.quote_in(&atom.object, binding, true)?,
                    });
                }
                Some(self.store.formula(triples))
            }
            other => self.ground(other, binding),
        }
    }

    /// The node behind an id
    pub(crate) fn node(&self, id: u32) -> Node {
        self.store.node(id)
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
        if let (Some(Compound::List(a)), Some(Compound::List(b))) =
            (self.store.compound(left), self.store.compound(right))
        {
            return a.len() == b.len() && a.iter().zip(b).all(|(x, y)| self.same_term(*x, *y));
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
