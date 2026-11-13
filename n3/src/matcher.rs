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
use crate::rules::{Atom, N3Rule, PTerm};
use crate::syntax::Node;
use crate::terms::{Compound, TermStore};
use shared::dataset_index::DatasetIndex;
use shared::id_hash::TermBuildHasher;
use shared::pattern::matches_rule_pattern;
use shared::terms::Binding;
use shared::triple::Triple;
use std::cell::{Cell, OnceCell, RefCell};
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

/// The facts new in a round, which semi-naive evaluation matches one premise against
#[derive(Debug, Clone, Copy)]
pub(crate) enum Delta<'d> {
    /// Many new facts, grouped by the constants of the premises that read them
    Grouped(&'d DeltaGroups),
    /// A few new facts, scanned in the order they were added
    Facts(&'d [Triple]),
}

impl<'d> Delta<'d> {
    /// The new facts that agree with these constants, in the order they were added
    pub(crate) fn matching(self, constants: [Option<u32>; 3]) -> &'d [Triple] {
        match self {
            Delta::Facts(all) => all,
            Delta::Grouped(groups) => groups.matching(constants),
        }
    }
}

/// The position each fact was added at
pub(crate) type Sequence = HashMap<Triple, usize, TermBuildHasher>;

/// A round's new facts, grouped once for each shape of constants a premise looks them up by
#[derive(Debug, Default)]
pub(crate) struct DeltaGroups {
    all: Vec<Triple>,
    /// By which positions are constant, then by those constants' values
    by_shape: HashMap<u8, HashMap<[u32; 3], Vec<Triple>, TermBuildHasher>>,
}

impl DeltaGroups {
    /// Groups `facts`, kept in order, for every shape in `shapes`
    pub(crate) fn new(facts: &[Triple], shapes: &BTreeSet<u8>) -> Self {
        let mut by_shape: HashMap<u8, HashMap<[u32; 3], Vec<Triple>, TermBuildHasher>> =
            HashMap::new();
        for &shape in shapes.iter().filter(|&&shape| shape != 0) {
            let groups = by_shape.entry(shape).or_default();
            for fact in facts {
                groups
                    .entry(shape_key(
                        shape,
                        [fact.subject, fact.predicate, fact.object],
                    ))
                    .or_default()
                    .push(fact.clone());
            }
        }
        DeltaGroups {
            all: facts.to_vec(),
            by_shape,
        }
    }

    fn matching(&self, constants: [Option<u32>; 3]) -> &[Triple] {
        let shape = shape_of(constants);
        if shape == 0 {
            return &self.all;
        }
        let key = shape_key(shape, constants.map(|c| c.unwrap_or(0)));
        match self.by_shape.get(&shape) {
            Some(groups) => groups.get(&key).map_or(&[], Vec::as_slice),
            // A shape no candidate premise has: nothing grouped it, so every fact is a candidate.
            None => &self.all,
        }
    }
}

/// Which of the three positions hold a constant, one bit each
pub(crate) fn shape_of(constants: [Option<u32>; 3]) -> u8 {
    constants
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_some())
        .fold(0, |shape, (i, _)| shape | (1 << i))
}

fn shape_key(shape: u8, ids: [u32; 3]) -> [u32; 3] {
    [0, 1, 2].map(|i| if shape & (1 << i) != 0 { ids[i] } else { 0 })
}

/// The constants a premise's positions hold, `None` for any other position
pub(crate) fn atom_constants(atom: &Atom) -> [Option<u32>; 3] {
    atom.positions().map(|position| match position {
        PTerm::Const(id) => Some(*id),
        _ => None,
    })
}

/// The term a logic variable stands for under `binding`, following chains; any other id as is
pub(crate) fn deref(store: &TermStore, mut id: u32, binding: &Binding) -> u32 {
    if !store.has_logic_vars() {
        return id;
    }
    while let Some(Compound::Logic(_)) = store.compound(id) {
        match binding.get(&TermStore::logic_key(id)) {
            Some(&next) => id = next,
            None => break,
        }
    }
    id
}

/// Whether `id` is, under `binding`, a logic variable nothing has bound yet
pub(crate) fn is_open(store: &TermStore, id: u32, binding: &Binding) -> bool {
    store.has_logic_vars()
        && matches!(
            store.compound(deref(store, id, binding)),
            Some(Compound::Logic(_))
        )
}

/// Whether an open logic variable occurs anywhere in `id` under `binding`
pub(crate) fn contains_open(store: &TermStore, id: u32, binding: &Binding) -> bool {
    if !store.has_logic_vars() {
        return false;
    }
    let id = deref(store, id, binding);
    match store.compound(id) {
        Some(Compound::Logic(_)) => true,
        Some(Compound::List(items)) => items.iter().any(|&i| contains_open(store, i, binding)),
        Some(Compound::Formula(triples)) => triples.iter().any(|t| {
            [t.subject, t.predicate, t.object]
                .into_iter()
                .any(|i| contains_open(store, i, binding))
        }),
        _ => false,
    }
}

/// `id` with every logic variable `binding` binds replaced by its value, at any depth
pub(crate) fn resolve(store: &mut TermStore, id: u32, binding: &Binding) -> u32 {
    if !store.has_logic_vars() {
        return id;
    }
    let id = deref(store, id, binding);
    match store.compound(id).cloned() {
        Some(Compound::List(items)) => {
            let resolved: Vec<u32> = items.iter().map(|&i| resolve(store, i, binding)).collect();
            if resolved == items {
                id
            } else {
                store.list(resolved)
            }
        }
        Some(Compound::Formula(triples)) => {
            let resolved: Vec<Triple> = triples
                .iter()
                .map(|t| Triple {
                    subject: resolve(store, t.subject, binding),
                    predicate: resolve(store, t.predicate, binding),
                    object: resolve(store, t.object, binding),
                })
                .collect();
            if resolved == triples {
                id
            } else {
                store.formula(resolved)
            }
        }
        _ => id,
    }
}

/// The ground term `id` stands for under `binding`, or `None` while a logic variable in it is open
pub(crate) fn settle(store: &mut TermStore, id: u32, binding: &Binding) -> Option<u32> {
    let id = resolve(store, id, binding);
    (!contains_open(store, id, binding)).then_some(id)
}

/// The fact base and term store one join reads, borrowed for the join's duration
#[derive(Debug)]
pub(crate) struct Matcher<'a> {
    pub(crate) store: &'a mut TermStore,
    pub(crate) facts: &'a DatasetIndex,
    pub(crate) rule_facts: &'a DatasetIndex,
    /// The non-empty native lists occurring in facts, which `rdf:first`/`rdf:rest` also match
    pub(crate) list_terms: &'a BTreeSet<u32>,
    /// The order facts were added in, which lookups keep so joins are deterministic
    pub(crate) sequence: &'a Sequence,
    pub(crate) vocabulary: Vocabulary,
    /// Rules proved on demand when a premise asks for their head
    pub(crate) backward: &'a [N3Rule],
    /// The backward goals being proved, innermost last, so a goal never proves itself
    pub(crate) goals: Vec<(usize, [Option<u32>; 3])>,
    /// Set when a proof was cut off at `max_depth`, so the answer may be incomplete
    pub(crate) cut_off: &'a Cell<bool>,
    /// Proved goals and their answers, one triple per proof, valid while the facts are unchanged
    pub(crate) proofs: &'a RefCell<HashMap<[Option<u32>; 3], Vec<Triple>>>,
    /// How many proofs were cut short by the cycle check or the depth limit, so a goal proved
    /// meanwhile is not cached as complete
    pub(crate) cuts: &'a Cell<usize>,
    /// How deep backward rules may call each other
    pub(crate) max_depth: usize,
    /// The instant `time:localTime` reports, read once per run
    pub(crate) clock: &'a OnceCell<(i64, u32)>,
    /// Set while a premise is matched against a rule stored as data, whose own variables may
    /// then stand for any term
    pub(crate) in_rule: Cell<bool>,
}

/// The prefix of the binding keys for a stored rule's variables during one match
const RULE_VAR: &str = "?rule ";

impl Matcher<'_> {
    /// Joins `atoms`, matching the atom `seed.0` against `seed.1` and the rest against the store
    pub(crate) fn solve(
        &mut self,
        atoms: &[Atom],
        seed: Option<(usize, Delta<'_>)>,
        initial: Binding,
    ) -> Vec<Binding> {
        let mut remaining: Vec<usize> = (0..atoms.len()).collect();
        // A variable bound to an open logic variable is not known yet, though it may sit inside a
        // list that a built-in takes apart.
        let open: BTreeSet<String> = initial
            .iter()
            .filter(|(_, &id)| is_open(self.store, id, &initial))
            .map(|(name, _)| name.clone())
            .collect();
        let mut bound: BTreeSet<String> = initial
            .keys()
            .filter(|name| !open.contains(*name))
            .cloned()
            .collect();
        let mut bindings = vec![initial];
        // Each atom's variables, whole and by position, worked out once for the whole join.
        let position_vars: Vec<[BTreeSet<String>; 3]> = atoms
            .iter()
            .map(|atom| {
                atom.positions().map(|position| {
                    let mut vars = BTreeSet::new();
                    position.variables(&mut vars);
                    vars
                })
            })
            .collect();
        let atom_vars: Vec<BTreeSet<String>> = position_vars
            .iter()
            .map(|[s, p, o]| s.iter().chain(p).chain(o).cloned().collect())
            .collect();
        // How many of each atom's variables, and of each position's, are still unbound, kept up to
        // date as variables bind, so picking the next atom costs no set comparisons.
        let mut open_atom: Vec<usize> = atom_vars
            .iter()
            .map(|vars| vars.difference(&bound).count())
            .collect();
        let mut open_position: Vec<[usize; 3]> = position_vars
            .iter()
            .map(|vars| vars.each_ref().map(|v| v.difference(&bound).count()))
            .collect();
        let mut in_atoms: HashMap<&str, Vec<usize>> = HashMap::new();
        let mut in_positions: HashMap<&str, Vec<(usize, usize)>> = HashMap::new();
        for (i, positions) in position_vars.iter().enumerate() {
            for v in &atom_vars[i] {
                in_atoms.entry(v.as_str()).or_default().push(i);
            }
            for (k, vars) in positions.iter().enumerate() {
                for v in vars {
                    in_positions.entry(v.as_str()).or_default().push((i, k));
                }
            }
        }
        let mark = |vars: &BTreeSet<String>,
                    bound: &mut BTreeSet<String>,
                    open_atom: &mut Vec<usize>,
                    open_position: &mut Vec<[usize; 3]>| {
            for v in vars {
                if !bound.insert(v.clone()) {
                    continue;
                }
                for &i in in_atoms.get(v.as_str()).into_iter().flatten() {
                    open_atom[i] -= 1;
                }
                for &(i, k) in in_positions.get(v.as_str()).into_iter().flatten() {
                    open_position[i][k] -= 1;
                }
            }
        };
        if let Some((first, delta)) = seed {
            remaining.retain(|&i| i != first);
            let candidates = delta.matching(atom_constants(&atoms[first]));
            bindings = self.scan(&atoms[first], candidates, bindings);
            mark(
                &atom_vars[first],
                &mut bound,
                &mut open_atom,
                &mut open_position,
            );
        }
        while !remaining.is_empty() && !bindings.is_empty() {
            let is_ground = |position: &PTerm| {
                let mut vars = BTreeSet::new();
                position.variables(&mut vars);
                let known = |v: &String| {
                    bound.contains(v) || (open.contains(v) && !matches!(position, PTerm::Var(_)))
                };
                vars.iter().all(known) && !position.has_exists()
            };
            let builtin_where = |test: &dyn Fn(Builtin, &Atom) -> bool| {
                remaining
                    .iter()
                    .position(|&i| atoms[i].builtin.is_some_and(|b| test(b, &atoms[i])))
            };
            let forward = builtin_where(&|b, a| b.forward(&a.subject, &a.object, &is_ground));
            let backward = self.backward;
            let sample = &bindings[0];
            let settled = |i: usize| open_atom[i] == 0 || !asks(backward, &atoms[i], sample);
            // Settled premises go most-bound first, the earliest on a tie; open backward goals go
            // in the order written, since each one's inputs come from those before it.
            let data = |wanted: bool| {
                let mut candidates = remaining
                    .iter()
                    .enumerate()
                    .filter(|(_, &i)| atoms[i].builtin.is_none() && settled(i) == wanted);
                if wanted {
                    candidates
                        .min_by_key(|(position, &i)| {
                            let fixed = atoms[i]
                                .positions()
                                .into_iter()
                                .zip(open_position[i])
                                .filter(|(position, open)| {
                                    *open == 0
                                        && !matches!(position, PTerm::Exists(_) | PTerm::Formula(_))
                                })
                                .count();
                            (std::cmp::Reverse(fixed), *position)
                        })
                        .map(|(position, _)| position)
                } else {
                    candidates.next().map(|(position, _)| position)
                }
            };
            let reverse = || {
                builtin_where(&|b, a| {
                    !matches!(b, Builtin::Scoped(_)) && b.ready(&a.subject, &a.object, &is_ground)
                })
            };
            let scoped = || {
                builtin_where(&|b, a| {
                    matches!(b, Builtin::Scoped(_)) && b.ready(&a.subject, &a.object, &is_ground)
                })
            };
            // A scoped test can run once every variable it shares with the rest of the premise is
            // bound; its own clause variables and its outputs need not be.
            let inputs_bound = |index: usize| {
                let atom = &atoms[index];
                let Some(Builtin::Scoped(op)) = atom.builtin else {
                    return false;
                };
                let mut outputs = BTreeSet::new();
                op.outputs(&atom.subject, &atom.object, &mut outputs);
                let mut mine = BTreeSet::new();
                atom.variables(&mut mine);
                remaining.iter().filter(|&&i| i != index).all(|&other| {
                    let mut theirs = BTreeSet::new();
                    atoms[other].variables(&mut theirs);
                    theirs
                        .intersection(&mine)
                        .all(|v| bound.contains(v) || outputs.contains(v))
                })
            };
            let scoped_ready = || remaining.iter().position(|&i| inputs_bound(i));
            // A premise only backward rules can answer goes late while its inputs are open, so the
            // proof starts with every constraint its other premises can supply; a scoped test goes
            // last of all, because it reads whatever the rest of the premise binds.
            let Some(pick) = forward
                .or_else(|| data(true))
                .or_else(reverse)
                .or_else(scoped_ready)
                .or_else(|| data(false))
                .or_else(scoped)
            else {
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
                    if self.backward.is_empty() {
                        self.extend(atom, sources, true, bindings)
                    } else {
                        // Each fact and each proof is its own solution, as in eyeron, so a
                        // collection over proofs counts them all.
                        let mut out = Vec::new();
                        for binding in bindings {
                            out.extend(self.extend(atom, sources, true, vec![binding.clone()]));
                            out.extend(self.prove(atom, &binding));
                        }
                        out
                    }
                }
            };
            mark(
                &atom_vars[next],
                &mut bound,
                &mut open_atom,
                &mut open_position,
            );
        }
        bindings
    }

    /// Every extension of `binding` under which a backward rule proves `goal`, one per proof,
    /// reusing an earlier proof of the same goal over the same facts
    fn prove(&mut self, goal: &Atom, binding: &Binding) -> Vec<Binding> {
        if self.backward.is_empty() {
            return Vec::new();
        }
        let open_or_ground = |position: &PTerm| match position {
            PTerm::Var(_) => true,
            other => {
                let mut vars = BTreeSet::new();
                other.variables(&mut vars);
                vars.iter().all(|v| binding.contains_key(v))
            }
        };
        let keys = [
            self.ground(&goal.subject, binding),
            self.ground(&goal.predicate, binding),
            self.ground(&goal.object, binding),
        ];
        let cacheable = goal.positions().into_iter().all(open_or_ground)
            && keys
                .iter()
                .flatten()
                .all(|&key| !contains_open(self.store, key, binding));
        if cacheable {
            let cached = self.proofs.borrow().get(&keys).cloned();
            if let Some(proved) = cached {
                return proved
                    .into_iter()
                    .flat_map(|triple| self.unify_triple(goal, &triple, binding.clone()))
                    .collect();
            }
        }
        let cuts = self.cuts.get();
        let answers = self.prove_uncached(goal, binding, keys);
        if cacheable && self.cuts.get() == cuts {
            let proved: Vec<Triple> = answers
                .iter()
                .filter_map(|answer| {
                    Some(Triple {
                        subject: self.ground(&goal.subject, answer)?,
                        predicate: self.ground(&goal.predicate, answer)?,
                        object: self.ground(&goal.object, answer)?,
                    })
                })
                .collect();
            let ground_answers = proved.iter().all(|t| {
                [t.subject, t.predicate, t.object]
                    .into_iter()
                    .all(|id| !contains_open(self.store, id, &Binding::new()))
            });
            if proved.len() == answers.len() && ground_answers {
                self.proofs.borrow_mut().insert(keys, proved);
            }
        }
        answers
    }

    /// One backward proof of a ground goal, for a proof to cite: the rule's index, the solution
    /// of its body, and the body as solved, its variables renamed apart
    pub(crate) fn trace(&mut self, goal: &Triple) -> Option<(usize, Binding, Vec<Atom>)> {
        let keys = [Some(goal.subject), Some(goal.predicate), Some(goal.object)];
        let backward = self.backward;
        for (rule_index, rule) in backward.iter().enumerate() {
            if self.goals.contains(&(rule_index, keys)) {
                continue;
            }
            let depth = self.goals.len();
            let rename = |name: &str| format!("#{depth}#{name}");
            for head in &rule.head {
                let head = head.renamed(&rename);
                if head.has_exists() {
                    continue;
                }
                let body: Vec<Atom> = rule.body.iter().map(|a| a.renamed(&rename)).collect();
                let mut starts = vec![Binding::new()];
                for (position, id) in
                    head.positions()
                        .into_iter()
                        .zip([goal.subject, goal.predicate, goal.object])
                {
                    starts = starts
                        .into_iter()
                        .flat_map(|b| self.unify_value(position, id, b))
                        .collect();
                }
                let mut head_vars = BTreeSet::new();
                head.variables(&mut head_vars);
                for mut start in starts {
                    for name in &head_vars {
                        if !start.contains_key(name) {
                            let logic = self.store.fresh_logic_var();
                            start.insert(name.clone(), logic);
                        }
                    }
                    self.goals.push((rule_index, keys));
                    let solution = self.solve(&body, None, start).into_iter().next();
                    self.goals.pop();
                    if let Some(solution) = solution {
                        return Some((rule_index, solution, body));
                    }
                }
            }
        }
        None
    }

    /// [`Matcher::prove`] without the cache
    fn prove_uncached(
        &mut self,
        goal: &Atom,
        binding: &Binding,
        keys: [Option<u32>; 3],
    ) -> Vec<Binding> {
        let mut out = Vec::new();
        let backward = self.backward;
        for (rule_index, rule) in backward.iter().enumerate() {
            if self.goals.contains(&(rule_index, keys)) {
                self.cuts.set(self.cuts.get() + 1);
                continue;
            }
            let depth = self.goals.len();
            let rename = |name: &str| format!("#{depth}#{name}");
            for head in &rule.head {
                if !goal
                    .positions()
                    .into_iter()
                    .zip(head.positions())
                    .all(|(g, h)| compatible_position(g, h))
                {
                    continue;
                }
                if depth >= self.max_depth {
                    self.cut_off.set(true);
                    self.cuts.set(self.cuts.get() + 1);
                    continue;
                }
                let mut witnesses = Binding::new();
                let head = open_exists(&head.renamed(&rename), &mut |label| {
                    let name = format!("#{depth}#!{label}");
                    let spelling = format!(
                        "_:kolibrie-n3-.w{rule_index}-{label}-{}",
                        keys.map(|k| k.map_or_else(|| "?".to_string(), |id| id.to_string()))
                            .join("-")
                    );
                    let id = self.store.dictionary.encode(&spelling);
                    witnesses.insert(name.clone(), id);
                    name
                });
                let body: Vec<Atom> = rule.body.iter().map(|a| a.renamed(&rename)).collect();
                let mut starts = vec![witnesses];
                for (position, key) in head.positions().into_iter().zip(keys) {
                    if let Some(id) = key {
                        starts = starts
                            .into_iter()
                            .flat_map(|b| self.unify_value(position, id, b))
                            .collect();
                    }
                }
                let mut head_vars = BTreeSet::new();
                head.variables(&mut head_vars);
                for mut start in starts {
                    // A head variable the goal leaves open is a logic variable the proof may bind,
                    // or leave open for the caller to bind later.
                    for name in &head_vars {
                        if !start.contains_key(name) {
                            let logic = self.store.fresh_logic_var();
                            start.insert(name.clone(), logic);
                        }
                    }
                    self.goals.push((rule_index, keys));
                    let solutions = self.solve(&body, None, start);
                    self.goals.pop();
                    for solution in solutions {
                        let mut carried = binding.clone();
                        for (name, &value) in &solution {
                            if name.starts_with("#lv") {
                                carried.insert(name.clone(), value);
                            }
                        }
                        let mut answers = vec![carried];
                        for ((goal_position, head_position), key) in
                            goal.positions().into_iter().zip(head.positions()).zip(keys)
                        {
                            if key.is_some() {
                                continue;
                            }
                            let Some(value) = self.value_of(head_position, &solution) else {
                                answers.clear();
                                break;
                            };
                            answers = answers
                                .into_iter()
                                .flat_map(|b| self.unify_value(goal_position, value, b))
                                .collect();
                        }
                        out.extend(answers);
                    }
                }
            }
        }
        out
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
            let may_hold_rules = indexes.len() > 1;
            if !may_hold_rules {
                candidates.retain(|fact| {
                    atom.positions()
                        .into_iter()
                        .zip([fact.subject, fact.predicate, fact.object])
                        .all(|(position, id)| self.could_match(position, id, &binding))
                });
            }
            self.in_order(&mut candidates);
            for fact in candidates {
                let from_rule = may_hold_rules
                    && !self
                        .rule_facts
                        .query(Some(fact.subject), Some(fact.predicate), Some(fact.object))
                        .is_empty();
                if from_rule {
                    self.in_rule.set(true);
                    let matched = self.unify_triple(atom, &fact, binding.clone());
                    self.in_rule.set(false);
                    out.extend(matched.into_iter().map(|mut b| {
                        b.retain(|name, _| !name.starts_with(RULE_VAR));
                        b
                    }));
                    continue;
                }
                if let Some(flat) = atom.flat.as_ref().filter(|_| !self.store.has_logic_vars()) {
                    let mut extended = binding.clone();
                    if matches_rule_pattern(flat, &fact, &mut extended) {
                        out.push(extended);
                    }
                    continue;
                }
                let possible = atom
                    .positions()
                    .into_iter()
                    .zip([fact.subject, fact.predicate, fact.object])
                    .all(|(position, id)| self.could_match(position, id, &binding));
                if possible {
                    out.extend(self.unify_triple(atom, &fact, binding.clone()));
                }
            }
        }
        out
    }

    /// Extends each binding with every way `atom` matches one of `facts`, without an index
    fn scan(&self, atom: &Atom, facts: &[Triple], bindings: Vec<Binding>) -> Vec<Binding> {
        let mut out = Vec::new();
        for binding in bindings {
            for fact in facts {
                let possible = atom
                    .positions()
                    .into_iter()
                    .zip([fact.subject, fact.predicate, fact.object])
                    .all(|(position, id)| self.could_match(position, id, &binding));
                if !possible {
                    continue;
                }
                match atom.flat.as_ref().filter(|_| !self.store.has_logic_vars()) {
                    Some(flat) => {
                        let mut extended = binding.clone();
                        if matches_rule_pattern(flat, fact, &mut extended) {
                            out.push(extended);
                        }
                    }
                    None => out.extend(self.unify_triple(atom, fact, binding.clone())),
                }
            }
        }
        out
    }

    /// Sorts facts into the order they were added, unknown ones last in a fixed order
    pub(crate) fn in_order(&self, facts: &mut [Triple]) {
        if facts.len() > 1 {
            facts.sort_by_cached_key(|t| {
                (
                    self.sequence.get(t).copied().unwrap_or(usize::MAX),
                    t.clone(),
                )
            });
        }
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
            if contains_open(self.store, known, &binding) || contains_open(self.store, id, &binding)
            {
                return self.unify_ids(known, id, binding).into_iter().collect();
            }
            return if self.equivalent(known, id) {
                vec![binding]
            } else {
                Vec::new()
            };
        }
        let PTerm::List(patterns) = position else {
            return self.unify(position, id, binding);
        };
        let id = deref(self.store, id, &binding);
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

    /// A cheap test, without allocating, that rules out ids `position` cannot match under
    /// `binding`: a different constant or bound value, or a list of another shape
    fn could_match(&self, position: &PTerm, id: u32, binding: &Binding) -> bool {
        match position {
            PTerm::Const(c) => *c == id,
            PTerm::Var(name) => binding.get(name).is_none_or(|&bound| {
                bound == id
                    || deref(self.store, bound, binding) == id
                    || contains_open(self.store, bound, binding)
            }),
            PTerm::List(items) => match self.store.compound(id) {
                Some(Compound::List(members)) => {
                    members.len() == items.len()
                        && items
                            .iter()
                            .zip(members)
                            .all(|(item, &member)| self.could_match(item, member, binding))
                }
                _ => false,
            },
            PTerm::Formula(_) | PTerm::Exists(_) => true,
        }
    }

    /// The id a position must equal under `binding`, if it can be known without a lookup scan
    fn key(&self, position: &PTerm, binding: &Binding) -> Key {
        match position {
            PTerm::Const(id) => Key::Bound(*id),
            PTerm::Var(name) => match binding.get(name) {
                Some(&id) if contains_open(self.store, id, binding) => Key::Free,
                Some(&id) => Key::Bound(deref(self.store, id, binding)),
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
                    return vec![binding];
                }
                if self.store.has_logic_vars() {
                    if let Some(unified) = self.unify_ids(*c, id, binding.clone()) {
                        return vec![unified];
                    }
                }
                let Some(Compound::Var(name)) = self
                    .in_rule
                    .get()
                    .then(|| self.store.compound(id))
                    .flatten()
                else {
                    return Vec::new();
                };
                let key = format!("{RULE_VAR}{name}");
                match binding.get(&key) {
                    Some(&bound) if bound != *c => Vec::new(),
                    Some(_) => vec![binding],
                    None => {
                        binding.insert(key, *c);
                        vec![binding]
                    }
                }
            }
            PTerm::Var(name) => match binding.get(name) {
                Some(&bound) if bound != id && self.store.has_logic_vars() => {
                    match self.unify_ids(bound, id, binding.clone()) {
                        Some(unified) => vec![unified],
                        None if self.in_rule.get() => self.unify(&PTerm::Const(bound), id, binding),
                        None => Vec::new(),
                    }
                }
                Some(&bound) if bound != id => {
                    if self.in_rule.get() {
                        self.unify(&PTerm::Const(bound), id, binding)
                    } else {
                        Vec::new()
                    }
                }
                Some(_) => vec![binding],
                None => {
                    binding.insert(name.clone(), id);
                    vec![binding]
                }
            },
            PTerm::List(items) => {
                let id = deref(self.store, id, &binding);
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

    /// The id `position` denotes under `binding`, interning lists and formulas it builds; `None`
    /// when it is unbound or an open logic variable, though a list may hold open ones
    pub(crate) fn ground(&mut self, position: &PTerm, binding: &Binding) -> Option<u32> {
        self.ground_in(position, binding, false)
    }

    /// Like [`Matcher::ground`], but an open logic variable is a value too
    fn value_of(&mut self, position: &PTerm, binding: &Binding) -> Option<u32> {
        self.ground_in(position, binding, true)
    }

    fn ground_in(&mut self, position: &PTerm, binding: &Binding, open_ok: bool) -> Option<u32> {
        match position {
            PTerm::Const(id) => Some(*id),
            PTerm::Var(name) => {
                let id = *binding.get(name)?;
                if !open_ok && is_open(self.store, id, binding) {
                    return None;
                }
                Some(resolve(self.store, id, binding))
            }
            PTerm::List(items) => {
                let ids = items
                    .iter()
                    .map(|item| self.ground_in(item, binding, true))
                    .collect::<Option<Vec<u32>>>()?;
                Some(self.store.list(ids))
            }
            PTerm::Formula(atoms) => {
                let mut triples = Vec::with_capacity(atoms.len());
                for atom in atoms {
                    triples.push(Triple {
                        subject: self.ground_in(&atom.subject, binding, true)?,
                        predicate: self.ground_in(&atom.predicate, binding, true)?,
                        object: self.ground_in(&atom.object, binding, true)?,
                    });
                }
                Some(self.store.formula(triples))
            }
            PTerm::Exists(_) => None,
        }
    }

    /// Unifies two terms that may hold open logic variables, binding those as needed
    pub(crate) fn unify_ids(&self, left: u32, right: u32, mut binding: Binding) -> Option<Binding> {
        let (left, right) = (
            deref(self.store, left, &binding),
            deref(self.store, right, &binding),
        );
        if left == right {
            return Some(binding);
        }
        for (open, value) in [(left, right), (right, left)] {
            if let Some(Compound::Logic(_)) = self.store.compound(open) {
                binding.insert(TermStore::logic_key(open), value);
                return Some(binding);
            }
        }
        if let (Some(Compound::List(a)), Some(Compound::List(b))) =
            (self.store.compound(left), self.store.compound(right))
        {
            if a.len() != b.len() {
                return None;
            }
            let pairs: Vec<(u32, u32)> = a.iter().copied().zip(b.iter().copied()).collect();
            for (x, y) in pairs {
                binding = self.unify_ids(x, y, binding)?;
            }
            return Some(binding);
        }
        self.equivalent(left, right).then_some(binding)
    }

    /// The id `position` denotes as data: like [`Matcher::ground`], except that a variable still
    /// unbound inside a formula stays in it as a quoted variable
    pub(crate) fn quote(&mut self, position: &PTerm, binding: &Binding) -> Option<u32> {
        self.quote_in(position, binding, false)
    }

    /// Like [`Matcher::quote`], but every unbound variable stays as a quoted variable, as a
    /// proof records a scoped test whose clause variables are its own
    pub(crate) fn quote_all(&mut self, position: &PTerm, binding: &Binding) -> Option<u32> {
        self.quote_in(position, binding, true)
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

    /// The run's instant as Unix seconds and milliseconds, read from the system clock on first use
    pub(crate) fn now(&self) -> (i64, u32) {
        *self.clock.get_or_init(|| {
            let elapsed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            (elapsed.as_secs() as i64, elapsed.subsec_millis())
        })
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

/// An atom with each existential replaced by the variable `name` gives it
fn open_exists(atom: &Atom, name: &mut dyn FnMut(&str) -> String) -> Atom {
    fn position(p: &PTerm, name: &mut dyn FnMut(&str) -> String) -> PTerm {
        match p {
            PTerm::Exists(label) => PTerm::Var(name(label)),
            PTerm::List(items) => PTerm::List(items.iter().map(|i| position(i, name)).collect()),
            other => other.clone(),
        }
    }
    Atom::new(
        position(&atom.subject, name),
        position(&atom.predicate, name),
        position(&atom.object, name),
    )
}

/// Whether some backward rule could answer `atom`, with the variables `binding` binds read as
/// their values
fn asks(backward: &[N3Rule], atom: &Atom, binding: &Binding) -> bool {
    let resolve = |position: &PTerm| match position {
        PTerm::Var(name) => binding
            .get(name)
            .map_or_else(|| position.clone(), |&id| PTerm::Const(id)),
        other => other.clone(),
    };
    backward.iter().flat_map(|rule| &rule.head).any(|head| {
        atom.positions()
            .into_iter()
            .zip(head.positions())
            .all(|(g, h)| compatible_position(&resolve(g), h))
    })
}

/// Whether a goal position and a rule head position could denote the same term
fn compatible_position(goal: &PTerm, head: &PTerm) -> bool {
    match (goal, head) {
        (PTerm::Const(a), PTerm::Const(b)) => a == b,
        (PTerm::List(a), PTerm::List(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| compatible_position(x, y))
        }
        _ => true,
    }
}
