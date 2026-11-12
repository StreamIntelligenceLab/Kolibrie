/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The `log:` built-ins that read a graph: a quoted formula, or the current graph

use crate::builtins::Builtin;
use crate::matcher::Matcher;
use crate::reasoner::N3Reasoner;
use crate::rules::{Atom, PTerm};
use crate::terms::Compound;
use shared::dataset_index::DatasetIndex;
use shared::terms::Binding;
use shared::triple::Triple;
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap};

/// A scoped `log:` built-in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopedOp {
    /// `log:includes`
    Includes,
    /// `log:notIncludes`
    NotIncludes,
    /// `log:collectAllIn`
    CollectAllIn,
    /// `log:forAllIn`
    ForAllIn,
    /// `log:conjunction`
    Conjunction,
    /// `log:conclusion`
    Conclusion,
}

/// Where a clause is matched
enum Scope {
    /// The facts the reasoner holds
    Current,
    /// The triples of a quoted formula
    Quoted(Vec<Triple>),
}

/// The prefix that keeps a quoted formula's variables apart from the rule's own
const QUOTED: &str = "?quoted ";

impl ScopedOp {
    /// The operation a `log:` local name names
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "includes" => ScopedOp::Includes,
            "notIncludes" => ScopedOp::NotIncludes,
            "collectAllIn" => ScopedOp::CollectAllIn,
            "forAllIn" => ScopedOp::ForAllIn,
            "conjunction" => ScopedOp::Conjunction,
            "conclusion" => ScopedOp::Conclusion,
            _ => return None,
        })
    }

    /// Whether the operation reads the whole current graph and so must wait for saturation
    pub(crate) fn reads_whole_graph(self) -> bool {
        matches!(
            self,
            ScopedOp::NotIncludes | ScopedOp::CollectAllIn | ScopedOp::ForAllIn
        )
    }

    /// Whether the atom can run before the premises that might still bind its arguments
    pub(crate) fn forward(self, subject: &PTerm, ground: &dyn Fn(&PTerm) -> bool) -> bool {
        match self {
            ScopedOp::Conjunction | ScopedOp::Conclusion => ground(subject),
            _ => false,
        }
    }

    /// The variables the atom binds when it runs: the result of a collection, the clause of an
    /// inclusion, the object of a conjunction or conclusion
    pub(crate) fn outputs(self, subject: &PTerm, object: &PTerm, out: &mut BTreeSet<String>) {
        match self {
            ScopedOp::CollectAllIn => {
                if let PTerm::List(parts) = subject {
                    if let Some(result) = parts.get(2) {
                        result.variables(out);
                    }
                }
            }
            ScopedOp::Includes | ScopedOp::Conjunction | ScopedOp::Conclusion => {
                object.variables(out);
            }
            ScopedOp::NotIncludes | ScopedOp::ForAllIn => {}
        }
    }

    /// Whether the atom can run at all with what is bound; scoped tests and the built-ins
    /// reading formulas as data always can, once nothing else is left to bind their arguments
    pub(crate) fn ready(self) -> bool {
        true
    }

    /// Every extension of `binding` under which the atom holds
    pub(crate) fn evaluate(
        self,
        m: &mut Matcher<'_>,
        subject: &PTerm,
        object: &PTerm,
        binding: Binding,
    ) -> Vec<Binding> {
        match self {
            ScopedOp::Includes => {
                let scope = scope_of(m, subject, &binding);
                let Some(clause) = clause(m, object, &binding) else {
                    return Vec::new();
                };
                let data_only: Vec<Atom> = clause
                    .into_iter()
                    .map(|mut atom| {
                        atom.builtin = None;
                        atom
                    })
                    .collect();
                let held = match object {
                    PTerm::Var(name) => binding.get(name).map(|&id| (name.clone(), id)),
                    _ => None,
                };
                solve_in(m, &scope, &data_only, binding)
                    .into_iter()
                    .map(|mut solution| {
                        if let Some((name, formula)) = &held {
                            let instance = substitute(m, *formula, &solution);
                            solution.insert(name.clone(), instance);
                        }
                        drop_quoted(solution)
                    })
                    .collect()
            }
            ScopedOp::NotIncludes => {
                let scope = scope_of(m, subject, &binding);
                let Some(mut clause) = clause(m, object, &binding) else {
                    return Vec::new();
                };
                if matches!(scope, Scope::Quoted(_)) {
                    clause.iter_mut().for_each(|atom| atom.builtin = None);
                }
                if solve_in(m, &scope, &clause, binding.clone()).is_empty() {
                    vec![binding]
                } else {
                    Vec::new()
                }
            }
            ScopedOp::CollectAllIn => collect_all_in(m, subject, object, binding),
            ScopedOp::ForAllIn => for_all_in(m, subject, object, binding),
            ScopedOp::Conjunction => conjunction(m, subject, object, binding),
            ScopedOp::Conclusion => conclusion(m, subject, object, binding),
        }
    }
}

/// The graph a scope argument names: a formula's triples, or for anything else the current graph
fn scope_of(m: &mut Matcher<'_>, position: &PTerm, binding: &Binding) -> Scope {
    match m
        .ground(position, binding)
        .and_then(|id| m.store.compound(id).cloned())
    {
        Some(Compound::Formula(triples)) => Scope::Quoted(triples),
        _ => Scope::Current,
    }
}

/// The atoms of a clause argument, whether written in the rule or bound to a quoted formula
fn clause(m: &Matcher<'_>, position: &PTerm, binding: &Binding) -> Option<Vec<Atom>> {
    match position {
        PTerm::Formula(atoms) => Some(atoms.clone()),
        PTerm::Const(id) => formula_atoms(m, *id),
        PTerm::Var(name) => formula_atoms(m, *binding.get(name)?),
        _ => None,
    }
}

/// The triples of a formula id as atoms, its quoted variables read as variables again
fn formula_atoms(m: &Matcher<'_>, id: u32) -> Option<Vec<Atom>> {
    let Some(Compound::Formula(triples)) = m.store.compound(id) else {
        return None;
    };
    Some(
        triples
            .iter()
            .map(|t| {
                let mut atom =
                    Atom::new(open(m, t.subject), open(m, t.predicate), open(m, t.object));
                atom.builtin = m
                    .store
                    .dictionary
                    .decode(t.predicate)
                    .and_then(Builtin::from_iri);
                atom
            })
            .collect(),
    )
}

/// A term as a pattern: its quoted variables become variables apart from the rule's own
fn open(m: &Matcher<'_>, id: u32) -> PTerm {
    match m.store.compound(id) {
        Some(Compound::Var(name)) => PTerm::Var(format!("{QUOTED}{name}")),
        Some(Compound::List(items)) => {
            let items: Vec<PTerm> = items.iter().map(|&item| open(m, item)).collect();
            if items.iter().all(|item| matches!(item, PTerm::Const(_))) {
                PTerm::Const(id)
            } else {
                PTerm::List(items)
            }
        }
        _ => PTerm::Const(id),
    }
}

/// A term with the quoted variables a clause solution bound replaced by their values
fn substitute(m: &mut Matcher<'_>, id: u32, solution: &Binding) -> u32 {
    match m.store.compound(id).cloned() {
        Some(Compound::Var(name)) => solution
            .get(&format!("{QUOTED}{name}"))
            .copied()
            .unwrap_or(id),
        Some(Compound::List(items)) => {
            let items = items
                .iter()
                .map(|&item| substitute(m, item, solution))
                .collect();
            m.store.list(items)
        }
        Some(Compound::Formula(triples)) => {
            let triples = triples
                .iter()
                .map(|t| Triple {
                    subject: substitute(m, t.subject, solution),
                    predicate: substitute(m, t.predicate, solution),
                    object: substitute(m, t.object, solution),
                })
                .collect();
            m.store.formula(triples)
        }
        Some(Compound::Logic(_)) | None => id,
    }
}

/// Removes the bindings of quoted variables, which belong to the clause and not the rule
fn drop_quoted(mut binding: Binding) -> Binding {
    binding.retain(|name, _| !name.starts_with(QUOTED));
    binding
}

/// Solves a clause in a scope, extending `binding`
fn solve_in(m: &mut Matcher<'_>, scope: &Scope, clause: &[Atom], binding: Binding) -> Vec<Binding> {
    match scope {
        Scope::Current => m.solve(clause, None, binding),
        Scope::Quoted(triples) => {
            let mut index = DatasetIndex::new();
            for triple in triples {
                index.insert(triple);
            }
            let sequence: HashMap<Triple, usize> = triples
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, t)| (t, i))
                .collect();
            let no_rules = DatasetIndex::new();
            let no_lists = BTreeSet::new();
            let mut inner = Matcher {
                store: &mut *m.store,
                facts: &index,
                rule_facts: &no_rules,
                list_terms: &no_lists,
                sequence: &sequence,
                vocabulary: m.vocabulary,
                backward: &[],
                goals: Vec::new(),
                cut_off: m.cut_off,
                proofs: m.proofs,
                cuts: m.cuts,
                max_depth: m.max_depth,
                clock: m.clock,
                in_rule: Cell::new(false),
            };
            inner.solve(clause, None, binding)
        }
    }
}

/// `(template clause result) log:collectAllIn scope`: the list of every template instance
fn collect_all_in(
    m: &mut Matcher<'_>,
    subject: &PTerm,
    object: &PTerm,
    binding: Binding,
) -> Vec<Binding> {
    let PTerm::List(parts) = subject else {
        return Vec::new();
    };
    let [template, clause_term, result] = parts.as_slice() else {
        return Vec::new();
    };
    let scope = scope_of(m, object, &binding);
    let Some(clause) = clause(m, clause_term, &binding) else {
        return Vec::new();
    };
    let mut collected = Vec::new();
    for solution in solve_in(m, &scope, &clause, binding.clone()) {
        match m.ground(template, &solution) {
            Some(value) => collected.push(value),
            None => return Vec::new(),
        }
    }
    let list = m.store.list(collected);
    m.unify_value(result, list, binding)
}

/// `(generator condition) log:forAllIn scope`: every generator solution satisfies the condition
fn for_all_in(
    m: &mut Matcher<'_>,
    subject: &PTerm,
    object: &PTerm,
    binding: Binding,
) -> Vec<Binding> {
    let PTerm::List(parts) = subject else {
        return Vec::new();
    };
    let [generator, condition] = parts.as_slice() else {
        return Vec::new();
    };
    let scope = scope_of(m, object, &binding);
    let (Some(generator), Some(condition)) = (
        clause(m, generator, &binding),
        clause(m, condition, &binding),
    ) else {
        return Vec::new();
    };
    for solution in solve_in(m, &scope, &generator, binding.clone()) {
        if solve_in(m, &scope, &condition, solution).is_empty() {
            return Vec::new();
        }
    }
    vec![binding]
}

/// `(f1 f2 …) log:conjunction f`: the union of the formulas' triples
fn conjunction(
    m: &mut Matcher<'_>,
    subject: &PTerm,
    object: &PTerm,
    binding: Binding,
) -> Vec<Binding> {
    let Some(id) = m.quote(subject, &binding) else {
        return Vec::new();
    };
    let parts = match m.store.compound(id) {
        Some(Compound::Formula(_)) => vec![id],
        _ => match m.list_items(id) {
            Some(items) => items,
            None => return Vec::new(),
        },
    };
    let mut triples = Vec::new();
    for part in parts {
        let Some(Compound::Formula(part)) = m.store.compound(part) else {
            return Vec::new();
        };
        triples.extend(part.iter().cloned());
    }
    let merged = m.store.formula(triples);
    m.unify_value(object, merged, binding)
}

/// `f log:conclusion c`: `c` is `f` together with everything its own rules derive from it
fn conclusion(
    m: &mut Matcher<'_>,
    subject: &PTerm,
    object: &PTerm,
    binding: Binding,
) -> Vec<Binding> {
    let Some(id) = m.quote(subject, &binding) else {
        return Vec::new();
    };
    let Some(Compound::Formula(input)) = m.store.compound(id).cloned() else {
        return Vec::new();
    };
    let mut nested = N3Reasoner::with_store(m.store.clone());
    if nested.load_triples(&input).is_err() || nested.run().is_err() {
        return Vec::new();
    }
    let mut closure = input;
    for triple in nested.derived().to_vec() {
        let statement = nested.store.statement(&triple);
        closure.push(m.store.encode_statement(&statement));
    }
    let closure = m.store.formula(closure);
    m.unify_value(object, closure, binding)
}
