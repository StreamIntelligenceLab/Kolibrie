/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! N3 rules compiled to patterns over dictionary ids

use crate::builtins::Builtin;
use crate::error::N3Error;
use crate::syntax::{Node, Statement};
use crate::terms::{Compound, TermStore};
use shared::terms::{Term, TriplePattern};
use std::collections::BTreeSet;

/// One position of a rule atom
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PTerm {
    /// A ground term
    Const(u32),
    /// A rule variable; blank nodes in a rule body are variables too
    Var(String),
    /// A list with a variable somewhere inside
    List(Vec<PTerm>),
    /// A quoted formula with a variable somewhere inside
    Formula(Vec<Atom>),
    /// A blank node in a rule conclusion: an existential witness, fresh per firing
    Exists(String),
}

/// A triple pattern of [`PTerm`]s
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Atom {
    /// The subject
    pub subject: PTerm,
    /// The predicate
    pub predicate: PTerm,
    /// The object
    pub object: PTerm,
    /// The same atom as a Kolibrie [`TriplePattern`], when every position is a constant or a
    /// variable
    pub flat: Option<TriplePattern>,
    /// The built-in the predicate names, for a premise atom that is computed, not looked up
    pub builtin: Option<Builtin>,
}

/// A forward rule `{ body } => { head }`
#[derive(Debug, Clone)]
pub struct N3Rule {
    /// The premise atoms, all of which must match
    pub body: Vec<Atom>,
    /// The conclusion atoms
    pub head: Vec<Atom>,
    /// Whether the conclusion introduces existential witnesses
    pub has_existentials: bool,
    /// Whether the premise tests the whole current graph, so the rule waits for saturation
    pub deferred: bool,
    /// Whether the rule is an inference fuse, `{ body } => false`, forbidding its premise
    pub fuse: bool,
    /// For `{ body } => ?f`, the term whose formula the rule concludes
    pub unquote: Option<PTerm>,
}

impl N3Rule {
    /// Whether the rule waits for ordinary rules to saturate before it fires
    pub fn waits(&self) -> bool {
        self.has_existentials || self.deferred
    }
}

impl PTerm {
    fn flat(&self) -> Option<Term> {
        match self {
            PTerm::Const(id) => Some(Term::Constant(*id)),
            PTerm::Var(name) => Some(Term::Variable(name.clone())),
            _ => None,
        }
    }

    /// Adds every variable in this position to `out`
    pub fn variables(&self, out: &mut BTreeSet<String>) {
        match self {
            PTerm::Var(name) => {
                out.insert(name.clone());
            }
            PTerm::List(items) => items.iter().for_each(|item| item.variables(out)),
            PTerm::Formula(atoms) => atoms.iter().for_each(|atom| atom.variables(out)),
            PTerm::Const(_) | PTerm::Exists(_) => {}
        }
    }

    /// Whether an existential occurs anywhere in this position
    pub(crate) fn has_exists(&self) -> bool {
        match self {
            PTerm::Exists(_) => true,
            PTerm::List(items) => items.iter().any(PTerm::has_exists),
            PTerm::Formula(atoms) => atoms.iter().any(Atom::has_exists),
            _ => false,
        }
    }
}

impl Atom {
    /// An atom from three positions
    pub fn new(subject: PTerm, predicate: PTerm, object: PTerm) -> Self {
        let flat = match (subject.flat(), predicate.flat(), object.flat()) {
            (Some(s), Some(p), Some(o)) => Some((s, p, o)),
            _ => None,
        };
        Atom {
            subject,
            predicate,
            object,
            flat,
            builtin: None,
        }
    }

    /// The three positions in order
    pub fn positions(&self) -> [&PTerm; 3] {
        [&self.subject, &self.predicate, &self.object]
    }

    /// Adds every variable in this atom to `out`
    pub fn variables(&self, out: &mut BTreeSet<String>) {
        for position in self.positions() {
            position.variables(out);
        }
    }

    /// Whether some position holds an existential
    pub(crate) fn has_exists(&self) -> bool {
        self.positions().into_iter().any(PTerm::has_exists)
    }
}

/// Where a node sits in a rule, which decides what its blank nodes and variables mean
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Body,
    Head,
}

struct Compiler<'a> {
    store: &'a mut TermStore,
    side: Side,
    bound: Option<&'a BTreeSet<String>>,
}

impl Compiler<'_> {
    /// Compiles one node; `quoted` is true inside a formula
    fn term(&mut self, node: &Node, quoted: bool) -> PTerm {
        match node {
            Node::Var(name) => match (self.side, self.bound) {
                (Side::Head, Some(bound)) if quoted && !bound.contains(name) => {
                    PTerm::Const(self.store.quoted_var(name))
                }
                _ => PTerm::Var(name.clone()),
            },
            Node::Blank(label) => match (self.side, quoted) {
                (Side::Body, _) => PTerm::Var(format!("_:{label}")),
                (Side::Head, false) => PTerm::Exists(label.clone()),
                (Side::Head, true) => PTerm::Const(self.store.encode(node)),
            },
            Node::List(items) => {
                let compiled: Vec<PTerm> = items.iter().map(|i| self.term(i, quoted)).collect();
                if compiled.iter().all(|c| matches!(c, PTerm::Const(_))) {
                    let ids = compiled
                        .iter()
                        .map(|c| match c {
                            PTerm::Const(id) => *id,
                            _ => unreachable!("every member was checked to be a constant"),
                        })
                        .collect();
                    PTerm::Const(self.store.list(ids))
                } else {
                    PTerm::List(compiled)
                }
            }
            Node::Formula(statements) => {
                let atoms: Vec<Atom> = statements.iter().map(|s| self.atom(s, true)).collect();
                let ground: Option<Vec<shared::triple::Triple>> = atoms
                    .iter()
                    .map(|atom| match atom.positions() {
                        [PTerm::Const(s), PTerm::Const(p), PTerm::Const(o)] => {
                            Some(shared::triple::Triple {
                                subject: *s,
                                predicate: *p,
                                object: *o,
                            })
                        }
                        _ => None,
                    })
                    .collect();
                match ground {
                    Some(triples) => PTerm::Const(self.store.formula(triples)),
                    None => PTerm::Formula(atoms),
                }
            }
            Node::Iri(_) | Node::Literal { .. } => PTerm::Const(self.store.encode(node)),
        }
    }

    fn atom(&mut self, statement: &Statement, quoted: bool) -> Atom {
        let mut atom = Atom::new(
            self.term(&statement.subject, quoted),
            self.term(&statement.predicate, quoted),
            self.term(&statement.object, quoted),
        );
        if quoted && self.side == Side::Body {
            if let Node::Iri(iri) = &statement.predicate {
                atom.builtin = Builtin::from_iri(iri);
            }
        }
        atom
    }
}

/// The built-in a premise statement calls, or an error for a built-in this engine lacks
fn builtin_of(statement: &Statement) -> Result<Option<Builtin>, N3Error> {
    let Node::Iri(iri) = &statement.predicate else {
        return Ok(None);
    };
    if let Some(builtin) = Builtin::from_iri(iri) {
        return Ok(Some(builtin));
    }
    if Builtin::not_yet(iri) {
        return Err(N3Error::unsupported(format!("built-in <{iri}>")));
    }
    Ok(None)
}

fn formula_statements(node: &Node) -> Option<&[Statement]> {
    match node {
        Node::Formula(statements) => Some(statements),
        Node::Literal {
            lexical, datatype, ..
        } if lexical == "true" && datatype.as_deref() == Some(crate::syntax::XSD_BOOLEAN) => {
            Some(&[])
        }
        _ => None,
    }
}

/// Compiles `{ body } => { head }`, given as the two sides of a `log:implies` statement
///
/// # Errors
/// Returns [`N3Error::Unsupported`] for a side that is not a formula, for a built-in this
/// engine does not implement, and for a conclusion variable the body never binds
pub fn compile_rule(store: &mut TermStore, body: &Node, head: &Node) -> Result<N3Rule, N3Error> {
    let Some(body) = formula_statements(body) else {
        return Err(N3Error::unsupported("a rule premise that is not a formula"));
    };
    let fuse = matches!(head, Node::Literal { lexical, datatype, .. }
        if lexical == "false" && datatype.as_deref() == Some(crate::syntax::XSD_BOOLEAN));
    let unquoted = (!fuse && formula_statements(head).is_none()).then_some(head);
    let head: &[Statement] = match head {
        _ if fuse || unquoted.is_some() => &[],
        other => formula_statements(other).unwrap_or(&[]),
    };
    let mut body_atoms: Vec<Atom> = {
        let mut compiler = Compiler {
            store,
            side: Side::Body,
            bound: None,
        };
        body.iter().map(|s| compiler.atom(s, false)).collect()
    };
    for (atom, statement) in body_atoms.iter_mut().zip(body) {
        atom.builtin = builtin_of(statement)?;
    }
    let mut bound = BTreeSet::new();
    for atom in &body_atoms {
        atom.variables(&mut bound);
    }
    let head_atoms: Vec<Atom> = {
        let mut compiler = Compiler {
            store,
            side: Side::Head,
            bound: Some(&bound),
        };
        head.iter().map(|s| compiler.atom(s, false)).collect()
    };
    let mut used = BTreeSet::new();
    for atom in &head_atoms {
        atom.variables(&mut used);
    }
    let unquote = match unquoted {
        Some(Node::Var(name)) if bound.contains(name) => Some(PTerm::Var(name.clone())),
        Some(Node::Var(_)) => None,
        Some(_) => {
            return Err(N3Error::unsupported(
                "a rule conclusion that is not a formula",
            ))
        }
        None => None,
    };
    let has_existentials = head_atoms.iter().any(Atom::has_exists);
    let deferred = body_atoms
        .iter()
        .any(|atom| atom.builtin.is_some_and(Builtin::reads_whole_graph));
    Ok(N3Rule {
        body: body_atoms,
        head: head_atoms,
        has_existentials,
        deferred,
        fuse,
        unquote,
    })
}

/// Compiles `{ head } <= { body }`, a rule proved on demand when a premise asks for its head
///
/// Both sides compile as patterns: the head is matched against a goal, so its blank nodes are
/// variables and it may name variables the body leaves open.
///
/// # Errors
/// Returns [`N3Error::Unsupported`] for a side that is not a formula and for a built-in this
/// engine does not implement
pub fn compile_backward_rule(
    store: &mut TermStore,
    head: &Node,
    body: &Node,
) -> Result<N3Rule, N3Error> {
    let (Some(head), Some(body)) = (formula_statements(head), formula_statements(body)) else {
        return Err(N3Error::unsupported(
            "a backward rule side that is not a formula",
        ));
    };
    let mut compiler = Compiler {
        store,
        side: Side::Body,
        bound: None,
    };
    let head_atoms: Vec<Atom> = head.iter().map(|s| compiler.atom(s, false)).collect();
    let mut body_atoms: Vec<Atom> = body.iter().map(|s| compiler.atom(s, false)).collect();
    let mut body_vars = BTreeSet::new();
    for atom in &body_atoms {
        atom.variables(&mut body_vars);
    }
    // A head blank node the body does not bind is an existential, with a witness per goal.
    let open = |name: &str| {
        name.strip_prefix("_:")
            .filter(|_| !body_vars.contains(name))
            .map(str::to_string)
    };
    let head_atoms: Vec<Atom> = head_atoms
        .iter()
        .map(|atom| {
            Atom::new(
                blank_exists(&atom.subject, &open),
                blank_exists(&atom.predicate, &open),
                blank_exists(&atom.object, &open),
            )
        })
        .collect();
    for (atom, statement) in body_atoms.iter_mut().zip(body) {
        atom.builtin = builtin_of(statement)?;
    }
    let deferred = body_atoms
        .iter()
        .any(|atom| atom.builtin.is_some_and(Builtin::reads_whole_graph));
    Ok(N3Rule {
        body: body_atoms,
        head: head_atoms,
        has_existentials: false,
        deferred,
        fuse: false,
        unquote: None,
    })
}

/// A position with each variable `open` names turned into that existential
fn blank_exists(position: &PTerm, open: &dyn Fn(&str) -> Option<String>) -> PTerm {
    match position {
        PTerm::Var(name) => open(name).map_or_else(|| position.clone(), PTerm::Exists),
        PTerm::List(items) => PTerm::List(items.iter().map(|i| blank_exists(i, open)).collect()),
        other => other.clone(),
    }
}

impl Atom {
    /// The atom with every variable renamed by `rename`, built-in kept
    pub(crate) fn renamed(&self, rename: &dyn Fn(&str) -> String) -> Atom {
        let mut atom = Atom::new(
            self.subject.renamed(rename),
            self.predicate.renamed(rename),
            self.object.renamed(rename),
        );
        atom.builtin = self.builtin;
        atom
    }
}

impl PTerm {
    /// The position with every variable renamed by `rename`
    pub(crate) fn renamed(&self, rename: &dyn Fn(&str) -> String) -> PTerm {
        match self {
            PTerm::Var(name) => PTerm::Var(rename(name)),
            PTerm::List(items) => PTerm::List(items.iter().map(|i| i.renamed(rename)).collect()),
            PTerm::Formula(atoms) => {
                PTerm::Formula(atoms.iter().map(|a| a.renamed(rename)).collect())
            }
            other => other.clone(),
        }
    }
}

/// Rebuilds the node behind a formula id, so a derived `log:implies` triple can become a rule
pub fn formula_node(store: &TermStore, id: u32) -> Option<Node> {
    match store.compound(id) {
        Some(Compound::Formula(_)) => Some(store.node(id)),
        _ => None,
    }
}
