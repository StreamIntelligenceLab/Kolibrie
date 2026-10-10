/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

/// One position of a triple pattern: either bound to a term or left open.
///
/// This is the pattern-side counterpart to a [`Triple`](crate::triple::Triple)'s
/// plain `u32`: where a fact has an id in every position, a pattern may have a
/// variable instead.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Term {
    /// A variable, held by name. Two positions carrying the same name must bind to
    /// the same value — that constraint is what makes a pattern a join.
    Variable(String),
    /// A fixed term, as a dictionary id. Comparable only against ids from the same
    /// [`Dictionary`](crate::dictionary::Dictionary).
    Constant(u32),
    /// RDF-star: a quoted triple pattern with potentially variable components.
    /// Used in SPARQL-star WHERE clauses like `<< ?s :p ?o >>`.
    QuotedTriple(Box<(Term, Term, Term)>),
}

/// A triple pattern: subject, predicate and object, each bound or variable.
pub type TriplePattern = (Term, Term, Term);

/// A solution sequence: one map of variable name to encoded value per result row.
///
/// This is a transparent alias rather than a type, so it is not something callers can
/// build against, and every engine has grown its own variant — which is why §4 of the
/// architecture proposal replaces it with a schema-plus-tuples relation. Measured, the
/// replacement joins the same data about 4.7x faster, because this shape re-hashes a
/// `String` key for every column of every row and allocates a map per row.
pub type Bindings = Vec<HashMap<String, u32>>;

/// A term as it appears in source text, before prefixes are resolved and before it is
/// interned.
///
/// Parsing produces these; binding them to a dictionary turns them into [`Term`]s.
/// Keeping the two apart is what stops a rule parsed against one vocabulary from
/// silently matching nothing against another — a failure this codebase has hit.
#[derive(Debug)]
pub enum UnresolvedTerm {
    /// A variable, by name.
    Var(String),
    /// A prefixed name such as `ex:thing`, still needing its prefix expanded.
    Prefixed(String),
    /// A full IRI, needing only interning.
    Iri(String),
}

/// A triple of [`UnresolvedTerm`]s, as parsed.
pub type UnresolvedTriple = (UnresolvedTerm, UnresolvedTerm, UnresolvedTerm);

impl Term {
    /// Whether this position is a variable.
    pub fn is_var(&self) -> bool {
        matches!(self, Term::Variable(_))
    }

    /// Whether this position is a quoted triple.
    pub fn is_quoted_triple(&self) -> bool {
        matches!(self, Term::QuotedTriple(_))
    }
}
