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

/// One solution mapping: the variables a single result row binds, to encoded values.
///
/// A *partial* function, as SPARQL requires: `OPTIONAL` and `VALUES ... UNDEF` both
/// produce rows binding different variable sets, and a variable this does not contain
/// is unbound rather than empty.
pub type Binding = HashMap<String, u32>;

/// A solution sequence: the rows a pattern matched.
///
/// # Why this is a type and not an alias
///
/// It was `Vec<HashMap<String, u32>>`, spelled out at 46 boundaries. Nothing could be
/// said about it, callers built it by hand, and every engine grew its own variant,
/// which is why §4 of the architecture proposal wants it replaced by a schema and
/// tuples. That replacement joins the same data about 4.3x faster, because the map
/// shape re-hashes a `String` key for every column of every row and allocates a map
/// per row.
///
/// Changing the layout under 46 signatures at once is a near-total rewrite of the
/// execution engine — measured at 1,092 of its 1,187 production lines, for about
/// 1.15x on real queries. Naming the type first costs nothing and is worth doing on
/// its own: a benchmark control arm put the map shape behind a struct with accessors
/// and measured 51.11ms against the raw shape's 51.01ms, 0.2% apart, so wrapping in
/// Rust is free. What the wrapping buys is that the shape is now stated once here
/// rather than spelled out at every boundary.
///
/// # What it is not, yet
///
/// [`Binding`] is still a `HashMap`, and [`Deref`](std::ops::Deref) hands out a slice
/// of them, so this hides the *collection* but not the row. Code that treats a row as
/// a map still compiles, and will still need changing when the layout does. The win
/// is that the 46 boundaries will not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bindings {
    rows: Vec<Binding>,
}

impl Bindings {
    /// No solutions: a pattern that matched nothing.
    ///
    /// Distinct from [`unit`](Self::unit), and the difference is load-bearing: this
    /// is failure, that is success binding nothing.
    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }

    /// The unit solution sequence: one row, binding no variables.
    ///
    /// What the engine starts every plan from, and what `SELECT` with no pattern
    /// returns. Written `vec![HashMap::new()]` at the call sites that needed it, where
    /// it read as an implementation detail rather than as the identity of a join.
    pub fn unit() -> Self {
        Self {
            rows: vec![Binding::new()],
        }
    }

    /// Room for `rows` solutions, allocated up front.
    pub fn with_capacity(rows: usize) -> Self {
        Self {
            rows: Vec::with_capacity(rows),
        }
    }

    /// Appends a solution.
    pub fn push(&mut self, row: Binding) {
        self.rows.push(row);
    }

    /// Appends every solution of `other`.
    pub fn extend(&mut self, other: impl IntoIterator<Item = Binding>) {
        self.rows.extend(other);
    }

    /// Keeps only the solutions satisfying `keep`.
    pub fn retain(&mut self, keep: impl FnMut(&Binding) -> bool) {
        self.rows.retain(keep);
    }

    /// Discards all but the first `rows` solutions.
    pub fn truncate(&mut self, rows: usize) {
        self.rows.truncate(rows);
    }

    /// Removes every solution.
    pub fn clear(&mut self) {
        self.rows.clear();
    }

    /// Removes consecutive duplicate solutions.
    pub fn dedup(&mut self) {
        self.rows.dedup();
    }

    /// Removes and returns the solutions in `range`.
    pub fn drain(
        &mut self,
        range: impl std::ops::RangeBounds<usize>,
    ) -> std::vec::Drain<'_, Binding> {
        self.rows.drain(range)
    }

    /// Orders the solutions by `compare`.
    pub fn sort_by(&mut self, compare: impl FnMut(&Binding, &Binding) -> std::cmp::Ordering) {
        self.rows.sort_by(compare);
    }

    /// Iterates the solutions, mutably.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Binding> {
        self.rows.iter_mut()
    }

    /// The solutions as a plain `Vec`, for code not yet converted.
    pub fn into_inner(self) -> Vec<Binding> {
        self.rows
    }
}

impl std::ops::Deref for Bindings {
    type Target = [Binding];

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl std::ops::DerefMut for Bindings {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rows
    }
}

impl From<Vec<Binding>> for Bindings {
    fn from(rows: Vec<Binding>) -> Self {
        Self { rows }
    }
}

impl From<Bindings> for Vec<Binding> {
    fn from(bindings: Bindings) -> Self {
        bindings.rows
    }
}

impl FromIterator<Binding> for Bindings {
    fn from_iter<I: IntoIterator<Item = Binding>>(rows: I) -> Self {
        Self {
            rows: rows.into_iter().collect(),
        }
    }
}

impl IntoIterator for Bindings {
    type Item = Binding;
    type IntoIter = std::vec::IntoIter<Binding>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a Bindings {
    type Item = &'a Binding;
    type IntoIter = std::slice::Iter<'a, Binding>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

/// Collected from a parallel iterator, which is how the engine's filters and joins
/// produce a sequence.
impl rayon::iter::FromParallelIterator<Binding> for Bindings {
    fn from_par_iter<I>(rows: I) -> Self
    where
        I: rayon::iter::IntoParallelIterator<Item = Binding>,
    {
        Self {
            rows: rayon::iter::FromParallelIterator::from_par_iter(rows),
        }
    }
}

impl rayon::iter::ParallelExtend<Binding> for Bindings {
    fn par_extend<I>(&mut self, rows: I)
    where
        I: rayon::iter::IntoParallelIterator<Item = Binding>,
    {
        rayon::iter::ParallelExtend::par_extend(&mut self.rows, rows);
    }
}

/// Consumed in parallel, which is how the engine's filters and joins read a sequence.
impl rayon::iter::IntoParallelIterator for Bindings {
    type Item = Binding;
    type Iter = rayon::vec::IntoIter<Binding>;

    fn into_par_iter(self) -> Self::Iter {
        rayon::iter::IntoParallelIterator::into_par_iter(self.rows)
    }
}

impl<'a> rayon::iter::IntoParallelIterator for &'a Bindings {
    type Item = &'a Binding;
    type Iter = rayon::slice::Iter<'a, Binding>;

    fn into_par_iter(self) -> Self::Iter {
        rayon::iter::IntoParallelIterator::into_par_iter(&self.rows[..])
    }
}

impl<'a> rayon::iter::IntoParallelIterator for &'a mut Bindings {
    type Item = &'a mut Binding;
    type Iter = rayon::slice::IterMut<'a, Binding>;

    fn into_par_iter(self) -> Self::Iter {
        rayon::iter::IntoParallelIterator::into_par_iter(&mut self.rows[..])
    }
}

impl<'a> IntoIterator for &'a mut Bindings {
    type Item = &'a mut Binding;
    type IntoIter = std::slice::IterMut<'a, Binding>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter_mut()
    }
}

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
