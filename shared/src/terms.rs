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

/// One solution mapping, as a caller builds one.
///
/// Kept as a map because that is how a row is *constructed* — a scan binds variables
/// as it finds them, and the names are not known until it does. [`Bindings`] stores
/// something narrower; this is the shape at its door.
pub type Binding = HashMap<String, u32>;

/// The value a cell holds where a row binds nothing.
///
/// Re-exported from [`relation`](crate::relation) so the two sequence types agree on
/// it. Safe to reserve because neither half of the id space will mint it; see
/// [`relation::UNBOUND`](crate::relation::UNBOUND).
pub use crate::relation::UNBOUND;

/// A solution sequence: the rows a pattern matched.
///
/// # Schema and tuples
///
/// One schema for the sequence, and a row of `u32` per solution, rather than a
/// `HashMap<String, u32>` per row. A variable name is resolved to a column once for
/// the whole sequence instead of being hashed once per row per column, and a row
/// costs one allocation of `4 × columns` bytes instead of a map with owned `String`
/// keys. Measured on a 20,000 by 20,000 hash join over 2,000 keys, the tuple shape
/// joins the same data about 4.3x faster.
///
/// # Solution mappings are partial
///
/// `OPTIONAL` and `VALUES ... UNDEF` produce rows binding different variable sets.
/// The map shape said so by leaving a key out; a fixed-width row says it with
/// [`UNBOUND`], which no dictionary can mint. `0` would not have worked: the first
/// term ever interned has id `0`.
///
/// # Built from maps, read as rows
///
/// A row is still *constructed* as a [`Binding`], because a scan does not know which
/// variables it will bind until it binds them. [`push`](Self::push) and
/// [`FromIterator`] take that shape and widen the schema as new variables appear, so
/// the sequence stays rectangular without the caller tracking it. Reading goes the
/// other way: [`iter`](Self::iter) yields a [`BindingRef`], which answers by name
/// without rebuilding a map.
#[derive(Debug, Clone, Default)]
pub struct Bindings {
    /// Variable names, in column order. Grows as rows introduce variables.
    schema: Vec<String>,
    /// One row per solution, each `schema.len()` wide.
    cells: Vec<Vec<u32>>,
}

/// One solution of a [`Bindings`], answered by name without a map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingRef<'a> {
    schema: &'a [String],
    cells: &'a [u32],
}

impl<'a> BindingRef<'a> {
    /// The value bound to `variable`, or `None` where this solution binds nothing.
    pub fn get(&self, variable: &str) -> Option<u32> {
        let column = self.schema.iter().position(|name| name == variable)?;
        match self.cells.get(column).copied()? {
            UNBOUND => None,
            id => Some(id),
        }
    }

    /// Whether this solution binds `variable`.
    pub fn contains_key(&self, variable: &str) -> bool {
        self.get(variable).is_some()
    }

    /// The variables this solution binds, in column order.
    pub fn keys(&self) -> impl Iterator<Item = &'a str> + 'a {
        let schema = self.schema;
        self.cells
            .iter()
            .enumerate()
            .filter(|(_, id)| **id != UNBOUND)
            .map(move |(column, _)| schema[column].as_str())
    }

    /// The values this solution binds, in column order.
    pub fn values(&self) -> impl Iterator<Item = u32> + 'a {
        self.cells.iter().copied().filter(|id| *id != UNBOUND)
    }

    /// The variables and their values, in column order.
    pub fn iter(&self) -> impl Iterator<Item = (&'a str, u32)> + 'a {
        let schema = self.schema;
        self.cells
            .iter()
            .enumerate()
            .filter(|(_, id)| **id != UNBOUND)
            .map(move |(column, id)| (schema[column].as_str(), *id))
    }

    /// How many variables this solution binds.
    pub fn len(&self) -> usize {
        self.cells.iter().filter(|id| **id != UNBOUND).count()
    }

    /// Whether this solution binds nothing: the unit solution mapping.
    pub fn is_empty(&self) -> bool {
        self.cells.iter().all(|id| *id == UNBOUND)
    }

    /// This solution as a map, for a caller that needs to own and mutate one.
    pub fn to_binding(&self) -> Binding {
        self.iter().map(|(name, id)| (name.to_string(), id)).collect()
    }

    /// The raw cells, in the sequence's column order.
    pub fn cells(&self) -> &'a [u32] {
        self.cells
    }
}

impl Bindings {
    /// No solutions: a pattern that matched nothing.
    ///
    /// Distinct from [`unit`](Self::unit), and the difference is load-bearing: this
    /// is failure, that is success binding nothing.
    pub fn empty() -> Self {
        Self::default()
    }

    /// The unit solution sequence: one row, binding no variables.
    ///
    /// What the engine starts every plan from, and what a request with no pattern
    /// returns. It is not the empty sequence, and a join against it is the identity.
    pub fn unit() -> Self {
        Self {
            schema: Vec::new(),
            cells: vec![Vec::new()],
        }
    }

    /// A sequence of exactly one solution.
    pub fn single(binding: Binding) -> Self {
        let mut bindings = Self::empty();
        bindings.push(binding);
        bindings
    }

    /// An empty sequence over a known schema.
    pub fn with_schema(schema: Vec<String>) -> Self {
        Self {
            schema,
            cells: Vec::new(),
        }
    }

    /// Room for `rows` solutions, allocated up front.
    pub fn with_capacity(rows: usize) -> Self {
        Self {
            schema: Vec::new(),
            cells: Vec::with_capacity(rows),
        }
    }

    /// The variables any solution binds, in column order.
    pub fn schema(&self) -> &[String] {
        &self.schema
    }

    /// The column `variable` occupies, resolved once for the whole sequence.
    ///
    /// Resolve here and index rows with the result. Looking a name up per row is what
    /// the map shape did and what this type exists to stop.
    pub fn column(&self, variable: &str) -> Option<usize> {
        self.schema.iter().position(|name| name == variable)
    }

    /// How many solutions.
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Whether the pattern matched nothing.
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// The first solution, if any.
    pub fn first(&self) -> Option<BindingRef<'_>> {
        self.row(0)
    }

    /// One solution, by position.
    pub fn row(&self, index: usize) -> Option<BindingRef<'_>> {
        Some(BindingRef {
            schema: &self.schema,
            cells: self.cells.get(index)?,
        })
    }

    /// The solutions, in result order.
    pub fn iter(&self) -> impl Iterator<Item = BindingRef<'_>> + '_ {
        self.cells.iter().map(|cells| BindingRef {
            schema: &self.schema,
            cells,
        })
    }

    /// Adds `variable` as a column if the schema lacks it, padding existing rows.
    ///
    /// Growing is bounded by the number of distinct variables, not by the number of
    /// rows, so a sequence widens a handful of times however long it is.
    fn column_for(&mut self, variable: &str) -> usize {
        if let Some(column) = self.column(variable) {
            return column;
        }
        self.schema.push(variable.to_string());
        for row in &mut self.cells {
            row.push(UNBOUND);
        }
        self.schema.len() - 1
    }

    /// Appends a solution, widening the schema for any variable it introduces.
    pub fn push(&mut self, binding: Binding) {
        let mut names: Vec<&String> = binding.keys().collect();
        // Sorted so a new column's position does not depend on map iteration order.
        names.sort();
        let mut row = vec![UNBOUND; self.schema.len()];
        for name in names {
            let column = self.column_for(name);
            if column >= row.len() {
                row.resize(column + 1, UNBOUND);
            }
            row[column] = binding[name];
        }
        row.resize(self.schema.len(), UNBOUND);
        self.cells.push(row);
    }

    /// Appends a row already laid out against this sequence's schema.
    ///
    /// # Panics
    ///
    /// In debug builds, if the row's width is not the schema's.
    pub fn push_cells(&mut self, row: Vec<u32>) {
        debug_assert_eq!(
            row.len(),
            self.schema.len(),
            "a row must hold one value per column"
        );
        self.cells.push(row);
    }

    /// Appends another sequence's solutions, unifying the two schemas.
    ///
    /// A column only one side has is unbound in the other side's rows, which is what
    /// a union of branches binding different variables produces.
    ///
    /// This is what [`extend`](Self::extend) should be reached for instead of: that
    /// takes maps, so concatenating two sequences through it rebuilds a
    /// `HashMap<String, u32>` for every row and then takes it apart again. Measured
    /// on `optimizer_hotpath`, going through maps cost `union_branches` about twice
    /// its runtime.
    pub fn append(&mut self, other: Self) {
        if self.schema.is_empty() && self.cells.is_empty() {
            *self = other;
            return;
        }
        if other.cells.is_empty() {
            return;
        }
        if self.schema == other.schema {
            // The common case by far: branches of a union, or chunks of one plan's
            // output, agree on their columns, so the rows move as they are. Mapping
            // them cell by cell would allocate a row and index it per solution for
            // no reason.
            self.cells.extend(other.cells);
            return;
        }
        // Every missing column added in one pass, then the rows padded once.
        // Calling `column_for` per column pads all the rows already present each
        // time, so a branch introducing three variables walked them three times.
        let added: Vec<&String> = other
            .schema
            .iter()
            .filter(|name| !self.schema.iter().any(|existing| existing == *name))
            .collect();
        if !added.is_empty() {
            let grown = self.schema.len() + added.len();
            for name in added {
                self.schema.push(name.clone());
            }
            for row in &mut self.cells {
                row.resize(grown, UNBOUND);
            }
        }
        let mapping: Vec<usize> = other
            .schema
            .iter()
            .map(|name| {
                self.column(name)
                    .expect("every column was just added if missing")
            })
            .collect();
        let width = self.schema.len();
        self.cells.reserve(other.cells.len());
        for row in other.cells {
            let mut appended = vec![UNBOUND; width];
            for (column, value) in row.into_iter().enumerate() {
                if value != UNBOUND {
                    appended[mapping[column]] = value;
                }
            }
            self.cells.push(appended);
        }
    }

    /// Appends every solution of `other`.
    pub fn extend(&mut self, other: impl IntoIterator<Item = Binding>) {
        for binding in other {
            self.push(binding);
        }
    }

    /// Keeps only the solutions satisfying `keep`.
    pub fn retain(&mut self, mut keep: impl FnMut(BindingRef<'_>) -> bool) {
        let schema = &self.schema;
        self.cells.retain(|cells| keep(BindingRef { schema, cells }));
    }

    /// Discards all but the first `rows` solutions.
    pub fn truncate(&mut self, rows: usize) {
        self.cells.truncate(rows);
    }

    /// Removes every solution, keeping the schema.
    pub fn clear(&mut self) {
        self.cells.clear();
    }

    /// Removes consecutive duplicate solutions.
    pub fn dedup(&mut self) {
        self.cells.dedup();
    }

    /// Orders the solutions by `compare`.
    pub fn sort_by(&mut self, mut compare: impl FnMut(BindingRef<'_>, BindingRef<'_>) -> std::cmp::Ordering) {
        let schema = self.schema.clone();
        self.cells.sort_by(|left, right| {
            compare(
                BindingRef { schema: &schema, cells: left },
                BindingRef { schema: &schema, cells: right },
            )
        });
    }

    /// Binds `variable` in every solution, to one value per row in order.
    ///
    /// What `BIND`, `ML.PREDICT` and the function-call operators all do: compute a
    /// value per solution and attach it under a new name. In the map shape each of
    /// them inserted into every row's map, hashing the same name once per row; here
    /// the name is resolved once and the column is filled.
    ///
    /// A solution with no corresponding value is left unbound, and an existing
    /// column of that name is overwritten, which is what inserting into a map did.
    ///
    /// # Panics
    ///
    /// In debug builds, if `values` is longer than the sequence.
    pub fn bind_column(&mut self, variable: &str, values: impl IntoIterator<Item = u32>) {
        debug_assert!(self.schema.iter().all(|name| name != variable) || self.column(variable).is_some());
        let column = self.column_for(variable);
        let mut filled = 0usize;
        for (row, value) in self.cells.iter_mut().zip(values) {
            row[column] = value;
            filled += 1;
        }
        debug_assert!(filled <= self.cells.len(), "more values than solutions");
    }

    /// The raw cells, one row per solution, each `schema().len()` wide.
    ///
    /// For code that has resolved its columns and wants to index them. The schema is
    /// [`schema`](Self::schema); the two together are the whole sequence.
    pub fn cells(&self) -> &[Vec<u32>] {
        &self.cells
    }

    /// The cells, mutably. The schema is fixed, so rows keep their width.
    pub fn cells_mut(&mut self) -> &mut [Vec<u32>] {
        &mut self.cells
    }

    /// Takes the sequence apart into its schema and its rows.
    pub fn into_parts(self) -> (Vec<String>, Vec<Vec<u32>>) {
        (self.schema, self.cells)
    }

    /// Builds a sequence from a schema and rows already laid out against it.
    ///
    /// # Panics
    ///
    /// In debug builds, if any row's width is not the schema's.
    pub fn from_parts(schema: Vec<String>, cells: Vec<Vec<u32>>) -> Self {
        debug_assert!(
            cells.iter().all(|row| row.len() == schema.len()),
            "every row must hold one value per column"
        );
        Self { schema, cells }
    }

    /// The solutions in parallel, which is how the engine's filters and joins read
    /// a sequence.
    ///
    /// Yields views rather than maps, so a parallel pass over a long sequence
    /// allocates nothing per row.
    pub fn par_iter(&self) -> impl rayon::iter::IndexedParallelIterator<Item = BindingRef<'_>> {
        use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
        let schema = &self.schema;
        self.cells
            .par_iter()
            .map(move |cells| BindingRef { schema, cells })
    }

    /// The solutions as maps, for code not yet converted.
    ///
    /// Allocates a map per row, so this is a boundary with code that still wants the
    /// old shape, not something to reach for in a loop.
    pub fn to_maps(&self) -> Vec<Binding> {
        self.iter().map(|row| row.to_binding()).collect()
    }
}

/// Compared by what the solutions bind, not by how the schema happens to be ordered.
impl PartialEq for Bindings {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self.iter().zip(other.iter()).all(|(left, right)| {
                left.len() == right.len()
                    && left.iter().all(|(name, id)| right.get(name) == Some(id))
            })
    }
}

impl Eq for Bindings {}

impl From<Vec<Binding>> for Bindings {
    fn from(rows: Vec<Binding>) -> Self {
        rows.into_iter().collect()
    }
}

impl From<Bindings> for Vec<Binding> {
    fn from(bindings: Bindings) -> Self {
        bindings.to_maps()
    }
}

impl FromIterator<Binding> for Bindings {
    fn from_iter<I: IntoIterator<Item = Binding>>(rows: I) -> Self {
        let mut bindings = Self::empty();
        for row in rows {
            bindings.push(row);
        }
        bindings
    }
}

impl IntoIterator for Bindings {
    type Item = Binding;
    type IntoIter = std::vec::IntoIter<Binding>;

    fn into_iter(self) -> Self::IntoIter {
        self.to_maps().into_iter()
    }
}

impl<'a> IntoIterator for &'a Bindings {
    type Item = BindingRef<'a>;
    type IntoIter = Box<dyn Iterator<Item = BindingRef<'a>> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// Collected from a parallel iterator, which is how the engine's filters and joins
/// produce a sequence.
impl rayon::iter::FromParallelIterator<Binding> for Bindings {
    fn from_par_iter<I>(rows: I) -> Self
    where
        I: rayon::iter::IntoParallelIterator<Item = Binding>,
    {
        use rayon::iter::ParallelIterator;
        let rows: Vec<Binding> = rows.into_par_iter().collect();
        rows.into_iter().collect()
    }
}

impl rayon::iter::IntoParallelIterator for Bindings {
    type Item = Binding;
    type Iter = rayon::vec::IntoIter<Binding>;

    fn into_par_iter(self) -> Self::Iter {
        rayon::iter::IntoParallelIterator::into_par_iter(self.to_maps())
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
