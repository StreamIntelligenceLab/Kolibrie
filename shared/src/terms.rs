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

/// One position of a triple pattern: either bound to a term or left open
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Term {
    /// A variable, held by name
    Variable(String),
    /// A fixed term, as a dictionary id
    Constant(u32),
    /// RDF-star: a quoted triple pattern with potentially variable components
    QuotedTriple(Box<(Term, Term, Term)>),
}

/// A triple pattern: subject, predicate and object, each bound or variable
pub type TriplePattern = (Term, Term, Term);

/// One solution mapping, as a caller builds one
pub type Binding = HashMap<String, u32>;

/// The value a cell holds where a row binds nothing
pub const UNBOUND: u32 = u32::MAX;

/// A solution sequence: the rows a pattern matched
#[derive(Debug, Clone, Default)]
pub struct Bindings {
    schema: Vec<String>,
    cells: Vec<Vec<u32>>,
}

/// One solution of a [`Bindings`], answered by name without a map
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingRef<'a> {
    schema: &'a [String],
    cells: &'a [u32],
}

impl<'a> BindingRef<'a> {
    /// The value bound to `variable`, or `None` where this solution binds nothing
    pub fn get(&self, variable: &str) -> Option<u32> {
        let column = self.schema.iter().position(|name| name == variable)?;
        match self.cells.get(column).copied()? {
            UNBOUND => None,
            id => Some(id),
        }
    }

    /// Whether this solution binds `variable`
    pub fn contains_key(&self, variable: &str) -> bool {
        self.get(variable).is_some()
    }

    /// The variables this solution binds, in column order
    pub fn keys(&self) -> impl Iterator<Item = &'a str> + 'a {
        let schema = self.schema;
        self.cells
            .iter()
            .enumerate()
            .filter(|(_, id)| **id != UNBOUND)
            .map(move |(column, _)| schema[column].as_str())
    }

    /// The values this solution binds, in column order
    pub fn values(&self) -> impl Iterator<Item = u32> + 'a {
        self.cells.iter().copied().filter(|id| *id != UNBOUND)
    }

    /// The variables and their values, in column order
    pub fn iter(&self) -> impl Iterator<Item = (&'a str, u32)> + 'a {
        let schema = self.schema;
        self.cells
            .iter()
            .enumerate()
            .filter(|(_, id)| **id != UNBOUND)
            .map(move |(column, id)| (schema[column].as_str(), *id))
    }

    /// How many variables this solution binds
    pub fn len(&self) -> usize {
        self.cells.iter().filter(|id| **id != UNBOUND).count()
    }

    /// Whether this solution binds nothing: the unit solution mapping
    pub fn is_empty(&self) -> bool {
        self.cells.iter().all(|id| *id == UNBOUND)
    }

    /// This solution as a map, for a caller that needs to own and mutate one
    pub fn to_binding(&self) -> Binding {
        self.iter()
            .map(|(name, id)| (name.to_string(), id))
            .collect()
    }

    /// The raw cells, in the sequence's column order
    pub fn cells(&self) -> &'a [u32] {
        self.cells
    }
}

impl Bindings {
    /// No solutions: a pattern that matched nothing
    pub fn empty() -> Self {
        Self::default()
    }

    /// The unit solution sequence: one row, binding no variables
    pub fn unit() -> Self {
        Self {
            schema: Vec::new(),
            cells: vec![Vec::new()],
        }
    }

    /// A sequence of exactly one solution
    pub fn single(binding: Binding) -> Self {
        let mut bindings = Self::empty();
        bindings.push(binding);
        bindings
    }

    /// An empty sequence over a known schema
    pub fn with_schema(schema: Vec<String>) -> Self {
        Self {
            schema,
            cells: Vec::new(),
        }
    }

    /// Room for `rows` solutions, allocated up front
    pub fn with_capacity(rows: usize) -> Self {
        Self {
            schema: Vec::new(),
            cells: Vec::with_capacity(rows),
        }
    }

    /// The variables any solution binds, in column order
    pub fn schema(&self) -> &[String] {
        &self.schema
    }

    /// The column `variable` occupies, resolved once for the whole sequence
    pub fn column(&self, variable: &str) -> Option<usize> {
        self.schema.iter().position(|name| name == variable)
    }

    /// How many solutions
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Whether the pattern matched nothing
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// The first solution, if any
    pub fn first(&self) -> Option<BindingRef<'_>> {
        self.row(0)
    }

    /// One solution, by position
    pub fn row(&self, index: usize) -> Option<BindingRef<'_>> {
        Some(BindingRef {
            schema: &self.schema,
            cells: self.cells.get(index)?,
        })
    }

    /// The solutions, in result order
    pub fn iter(&self) -> impl Iterator<Item = BindingRef<'_>> + '_ {
        self.cells.iter().map(|cells| BindingRef {
            schema: &self.schema,
            cells,
        })
    }

    /// Adds `variable` as a column if the schema lacks it, padding existing rows
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

    /// Appends a solution, widening the schema for any variable it introduces
    pub fn push(&mut self, binding: Binding) {
        let mut names: Vec<&String> = binding.keys().collect();
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

    /// Appends a row already laid out against this sequence's schema
    pub fn push_cells(&mut self, row: Vec<u32>) {
        debug_assert_eq!(
            row.len(),
            self.schema.len(),
            "a row must hold one value per column"
        );
        self.cells.push(row);
    }

    /// Appends another sequence's solutions, unifying the two schemas
    pub fn append(&mut self, other: Self) {
        if self.schema.is_empty() && self.cells.is_empty() {
            *self = other;
            return;
        }
        if other.cells.is_empty() {
            return;
        }
        if self.schema == other.schema {
            self.cells.extend(other.cells);
            return;
        }
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

    /// Appends every solution of `other`
    pub fn extend(&mut self, other: impl IntoIterator<Item = Binding>) {
        for binding in other {
            self.push(binding);
        }
    }

    /// Keeps only the solutions satisfying `keep`
    pub fn retain(&mut self, mut keep: impl FnMut(BindingRef<'_>) -> bool) {
        let schema = &self.schema;
        self.cells
            .retain(|cells| keep(BindingRef { schema, cells }));
    }

    /// Discards all but the first `rows` solutions
    pub fn truncate(&mut self, rows: usize) {
        self.cells.truncate(rows);
    }

    /// Removes every solution, keeping the schema
    pub fn clear(&mut self) {
        self.cells.clear();
    }

    /// Removes consecutive duplicate solutions
    pub fn dedup(&mut self) {
        self.cells.dedup();
    }

    /// Orders the solutions by `compare`
    pub fn sort_by(
        &mut self,
        mut compare: impl FnMut(BindingRef<'_>, BindingRef<'_>) -> std::cmp::Ordering,
    ) {
        let schema = self.schema.clone();
        self.cells.sort_by(|left, right| {
            compare(
                BindingRef {
                    schema: &schema,
                    cells: left,
                },
                BindingRef {
                    schema: &schema,
                    cells: right,
                },
            )
        });
    }

    /// Binds `variable` in every solution, to one value per row in order
    pub fn bind_column(&mut self, variable: &str, values: impl IntoIterator<Item = u32>) {
        debug_assert!(
            self.schema.iter().all(|name| name != variable) || self.column(variable).is_some()
        );
        let column = self.column_for(variable);
        let mut filled = 0usize;
        for (row, value) in self.cells.iter_mut().zip(values) {
            row[column] = value;
            filled += 1;
        }
        debug_assert!(filled <= self.cells.len(), "more values than solutions");
    }

    /// The raw cells, one row per solution, each `schema().len()` wide
    pub fn cells(&self) -> &[Vec<u32>] {
        &self.cells
    }

    /// The cells, mutably
    pub fn cells_mut(&mut self) -> &mut [Vec<u32>] {
        &mut self.cells
    }

    /// Takes the sequence apart into its schema and its rows
    pub fn into_parts(self) -> (Vec<String>, Vec<Vec<u32>>) {
        (self.schema, self.cells)
    }

    /// Builds a sequence from a schema and rows already laid out against it
    pub fn from_parts(schema: Vec<String>, cells: Vec<Vec<u32>>) -> Self {
        debug_assert!(
            cells.iter().all(|row| row.len() == schema.len()),
            "every row must hold one value per column"
        );
        Self { schema, cells }
    }

    /// The solutions in parallel, which is how the engine's filters and joins read a sequence
    pub fn par_iter(&self) -> impl rayon::iter::IndexedParallelIterator<Item = BindingRef<'_>> {
        use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
        let schema = &self.schema;
        self.cells
            .par_iter()
            .map(move |cells| BindingRef { schema, cells })
    }

    /// The solutions as maps, for code not yet converted
    pub fn to_maps(&self) -> Vec<Binding> {
        self.iter().map(|row| row.to_binding()).collect()
    }
}

/// Compared by what the solutions bind, not by how the schema happens to be ordered
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

/// Collected from a parallel iterator, which is how the engine's filters and joins produce a
/// sequence
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

/// A term as it appears in source text, before prefixes are resolved and before it is interned
#[derive(Debug)]
pub enum UnresolvedTerm {
    /// A variable, by name
    Var(String),
    /// A prefixed name such as `ex:thing`, still needing its prefix expanded
    Prefixed(String),
    /// A full IRI, needing only interning
    Iri(String),
}

/// A triple of [`UnresolvedTerm`]s, as parsed
pub type UnresolvedTriple = (UnresolvedTerm, UnresolvedTerm, UnresolvedTerm);

impl Term {
    /// Whether this position is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, Term::Variable(_))
    }

    /// Whether this position is a quoted triple
    pub fn is_quoted_triple(&self) -> bool {
        matches!(self, Term::QuotedTriple(_))
    }
}

#[cfg(test)]
mod bindings_tests {
    use super::*;

    fn binding(entries: &[(&str, u32)]) -> Binding {
        entries
            .iter()
            .map(|(name, id)| ((*name).to_string(), *id))
            .collect()
    }

    fn sequence(rows: &[&[(&str, u32)]]) -> Bindings {
        rows.iter().map(|row| binding(row)).collect()
    }

    #[test]
    fn an_empty_sequence_is_not_the_unit_sequence() {
        assert!(Bindings::empty().is_empty());
        assert_eq!(Bindings::empty().len(), 0);
        assert!(!Bindings::unit().is_empty());
        assert_eq!(Bindings::unit().len(), 1);
        assert!(Bindings::unit().schema().is_empty());
        assert!(Bindings::unit().row(0).expect("one row").is_empty());
    }

    #[test]
    fn a_column_is_resolved_once_and_indexes_every_row() {
        let bindings = sequence(&[&[("x", 10), ("y", 20)], &[("x", 30), ("y", 40)]]);
        let x = bindings.column("x").expect("x is a column");
        let values: Vec<u32> = bindings.cells().iter().map(|row| row[x]).collect();
        assert_eq!(values, vec![10, 30]);
        assert_eq!(bindings.column("absent"), None);
    }

    #[test]
    fn pushing_a_row_widens_the_schema_for_a_new_variable() {
        let mut bindings = Bindings::empty();
        bindings.push(binding(&[("x", 1)]));
        bindings.push(binding(&[("x", 2), ("y", 3)]));

        assert_eq!(bindings.schema(), ["x".to_string(), "y".to_string()]);
        let first = bindings.row(0).expect("first row");
        assert_eq!(first.get("x"), Some(1));
        assert_eq!(first.get("y"), None, "the earlier row never bound y");
        assert_eq!(bindings.row(1).expect("second row").get("y"), Some(3));
    }

    #[test]
    fn a_row_that_does_not_bind_a_variable_reads_as_absent() {
        let bindings = sequence(&[&[("x", 1), ("y", 2)], &[("x", 3)]]);
        let second = bindings.row(1).expect("second row");

        assert_eq!(second.get("y"), None);
        assert!(!second.contains_key("y"));
        assert_eq!(second.keys().collect::<Vec<_>>(), vec!["x"]);
        assert_eq!(second.iter().collect::<Vec<_>>(), vec![("x", 3)]);
        assert_eq!(second.len(), 1, "one variable bound, not two");
        assert_eq!(second.to_binding(), binding(&[("x", 3)]));
    }

    #[test]
    fn term_id_zero_is_a_term_and_not_unbound() {
        let bindings = sequence(&[&[("x", 0)], &[("y", 0)]]);
        let x = bindings.column("x").expect("x is a column");
        assert_eq!(bindings.row(0).expect("first").get("x"), Some(0));
        assert_eq!(bindings.row(1).expect("second").get("x"), None);
        assert_eq!(bindings.cells()[1][x], UNBOUND);
    }

    #[test]
    fn appending_sequences_with_the_same_schema_keeps_every_row() {
        let mut left = sequence(&[&[("x", 1)], &[("x", 2)]]);
        left.append(sequence(&[&[("x", 3)]]));
        assert_eq!(left.len(), 3);
        assert_eq!(left.schema(), ["x".to_string()]);
        let seen: Vec<Option<u32>> = left.iter().map(|row| row.get("x")).collect();
        assert_eq!(seen, vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn appending_sequences_with_different_schemas_unifies_them() {
        let mut left = sequence(&[&[("s", 1), ("v", 2)]]);
        left.append(sequence(&[&[("s", 3), ("d", 4)]]));

        assert_eq!(left.len(), 2);
        assert_eq!(left.schema().len(), 3, "s, v and d are all columns");
        let first = left.row(0).expect("first");
        assert_eq!(
            (first.get("s"), first.get("v"), first.get("d")),
            (Some(1), Some(2), None)
        );
        let second = left.row(1).expect("second");
        assert_eq!(
            (second.get("s"), second.get("v"), second.get("d")),
            (Some(3), None, Some(4))
        );
    }

    #[test]
    fn appending_onto_an_empty_sequence_adopts_its_schema() {
        let mut empty = Bindings::empty();
        empty.append(sequence(&[&[("x", 1)]]));
        assert_eq!(empty.schema(), ["x".to_string()]);
        assert_eq!(empty.row(0).expect("one row").get("x"), Some(1));
    }

    #[test]
    fn binding_a_column_gives_every_solution_its_own_value() {
        let mut bindings = sequence(&[&[("x", 1)], &[("x", 2)]]);
        bindings.bind_column("doubled", vec![10, 20]);
        assert_eq!(bindings.row(0).expect("first").get("doubled"), Some(10));
        assert_eq!(bindings.row(1).expect("second").get("doubled"), Some(20));
    }

    #[test]
    fn binding_a_column_leaves_a_solution_without_a_value_unbound() {
        let mut bindings = sequence(&[&[("x", 1)], &[("x", 2)]]);
        bindings.bind_column("maybe", vec![7, UNBOUND]);
        assert_eq!(bindings.row(0).expect("first").get("maybe"), Some(7));
        assert_eq!(bindings.row(1).expect("second").get("maybe"), None);
    }

    #[test]
    fn retaining_and_ordering_see_the_values_not_the_cells() {
        let mut bindings = sequence(&[&[("x", 3)], &[("x", 1)], &[("x", 2)]]);
        bindings.sort_by(|left, right| left.get("x").cmp(&right.get("x")));
        assert_eq!(
            bindings.iter().map(|row| row.get("x")).collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );
        bindings.retain(|row| row.get("x") != Some(2));
        assert_eq!(
            bindings.iter().map(|row| row.get("x")).collect::<Vec<_>>(),
            vec![Some(1), Some(3)]
        );
    }

    #[test]
    fn equality_compares_what_the_solutions_bind() {
        let left = sequence(&[&[("x", 1), ("y", 2)]]);
        assert_eq!(left.schema(), ["x".to_string(), "y".to_string()]);

        let mut reversed = Bindings::with_schema(vec!["y".into(), "x".into()]);
        reversed.push_cells(vec![2, 1]);
        assert_eq!(
            left, reversed,
            "column order is not part of what a result says"
        );

        let mut padded = Bindings::with_schema(vec!["x".into(), "y".into(), "gone".into()]);
        padded.push_cells(vec![1, 2, UNBOUND]);
        assert_eq!(
            left, padded,
            "a column no solution binds is not part of what a result says"
        );

        assert_ne!(left, sequence(&[&[("x", 1)]]), "binding less is not equal");
        assert_ne!(left, sequence(&[&[("x", 1), ("y", 9)]]), "values matter");
    }

    #[test]
    fn a_round_trip_through_the_map_shape_preserves_partial_rows() {
        let original = sequence(&[&[("x", 1), ("y", 2)], &[("x", 3)]]);
        let maps = original.to_maps();
        assert_eq!(maps.len(), 2);
        assert!(
            !maps[1].contains_key("y"),
            "unbound must not become a term id"
        );
        assert_eq!(Bindings::from(maps), original);
    }

    #[test]
    fn neither_half_of_the_id_space_can_mint_the_unbound_sentinel() {
        use crate::dictionary::Dictionary;
        use crate::quoted_triple_store::{QuotedTripleStore, QUOTED_TRIPLE_ID_BIT};

        let refusal = |f: Box<dyn FnOnce() + 'static>| -> String {
            let hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
            std::panic::set_hook(hook);
            let payload = caught.expect_err("minting the reserved id must panic");
            payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default()
        };

        let mut dictionary = Dictionary::new();
        dictionary.next_id = QUOTED_TRIPLE_ID_BIT - 1;
        assert_ne!(dictionary.encode("urn:the-last-ordinary-term"), UNBOUND);
        let message = refusal(Box::new(move || {
            dictionary.encode("urn:one-too-many");
        }));
        assert!(
            message.contains("ID space exhausted"),
            "the ordinary half must refuse to cross into the quoted triple range: {message}"
        );

        let mut quoted = QuotedTripleStore::new();
        quoted.next_qt_id = u32::MAX - 1;
        assert_ne!(quoted.encode(1, 2, 3), UNBOUND);
        let message = refusal(Box::new(move || {
            quoted.encode(4, 5, 6);
        }));
        assert!(
            message.contains("is reserved"),
            "the quoted half must refuse u32::MAX by its own guard, not by the \
             debug-only overflow check that disappears in release: {message}"
        );
    }

    #[test]
    #[should_panic(expected = "one value per column")]
    fn a_row_of_the_wrong_width_is_caught_in_debug() {
        let mut bindings = Bindings::with_schema(vec!["x".into(), "y".into()]);
        bindings.push_cells(vec![1]);
    }
}

/// Solution rows whose cells are terms as text, not dictionary ids
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextBindings {
    schema: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

impl TextBindings {
    /// No rows and no variables
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty sequence over a known schema
    pub fn with_schema(schema: Vec<String>) -> Self {
        Self {
            schema,
            rows: Vec::new(),
        }
    }

    /// The variables, in column order
    pub fn schema(&self) -> &[String] {
        &self.schema
    }

    /// How many solutions
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether there are no solutions
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The column `variable` occupies, if it is in the schema
    pub fn column(&self, variable: &str) -> Option<usize> {
        self.schema.iter().position(|name| name == variable)
    }

    /// The value of `variable` in row `row`, or `None` when unbound or absent
    pub fn get(&self, row: usize, variable: &str) -> Option<&str> {
        let column = self.column(variable)?;
        self.rows.get(row)?.get(column)?.as_deref()
    }

    /// The rows, each as a slice aligned with [`schema`](Self::schema)
    pub fn rows(&self) -> &[Vec<Option<String>>] {
        &self.rows
    }

    /// The bound cells of row `row`, as variable and value
    pub fn pairs(&self, row: usize) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.schema
            .iter()
            .zip(self.rows.get(row).map(Vec::as_slice).unwrap_or(&[]))
            .filter_map(|(name, cell)| cell.as_deref().map(|value| (name.as_str(), value)))
    }

    /// Adds a row from variable and value pairs, widening the schema as needed
    pub fn push_pairs<I, K, V>(&mut self, pairs: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let mut row = vec![None; self.schema.len()];
        for (variable, value) in pairs {
            let variable = variable.as_ref();
            let column = match self.column(variable) {
                Some(column) => column,
                None => {
                    self.schema.push(variable.to_string());
                    for existing in &mut self.rows {
                        existing.push(None);
                    }
                    row.push(None);
                    self.schema.len() - 1
                }
            };
            row[column] = Some(value.into());
        }
        self.rows.push(row);
    }

    /// Natural join: rows that agree on every shared variable, merged
    pub fn join(&self, other: &TextBindings) -> TextBindings {
        if self.is_empty() || other.is_empty() {
            return TextBindings::new();
        }

        let shared: Vec<(usize, usize)> = self
            .schema
            .iter()
            .enumerate()
            .filter_map(|(left, name)| other.column(name).map(|right| (left, right)))
            .collect();

        let mut schema = self.schema.clone();
        let carried: Vec<(usize, usize)> = other
            .schema
            .iter()
            .enumerate()
            .filter(|(_, name)| !self.schema.contains(name))
            .map(|(right, name)| {
                schema.push(name.clone());
                (right, schema.len() - 1)
            })
            .collect();

        let mut rows = Vec::new();
        for left_row in &self.rows {
            for right_row in &other.rows {
                let compatible = shared.iter().all(|&(left, right)| {
                    match (&left_row[left], &right_row[right]) {
                        (Some(left), Some(right)) => left == right,
                        _ => true,
                    }
                });
                if !compatible {
                    continue;
                }
                let mut row = vec![None; schema.len()];
                row[..left_row.len()].clone_from_slice(left_row);
                for &(right, target) in &carried {
                    row[target] = right_row[right].clone();
                }
                for &(left, right) in &shared {
                    if row[left].is_none() {
                        row[left] = right_row[right].clone();
                    }
                }
                rows.push(row);
            }
        }
        TextBindings { schema, rows }
    }

    /// Appends another sequence's rows, widening the schema to cover both
    pub fn extend(&mut self, other: TextBindings) {
        if self.schema == other.schema {
            self.rows.extend(other.rows);
            return;
        }
        let columns: Vec<usize> = other
            .schema
            .iter()
            .map(|name| match self.column(name) {
                Some(column) => column,
                None => {
                    self.schema.push(name.clone());
                    for existing in &mut self.rows {
                        existing.push(None);
                    }
                    self.schema.len() - 1
                }
            })
            .collect();
        for incoming in other.rows {
            let mut row = vec![None; self.schema.len()];
            for (cell, &column) in incoming.into_iter().zip(&columns) {
                row[column] = cell;
            }
            self.rows.push(row);
        }
    }

    /// Builds from the map-per-row shape, for a boundary that still produces it
    pub fn from_maps(maps: &[HashMap<String, String>]) -> Self {
        let mut bindings = TextBindings::new();
        for map in maps {
            bindings.push_pairs(map.iter().map(|(k, v)| (k.as_str(), v.clone())));
        }
        bindings
    }
}

#[cfg(test)]
mod text_bindings_tests {
    use super::TextBindings;

    fn row(bindings: &TextBindings, index: usize) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = bindings
            .pairs(index)
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        pairs.sort();
        pairs
    }

    #[test]
    fn a_row_answers_by_name_without_a_map() {
        let mut bindings = TextBindings::new();
        bindings.push_pairs([("s", "urn:a"), ("o", "25")]);
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings.get(0, "s"), Some("urn:a"));
        assert_eq!(bindings.get(0, "o"), Some("25"));
        assert_eq!(bindings.get(0, "missing"), None);
    }

    #[test]
    fn a_later_row_can_introduce_a_variable_and_the_earlier_rows_stay_aligned() {
        let mut bindings = TextBindings::new();
        bindings.push_pairs([("s", "urn:a")]);
        bindings.push_pairs([("s", "urn:b"), ("extra", "1")]);
        assert_eq!(bindings.schema().len(), 2);
        assert_eq!(bindings.rows()[0].len(), 2, "the first row was widened");
        assert_eq!(bindings.get(0, "extra"), None, "and left unbound");
        assert_eq!(bindings.get(1, "extra"), Some("1"));
    }

    #[test]
    fn an_unbound_cell_is_not_an_empty_string() {
        let mut bindings = TextBindings::new();
        bindings.push_pairs([("s", ""), ("t", "x")]);
        assert_eq!(bindings.get(0, "s"), Some(""));
        assert_eq!(
            row(&bindings, 0),
            vec![
                ("s".to_string(), String::new()),
                ("t".to_string(), "x".to_string()),
            ]
        );
    }

    #[test]
    fn a_partial_row_yields_only_its_bound_cells() {
        let mut bindings = TextBindings::new();
        bindings.push_pairs([("s", "urn:a"), ("o", "25")]);
        bindings.push_pairs([("s", "urn:b")]);
        assert_eq!(
            row(&bindings, 1),
            vec![("s".to_string(), "urn:b".to_string())]
        );
    }

    #[test]
    fn joining_on_no_shared_variable_is_the_cartesian_product() {
        let mut left = TextBindings::new();
        left.push_pairs([("s", "a")]);
        left.push_pairs([("s", "b")]);
        let mut right = TextBindings::new();
        right.push_pairs([("t", "x")]);
        right.push_pairs([("t", "y")]);
        let joined = left.join(&right);
        assert_eq!(joined.len(), 4);
        assert_eq!(joined.schema(), ["s", "t"]);
    }

    #[test]
    fn joining_on_a_shared_variable_keeps_only_agreeing_rows() {
        let mut left = TextBindings::new();
        left.push_pairs([("s", "a"), ("x", "1")]);
        left.push_pairs([("s", "b"), ("x", "2")]);
        let mut right = TextBindings::new();
        right.push_pairs([("s", "a"), ("y", "9")]);
        let joined = left.join(&right);
        assert_eq!(joined.len(), 1);
        assert_eq!(
            row(&joined, 0),
            vec![
                ("s".to_string(), "a".to_string()),
                ("x".to_string(), "1".to_string()),
                ("y".to_string(), "9".to_string()),
            ]
        );
    }

    #[test]
    fn an_unbound_shared_variable_is_compatible_with_any_value() {
        let mut left = TextBindings::new();
        left.push_pairs([("s", "a"), ("x", "1")]);
        left.push_pairs([("x", "2")]);
        let mut right = TextBindings::new();
        right.push_pairs([("s", "a")]);
        right.push_pairs([("s", "b")]);
        let joined = left.join(&right);
        assert_eq!(joined.len(), 3, "got {joined:?}");
        let values: Vec<Option<&str>> = (0..joined.len()).map(|i| joined.get(i, "s")).collect();
        assert_eq!(values, vec![Some("a"), Some("a"), Some("b")]);
    }

    #[test]
    fn joining_with_an_empty_side_is_empty() {
        let mut left = TextBindings::new();
        left.push_pairs([("s", "a")]);
        assert!(left.join(&TextBindings::new()).is_empty());
        assert!(TextBindings::new().join(&left).is_empty());
    }

    #[test]
    fn extending_concatenates_rows_rather_than_joining_them() {
        let mut left = TextBindings::new();
        left.push_pairs([("s", "a")]);
        let mut right = TextBindings::new();
        right.push_pairs([("s", "b")]);
        left.extend(right);
        assert_eq!(left.len(), 2, "two rows, not a join");
        assert_eq!(left.get(0, "s"), Some("a"));
        assert_eq!(left.get(1, "s"), Some("b"));
    }

    #[test]
    fn extending_with_a_wider_schema_leaves_the_earlier_rows_unbound() {
        let mut left = TextBindings::new();
        left.push_pairs([("s", "a")]);
        let mut right = TextBindings::new();
        right.push_pairs([("s", "b"), ("t", "x")]);
        left.extend(right);
        assert_eq!(left.schema().len(), 2);
        assert_eq!(left.get(0, "t"), None, "the row that predates the column");
        assert_eq!(left.get(1, "t"), Some("x"));
    }

    #[test]
    fn extending_realigns_a_schema_given_in_another_order() {
        let mut left = TextBindings::with_schema(vec!["s".into(), "t".into()]);
        left.push_pairs([("s", "a"), ("t", "x")]);
        let mut right = TextBindings::with_schema(vec!["t".into(), "s".into()]);
        right.push_pairs([("t", "y"), ("s", "b")]);
        left.extend(right);
        assert_eq!(left.get(1, "s"), Some("b"), "not y");
        assert_eq!(left.get(1, "t"), Some("y"), "not b");
    }

    #[test]
    fn building_from_maps_matches_building_from_pairs() {
        use std::collections::HashMap;
        let maps = vec![HashMap::from([
            ("s".to_string(), "urn:a".to_string()),
            ("o".to_string(), "25".to_string()),
        ])];
        let from_maps = TextBindings::from_maps(&maps);
        let mut from_pairs = TextBindings::new();
        from_pairs.push_pairs([("s", "urn:a"), ("o", "25")]);
        assert_eq!(row(&from_maps, 0), row(&from_pairs, 0));
    }
}
