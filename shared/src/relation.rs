/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`Relation`]: solution bindings as a schema and tuples.
//!
//! The representation [`Bindings`](crate::terms::Bindings) will become. Variable
//! names are resolved to column positions once, for the whole relation, and a row is
//! a `Vec<u32>` rather than its own `HashMap<String, u32>`. `benches/micro_join.rs`
//! measured the difference on a 20,000 by 20,000 hash join over 2,000 keys: 11.6ms
//! against 54.6ms, so 4.7 times faster.
//!
//! # Named `Relation` only while both shapes exist
//!
//! `Bindings` is the better name — it says what the thing is rather than how it is
//! stored — but it currently *means* the map shape at 55 production sites. Reusing
//! it now would silently change the meaning of every one of them. This takes the
//! architecture proposal's name until the old alias is gone, and is renamed then.
//!
//! # Convert inputs, never results
//!
//! [`from_bindings`](Relation::from_bindings) is cheap and
//! [`to_bindings`](Relation::to_bindings) is not, because a join multiplies rows.
//! Measured on that same join: converting the 20,000-row input costs 0.85ms, and
//! converting the 200,000-row result costs 32.1ms — 38 times more, since every
//! output row needs its own map with five string keys.
//!
//! The consequence for anything migrating onto this type: convert at the edges that
//! *supply* bindings, and make the edges that *return* them native. Joining in this
//! shape and handing the map shape back keeps 1.23x of the 4.7x. Doing it the other
//! way keeps 4.1x.
//!
//! # One invariant the map shape could not break
//!
//! Every row must hold exactly `schema().len()` values. A `HashMap` per row was
//! self-describing and so could not be the wrong width; a tuple can. [`push`](Relation::push)
//! checks it with a `debug_assert`, which is free in release and catches it in tests,
//! and that is a real safety property traded for the speed above rather than a free win.

use crate::terms::Bindings;
use std::collections::HashMap;

/// Solution bindings as a schema and tuples of term ids.
///
/// Column order is the schema's order, and every row matches its width.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Relation {
    schema: Vec<String>,
    rows: Vec<Vec<u32>>,
}

impl Relation {
    /// An empty relation over `schema`.
    pub fn new(schema: Vec<String>) -> Self {
        Self {
            schema,
            rows: Vec::new(),
        }
    }

    /// A relation over `schema` holding `rows`.
    ///
    /// # Panics
    ///
    /// In debug builds, if any row's width differs from the schema's.
    pub fn with_rows(schema: Vec<String>, rows: Vec<Vec<u32>>) -> Self {
        debug_assert!(
            rows.iter().all(|row| row.len() == schema.len()),
            "every row must hold one value per column"
        );
        Self { schema, rows }
    }

    /// The variables, in column order.
    pub fn schema(&self) -> &[String] {
        &self.schema
    }

    /// The column index of `name`, or `None` if it is not projected.
    ///
    /// Resolve once and index rows with the result. Looking a name up per row is
    /// what the map shape did and what this exists to stop, so there is
    /// deliberately no method that takes a row and a name together.
    pub fn column(&self, name: &str) -> Option<usize> {
        self.schema.iter().position(|v| v == name)
    }

    /// The rows, each one value per column.
    pub fn rows(&self) -> &[Vec<u32>] {
        &self.rows
    }

    /// Appends a row.
    ///
    /// # Panics
    ///
    /// In debug builds, if `row`'s width differs from the schema's.
    pub fn push(&mut self, row: Vec<u32>) {
        debug_assert_eq!(
            row.len(),
            self.schema.len(),
            "a row must hold one value per column"
        );
        self.rows.push(row);
    }

    /// How many rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether there are no rows.
    ///
    /// A relation with a schema and no rows is different from one with no schema:
    /// the first matched nothing, the second projects nothing.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The variables both relations project, in `self`'s column order.
    ///
    /// What a join needs in order to know what to key on.
    pub fn shared_columns(&self, other: &Self) -> Vec<String> {
        self.schema
            .iter()
            .filter(|v| other.schema.contains(v))
            .cloned()
            .collect()
    }

    /// Builds a relation from the map shape, taking the schema from the first row.
    ///
    /// The cheap direction: 0.85ms for 20,000 rows in `micro_join`. Use this at an
    /// edge that supplies bindings.
    ///
    /// Rows missing a variable the first row had contribute `0` in that column,
    /// which is the same silent default `construct_triple` already uses for an
    /// unbound term. A caller that needs to distinguish unbound from id `0` cannot
    /// use either, and that predates this type.
    pub fn from_bindings(bindings: &Bindings) -> Self {
        let schema: Vec<String> = bindings
            .first()
            .map(|row| row.keys().cloned().collect())
            .unwrap_or_default();
        let rows = bindings
            .iter()
            .map(|row| {
                schema
                    .iter()
                    .map(|v| row.get(v).copied().unwrap_or(0))
                    .collect()
            })
            .collect();
        Self { schema, rows }
    }

    /// Rebuilds the map shape.
    ///
    /// The expensive direction: 32.1ms for the 200,000-row join result in
    /// `micro_join`, against 11.6ms for the join that produced it. Every row
    /// allocates its own map and re-clones every variable name. Converting a join
    /// result this way gives back most of what the representation saves, so this is
    /// for an edge that cannot yet be made native, not for a hot path.
    pub fn to_bindings(&self) -> Bindings {
        self.rows
            .iter()
            .map(|row| {
                self.schema
                    .iter()
                    .cloned()
                    .zip(row.iter().copied())
                    .collect::<HashMap<String, u32>>()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bindings(rows: &[&[(&str, u32)]]) -> Bindings {
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|(k, v)| ((*k).to_string(), *v))
                    .collect::<HashMap<String, u32>>()
            })
            .collect()
    }

    #[test]
    fn a_round_trip_through_the_map_shape_preserves_every_row() {
        let original = bindings(&[&[("x", 1), ("y", 2)], &[("x", 3), ("y", 4)]]);
        let relation = Relation::from_bindings(&original);
        assert_eq!(relation.len(), 2);
        assert_eq!(relation.schema().len(), 2);
        // Compared as maps, because the schema's column order is the first row's
        // key order, which a HashMap does not fix.
        assert_eq!(relation.to_bindings(), original);
    }

    #[test]
    fn a_column_is_resolved_once_and_indexes_every_row() {
        let relation = Relation::from_bindings(&bindings(&[
            &[("x", 10), ("y", 20)],
            &[("x", 30), ("y", 40)],
        ]));
        let x = relation.column("x").expect("x is projected");
        let values: Vec<u32> = relation.rows().iter().map(|row| row[x]).collect();
        assert_eq!(values, vec![10, 30]);
        assert_eq!(relation.column("absent"), None);
    }

    #[test]
    fn shared_columns_are_what_a_join_keys_on() {
        let left = Relation::new(vec!["x".into(), "a".into()]);
        let right = Relation::new(vec!["x".into(), "b".into()]);
        assert_eq!(left.shared_columns(&right), vec!["x".to_string()]);
        let disjoint = Relation::new(vec!["q".into()]);
        assert!(left.shared_columns(&disjoint).is_empty());
    }

    #[test]
    fn an_empty_relation_keeps_its_schema() {
        let relation = Relation::new(vec!["x".into()]);
        assert!(relation.is_empty());
        assert_eq!(relation.schema(), ["x".to_string()]);
        assert!(relation.to_bindings().is_empty());
    }

    #[test]
    #[should_panic(expected = "one value per column")]
    fn a_row_of_the_wrong_width_is_caught_in_debug() {
        let mut relation = Relation::new(vec!["x".into(), "y".into()]);
        relation.push(vec![1]);
    }
}
