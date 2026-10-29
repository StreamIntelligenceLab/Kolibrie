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
//! The representation [`crate::terms::Bindings`] will become. Variable
//! names are resolved to column positions once, for the whole relation, and a row is
//! a `Vec<u32>` rather than its own `HashMap<String, u32>`. `benches/micro_join.rs`
//! measured the difference on a 20,000 by 20,000 hash join over 2,000 keys: 12.7ms
//! against 54.4ms, so 4.3 times faster.
//!
//! The win is the layout, not the type. A control arm in that benchmark puts the
//! map shape behind a struct with accessors, keeping one `HashMap` per row, and
//! measures 51.11ms against the raw shape's 51.01ms -- 0.2% apart. Wrapping a
//! `HashMap` in Rust costs nothing. What costs is *having* one per row: the map
//! join hashes a string per key variable per row, and allocates 200,000 maps with
//! five cloned `String` keys for its output. This type keys on `row[i]` and pushes
//! a `Vec<u32>`.
//!
//! That also explains why [`to_bindings`](Relation::to_bindings) is the expensive
//! direction, at roughly 40ms: building those 200,000 maps is most of what the map
//! join spends its 54ms on. Converting a result back does not add a cost, it
//! re-does the work this representation exists to avoid.
//!
//! That benchmark binds every variable in every row. The engine does not: see
//! [partial rows](#partial-rows) below. The join's unbound paths are work this
//! figure does not include, so treat 4.3x as an upper bound measured on uniform
//! data, not as what the engine would gain.
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
//! Measured on that same join: converting the 20,000-row input costs 2.3ms, and
//! converting the 200,000-row result costs around 40ms — roughly 17 times more,
//! since every output row needs its own map with five string keys. The result
//! figure is allocator-bound and drifts: four runs gave 34, 40, 42 and 46ms, so
//! it is a magnitude rather than a number.
//!
//! The consequence for anything migrating onto this type: convert at the edges that
//! *supply* bindings, and make the edges that *return* them native. Joining in this
//! shape and handing the map shape back keeps 1.18x of the 4.3x. Doing it the other
//! way keeps 3.2x.
//!
//! Both conversion costs rose once the conversions became correct for partial rows:
//! the input from 0.85ms, because the schema is the union of every row's variables
//! and so needs a pass over all of them, and the result by about 8%, for the
//! per-cell unbound test. The earlier 4.1x for converting inputs was measured
//! against a first-row schema, which cannot represent what the engine produces.
//!
//! # Partial rows
//!
//! A SPARQL solution mapping is a *partial* function: `OPTIONAL` and `VALUES ... UNDEF`
//! produce sequences whose rows bind different variable sets, and the engine's join
//! already branches on it. The map shape said "unbound" by leaving a key out. A
//! fixed-width row cannot, so it says it in the cell, with [`UNBOUND`].
//!
//! # One invariant the map shape could not break
//!
//! Every row must hold exactly `schema().len()` values. A `HashMap` per row was
//! self-describing and so could not be the wrong width; a tuple can. [`push`](Relation::push)
//! checks it with a `debug_assert`, which is free in release and catches it in tests,
//! and that is a real safety property traded for the speed above rather than a free win.

use crate::terms::Bindings;
use std::collections::HashMap;

/// The value a column holds where a row binds nothing.
///
/// A solution mapping in SPARQL is *partial*: `OPTIONAL` and `VALUES ... UNDEF`
/// both produce sequences whose rows bind different variable sets. The map shape
/// expressed that by leaving a key out; a fixed-width row has to say it in the cell.
///
/// `u32::MAX` is safe to reserve because *both* halves of the id space refuse it.
/// [`Dictionary::encode`](crate::dictionary::Dictionary::encode) asserts
/// `next_id < QUOTED_TRIPLE_ID_BIT` (`0x8000_0000`), so an ordinary term id stays in
/// the lower half. Quoted triple ids climb from that bit upwards and so *could* have
/// reached the top, which is why
/// [`QuotedTripleStore::encode`](crate::quoted_triple_store::QuotedTripleStore::encode)
/// now asserts `next_qt_id < u32::MAX`. A variable can be bound to a quoted triple,
/// so without that second guard this sentinel would have been sound only by accident.
/// Both are pinned by tests below.
///
/// Note that `0` would **not** have been safe: `next_id` starts at zero, so the first
/// term interned has id `0`. Conflating that with "unbound" is the bug this replaces.
pub const UNBOUND: u32 = u32::MAX;

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

    /// Builds a relation from the map shape.
    ///
    /// The cheap direction: 0.85ms for 20,000 rows in `micro_join`. Use this at an
    /// edge that supplies bindings.
    ///
    /// The schema is the **union** of every row's variables, in first-seen order,
    /// not the first row's. Solution mappings are partial, so a variable may appear
    /// only in a later row; taking the first row's keys would drop it silently.
    ///
    /// A variable a row does not bind becomes [`UNBOUND`] in that column, which
    /// [`to_bindings`](Self::to_bindings) turns back into an absent key. Note that
    /// this is not interchangeable with `0`: the first term a dictionary interns has
    /// id `0`, so `0` means "the term numbered zero", never "nothing".
    pub fn from_bindings(bindings: &Bindings) -> Self {
        // `Bindings` is itself a schema and tuples now, so this is a copy rather
        // than a conversion, and the two types are one deduplication away.
        let schema: Vec<String> = bindings.schema().to_vec();
        let rows = bindings
            .iter()
            .map(|row| row.cells().to_vec())
            .collect();
        Self { schema, rows }
    }

    /// The value at `row`, `column`, or `None` where the row binds nothing there.
    ///
    /// Takes an index rather than a name on purpose; resolve the name once with
    /// [`column`](Self::column).
    pub fn get(&self, row: usize, column: usize) -> Option<u32> {
        match self.rows.get(row)?.get(column).copied()? {
            UNBOUND => None,
            id => Some(id),
        }
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
                // Sized up front. Collecting a filtered iterator cannot: the filter
                // turns an exact size hint into an upper bound, so the map starts
                // small and rehashes its way up. Measured at 58ms against 36ms on
                // the 200,000-row join result in `micro_join`.
                let mut out = HashMap::with_capacity(self.schema.len());
                for (name, id) in self.schema.iter().zip(row.iter().copied()) {
                    if id != UNBOUND {
                        out.insert(name.clone(), id);
                    }
                }
                out
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
        // Compared as maps, since the column order is not part of the map shape.
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
    fn a_row_that_does_not_bind_a_variable_round_trips_as_absent() {
        // What OPTIONAL produces: ?y bound in the first row, unbound in the second.
        let original = bindings(&[&[("x", 1), ("y", 2)], &[("x", 3)]]);
        let relation = Relation::from_bindings(&original);

        assert_eq!(relation.schema().len(), 2, "both variables are columns");
        let y = relation.column("y").expect("y is a column");
        assert_eq!(relation.get(0, y), Some(2));
        assert_eq!(relation.get(1, y), None, "the second row binds no y");

        // The round trip must give back an absent key, not a key bound to a sentinel.
        let back = relation.to_bindings();
        assert_eq!(back, original);
        assert!(
            !back.row(1).expect("two rows").contains_key("y"),
            "unbound must not become a term id"
        );
    }

    #[test]
    fn a_variable_only_a_later_row_binds_is_still_a_column() {
        // Taking the schema from the first row dropped ?y entirely. The rows are
        // partial, so there is no row guaranteed to carry every variable.
        let original = bindings(&[&[("x", 1)], &[("x", 2), ("y", 9)]]);
        let relation = Relation::from_bindings(&original);

        assert_eq!(
            relation.column("y").map(|y| relation.get(1, y)),
            Some(Some(9)),
            "a variable bound only in a later row must survive the conversion"
        );
        assert_eq!(relation.to_bindings(), original);
    }

    #[test]
    fn term_id_zero_is_a_term_and_not_unbound() {
        // Dictionary::next_id starts at 0, so the first term ever interned has id 0.
        // The conversion used to write 0 for a variable a row did not bind, which
        // made these two cases indistinguishable.
        let original = bindings(&[&[("x", 0)], &[("y", 0)]]);
        let relation = Relation::from_bindings(&original);

        let x = relation.column("x").expect("x is a column");
        assert_eq!(relation.get(0, x), Some(0), "id 0 is a real binding");
        assert_eq!(relation.get(1, x), None, "the second row binds no x");
        assert_eq!(relation.to_bindings(), original);
    }

    #[test]
    fn a_row_binding_nothing_is_the_unit_solution_mapping() {
        // One row, no variables: what a query with no projected variables matches,
        // and what the engine spells `vec![HashMap::new()]`. It is not "no rows".
        let original = Bindings::unit();
        let relation = Relation::from_bindings(&original);

        assert_eq!(relation.len(), 1, "one row");
        assert!(relation.schema().is_empty(), "over no columns");
        assert!(!relation.is_empty(), "which is not the empty relation");
        assert_eq!(relation.to_bindings(), original);
    }

    #[test]
    fn neither_half_of_the_id_space_can_mint_the_unbound_sentinel() {
        // UNBOUND is only sound because no id can equal it. Pin both guards: a
        // relaxed assert in either encoder would make a real term read as unbound.
        use crate::dictionary::Dictionary;
        use crate::quoted_triple_store::{QuotedTripleStore, QUOTED_TRIPLE_ID_BIT};

        // Each guard is identified by its message. Asserting only that the call
        // panics is not enough: these tests run in debug, where the `+= 1` past the
        // end overflows and panics on its own. That would make this test pass with
        // the guard deleted, while a release build wrapped around and handed back
        // `u32::MAX` as a real id -- which is the whole failure being prevented.
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
            "the ordinary half must refuse to cross into the quoted triple range, \
             not fail some other way: {message}"
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
        let mut relation = Relation::new(vec!["x".into(), "y".into()]);
        relation.push(vec![1]);
    }
}
