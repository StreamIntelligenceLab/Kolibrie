/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::terms::{Term, TriplePattern};
use serde::{Deserialize, Serialize};

/// An RDF triple with all three positions already encoded to dictionary ids.
///
/// The ids are only meaningful relative to the [`Dictionary`] that produced them, so
/// a `Triple` cannot be decoded — or compared against one from another store —
/// without that dictionary. Two engines exchanging triples must share one.
///
/// An id in `subject` or `object` may be a quoted triple rather than an ordinary
/// term, which the top bit indicates; see
/// [`is_quoted_triple_id`](crate::quoted_triple_store::is_quoted_triple_id).
///
/// `Ord` is derived, so triples sort by `(subject, predicate, object)` as integers.
/// That ordering has no relationship to the lexical order of the terms, and the
/// indexes in [`dataset_index`](crate::dataset_index) rely on it.
///
/// [`Dictionary`]: crate::dictionary::Dictionary
#[derive(PartialEq, Debug, Clone, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Triple {
    /// Encoded subject.
    pub subject: u32,
    /// Encoded predicate.
    pub predicate: u32,
    /// Encoded object.
    pub object: u32,
}

impl Triple {
    /// Converts a triple (subject, predicate, object) (all u32 values) into a TriplePattern
    /// Simply wraps each element into a constant
    pub fn to_pattern(&self) -> TriplePattern {
        (
            Term::Constant(self.subject),
            Term::Constant(self.predicate),
            Term::Constant(self.object),
        )
    }
}
