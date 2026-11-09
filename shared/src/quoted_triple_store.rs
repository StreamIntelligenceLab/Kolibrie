/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Bit mask for identifying quoted triple IDs
pub const QUOTED_TRIPLE_ID_BIT: u32 = 0x8000_0000;

/// Whether `id` denotes a quoted triple rather than an ordinary interned term
#[inline]
pub fn is_quoted_triple_id(id: u32) -> bool {
    id & QUOTED_TRIPLE_ID_BIT != 0
}

/// Stores quoted triples (RDF-star) as u32 IDs with bidirectional lookup
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Interns RDF-star quoted triples, in an id range disjoint from the dictionary's
pub struct QuotedTripleStore {
    /// Id to its three component ids
    pub id_to_components: HashMap<u32, (u32, u32, u32)>,
    /// Components back to their id, so re-encoding is idempotent
    pub components_to_id: HashMap<(u32, u32, u32), u32>,
    /// Next id to hand out
    pub next_qt_id: u32,
}

impl QuotedTripleStore {
    /// An empty store, assigning ids from [`QUOTED_TRIPLE_ID_BIT`]
    pub fn new() -> Self {
        Self {
            id_to_components: HashMap::new(),
            components_to_id: HashMap::new(),
            next_qt_id: QUOTED_TRIPLE_ID_BIT,
        }
    }

    /// Encode a quoted triple (s, p, o) -> returns a quoted triple ID
    pub fn encode(&mut self, subject: u32, predicate: u32, object: u32) -> u32 {
        let key = (subject, predicate, object);
        if let Some(&id) = self.components_to_id.get(&key) {
            return id;
        }
        assert!(
            self.next_qt_id < u32::MAX,
            "quoted triple ID space exhausted: next_qt_id {} is reserved",
            self.next_qt_id
        );
        let id = self.next_qt_id;
        self.next_qt_id += 1;
        self.id_to_components.insert(id, key);
        self.components_to_id.insert(key, id);
        id
    }

    /// Decode a quoted triple ID back to its (s, p, o) components
    pub fn decode(&self, id: u32) -> Option<(u32, u32, u32)> {
        self.id_to_components.get(&id).copied()
    }

    /// Returns the number of quoted triples stored
    pub fn len(&self) -> usize {
        self.id_to_components.len()
    }

    /// Returns true if the store is empty
    pub fn is_empty(&self) -> bool {
        self.id_to_components.is_empty()
    }

    /// Merge another store into this one (needed for parallel parsing)
    pub fn merge(&mut self, other: &QuotedTripleStore) {
        for (&id, &components) in &other.id_to_components {
            self.id_to_components.entry(id).or_insert(components);
            self.components_to_id.entry(components).or_insert(id);
        }
        self.next_qt_id = self.next_qt_id.max(other.next_qt_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut store = QuotedTripleStore::new();
        let id = store.encode(1, 2, 3);
        assert!(is_quoted_triple_id(id));
        assert_eq!(store.decode(id), Some((1, 2, 3)));
    }

    #[test]
    fn test_deduplication() {
        let mut store = QuotedTripleStore::new();
        let id1 = store.encode(1, 2, 3);
        let id2 = store.encode(1, 2, 3);
        assert_eq!(id1, id2);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_different_triples_get_different_ids() {
        let mut store = QuotedTripleStore::new();
        let id1 = store.encode(1, 2, 3);
        let id2 = store.encode(4, 5, 6);
        assert_ne!(id1, id2);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_nested_quoted_triples() {
        let mut store = QuotedTripleStore::new();
        let inner_id = store.encode(1, 2, 3);
        let outer_id = store.encode(inner_id, 4, 5);
        assert!(is_quoted_triple_id(inner_id));
        assert!(is_quoted_triple_id(outer_id));
        assert_ne!(inner_id, outer_id);
        let (s, p, o) = store.decode(outer_id).unwrap();
        assert_eq!(s, inner_id);
        assert_eq!(p, 4);
        assert_eq!(o, 5);
    }

    #[test]
    fn test_is_quoted_triple_id() {
        assert!(!is_quoted_triple_id(0));
        assert!(!is_quoted_triple_id(100));
        assert!(!is_quoted_triple_id(0x7FFF_FFFF));
        assert!(is_quoted_triple_id(0x8000_0000));
        assert!(is_quoted_triple_id(0x8000_0001));
        assert!(is_quoted_triple_id(0xFFFF_FFFF));
    }

    #[test]
    fn test_merge() {
        let mut store1 = QuotedTripleStore::new();
        let id1 = store1.encode(1, 2, 3);

        let mut store2 = QuotedTripleStore::new();
        store2.next_qt_id = store1.next_qt_id;
        let id2 = store2.encode(4, 5, 6);

        store1.merge(&store2);
        assert_eq!(store1.len(), 2);
        assert_eq!(store1.decode(id1), Some((1, 2, 3)));
        assert_eq!(store1.decode(id2), Some((4, 5, 6)));
    }

    #[test]
    fn test_decode_nonexistent() {
        let store = QuotedTripleStore::new();
        assert_eq!(store.decode(0x8000_0000), None);
    }
}
