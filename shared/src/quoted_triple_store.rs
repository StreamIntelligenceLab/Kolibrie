/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Bit mask for identifying quoted triple IDs.
/// Regular dictionary IDs use bits 0–30 (up to 2,147,483,647).
/// Quoted triple IDs have bit 31 set (0x8000_0000 and above).
pub const QUOTED_TRIPLE_ID_BIT: u32 = 0x8000_0000;

#[inline]
pub fn is_quoted_triple_id(id: u32) -> bool {
    id & QUOTED_TRIPLE_ID_BIT != 0
}

/// Stores quoted triples (RDF-star) as u32 IDs with bidirectional lookup.
/// Each quoted triple `<< s p o >>` gets a unique ID with the high bit set.
/// Component IDs (s, p, o) may themselves be quoted triple IDs for nesting.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotedTripleStore {
    pub id_to_components: HashMap<u32, (u32, u32, u32)>,
    pub components_to_id: HashMap<(u32, u32, u32), u32>,
    pub next_qt_id: u32,
}

impl QuotedTripleStore {
    pub fn new() -> Self {
        Self {
            id_to_components: HashMap::new(),
            components_to_id: HashMap::new(),
            next_qt_id: QUOTED_TRIPLE_ID_BIT,
        }
    }

    /// Encode a quoted triple (s, p, o) → returns a quoted triple ID.
    /// Reuses an existing ID if this triple was already encoded.
    pub fn encode(&mut self, subject: u32, predicate: u32, object: u32) -> u32 {
        let key = (subject, predicate, object);
        if let Some(&id) = self.components_to_id.get(&key) {
            return id;
        }
        let id = self.next_qt_id;
        self.next_qt_id += 1;
        self.id_to_components.insert(id, key);
        self.components_to_id.insert(key, id);
        id
    }

    /// Decode a quoted triple ID back to its (s, p, o) components.
    pub fn decode(&self, id: u32) -> Option<(u32, u32, u32)> {
        self.id_to_components.get(&id).copied()
    }

    /// Returns the number of quoted triples stored.
    pub fn len(&self) -> usize {
        self.id_to_components.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.id_to_components.is_empty()
    }

    /// Merge another store into this one (needed for parallel parsing).
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
        // Inner: << 1 2 3 >>
        let inner_id = store.encode(1, 2, 3);
        // Outer: << <<1 2 3>> 4 5 >>
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
        store2.next_qt_id = store1.next_qt_id; // simulate separate counter
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
