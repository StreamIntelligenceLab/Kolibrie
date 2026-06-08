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
use crate::triple::Triple;
use crate::quoted_triple_store::{QuotedTripleStore, is_quoted_triple_id, QUOTED_TRIPLE_ID_BIT};

// Dictionary for encoding and decoding strings
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Dictionary {
    pub string_to_id: HashMap<String, u32>,
    pub id_to_string: HashMap<u32, String>,
    pub next_id: u32,
}

impl Dictionary {
    pub fn new() -> Self {
        Dictionary {
            string_to_id: HashMap::new(),
            id_to_string: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn encode(&mut self, value: &str) -> u32 {
        if let Some(&id) = self.string_to_id.get(value) {
            id
        } else {
            assert!(
                self.next_id < QUOTED_TRIPLE_ID_BIT,
                "Dictionary ID space exhausted: next_id {} would collide with quoted triple ID range",
                self.next_id
            );
            let id = self.next_id;
            self.string_to_id.insert(value.to_string(), id);
            self.id_to_string.insert(id, value.to_string());
            self.next_id += 1;
            id
        }
    }

    pub fn decode(&self, id: u32) -> Option<&str> {
        self.id_to_string.get(&id).map(|s| s.as_str())
    }

    pub fn decode_triple(&self, triple: &Triple) -> String {
        let s = self.decode(triple.subject).unwrap_or("unknown");
        let p = self.decode(triple.predicate).unwrap_or("unknown");
        let o = self.decode(triple.object).unwrap_or("unknown");
        format!("{} {} {} .", s, p, o)
    }

    /// Decode a term ID that may be either a regular dictionary ID or a quoted triple ID.
    /// For quoted triple IDs, recursively renders `<< s p o >>`.
    pub fn decode_term(&self, id: u32, qt_store: &QuotedTripleStore) -> Option<String> {
        if is_quoted_triple_id(id) {
            let (s, p, o) = qt_store.decode(id)?;
            let s_str = self.decode_term(s, qt_store)?;
            let p_str = self.decode_term(p, qt_store)?;
            let o_str = self.decode_term(o, qt_store)?;
            Some(format!("<< {} {} {} >>", s_str, p_str, o_str))
        } else {
            self.decode(id).map(|s| s.to_string())
        }
    }

    /// Decode a triple, handling quoted triple IDs in any position.
    pub fn decode_triple_star(&self, triple: &Triple, qt_store: &QuotedTripleStore) -> String {
        let s = self.decode_term(triple.subject, qt_store).unwrap_or_else(|| "unknown".to_string());
        let p = self.decode_term(triple.predicate, qt_store).unwrap_or_else(|| "unknown".to_string());
        let o = self.decode_term(triple.object, qt_store).unwrap_or_else(|| "unknown".to_string());
        format!("{} {} {} .", s, p, o)
    }

    pub fn merge(&mut self, other: &Dictionary) {
        for (key, value) in other.string_to_id.iter() {
            self.string_to_id.entry(key.clone()).or_insert(*value);
        }
        for (key, value) in other.id_to_string.iter() {
            self.id_to_string.entry(*key).or_insert(value.clone());
        }
        self.next_id = self.next_id.max(other.next_id);
    }
}
