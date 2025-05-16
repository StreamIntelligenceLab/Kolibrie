/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use crate::triple::Triple;

// Dictionary for encoding and decoding strings
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Dictionary {
    string_to_id: HashMap<String, u32>,
    id_to_string: HashMap<u32, String>,
    next_id: u32,
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
