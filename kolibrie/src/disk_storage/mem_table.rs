/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v.2.0.If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::triple::Triple;
use std::collections::BTreeMap;

// In-memory sorted table
#[derive(Debug, Clone)]
pub struct MemTable {
    /// Sorted map of triples
    pub data: BTreeMap<Triple, TripleMetadata>,
    /// Size in bytes (approximate)
    pub size: usize,
    /// Creation timestamp
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct TripleMetadata {
    /// Timestamp when triple was added
    // timestamp: u64,
    /// Whether this is a deletion marker
    pub is_deleted: bool,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            size: 0,
            created_at: Self::current_timestamp(),
        }
    }

    pub fn insert(&mut self, triple: Triple) -> bool {
        let metadata = TripleMetadata {
            // timestamp: Self::current_timestamp(),
            is_deleted: false,
        };
        
        let is_new = !self.data.contains_key(&triple);
        self.data.insert(triple, metadata);
        
        if is_new {
            self.size += std::mem::size_of::<Triple>() + std::mem::size_of::<TripleMetadata>();
        }
        
        is_new
    }

    pub fn delete(&mut self, triple: &Triple) -> bool {
        let metadata = TripleMetadata {
            // timestamp: Self::current_timestamp(),
            is_deleted: true,
        };
        
        self.data.insert(triple.clone(), metadata).is_some()
    }

    pub fn get(&self, triple: &Triple) -> Option<&TripleMetadata> {
        self.data.get(triple)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn size_bytes(&self) -> usize {
        self.size
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Triple, &TripleMetadata)> {
        self.data.iter()
    }

    pub fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}