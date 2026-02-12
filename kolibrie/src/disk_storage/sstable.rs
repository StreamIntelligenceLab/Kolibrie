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
use shared::index_manager::UnifiedIndex;
use std::path::{Path, PathBuf};
use std::fs::File;
use serde::{Serialize, Deserialize};
use crate::disk_storage::mem_table::MemTable;

/// SSTable (Sorted String Table) stored on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTable {
    /// Unique identifier
    pub id: usize,
    /// Level in LSM tree (0, 1, 2, ...)
    pub level: usize,
    /// UnifiedIndex containing all 6 permutations
    pub index: UnifiedIndex,
    /// Min and max keys for range queries (optimization)
    pub min_key: Triple,
    pub max_key: Triple,
    /// Number of triples
    pub num_triples: usize,
    /// File path
    pub file_path: PathBuf,
    /// Creation timestamp
    pub created_at: u64,
}

impl SSTable {
    /// Create SSTable from MemTable
    pub fn from_memtable(
        id: usize,
        level: usize,
        memtable: &MemTable,
        data_dir: &Path,
    ) -> Result<Self, String> {
        let mut index = UnifiedIndex::new();
        let mut triples: Vec<Triple> = Vec::new();

        // Only include non-deleted triples
        for (triple, metadata) in memtable.iter() {
            if !metadata.is_deleted {
                triples.push(triple.clone());
            }
        }

        if triples.is_empty() {
            return Err("Cannot create SSTable from empty memtable".to_string());
        }

        // Build unified index
        index.build_from_triples(&triples);

        let min_key = triples.iter().min().unwrap().clone();
        let max_key = triples.iter().max().unwrap().clone();

        // Create file path
        let file_path = data_dir.join(format!("sstable_L{}_{}_{}.sst", level, id, memtable.created_at));

        // Serialize to disk
        let sstable = Self {
            id,
            level,
            index,
            min_key,
            max_key,
            num_triples: triples.len(),
            file_path: file_path.clone(),
            created_at: memtable.created_at,
        };

        sstable.write_to_disk()?;

        Ok(sstable)
    }

    /// Merge multiple SSTables into one
    pub fn merge(
        id: usize,
        level: usize,
        sstables: Vec<&SSTable>,
        data_dir: &Path,
    ) -> Result<Self, String> {
        let mut merged_index = UnifiedIndex::new();
        
        // Merge all indexes
        for sstable in &sstables {
            merged_index.merge_from(sstable.index.clone());
        }

        // Optimize merged index
        merged_index.optimize();

        let all_triples = merged_index.query(None, None, None);
        
        if all_triples.is_empty() {
            return Err("Cannot create SSTable from empty merge".to_string());
        }

        let min_key = all_triples.iter().min().unwrap().clone();
        let max_key = all_triples.iter().max().unwrap().clone();

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let file_path = data_dir.join(format!("sstable_L{}_{}_{}.sst", level, id, timestamp));

        let sstable = Self {
            id,
            level,
            index: merged_index,
            min_key,
            max_key,
            num_triples: all_triples.len(),
            file_path: file_path.clone(),
            created_at: timestamp,
        };

        sstable.write_to_disk()?;

        Ok(sstable)
    }

    /// Query SSTable for triples
    pub fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        self.index.query(s, p, o)
    }

    /// Check if triple exists in SSTable
    pub fn contains(&self, triple: &Triple) -> bool {
        !self.index.query(Some(triple.subject), Some(triple.predicate), Some(triple.object)).is_empty()
    }

    /// Check if key range overlaps with another SSTable
    pub fn overlaps_with(&self, other: &SSTable) -> bool {
        !(self.max_key < other.min_key || self.min_key > other.max_key)
    }

    /// Write SSTable to disk
    fn write_to_disk(&self) -> Result<(), String> {
        let file = File::create(&self.file_path)
            .map_err(|e| format!("Failed to create SSTable file: {}", e))?;
        
        bincode::serialize_into(file, self)
            .map_err(|e| format!("Failed to serialize SSTable: {}", e))?;
        
        Ok(())
    }

    /// Load SSTable from disk
    pub fn load_from_disk(path: &Path) -> Result<Self, String> {
        let file = File::open(path)
            .map_err(|e| format!("Failed to open SSTable file: {}", e))?;
        
        bincode::deserialize_from(file)
            .map_err(|e| format!("Failed to deserialize SSTable: {}", e))
    }

    /// Delete SSTable file from disk
    pub fn delete_from_disk(&self) -> Result<(), String> {
        std::fs::remove_file(&self.file_path)
            .map_err(|e| format!("Failed to delete SSTable file: {}", e))
    }
}