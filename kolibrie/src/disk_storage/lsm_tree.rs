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
use std::collections::VecDeque;
use std::sync::{Arc, RwLock, Mutex};
use std::path::PathBuf;
use crate::disk_storage::wal::{WalOperation, WriteAheadLog};
use crate::disk_storage::sstable::SSTable;
use crate::disk_storage::mem_table::MemTable;

/// Configuration for LSM-Tree behavior
#[derive(Debug, Clone)]
pub struct LSMConfig {
    /// Max size of memtable before flushing (in number of triples)
    pub memtable_size_threshold: usize,
    /// Max number of Level 0 SSTables before compaction
    pub level0_compaction_threshold: usize,
    /// Size multiplier for each level
    pub level_size_multiplier: usize,
    /// Base size for Level 1 (in number of SSTables)
    pub level1_base_size: usize,
    /// Enable Write-Ahead Log
    pub enable_wal: bool,
    /// WAL sync on every write (slower but safer)
    pub wal_sync_on_write: bool,
    /// Directory for storing SSTables
    pub data_dir: PathBuf,
}

impl Default for LSMConfig {
    fn default() -> Self {
        Self {
            memtable_size_threshold: 10_000,
            level0_compaction_threshold: 4,
            level_size_multiplier: 10,
            level1_base_size: 10,
            enable_wal: true,
            wal_sync_on_write: false,
            data_dir: PathBuf::from("./lsm_data"),
        }
    }
}

/// Compaction strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStrategy {
    /// Size-tiered compaction
    SizeTiered,
    /// Leveled compaction (default)
    Leveled,
}

/// LSM-Tree main structure
#[derive(Debug)]
pub struct LSMTree {
    /// Active memtable for writes
    memtable: Arc<RwLock<MemTable>>,
    
    /// Immutable memtables waiting to be flushed
    immutable_memtables: Arc<Mutex<VecDeque<MemTable>>>,
    
    /// Write-Ahead Log
    wal: Arc<Mutex<WriteAheadLog>>,
    
    /// SSTable levels
    /// Level 0: Overlapping SSTables (newly flushed)
    /// Level 1+: Non-overlapping SSTables (compacted)
    levels: Arc<RwLock<Vec<Vec<SSTable>>>>,
    
    /// Configuration
    config: LSMConfig,
    
    /// Next SSTable ID
    next_sstable_id: Arc<Mutex<usize>>,
    
    /// Compaction strategy
    compaction_strategy: CompactionStrategy,
    
    /// Compaction in progress flag
    compaction_in_progress: Arc<Mutex<bool>>,
}

impl LSMTree {
    /// Create new LSM-Tree with default config
    pub fn new() -> Result<Self, String> {
        Self::with_config(LSMConfig::default())
    }

    /// Create new LSM-Tree with custom config
    pub fn with_config(config: LSMConfig) -> Result<Self, String> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| format!("Failed to create data directory: {}", e))?;

        // Initialize WAL
        let wal_path = config.data_dir.join("wal.log");
        let wal = WriteAheadLog::new(wal_path, config.wal_sync_on_write)?;

        let lsm_tree = Self {
            memtable: Arc::new(RwLock::new(MemTable::new())),
            immutable_memtables: Arc::new(Mutex::new(VecDeque::new())),
            wal: Arc::new(Mutex::new(wal)),
            levels: Arc::new(RwLock::new(vec![Vec::new(); 7])), // 7 levels (0-6)
            config,
            next_sstable_id: Arc::new(Mutex::new(0)),
            compaction_strategy: CompactionStrategy::Leveled,
            compaction_in_progress: Arc::new(Mutex::new(false)),
        };

        // Recover from WAL if exists
        lsm_tree.recover_from_wal()?;

        Ok(lsm_tree)
    }

    /// Insert a triple
    pub fn insert(&self, triple: Triple) -> Result<(), String> {
        // Log to WAL first
        if self.config.enable_wal {
            self.wal.lock().unwrap().log_insert(&triple)?;
        }

        // Insert into memtable
        let mut memtable = self.memtable.write().unwrap();
        memtable.insert(triple);

        // Check if memtable is full
        if memtable.len() >= self.config.memtable_size_threshold {
            drop(memtable); // Release lock
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Bulk insert triples (optimized)
    pub fn bulk_insert(&self, triples: &[Triple]) -> Result<(), String> {
        for triple in triples {
            self.insert(triple.clone())?;
        }
        Ok(())
    }

    /// Delete a triple
    pub fn delete(&self, triple: &Triple) -> Result<(), String> {
        // Log to WAL
        if self.config.enable_wal {
            self.wal.lock().unwrap().log_delete(triple)?;
        }

        // Mark as deleted in memtable
        let mut memtable = self.memtable.write().unwrap();
        memtable.delete(triple);

        Ok(())
    }

    /// Query for triples
    pub fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        let memtable = self.memtable.read().unwrap();
        for (triple, metadata) in memtable.iter() {
            if Self::matches_query(triple, s, p, o) && !metadata.is_deleted {
                results.push(triple.clone());
                seen.insert(triple.clone());
            } else if metadata.is_deleted {
                seen.insert(triple.clone());
            }
        }
        drop(memtable);

        let immutable_memtables = self.immutable_memtables.lock().unwrap();
        for memtable in immutable_memtables.iter() {
            for (triple, metadata) in memtable.iter() {
                if seen.contains(triple) {
                    continue;
                }
                if Self::matches_query(triple, s, p, o) && !metadata.is_deleted {
                    results.push(triple.clone());
                    seen.insert(triple.clone());
                } else if metadata.is_deleted {
                    seen.insert(triple.clone());
                }
            }
        }
        drop(immutable_memtables);

        let levels = self.levels.read().unwrap();
        for level_sstables in levels.iter() {
            for sstable in level_sstables.iter().rev() {
                let sstable_results = sstable.query(s, p, o);
                for triple in sstable_results {
                    if !seen.contains(&triple) {
                        results.push(triple.clone());
                        seen.insert(triple);
                    }
                }
            }
        }

        results
    }

    /// Check if triple exists
    pub fn contains(&self, triple: &Triple) -> bool {
        // Check memtable
        let memtable = self.memtable.read().unwrap();
        if let Some(metadata) = memtable.get(triple) {
            return !metadata.is_deleted;
        }
        drop(memtable);

        // Check immutable memtables
        let immutable_memtables = self.immutable_memtables.lock().unwrap();
        for memtable in immutable_memtables.iter() {
            if let Some(metadata) = memtable.get(triple) {
                return !metadata.is_deleted;
            }
        }
        drop(immutable_memtables);

        // Check SSTables
        let levels = self.levels.read().unwrap();
        for level_sstables in levels.iter() {
            for sstable in level_sstables {
                if sstable.contains(triple) {
                    return true;
                }
            }
        }

        false
    }

    /// Get all triples
    pub fn get_all_triples(&self) -> Vec<Triple> {
        self.query(None, None, None)
    }

    /// Flush memtable to Level 0 SSTable
    fn flush_memtable(&self) -> Result<(), String> {
        // Freeze current memtable
        let frozen_memtable = {
            let mut memtable = self.memtable.write().unwrap();
            let frozen = std::mem::replace(&mut *memtable, MemTable::new());
            frozen
        };

        if frozen_memtable.is_empty() {
            return Ok(());
        }

        // Add to immutable queue
        self.immutable_memtables.lock().unwrap().push_back(frozen_memtable.clone());

        // Create SSTable from frozen memtable
        let id = {
            let mut next_id = self.next_sstable_id.lock().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        let sstable = SSTable::from_memtable(id, 0, &frozen_memtable, &self.config.data_dir)?;

        {
            let mut levels = self.levels.write().unwrap();
            levels[0].push(sstable);
        }

        // Remove from immutable queue
        self.immutable_memtables.lock().unwrap().pop_front();

        // Clear WAL
        if self.config.enable_wal {
            self.wal.lock().unwrap().clear()?;
        }

        // Check if Level 0 needs compaction
        let level0_size = self.levels.read().unwrap()[0].len();
        if level0_size >= self.config.level0_compaction_threshold {
            self.trigger_compaction()?;
        }

        Ok(())
    }

    /// Trigger compaction
    fn trigger_compaction(&self) -> Result<(), String> {
        // Check if compaction is already running
        let mut compaction_flag = self.compaction_in_progress.lock().unwrap();
        if *compaction_flag {
            return Ok(()); // Compaction already in progress
        }
        *compaction_flag = true;
        drop(compaction_flag);

        // Run compaction in background
        let self_clone = self.clone_for_compaction();
        std::thread::spawn(move || {
            if let Err(e) = self_clone.run_compaction() {
                eprintln!("Compaction failed: {}", e);
            }
            *self_clone.compaction_in_progress.lock().unwrap() = false;
        });

        Ok(())
    }

    /// Run compaction process
    fn run_compaction(&self) -> Result<(), String> {
        match self.compaction_strategy {
            CompactionStrategy::Leveled => self.leveled_compaction(),
            CompactionStrategy::SizeTiered => self.size_tiered_compaction(),
        }
    }

    /// Leveled compaction strategy
    fn leveled_compaction(&self) -> Result<(), String> {
        let mut levels = self.levels.write().unwrap();

        // Compact Level 0 -> Level 1
        if levels[0].len() >= self.config.level0_compaction_threshold {
            let level0_sstables: Vec<&SSTable> = levels[0].iter().collect();
            
            let id = {
                let mut next_id = self.next_sstable_id.lock().unwrap();
                let id = *next_id;
                *next_id += 1;
                id
            };

            let merged_sstable = SSTable::merge(id, 1, level0_sstables, &self.config.data_dir)?;

            // Delete old SSTables
            for sstable in &levels[0] {
                sstable.delete_from_disk()?;
            }
            levels[0].clear();

            // Add to Level 1
            levels[1].push(merged_sstable);
        }

        // Compact Level N -> Level N+1 if needed
        for level in 1..levels.len() - 1 {
            let target_size = self.config.level1_base_size * self.config.level_size_multiplier.pow(level as u32);
            
            if levels[level].len() > target_size {
                // Pick overlapping SSTables for compaction
                let sstables_to_compact: Vec<&SSTable> = levels[level].iter().take(2).collect();
                
                if sstables_to_compact.len() < 2 {
                    continue;
                }

                let id = {
                    let mut next_id = self.next_sstable_id.lock().unwrap();
                    let id = *next_id;
                    *next_id += 1;
                    id
                };

                let merged_sstable = SSTable::merge(id, level + 1, sstables_to_compact.clone(), &self.config.data_dir)?;

                // Delete old SSTables
                for sstable in sstables_to_compact {
                    sstable.delete_from_disk()?;
                }
                levels[level].drain(0..2);

                // Add to next level
                levels[level + 1].push(merged_sstable);
            }
        }

        Ok(())
    }

    /// Size-tiered compaction strategy
    fn size_tiered_compaction(&self) -> Result<(), String> {
        // TODO: Implement size-tiered compaction
        Ok(())
    }

    /// Recover from Write-Ahead Log
    fn recover_from_wal(&self) -> Result<(), String> {
        if !self.config.enable_wal {
            return Ok(());
        }

        let operations = self.wal.lock().unwrap().recover()?;
        
        let mut memtable = self.memtable.write().unwrap();
        for (op, triple) in operations {
            match op {
                WalOperation::Insert => {
                    memtable.insert(triple);
                }
                WalOperation::Delete => {
                    memtable.delete(&triple);
                }
            }
        }

        Ok(())
    }

    /// Manually flush memtable (for testing or explicit control)
    pub fn flush(&self) -> Result<(), String> {
        self.flush_memtable()
    }

    /// Force compaction (for testing or explicit control)
    pub fn compact(&self) -> Result<(), String> {
        self.run_compaction()
    }

    /// Get statistics
    pub fn stats(&self) -> LSMStats {
        let memtable = self.memtable.read().unwrap();
        let immutable_memtables = self.immutable_memtables.lock().unwrap();
        let levels = self.levels.read().unwrap();

        let level_sizes: Vec<usize> = levels.iter().map(|level| level.len()).collect();
        let total_sstables: usize = level_sizes.iter().sum();

        LSMStats {
            memtable_size: memtable.len(),
            immutable_memtables_count: immutable_memtables.len(),
            total_sstables,
            level_sizes,
            compaction_in_progress: *self.compaction_in_progress.lock().unwrap(),
        }
    }

    /// Helper: Check if triple matches query pattern
    fn matches_query(triple: &Triple, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> bool {
        if let Some(ss) = s {
            if triple.subject != ss {
                return false;
            }
        }
        if let Some(pp) = p {
            if triple.predicate != pp {
                return false;
            }
        }
        if let Some(oo) = o {
            if triple.object != oo {
                return false;
            }
        }
        true
    }

    /// Clone for compaction thread
    fn clone_for_compaction(&self) -> Self {
        Self {
            memtable: Arc::clone(&self.memtable),
            immutable_memtables: Arc::clone(&self.immutable_memtables),
            wal: Arc::clone(&self.wal),
            levels: Arc::clone(&self.levels),
            config: self.config.clone(),
            next_sstable_id: Arc::clone(&self.next_sstable_id),
            compaction_strategy: self.compaction_strategy,
            compaction_in_progress: Arc::clone(&self.compaction_in_progress),
        }
    }

    /// Build UnifiedIndex from all data in LSM-Tree
    pub fn build_unified_index(&self) -> UnifiedIndex {
        let mut index = UnifiedIndex::new();
        let all_triples = self.get_all_triples();
        index.build_from_triples(&all_triples);
        index
    }

    /// Export to UnifiedIndex for use in SparqlDatabase
    pub fn export_to_unified_index(&self) -> UnifiedIndex {
        self.build_unified_index()
    }
}

/// LSM-Tree statistics
#[derive(Debug, Clone)]
pub struct LSMStats {
    pub memtable_size: usize,
    pub immutable_memtables_count: usize,
    pub total_sstables: usize,
    pub level_sizes: Vec<usize>,
    pub compaction_in_progress: bool,
}

impl std::fmt::Display for LSMStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "LSM-Tree Statistics:")?;
        writeln!(f, "  Memtable size: {}", self.memtable_size)?;
        writeln!(f, "  Immutable memtables: {}", self.immutable_memtables_count)?;
        writeln!(f, "  Total SSTables: {}", self.total_sstables)?;
        writeln!(f, "  Compaction in progress: {}", self.compaction_in_progress)?;
        writeln!(f, "  Level sizes:")?;
        for (i, size) in self.level_sizes.iter().enumerate() {
            writeln!(f, "    Level {}: {} SSTables", i, size)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_basic_ops() {
        let mut memtable = MemTable::new();
        let triple = Triple { subject: 1, predicate: 2, object: 3 };
        
        assert!(memtable.insert(triple.clone()));
        assert_eq!(memtable.len(), 1);
        
        let metadata = memtable.get(&triple).unwrap();
        assert!(!metadata.is_deleted);
    }

    #[test]
    fn test_lsm_tree_insert_query() {
        let config = LSMConfig {
            memtable_size_threshold: 100,
            data_dir: PathBuf::from("./test_lsm_data"),
            ..Default::default()
        };
        
        let lsm_tree = LSMTree::with_config(config).unwrap();
        
        let triple1 = Triple { subject: 1, predicate: 2, object: 3 };
        let triple2 = Triple { subject: 4, predicate: 5, object: 6 };
        
        lsm_tree.insert(triple1.clone()).unwrap();
        lsm_tree.insert(triple2.clone()).unwrap();
        
        assert!(lsm_tree.contains(&triple1));
        assert!(lsm_tree.contains(&triple2));
        
        let results = lsm_tree.query(Some(1), None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], triple1);
        
        // Cleanup
        std::fs::remove_dir_all("./test_lsm_data").ok();
    }

    #[test]
    fn test_lsm_tree_delete() {
        let config = LSMConfig {
            memtable_size_threshold: 100,
            data_dir: PathBuf::from("./test_lsm_data_delete"),
            ..Default::default()
        };
        
        let lsm_tree = LSMTree::with_config(config).unwrap();
        
        let triple = Triple { subject: 1, predicate: 2, object: 3 };
        
        lsm_tree.insert(triple.clone()).unwrap();
        assert!(lsm_tree.contains(&triple));
        
        lsm_tree.delete(&triple).unwrap();
        assert!(!lsm_tree.contains(&triple));
        
        // Cleanup
        std::fs::remove_dir_all("./test_lsm_data_delete").ok();
    }

    #[test]
    fn test_lsm_tree_flush() {
        let config = LSMConfig {
            memtable_size_threshold: 10,
            data_dir: PathBuf::from("./test_lsm_data_flush"),
            ..Default::default()
        };
        
        let lsm_tree = LSMTree::with_config(config).unwrap();
        
        // Insert enough triples to trigger flush
        for i in 0..15 {
            let triple = Triple { subject: i, predicate: i + 1, object: i + 2 };
            lsm_tree.insert(triple).unwrap();
        }
        
        let stats = lsm_tree.stats();
        assert!(stats.total_sstables > 0 || stats.immutable_memtables_count > 0);
        
        // Cleanup
        std::fs::remove_dir_all("./test_lsm_data_flush").ok();
    }

    #[test]
    fn test_wal_recovery() {
        let data_dir = PathBuf::from("./test_lsm_data_wal");
        std::fs::create_dir_all(&data_dir).ok();
        
        let config = LSMConfig {
            memtable_size_threshold: 100,
            enable_wal: true,
            data_dir: data_dir.clone(),
            ..Default::default()
        };
        
        // Create LSM-Tree and insert data
        {
            let lsm_tree = LSMTree::with_config(config.clone()).unwrap();
            let triple = Triple { subject: 1, predicate: 2, object: 3 };
            lsm_tree.insert(triple).unwrap();
            lsm_tree.wal.lock().unwrap().flush().unwrap();
        }
        
        // Recover from WAL
        let lsm_tree = LSMTree::with_config(config).unwrap();
        let triple = Triple { subject: 1, predicate: 2, object: 3 };
        assert!(lsm_tree.contains(&triple));
        
        // Cleanup
        std::fs::remove_dir_all(&data_dir).ok();
    }
}
