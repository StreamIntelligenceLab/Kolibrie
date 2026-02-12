/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven – Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::sparql_database::SparqlDatabase;
use crate::disk_storage::lsm_tree::{LSMTree, LSMConfig};
use crate::storage_trait::{StorageTrait, QueryAnalysis, QueryAnalyzer, StorageMode};
use crate::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use shared::triple::Triple;

/// Storage backend type - determines where data is physically stored
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    /// In-memory storage
    Memory,
    /// Disk-based storage using LSM-Tree
    Disk,
}

/// Storage Manager - implements the Storage Trait to coordinate between
/// static data storage and streaming (RSP) storage layers
pub struct StorageManager {
    /// In-memory database
    memory_database: SparqlDatabase,
    
    /// Disk-based LSM-Tree
    disk_database: Option<LSMTree>,
    
    /// Current storage mode (Static/Streaming/Hybrid)
    current_mode: StorageMode,
    
    /// Current backend (Memory/Disk)
    current_backend: StorageBackend,
}

#[allow(dead_code)]
impl StorageManager {
    /// Create a new StorageManager with in-memory storage only
    pub fn new() -> Self {
        Self {
            memory_database: SparqlDatabase::new(),
            disk_database: None,
            current_mode: StorageMode::Static,
            current_backend: StorageBackend::Memory,
        }
    }
    
    /// Create StorageManager with disk-based storage
    pub fn with_disk_storage(lsm_config: LSMConfig) -> Result<Self, String> {
        let lsm_tree = LSMTree::with_config(lsm_config)?;
        
        Ok(Self {
            memory_database: SparqlDatabase::new(),
            disk_database: Some(lsm_tree),
            current_mode: StorageMode::Static,
            current_backend: StorageBackend::Disk,
        })
    }
    
    /// Create StorageManager with both memory and disk backends available
    pub fn with_both_backends(lsm_config: LSMConfig) -> Result<Self, String> {
        let lsm_tree = LSMTree::with_config(lsm_config)?;
        
        Ok(Self {
            memory_database: SparqlDatabase::new(),
            disk_database: Some(lsm_tree),
            current_mode: StorageMode::Static,
            current_backend: StorageBackend::Memory, // Default to memory
        })
    }
    
    /// Switch storage backend (does NOT migrate existing data)
    pub fn set_backend(&mut self, backend: StorageBackend) -> Result<(), String> {
        match backend {
            StorageBackend::Disk => {
                if self.disk_database.is_none() {
                    return Err("Disk storage not initialized. Use with_disk_storage() or with_both_backends()".to_string());
                }
            }
            StorageBackend::Memory => {
                // Memory is always available
            }
        }
        
        self.current_backend = backend;
        Ok(())
    }
    
    /// Get current backend
    pub fn get_backend(&self) -> StorageBackend {
        self.current_backend.clone()
    }
    
    /// Add triple to CURRENT backend ONLY (not both)
    pub fn add_triple(&mut self, triple: Triple) -> Result<(), String> {
        match self.current_backend {
            StorageBackend::Memory => {
                // Add to memory database (both triples BTreeSet and UnifiedIndex)
                self.memory_database.add_triple(triple);
                Ok(())
            }
            StorageBackend::Disk => {
                if let Some(ref lsm) = self.disk_database {
                    // Add to LSM-Tree (it handles its own indexing)
                    lsm.insert(triple)?;
                    Ok(())
                } else {
                    Err("Disk storage not initialized".to_string())
                }
            }
        }
    }
    
    /// Bulk insert to CURRENT backend ONLY
    pub fn bulk_insert(&mut self, triples: &[Triple]) -> Result<(), String> {
        match self.current_backend {
            StorageBackend::Memory => {
                for triple in triples {
                    self.memory_database.add_triple(triple.clone());
                }
                Ok(())
            }
            StorageBackend::Disk => {
                if let Some(ref lsm) = self.disk_database {
                    lsm.bulk_insert(triples)?;
                    Ok(())
                } else {
                    Err("Disk storage not initialized".to_string())
                }
            }
        }
    }
    
    /// Delete triple from CURRENT backend ONLY
    pub fn delete_triple(&mut self, triple: &Triple) -> Result<(), String> {
        match self.current_backend {
            StorageBackend::Memory => {
                self.memory_database.delete_triple(triple);
                Ok(())
            }
            StorageBackend::Disk => {
                if let Some(ref lsm) = self.disk_database {
                    lsm.delete(triple)?;
                    Ok(())
                } else {
                    Err("Disk storage not initialized".to_string())
                }
            }
        }
    }
    
    /// Query CURRENT backend ONLY using UnifiedIndex
    pub fn query_triples(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        match self.current_backend {
            StorageBackend::Memory => {
                // Query the UnifiedIndex in memory database
                self.memory_database.index_manager.query(s, p, o)
            }
            StorageBackend::Disk => {
                if let Some(ref lsm) = self.disk_database {
                    // LSM-Tree has its own query method that uses UnifiedIndex internally
                    lsm.query(s, p, o)
                } else {
                    Vec::new()
                }
            }
        }
    }
    
    /// Execute query on static storage using Volcano optimizer
    fn execute_static_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String> {
        match self.current_backend {
            StorageBackend::Memory => {
                // Query directly on memory database
                Ok(execute_query_rayon_parallel2_volcano(query, &mut self.memory_database))
            }
            StorageBackend::Disk => {
                if let Some(ref lsm) = self.disk_database {
                    // Export LSM-Tree's UnifiedIndex to a temporary SparqlDatabase
                    let unified_index = lsm.export_to_unified_index();
                    
                    // Create temporary database with the disk's index
                    let mut temp_db = SparqlDatabase::new();
                    temp_db.index_manager = unified_index;
                    
                    // Share dictionary and prefixes from memory database
                    temp_db.dictionary = self.memory_database.dictionary.clone();
                    temp_db.prefixes = self.memory_database.prefixes.clone();
                    temp_db.get_or_build_stats();
                    
                    Ok(execute_query_rayon_parallel2_volcano(query, &mut temp_db))
                } else {
                    Err("Disk storage not initialized".to_string())
                }
            }
        }
    }
    
    /// Execute query on streaming storage
    fn execute_streaming_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String> {
        // Streaming queries always use memory (real-time processing)
        Ok(execute_query(query, &mut self.memory_database))
    }
    
    /// Execute hybrid query (both static and streaming)
    fn execute_hybrid_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String> {
        // For hybrid, use streaming execution which can handle both
        self.execute_streaming_query(query)
    }
    
    /// Migrate data from memory to disk
    pub fn migrate_memory_to_disk(&mut self) -> Result<(), String> {
        if self.disk_database.is_none() {
            return Err("Disk storage not initialized".to_string());
        }
        
        // Get all triples from memory's UnifiedIndex
        let triples = self.memory_database.index_manager.query(None, None, None);
        
        // Insert into disk
        if let Some(ref lsm) = self.disk_database {
            lsm.bulk_insert(&triples)?;
        }
        
        Ok(())
    }
    
    /// Migrate data from disk to memory
    pub fn migrate_disk_to_memory(&mut self) -> Result<(), String> {
        if let Some(ref lsm) = self.disk_database {
            let triples = lsm.get_all_triples();
            
            for triple in triples {
                self.memory_database.add_triple(triple);
            }
            
            Ok(())
        } else {
            Err("Disk storage not initialized".to_string())
        }
    }
    
    /// Get reference to memory database
    pub fn get_memory_database(&self) -> &SparqlDatabase {
        &self.memory_database
    }
    
    /// Get mutable reference to memory database
    pub fn get_memory_database_mut(&mut self) -> &mut SparqlDatabase {
        &mut self.memory_database
    }
    
    /// Get statistics about current storage
    pub fn get_storage_stats(&self) -> StorageStats {
        let memory_triples = self.memory_database.index_manager.query(None, None, None).len();
        let disk_triples = if let Some(ref lsm) = self.disk_database {
            lsm.get_all_triples().len()
        } else {
            0
        };
        
        let lsm_stats = if let Some(ref lsm) = self.disk_database {
            Some(lsm.stats())
        } else {
            None
        };
        
        StorageStats {
            backend: self.current_backend.clone(),
            mode: self.current_mode.clone(),
            memory_triple_count: memory_triples,
            disk_triple_count: disk_triples,
            lsm_stats,
        }
    }
}

impl StorageTrait for StorageManager {
    fn analyze_query(&self, query: &str) -> QueryAnalysis {
        QueryAnalyzer::analyze(query)
    }
    
    fn route_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String> {
        let analysis = self.analyze_query(query);
        
        // Update current storage mode
        self.current_mode = analysis.mode.clone();
        
        // Log routing decision
        println!("Storage Manager: Routing query to {:?} storage on {:?} backend", 
                 analysis.mode, self.current_backend);
        if analysis.has_window_operations {
            println!("  - Detected windowing operations: {:?}", analysis.window_clauses);
        }
        if analysis.requires_volcano_optimization {
            println!("  - Query eligible for Volcano optimization");
        }
        
        // Route to appropriate storage layer
        match analysis.mode {
            StorageMode::Static => self.execute_static_query(query),
            StorageMode::Streaming => self.execute_streaming_query(query),
            StorageMode::Hybrid => self.execute_hybrid_query(query),
        }
    }
    
    fn add_data(&mut self, triple: Triple, _is_streaming: bool) -> Result<(), String> {
        // Add to current backend only
        self.add_triple(triple)
    }
    
    fn get_storage_mode(&self) -> StorageMode {
        self.current_mode.clone()
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub backend: StorageBackend,
    pub mode: StorageMode,
    pub memory_triple_count: usize,
    pub disk_triple_count: usize,
    pub lsm_stats: Option<crate::disk_storage::lsm_tree::LSMStats>,
}

impl std::fmt::Display for StorageStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Storage Statistics:")?;
        writeln!(f, "  Current Backend: {:?}", self.backend)?;
        writeln!(f, "  Current Mode: {:?}", self.mode)?;
        writeln!(f, "  Memory Triples: {}", self.memory_triple_count)?;
        writeln!(f, "  Disk Triples: {}", self.disk_triple_count)?;
        
        if let Some(ref lsm) = self.lsm_stats {
            writeln!(f, "\n{}", lsm)?;
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_memory_only_storage() {
        let mut manager = StorageManager::new();
        
        let triple = Triple { subject: 1, predicate: 2, object: 3 };
        manager.add_triple(triple.clone()).unwrap();
        
        let results = manager.query_triples(Some(1), None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], triple);
    }
    
    #[test]
    fn test_disk_only_storage() {
        let config = LSMConfig {
            memtable_size_threshold: 10,
            data_dir: std::path::PathBuf::from("./test_storage_manager_disk"),
            ..Default::default()
        };
        
        let mut manager = StorageManager::with_disk_storage(config).unwrap();
        
        let triple = Triple { subject: 1, predicate: 2, object: 3 };
        manager.add_triple(triple.clone()).unwrap();
        
        let results = manager.query_triples(Some(1), None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], triple);
        
        // Cleanup
        std::fs::remove_dir_all("./test_storage_manager_disk").ok();
    }
    
    #[test]
    fn test_backend_switching() {
        let config = LSMConfig {
            memtable_size_threshold: 10,
            data_dir: std::path::PathBuf::from("./test_storage_manager_switch"),
            ..Default::default()
        };
        
        let mut manager = StorageManager::with_both_backends(config).unwrap();
        
        // Add to memory
        manager.set_backend(StorageBackend::Memory).unwrap();
        let triple1 = Triple { subject: 1, predicate: 2, object: 3 };
        manager.add_triple(triple1.clone()).unwrap();
        
        // Switch to disk
        manager.set_backend(StorageBackend::Disk).unwrap();
        let triple2 = Triple { subject: 4, predicate: 5, object: 6 };
        manager.add_triple(triple2.clone()).unwrap();
        
        // Query disk - should only have triple2
        let disk_results = manager.query_triples(None, None, None);
        assert_eq!(disk_results.len(), 1);
        assert_eq!(disk_results[0], triple2);
        
        // Switch back to memory - should only have triple1
        manager.set_backend(StorageBackend::Memory).unwrap();
        let memory_results = manager.query_triples(None, None, None);
        assert_eq!(memory_results.len(), 1);
        assert_eq!(memory_results[0], triple1);
        
        // Cleanup
        std::fs::remove_dir_all("./test_storage_manager_switch").ok();
    }
    
    #[test]
    fn test_migration() {
        let config = LSMConfig {
            memtable_size_threshold: 10,
            data_dir: std::path::PathBuf::from("./test_storage_manager_migrate"),
            ..Default::default()
        };
        
        let mut manager = StorageManager::with_both_backends(config).unwrap();
        
        // Add data to memory
        manager.set_backend(StorageBackend::Memory).unwrap();
        for i in 0..5 {
            let triple = Triple { subject: i, predicate: i+1, object: i+2 };
            manager.add_triple(triple).unwrap();
        }
        
        // Migrate to disk
        manager.migrate_memory_to_disk().unwrap();
        
        // Switch to disk and verify
        manager.set_backend(StorageBackend::Disk).unwrap();
        let disk_results = manager.query_triples(None, None, None);
        assert_eq!(disk_results.len(), 5);
        
        // Cleanup
        std::fs::remove_dir_all("./test_storage_manager_migrate").ok();
    }
}