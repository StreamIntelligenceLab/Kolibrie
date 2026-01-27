/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven – Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::storage_manager::{StorageManager, StorageBackend, StorageStats};
use crate::storage_trait::{StorageTrait, StorageMode, QueryAnalyzer};
use crate::disk_storage::lsm_tree::LSMConfig;
use std::path::PathBuf;

/// Configuration for QueryEngine
#[derive(Debug, Clone)]
pub struct QueryEngineConfig {
    /// Use disk storage
    pub use_disk_storage: bool,
    
    /// LSM-Tree configuration
    pub lsm_config: Option<LSMConfig>,
    
    /// Default backend to use
    pub default_backend: StorageBackend,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            use_disk_storage: false,
            lsm_config: None,
            default_backend: StorageBackend::Memory,
        }
    }
}

pub struct QueryEngine {
    storage_manager: StorageManager,
}

impl QueryEngine {
    /// Create a new query engine with in-memory storage only
    pub fn new() -> Self {
        Self {
            storage_manager: StorageManager::new(),
        }
    }
    
    /// Create query engine with custom configuration
    pub fn with_config(config: QueryEngineConfig) -> Result<Self, String> {
        let storage_manager = if config.use_disk_storage {
            let lsm_config = config.lsm_config.unwrap_or_else(|| LSMConfig {
                data_dir: PathBuf::from("./query_engine_data"),
                ..Default::default()
            });
            
            let mut manager = StorageManager::with_both_backends(lsm_config)?;
            manager.set_backend(config.default_backend)?;
            manager
        } else {
            StorageManager::new()
        };
        
        Ok(Self { storage_manager })
    }
    
    /// Create query engine with disk storage enabled
    pub fn with_disk_storage(data_dir: PathBuf) -> Result<Self, String> {
        let config = QueryEngineConfig {
            use_disk_storage: true,
            lsm_config: Some(LSMConfig {
                data_dir,
                ..Default::default()
            }),
            default_backend: StorageBackend::Disk,
        };
        
        Self::with_config(config)
    }
    
    /// Switch storage backend
    /// 
    /// IMPORTANT: This changes where NEW data will be stored.
    /// Existing data remains in its current backend.
    pub fn use_backend(&mut self, backend: StorageBackend) -> Result<(), String> {
        self.storage_manager.set_backend(backend)
    }
    
    /// Get current backend
    pub fn current_backend(&self) -> StorageBackend {
        self.storage_manager.get_backend()
    }
    
    /// Load data from N-Triples into current backend
    fn load_ntriples(&mut self, data: &str) -> Result<(), String> {
        let backend = self.current_backend();
        println!("Loading N-Triples into {:?} backend", backend);
        
        match backend {
            StorageBackend::Memory => {
                // Parse directly into memory database
                self.storage_manager.get_memory_database_mut().parse_ntriples_and_add(data);
                
                // Build statistics for StreamerTail optimizer
                self.storage_manager.get_memory_database_mut().get_or_build_stats();
            }
            StorageBackend::Disk => {
                // Parse into memory
                self.storage_manager.get_memory_database_mut().parse_ntriples_and_add(data);
                
                // Extract the encoded triples
                let triples = self.storage_manager.get_memory_database()
                    .index_manager
                    .query(None, None, None);
                
                // Insert into LSM-Tree
                self.storage_manager.bulk_insert(&triples)?;
                
                // Clear memory database
                self.storage_manager.get_memory_database_mut().triples.clear();
                self.storage_manager.get_memory_database_mut().index_manager = 
                    shared::index_manager::UnifiedIndex::new();
                
                // Build statistics
                self.storage_manager.get_memory_database_mut().get_or_build_stats();
            }
        }
        
        Ok(())
    }
    
    /// Load data from N-Triples into memory specifically
    pub fn load_ntriples_to_memory(&mut self, data: &str) -> Result<(), String> {
        let original_backend = self.current_backend();
        
        self.use_backend(StorageBackend::Memory)?;
        self.load_ntriples(data)?;
        
        // Restore original backend
        self.use_backend(original_backend)?;
        Ok(())
    }
    
    /// Load data from N-Triples into disk specifically
    pub fn load_ntriples_to_disk(&mut self, data: &str) -> Result<(), String> {
        let original_backend = self.current_backend();
        
        self.use_backend(StorageBackend::Disk)?;
        self.load_ntriples(data)?;
        
        // Restore original backend
        self.use_backend(original_backend)?;
        Ok(())
    }
    
    /// Add a single triple to current backend
    fn add_triple(&mut self, subject: &str, predicate: &str, object: &str) -> Result<(), String> {
        // This would need dictionary encoding
        self.storage_manager.get_memory_database_mut().add_triple_parts(subject, predicate, object);
        Ok(())
    }
    
    /// Add a single triple to memory specifically
    pub fn add_triple_to_memory(&mut self, subject: &str, predicate: &str, object: &str) -> Result<(), String> {
        let original_backend = self.current_backend();
        
        self.use_backend(StorageBackend::Memory)?;
        self.add_triple(subject, predicate, object)?;
        
        self.use_backend(original_backend)?;
        Ok(())
    }
    
    /// Add a single triple to disk specifically
    pub fn add_triple_to_disk(&mut self, subject: &str, predicate: &str, object: &str) -> Result<(), String> {
        let original_backend = self.current_backend();
        
        self.use_backend(StorageBackend::Disk)?;
        self.add_triple(subject, predicate, object)?;
        
        self.use_backend(original_backend)?;
        Ok(())
    }
    
    /// Migrate data from memory to disk
    pub fn migrate_to_disk(&mut self) -> Result<(), String> {
        println!("Migrating data from memory to disk...");
        self.storage_manager.migrate_memory_to_disk()
    }
    
    /// Migrate data from disk to memory
    pub fn migrate_to_memory(&mut self) -> Result<(), String> {
        println!("Migrating data from disk to memory...");
        self.storage_manager.migrate_disk_to_memory()
    }
    
    /// Execute SPARQL query on CURRENT backend
    pub fn query(&mut self, sparql: &str) -> Result<Vec<Vec<String>>, String> {
        self.storage_manager.route_query(sparql)
    }
    
    /// Explain how query will be executed
    pub fn explain(&self, sparql: &str) -> QueryExplanation {
        let analysis = QueryAnalyzer::analyze(sparql);
        
        QueryExplanation {
            storage_mode: analysis.mode,
            storage_backend: self.current_backend(),
            will_use_volcano: analysis.requires_volcano_optimization,
            has_windowing: analysis.has_window_operations,
            window_clauses: analysis.window_clauses,
        }
    }
    
    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        self.storage_manager.get_storage_stats()
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Query execution explanation
#[derive(Debug, Clone)]
pub struct QueryExplanation {
    pub storage_mode: StorageMode,
    pub storage_backend: StorageBackend,
    pub will_use_volcano: bool,
    pub has_windowing: bool,
    pub window_clauses: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_engine_basic_in_memory() {
        let mut engine = QueryEngine::new();
        
        engine.load_ntriples_to_memory(r#"
            <http://example.org/john> <http://example.org/name> "John" .
        "#).unwrap();
        
        let results = engine.query(r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?person ex:name ?name }
        "#).unwrap();
        
        assert!(!results.is_empty());
    }

    #[test]
    fn test_query_engine_basic_in_disk() {
        let mut engine = QueryEngine::with_disk_storage(
            PathBuf::from("./test_query_engine_disk")
        ).unwrap();
        engine.use_backend(StorageBackend::Disk).unwrap();
        
        engine.load_ntriples_to_disk(r#"
            <http://example.org/john> <http://example.org/name> "John" .
        "#).unwrap();
        
        let results = engine.query(r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?person ex:name ?name }
        "#).unwrap();

        println!("{:?}", results);
        
        assert!(!results.is_empty());

        // Cleanup
        std::fs::remove_dir_all("./test_query_engine_disk").ok();
    }
    
    #[test]
    fn test_memory_only_engine() {
        let mut engine = QueryEngine::new();
        
        // Default should be memory
        assert_eq!(engine.current_backend(), StorageBackend::Memory);
        
        // Add data
        engine.add_triple("s", "p", "o").unwrap();
        
        let stats = engine.stats();
        assert_eq!(stats.backend, StorageBackend::Memory);
        assert!(stats.memory_triple_count > 0);
        assert_eq!(stats.disk_triple_count, 0);
    }
    
    #[test]
    fn test_disk_storage_engine() {
        let mut engine = QueryEngine::with_disk_storage(
            PathBuf::from("./test_query_engine_disk")
        ).unwrap();
        
        // Should be using disk
        assert_eq!(engine.current_backend(), StorageBackend::Disk);
        
        // Add data to disk
        engine.add_triple("s", "p", "o").unwrap();
        
        let stats = engine.stats();
        assert_eq!(stats.backend, StorageBackend::Disk);
        assert!(stats.disk_triple_count > 0);
        
        // Cleanup
        std::fs::remove_dir_all("./test_query_engine_disk").ok();
    }
    
    #[test]
    fn test_migration() {
        let config = QueryEngineConfig {
            use_disk_storage: true,
            lsm_config: Some(LSMConfig {
                data_dir: PathBuf::from("./test_query_engine_migration"),
                ..Default::default()
            }),
            default_backend: StorageBackend::Memory,
        };
        
        let mut engine = QueryEngine::with_config(config).unwrap();
        
        // Add data to memory
        engine.load_ntriples_to_memory("<s> <p> <o> .").unwrap();
        
        // Migrate to disk
        engine.migrate_to_disk().unwrap();
        
        // Check disk has data
        engine.use_backend(StorageBackend::Disk).unwrap();
        let stats = engine.stats();
        assert!(stats.disk_triple_count > 0);
        
        // Cleanup
        std::fs::remove_dir_all("./test_query_engine_migration").ok();
    }
    
    #[test]
    fn test_explain() {
        let engine = QueryEngine::new();
        
        let static_query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }";
        let explanation = engine.explain(static_query);
        
        assert_eq!(explanation.storage_mode, StorageMode::Static);
        assert_eq!(explanation.storage_backend, StorageBackend::Memory);
        assert!(explanation.will_use_volcano);
        assert!(!explanation.has_windowing);
    }
}