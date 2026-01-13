/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::sparql_database::SparqlDatabase;
use crate::storage_trait::{StorageTrait, QueryAnalysis, QueryAnalyzer, StorageMode};
use crate::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use shared::triple::Triple;

/// Storage Manager - implements the Storage Trait to coordinate between
/// static data storage and streaming (RSP) storage layers
pub struct StorageManager<'a> {
    database: &'a mut SparqlDatabase,
    current_mode: StorageMode,
}

impl<'a> StorageManager<'a> {
    /// Create a new StorageManager
    pub fn new(database: &'a mut SparqlDatabase) -> Self {
        Self {
            database,
            current_mode: StorageMode::Static,
        }
    }
    
    /// Execute query on static storage using Volcano optimizer
    fn execute_static_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String> {
        Ok(execute_query_rayon_parallel2_volcano(query, self.database))
    }
    
    /// Execute query on streaming storage
    fn execute_streaming_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String> {
        // Use existing RSP processing
        // This would integrate with the RSP engine (windowing, etc.)
        Ok(execute_query(query, self.database))
    }
    
    /// Execute hybrid query (both static and streaming)
    fn execute_hybrid_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String> {
        // Split query into static and streaming parts
        // Execute both and combine results
        // For now, default to streaming execution which can handle both
        self.execute_streaming_query(query)
    }
}

impl<'a> StorageTrait for StorageManager<'a> {
    fn analyze_query(&self, query: &str) -> QueryAnalysis {
        QueryAnalyzer::analyze(query)
    }
    
    fn route_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String> {
        let analysis = self.analyze_query(query);
        
        // Update current storage mode
        self.current_mode = analysis.mode.clone();
        
        // Log routing decision
        println!("Storage Manager: Routing query to {:?} storage", analysis.mode);
        if analysis.has_window_operations {
            println!("  - Detected windowing operations:  {:?}", analysis.window_clauses);
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
    
    fn add_data(&mut self, triple: Triple, is_streaming: bool) -> Result<(), String> {
        if is_streaming {
            // Add to streaming storage
            self.database.add_triple(triple);
            // Could also add to streams vec if needed
        } else {
            // Add to static storage
            self.database.add_triple(triple);
        }
        Ok(())
    }
    
    fn get_storage_mode(&self) -> StorageMode {
        self.current_mode.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_storage_manager_routing() {
        let mut db = SparqlDatabase::new();
        let manager = StorageManager::new(&mut db);
        
        let static_query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }";
        let analysis = manager.analyze_query(static_query);
        
        assert_eq!(analysis.mode, StorageMode::Static);
        assert!(!analysis.has_window_operations);
    }
}