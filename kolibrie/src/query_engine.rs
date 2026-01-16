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
use crate::storage_manager::StorageManager;
use crate::storage_trait::{StorageTrait, StorageMode, QueryAnalyzer};

pub struct QueryEngine {
    static_storage: SparqlDatabase,
    // streaming_storage: Option<RSPEngine>,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new() -> Self {
        Self {
            static_storage: SparqlDatabase::new(),
        }
    }
    
    /// Load data from N-Triples
    pub fn load_ntriples(&mut self, data: &str) {
        self.static_storage.parse_ntriples_and_add(data);
        self.static_storage.get_or_build_stats();
    }
    
    /// Load data from RDF/XML
    pub fn load_rdf_xml(&mut self, data: &str) {
        self.static_storage.parse_rdf(data);
        self.static_storage.get_or_build_stats();
    }
    
    /// Add a single triple
    pub fn add_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        self.static_storage.add_triple_parts(subject, predicate, object);
    }

    /// Create storage manager to coordinate execution
    fn with_storage_manager<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut StorageManager) -> R,
    {
        let mut manager = StorageManager::new(&mut self.static_storage);
        f(&mut manager)
    }
    
    /// Execute SPARQL query & automatically routes to appropriate storage layer
    pub fn query(&mut self, sparql: &str) -> Result<Vec<Vec<String>>, String> {
        // Route query automatically based on analysis
        self.with_storage_manager(|manager| manager.route_query(sparql))
    }

    /// Explain how query will be executed
    pub fn explain(&self, sparql: &str) -> QueryExplanation {
        let analysis = QueryAnalyzer::analyze(sparql);
        
        QueryExplanation {
            storage_mode: analysis.mode,
            will_use_volcano: analysis.requires_volcano_optimization,
            has_windowing: analysis.has_window_operations,
            window_clauses: analysis.window_clauses,
        }
    }
    
    /// Get direct access to underlying database (for advanced operations)
    pub fn database(&self) -> &SparqlDatabase {
        &self.static_storage
    }
    
    /// Get mutable access to underlying database (for advanced operations)
    pub fn database_mut(&mut self) -> &mut SparqlDatabase {
        &mut self.static_storage
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only explanation of query execution plan
#[derive(Debug, Clone)]
pub struct QueryExplanation {
    pub storage_mode: StorageMode,
    pub will_use_volcano: bool,
    pub has_windowing: bool,
    pub window_clauses: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_query_engine_basic() {
        let mut engine = QueryEngine::new();
        
        engine.load_ntriples(r#"
            <http://example.org/john> <http://example.org/name> "John" .
        "#);
        
        let results = engine.query(r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?person ex:name ?name }
        "#).unwrap();
        
        assert!(!results.is_empty());
    }

    #[test]
    fn test_query_engine_explain() {
        let engine = QueryEngine::new();
        
        // Test static query explanation
        let static_query = r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?person ex:name ?name }
        "#;
        
        let explanation = engine.explain(static_query);
        assert_eq!(explanation.storage_mode, StorageMode::Static);
        assert!(explanation.will_use_volcano);
        assert!(!explanation.has_windowing);
    }

    #[test]
    fn test_query_engine_explain_streaming() {
        let engine = QueryEngine::new();
        
        // Test streaming query explanation
        let streaming_query = r#"
            PREFIX ex: <http://example.org#>
            REGISTER RSTREAM <http://out/stream> AS
            SELECT *
            FROM NAMED WINDOW :wind ON :stream [SLIDING 10 SLIDE 2]
            WHERE {
                WINDOW :wind {
                    ?obs ex:temperature ?temp .
                }
            }
        "#;
        
        let explanation = engine.explain(streaming_query);
        assert_eq!(explanation.storage_mode, StorageMode::Streaming);
        assert!(explanation.has_windowing);
    }
}