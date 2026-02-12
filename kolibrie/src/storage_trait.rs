/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v.  2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/. 
 */

use shared::triple::Triple;

/// Enum to determine which storage layer to use
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMode {
    /// Static data storage (no windowing operations)
    Static,
    /// Stream processing storage (with windowing operations)
    Streaming,
    /// Hybrid mode (both static and streaming)
    Hybrid,
}

/// Query analysis result
#[derive(Debug, Clone)]
pub struct QueryAnalysis {
    pub mode: StorageMode,
    pub has_window_operations: bool,
    pub window_clauses: Vec<String>,
    #[allow(dead_code)]
    pub static_patterns: Vec<(String, String, String)>,
    pub requires_volcano_optimization: bool,
}

/// Storage Trait - coordinates between static and streaming storage layers
#[allow(dead_code)]
pub trait StorageTrait {
    /// Analyze query to determine storage mode
    fn analyze_query(&self, query: &str) -> QueryAnalysis;
    
    /// Route query to appropriate storage layer
    fn route_query(&mut self, query: &str) -> Result<Vec<Vec<String>>, String>;
    
    /// Add data to appropriate storage
    fn add_data(&mut self, triple: Triple, is_streaming: bool) -> Result<(), String>;
    
    /// Get storage mode for current context
    fn get_storage_mode(&self) -> StorageMode;
}

/// Query Analyzer - detects windowing operations and query characteristics
pub struct QueryAnalyzer;

impl QueryAnalyzer {
    /// Detect if query contains windowing operations
    pub fn has_windowing_operations(query: &str) -> bool {
        let windowing_keywords = vec![
            "WINDOW",
            "FROM NAMED WINDOW",
            "SLIDING",
            "TUMBLING",
            "RANGE",
            "RSTREAM",
            "ISTREAM",
            "DSTREAM",
            "SLIDE",
        ];
        
        let query_upper = query.to_uppercase();
        windowing_keywords.iter().any(|kw| query_upper.contains(kw))
    }
    
    /// Detect if query is a combined RSP-QL query
    pub fn is_rspql_query(query: &str) -> bool {
        let query_upper = query.to_uppercase();
        query_upper.contains("REGISTER") 
            && (query_upper.contains("RSTREAM") 
                || query_upper.contains("ISTREAM") 
                || query_upper.contains("DSTREAM"))
    }
    
    /// Extract window clauses from query
    pub fn extract_window_clauses(query: &str) -> Vec<String> {
        let mut window_clauses = Vec::new();
        
        // Simple extraction - look for FROM NAMED WINDOW patterns
        if let Some(start) = query.to_uppercase().find("FROM NAMED WINDOW") {
            let remaining = &query[start..];
            if let Some(end) = remaining.find("WHERE") {
                window_clauses.push(remaining[..end].trim().to_string());
            } else {
                window_clauses.push(remaining.trim().to_string());
            }
        }
        
        window_clauses
    }
    
    /// Determine storage mode based on query analysis
    pub fn determine_storage_mode(query: &str) -> StorageMode {
        let has_windowing = Self::has_windowing_operations(query);
        let is_rspql = Self::is_rspql_query(query);
        
        if is_rspql || has_windowing {
            StorageMode::Streaming
        } else {
            StorageMode::Static
        }
    }
    
    /// Perform full query analysis
    pub fn analyze(query: &str) -> QueryAnalysis {
        let has_window_operations = Self::has_windowing_operations(query);
        let mode = Self::determine_storage_mode(query);
        let window_clauses = Self::extract_window_clauses(query);
        
        // Static queries benefit from volcano optimization
        // Streaming queries may also benefit but need special handling
        let requires_volcano_optimization = matches!(mode, StorageMode::Static | StorageMode::Hybrid);
        
        QueryAnalysis {
            mode,
            has_window_operations,
            window_clauses,
            static_patterns: Vec::new(), // TODO: Extract static patterns
            requires_volcano_optimization,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_windowing_detection() {
        let static_query = r#"
            PREFIX ex: <http://example.org#>
            SELECT ?person ?name 
            WHERE { 
                ?person ex:name ?name .
            }
        "#;
        
        let streaming_query = r#"
            PREFIX ex: <http://example.org#>
            REGISTER RSTREAM <http://out/stream> AS
            SELECT *
            FROM NAMED WINDOW : wind ON :stream [SLIDING 10 SLIDE 2]
            WHERE {
                WINDOW :wind {
                    ? obs ex:temperature ?temp .
                }
            }
        "#;
        
        assert! (!QueryAnalyzer::has_windowing_operations(static_query));
        assert!(QueryAnalyzer::has_windowing_operations(streaming_query));
    }
    
    #[test]
    fn test_storage_mode_determination() {
        let static_query = "SELECT ? s ?p ?o WHERE { ?s ?p ?o .  }";
        let streaming_query = "REGISTER RSTREAM <http://out> AS SELECT * FROM NAMED WINDOW :w ON :s [SLIDING 5]";
        
        assert_eq!(QueryAnalyzer::determine_storage_mode(static_query), StorageMode::Static);
        assert_eq!(QueryAnalyzer::determine_storage_mode(streaming_query), StorageMode::Streaming);
    }
}