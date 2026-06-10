/*
 * Copyright (c) 2026 Volodymyr Kadzhaia
 * Copyright (c) 2026 Pieter Bonte
 * KU Leuven - Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::execute_query::execute_query_rayon_parallel2_volcano;
use crate::sparql_database::SparqlDatabase;

#[derive(Debug, Clone, Default)]
pub struct QueryEngineConfig;

pub struct QueryEngine {
    database: SparqlDatabase,
}

impl QueryEngine {
    pub fn new() -> Self {
        Self {
            database: SparqlDatabase::new(),
        }
    }

    pub fn with_config(_config: QueryEngineConfig) -> Result<Self, String> {
        Ok(Self::new())
    }

    pub fn load_ntriples_to_memory(&mut self, data: &str) -> Result<(), String> {
        self.database.parse_ntriples_and_add(data);
        self.database.get_or_build_stats();
        Ok(())
    }

    fn add_triple(&mut self, subject: &str, predicate: &str, object: &str) -> Result<(), String> {
        let triple = {
            let mut dict = self.database.dictionary.write().unwrap();
            let encoded = shared::triple::Triple {
                subject: dict.encode(subject),
                predicate: dict.encode(predicate),
                object: dict.encode(object),
            };
            drop(dict);
            encoded
        };
        self.database.add_triple(triple);
        Ok(())
    }

    pub fn add_triple_to_memory(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> Result<(), String> {
        self.add_triple(subject, predicate, object)
    }

    pub fn query(&mut self, sparql: &str) -> Result<Vec<Vec<String>>, String> {
        Ok(execute_query_rayon_parallel2_volcano(
            sparql,
            &mut self.database,
        ))
    }

    pub fn explain(&self, sparql: &str) -> QueryExplanation {
        let has_windowing = has_windowing_operations(sparql);
        let storage_mode = if has_windowing || is_rspql_query(sparql) {
            StorageMode::Streaming
        } else {
            StorageMode::Static
        };

        QueryExplanation {
            will_use_volcano: matches!(storage_mode, StorageMode::Static | StorageMode::Hybrid),
            storage_mode,
            has_windowing,
            window_clauses: extract_window_clauses(sparql),
        }
    }

    pub fn stats(&self) -> QueryEngineStats {
        QueryEngineStats {
            memory_triple_count: self.database.index_manager.query(None, None, None).len(),
        }
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMode {
    Static,
    Streaming,
    Hybrid,
}

#[derive(Debug, Clone)]
pub struct QueryExplanation {
    pub storage_mode: StorageMode,
    pub will_use_volcano: bool,
    pub has_windowing: bool,
    pub window_clauses: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QueryEngineStats {
    pub memory_triple_count: usize,
}

fn has_windowing_operations(query: &str) -> bool {
    let windowing_keywords = [
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
    windowing_keywords
        .iter()
        .any(|keyword| query_upper.contains(keyword))
}

fn is_rspql_query(query: &str) -> bool {
    let query_upper = query.to_uppercase();
    query_upper.contains("REGISTER")
        && (query_upper.contains("RSTREAM")
            || query_upper.contains("ISTREAM")
            || query_upper.contains("DSTREAM"))
}

fn extract_window_clauses(query: &str) -> Vec<String> {
    let mut window_clauses = Vec::new();

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_engine_basic_in_memory() {
        let mut engine = QueryEngine::new();

        engine
            .load_ntriples_to_memory(
                r#"
            <http://example.org/john> <http://example.org/name> "John" .
        "#,
            )
            .unwrap();

        let results = engine
            .query(
                r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?person ex:name ?name }
        "#,
            )
            .unwrap();

        assert!(!results.is_empty());
    }

    #[test]
    fn test_memory_engine_stats() {
        let mut engine = QueryEngine::new();

        engine.add_triple("s", "p", "o").unwrap();

        let stats = engine.stats();
        assert!(stats.memory_triple_count > 0);
    }

    #[test]
    fn test_explain() {
        let engine = QueryEngine::new();

        let static_query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }";
        let explanation = engine.explain(static_query);

        assert_eq!(explanation.storage_mode, StorageMode::Static);
        assert!(explanation.will_use_volcano);
        assert!(!explanation.has_windowing);
    }
}
