/*
* Copyright © 2025 Volodymyr Kadzhaia
* Copyright © 2025 Pieter Bonte
* KU Leuven — Stream Intelligence Lab, Belgium
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this file,
* you can obtain one at https://mozilla.org/MPL/2.0/.
*/

use crate::rsp::r2r::{AsAnyMut, R2ROperator};
use crate::rsp_engine::QueryExecutionMode;
use crate::sparql_database::SparqlDatabase;
use crate::streamertail_optimizer::{ExecutionEngine, PhysicalOperator};
use datalog::parser_n3_logic::parse_n3_rule;
use datalog::reasoning::Reasoner;
use shared::rule::Rule;
use shared::triple::Triple;
use std::sync::Arc;

#[cfg(not(test))]
use log::{debug, error};
#[cfg(test)]
use std::{println as debug, println as error};

pub struct SimpleR2R {
    pub item: SparqlDatabase,
    pub execution_mode: QueryExecutionMode,
    pub rules: Vec<Rule>,
    derived_triples: Vec<Triple>,
}

impl SimpleR2R {
    pub fn new() -> Self {
        SimpleR2R {
            item: SparqlDatabase::new(),
            execution_mode: QueryExecutionMode::Standard,
            rules: Vec::new(),
            derived_triples: Vec::new(),
        }
    }

    pub fn with_execution_mode(execution_mode: QueryExecutionMode) -> Self {
        SimpleR2R {
            item: SparqlDatabase::new(),
            execution_mode,
            rules: Vec::new(),
            derived_triples: Vec::new(),
        }
    }

    pub fn add_reasoning_rules(&mut self, rules: Vec<Rule>) {
        self.rules.extend(rules);
    }
}

/// Allow downcasting from trait objects by exposing Any for mutable references.
/// This helps code that needs to access concrete `SimpleR2R` internals (e.g. the SparqlDatabase).
impl AsAnyMut for SimpleR2R {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Implement the R2R operator trait for SimpleR2R. Note: the `R` generic parameter
/// is `Vec<PhysicalOperator>` (a list of physical plans). The output `O` is
/// a binding map `HashMap<String, String>` per row.
impl R2ROperator<Triple, Vec<PhysicalOperator>, Vec<(String, String)>> for SimpleR2R {
    fn load_triples(&mut self, _data: &str, _syntax: String) -> Result<(), String> {
        error!("Unsupported operation");
        Err("something went wrong".to_string())
    }

    fn load_rules(&mut self, data: &str) -> Result<(), &'static str> {
        if data.trim().is_empty() {
            return Ok(());
        }
        let mut temp_reasoner = Reasoner::new();
        temp_reasoner.dictionary = Arc::clone(&self.item.dictionary);
        let mut remaining = data;
        loop {
            match parse_n3_rule(remaining, &mut temp_reasoner) {
                Ok((rest, (_, rule))) => {
                    self.rules.push(rule);
                    remaining = rest;
                    if remaining.trim().is_empty() {
                        break;
                    }
                }
                Err(_) => return Err("Failed to parse N3 rules"),
            }
        }
        Ok(())
    }

    fn add(&mut self, data: Triple) {
        self.item.add_triple(data);
    }

    fn remove(&mut self, data: &Triple) {
        self.item.delete_triple(data);
    }

    fn materialize(&mut self) -> Vec<Triple> {
        // Evict derived triples from the previous cycle
        for t in &self.derived_triples {
            self.item.delete_triple(t);
        }
        self.derived_triples.clear();

        if self.rules.is_empty() {
            return Vec::new();
        }

        let mut reasoner = Reasoner::new();
        reasoner.dictionary = Arc::clone(&self.item.dictionary);
        for triple in self.item.triples.iter() {
            reasoner.index_manager.insert(triple);
        }
        reasoner.rules = self.rules.clone();

        let derived = reasoner.infer_new_facts_semi_naive();
        debug!("materialize: {} facts derived by reasoning", derived.len());
        for t in &derived {
            self.item.add_triple(t.clone());
            self.derived_triples.push(t.clone());
        }
        derived
    }

    fn execute_query(&mut self, op: &PhysicalOperator) -> Vec<Vec<(String, String)>> {
        debug!("SimpleR2R executing query with PhysicalOperator");

        // Execute the physical operator using the Volcano execution engine.
        // The engine returns Vec<HashMap<String,String>> (bindings per row).
        ExecutionEngine::execute(op, &mut self.item)
            .into_iter()
            .map(|hashmap| {
                let mut v: Vec<(String, String)> = hashmap.into_iter().collect();
                v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                v
            })
            .collect()
    }

    fn parse_data(&mut self, data: &str) -> Vec<Triple> {
        self.item.parse_and_encode_ntriples(data)
    }
}
