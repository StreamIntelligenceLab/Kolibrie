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
use shared::triple::Triple;

#[cfg(not(test))]
use log::{debug, error};
#[cfg(test)]
use std::{println as debug, println as error};

pub struct SimpleR2R {
    pub item: SparqlDatabase,
    pub execution_mode: QueryExecutionMode,
}

impl SimpleR2R {
    pub fn new() -> Self {
        SimpleR2R {
            item: SparqlDatabase::new(),
            execution_mode: QueryExecutionMode::Standard,
        }
    }

    pub fn with_execution_mode(execution_mode: QueryExecutionMode) -> Self {
        SimpleR2R {
            item: SparqlDatabase::new(),
            execution_mode,
        }
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

    fn load_rules(&mut self, _data: &str) -> Result<(), &'static str> {
        error!("Unsupported operation load rules");
        Err("something went wrong")
    }

    fn add(&mut self, data: Triple) {
        self.item.add_triple(data);
    }

    fn remove(&mut self, data: &Triple) {
        self.item.delete_triple(data);
    }

    fn materialize(&mut self) -> Vec<Triple> {
        error!("Unsupported operation materialize");
        Vec::new()
    }

    fn execute_query(&mut self, op: &PhysicalOperator) -> Vec<Vec<(String, String)>> {
        debug!("SimpleR2R executing query with PhysicalOperator");

        // Execute the physical operator using the Volcano execution engine.
        // The engine returns Vec<HashMap<String,String>> (bindings per row).
        ExecutionEngine::execute(op, &mut self.item)
            .into_iter()
            .map(|hashmap| hashmap.into_iter().collect())
            .collect()
    }

    fn parse_data(&mut self, data: &str) -> Vec<Triple> {
        self.item.parse_and_encode_ntriples(data)
    }
}
