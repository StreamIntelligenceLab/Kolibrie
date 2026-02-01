/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::super::Condition;
use shared::terms::{Bindings, TriplePattern};

/// Physical operators represent the actual execution plan after optimization
#[derive(Debug, Clone)]
pub enum PhysicalOperator {
    TableScan {
        pattern: TriplePattern,
    },
    IndexScan {
        pattern: TriplePattern,
    },
    Filter {
        input: Box<PhysicalOperator>,
        condition: Condition,
    },
    HashJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    NestedLoopJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    ParallelJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    OptimizedHashJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    StarJoin {
        join_var: String,
        patterns: Vec<TriplePattern>,
    },
    Projection {
        input: Box<PhysicalOperator>,
        variables: Vec<String>,
    },
    InMemoryBuffer{
        content: Bindings,
        origin: String
    },
    Subquery {
        inner: Box<PhysicalOperator>,
        projected_vars:  Vec<String>,
    },
    Bind {
        input: Box<PhysicalOperator>,
        function_name: String,
        arguments: Vec<String>,
        output_variable: String,
    },
    Values {
        variables: Vec<String>,
        values: Vec<Vec<Option<String>>>,
    },
    MLPredict {
        input: Box<PhysicalOperator>,
        model_name: String,
        model_path: String,
        input_variables: Vec<String>,
        output_variable: String,
    },
}

impl PhysicalOperator {
    /// Creates a new table scan physical operator
    pub fn table_scan(pattern: TriplePattern) -> Self {
        Self::TableScan { pattern }
    }

    /// Creates a new index scan physical operator
    pub fn index_scan(pattern: TriplePattern) -> Self {
        Self::IndexScan { pattern }
    }

    /// Creates a new filter physical operator
    pub fn filter(input: PhysicalOperator, condition: Condition) -> Self {
        Self::Filter {
            input: Box::new(input),
            condition,
        }
    }

    /// Creates a new hash join physical operator
    pub fn hash_join(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Creates a new nested loop join physical operator
    pub fn nested_loop_join(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Creates a new parallel join physical operator
    pub fn parallel_join(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self::ParallelJoin {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Creates a new optimized hash join physical operator
    pub fn optimized_hash_join(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self::OptimizedHashJoin {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Creates a new projection physical operator
    pub fn projection(input: PhysicalOperator, variables: Vec<String>) -> Self {
        Self::Projection {
            input: Box::new(input),
            variables,
        }
    }

    pub fn buffer(content: Bindings, origin: String)-> Self {
        Self::InMemoryBuffer {content, origin}
    }

    /// Creates a new subquery physical operator
    pub fn subquery(inner: PhysicalOperator, projected_vars: Vec<String>) -> Self {
        Self::Subquery {
            inner: Box::new(inner),
            projected_vars,
        }
    }

    /// Creates a new bind physical operator
    pub fn bind(
        input: PhysicalOperator,
        function_name: String,
        arguments: Vec<String>,
        output_variable: String,
    ) -> Self {
        Self::Bind {
            input: Box::new(input),
            function_name,
            arguments,
            output_variable,
        }
    }

    /// Creates a new values physical operator
    pub fn values(variables: Vec<String>, values: Vec<Vec<Option<String>>>) -> Self {
        Self::Values { variables, values }
    }

    /// Creates a new ML.PREDICT physical operator
    pub fn ml_predict(
        input: PhysicalOperator,
        model_name: String,
        model_path: String,
        input_variables: Vec<String>,
        output_variable: String,
    ) -> Self {
        Self::MLPredict {
            input: Box::new(input),
            model_name,
            model_path,
            input_variables,
            output_variable,
        }
    }

    /// Executes the physical operator and returns string-based results
    pub fn execute(
        &self,
        database: &mut crate::sparql_database::SparqlDatabase,
    ) -> Vec<std::collections::HashMap<String, String>> {
        super::super::execution::ExecutionEngine::execute(self, database)
    }

    /// Executes the physical operator and returns ID-based results for performance
    pub fn execute_with_ids(
        &self,
        database: &mut crate::sparql_database::SparqlDatabase,
    ) -> Vec<std::collections::HashMap<String, u32>> {
        super::super::execution::ExecutionEngine::execute_with_ids(self, database)
    }
}
