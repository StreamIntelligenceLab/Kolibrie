/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::super::types::SubquerySpec;
use super::super::Condition;
use shared::dataset_index::{GraphTerm, QuadPattern};
use shared::terms::{Bindings, TriplePattern};

/// Physical operators represent the actual execution plan after optimization
#[derive(Debug, Clone)]
pub enum PhysicalOperator {
    /// The SPARQL unit table: one empty solution mapping.
    Unit,
    TableScan {
        pattern: QuadPattern,
    },
    IndexScan {
        pattern: QuadPattern,
    },
    /// Multiset union. No duplicate elimination is performed.
    Union {
        branches: Vec<PhysicalOperator>,
    },
    /// Evaluate an input plan under a fixed or variable named graph.
    Graph {
        input: Box<PhysicalOperator>,
        graph: GraphTerm,
    },
    Filter {
        input: Box<PhysicalOperator>,
        condition: Condition,
    },
    /// Pipelined dependent join whose left solutions feed the right side, so its scans probe indexes with bound values
    BindJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    /// Build/probe join over the shared-variable key of two independently executed sides
    HashJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    /// Materialized pairwise join over both sides, and the algorithm for Cartesian products
    NestedLoopJoin {
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
    InMemoryBuffer {
        content: Bindings,
        origin: String,
    },
    Subquery {
        inner: Box<PhysicalOperator>,
        spec: SubquerySpec,
    },
    Bind {
        input: Box<PhysicalOperator>,
        function_name: String,
        arguments: Vec<String>,
        output_variable: String,
    },
    Values {
        variables: Vec<String>,
        values: Vec<Vec<Option<u32>>>,
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
    /// Creates the SPARQL unit table.
    pub fn unit() -> Self {
        Self::Unit
    }

    /// Creates a new table scan physical operator
    pub fn table_scan(pattern: TriplePattern) -> Self {
        Self::quad_table_scan(QuadPattern {
            subject: pattern.0,
            predicate: pattern.1,
            object: pattern.2,
            graph: GraphTerm::Default,
        })
    }

    /// Creates a graph-aware table scan.
    pub fn quad_table_scan(pattern: QuadPattern) -> Self {
        Self::TableScan { pattern }
    }

    /// Creates a new index scan physical operator
    pub fn index_scan(pattern: TriplePattern) -> Self {
        Self::quad_index_scan(QuadPattern {
            subject: pattern.0,
            predicate: pattern.1,
            object: pattern.2,
            graph: GraphTerm::Default,
        })
    }

    /// Creates a graph-aware index scan.
    pub fn quad_index_scan(pattern: QuadPattern) -> Self {
        Self::IndexScan { pattern }
    }

    /// Creates a multiset UNION physical operator.
    pub fn union(branches: Vec<PhysicalOperator>) -> Self {
        Self::Union { branches }
    }

    /// Creates a GRAPH physical operator.
    pub fn graph(input: PhysicalOperator, graph: GraphTerm) -> Self {
        Self::Graph {
            input: Box::new(input),
            graph,
        }
    }

    /// Creates a new filter physical operator
    pub fn filter(input: PhysicalOperator, condition: Condition) -> Self {
        Self::Filter {
            input: Box::new(input),
            condition,
        }
    }

    /// Creates a new bind join physical operator
    pub fn bind_join(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self::BindJoin {
            left: Box::new(left),
            right: Box::new(right),
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

    /// Creates a new projection physical operator
    pub fn projection(input: PhysicalOperator, variables: Vec<String>) -> Self {
        Self::Projection {
            input: Box::new(input),
            variables,
        }
    }

    pub fn buffer(content: Bindings, origin: String) -> Self {
        Self::InMemoryBuffer { content, origin }
    }

    /// Creates a new subquery physical operator
    pub fn subquery(inner: PhysicalOperator, spec: SubquerySpec) -> Self {
        Self::Subquery {
            inner: Box::new(inner),
            spec,
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
    pub fn values(variables: Vec<String>, values: Vec<Vec<Option<u32>>>) -> Self {
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
