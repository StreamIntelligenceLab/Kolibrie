/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`PhysicalOperator`]: how a query is executed.
//!
//! The optimizer's output. Each node names a concrete algorithm — which join, which
//! index, whether a side is materialised — so running a physical plan involves no
//! further choices.

use super::super::types::SubquerySpec;
use super::super::Condition;
use shared::dataset_index::{GraphTerm, QuadPattern};
use shared::terms::{Bindings, TriplePattern};

/// Physical operators represent the actual execution plan after optimization
#[derive(Debug, Clone)]
/// How a query is executed: a concrete algorithm per node.
///
/// The optimizer's output. Nothing here is left to choose at run time, which is why
/// the join variants name their algorithm rather than saying "join".
pub enum PhysicalOperator {
    /// The SPARQL unit table: one empty solution mapping.
    Unit,
    /// Scan without an index, examining every candidate quad.
    ///
    /// Dispatches to the same executor as [`IndexScan`](Self::IndexScan); the two
    /// are distinguished for costing, not execution.
    TableScan {
        /// Pattern to match.
        pattern: QuadPattern,
    },
    /// Scan through an index, visiting only quads that can match.
    IndexScan {
        /// Pattern to match; its bound positions select the index.
        pattern: QuadPattern,
    },
    /// Multiset union. No duplicate elimination is performed.
    Union {
        /// Branches, concatenated in order.
        branches: Vec<PhysicalOperator>,
    },
    /// Evaluate an input plan under a fixed or variable named graph.
    Graph {
        /// Plan evaluated inside that graph.
        input: Box<PhysicalOperator>,
        /// Which graph: fixed, or a variable bound to each in turn.
        graph: GraphTerm,
    },
    /// Drop solutions failing a condition.
    Filter {
        /// Plan whose solutions are filtered.
        input: Box<PhysicalOperator>,
        /// Condition each surviving solution satisfies.
        condition: Condition,
    },
    /// Pipelined dependent join whose left solutions feed the right side, so its scans probe indexes with bound values
    BindJoin {
        /// Executed first; each of its solutions drives the right side.
        left: Box<PhysicalOperator>,
        /// Re-entered once per left solution, with those bindings pushed in.
        right: Box<PhysicalOperator>,
    },
    /// Build/probe join over the shared-variable key of two independently executed sides
    HashJoin {
        /// Build side.
        left: Box<PhysicalOperator>,
        /// Probe side.
        right: Box<PhysicalOperator>,
    },
    /// Materialized pairwise join over both sides, and the algorithm for Cartesian products
    NestedLoopJoin {
        /// Outer side.
        left: Box<PhysicalOperator>,
        /// Inner side, re-scanned per outer solution.
        right: Box<PhysicalOperator>,
    },
    /// Join several patterns that all share one variable, in a single pass.
    ///
    /// Cheaper than a chain of pairwise joins when the shared variable is the only
    /// link, because no intermediate result is materialised per pair.
    StarJoin {
        /// The variable every pattern shares.
        join_var: String,
        /// Patterns joined on it.
        patterns: Vec<TriplePattern>,
    },
    /// Restrict each solution to a set of variables, keeping duplicates.
    Projection {
        /// Plan being projected.
        input: Box<PhysicalOperator>,
        /// Variables retained.
        variables: Vec<String>,
    },
    /// Solutions supplied directly rather than computed.
    InMemoryBuffer {
        /// The solutions.
        content: Bindings,
        /// Where they came from, for diagnostics.
        origin: String,
    },
    /// A nested plan evaluated independently, then joined on its projection.
    Subquery {
        /// The nested plan.
        inner: Box<PhysicalOperator>,
        /// What it projects and how its modifiers apply.
        spec: SubquerySpec,
    },
    /// Extend each solution with a value computed by a registered function.
    Bind {
        /// Plan being extended.
        input: Box<PhysicalOperator>,
        /// Function resolved against the store's UDFs at execution time.
        function_name: String,
        /// Variables passed as arguments.
        arguments: Vec<String>,
        /// Variable the result binds to.
        output_variable: String,
    },
    /// An inline table of candidate solutions.
    Values {
        /// Column variables, in order.
        variables: Vec<String>,
        /// Rows; `None` is `UNDEF` and leaves the variable unbound.
        values: Vec<Vec<Option<u32>>>,
    },
    /// Bind a variable from a model's prediction over each input solution.
    MLPredict {
        /// Solutions to predict over.
        input: Box<PhysicalOperator>,
        /// Model name, as declared.
        model_name: String,
        /// Artifact resolved at planning time, so execution does not re-consult the
        /// ML policy.
        model_path: String,
        /// Feature variables, in model input order.
        input_variables: Vec<String>,
        /// Variable the prediction binds to.
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

    /// A plan node supplying solutions directly.
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
