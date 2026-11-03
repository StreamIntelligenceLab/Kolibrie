/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`LogicalOperator`]: what a query computes, independent of how.
//!
//! Relational algebra over solution mappings. A logical plan is what the parser's
//! output becomes and what the optimizer consumes; it says nothing about join
//! algorithms or index choice.

use super::super::types::SubquerySpec;
use super::super::Condition;
use shared::dataset_index::{GraphTerm, QuadPattern};
use shared::terms::{Bindings, TriplePattern};

/// Logical operators represent the high-level query structure before optimization
#[derive(Debug, Clone)]
/// What a query computes: relational algebra over solution mappings.
///
/// Says nothing about how. [`Join`](Self::Join) does not name an algorithm and
/// [`Scan`](Self::Scan) does not name an index — choosing those is what
/// [`Streamertail`](super::super::Streamertail) does.
pub enum LogicalOperator {
    /// The SPARQL unit table: one empty solution mapping.
    Unit,
    /// Match a quad pattern against the store.
    Scan {
        /// The pattern to match; variables are wildcards.
        pattern: QuadPattern,
    },
    /// Multiset union. Branches are optimizer boundaries and retain duplicates.
    Union {
        /// The branches, each optimised independently.
        branches: Vec<LogicalOperator>,
    },
    /// Evaluate an input pattern with the given active named graph.
    Graph {
        /// Pattern evaluated inside that graph.
        input: Box<LogicalOperator>,
        /// Which graph: fixed, or a variable bound to each in turn.
        graph: GraphTerm,
    },
    /// Keep only the solutions satisfying a condition.
    Selection {
        /// The input whose solutions are filtered.
        predicate: Box<LogicalOperator>,
        /// The condition each surviving solution must satisfy.
        condition: Condition,
    },
    /// Restrict each solution to a set of variables.
    ///
    /// Does not remove duplicates: SPARQL is bag semantics, and `DISTINCT` is a
    /// separate modifier.
    Projection {
        /// The input being projected.
        predicate: Box<LogicalOperator>,
        /// Variables to retain.
        variables: Vec<String>,
    },
    /// Join two inputs on the variables they share.
    ///
    /// With no shared variable this is a Cartesian product. The physical form is
    /// chosen by cost: hash, nested-loop, star, or a bind join that pushes the
    /// left side's bindings into the right side's index probes.
    Join {
        /// Left input.
        left: Box<LogicalOperator>,
        /// Right input.
        right: Box<LogicalOperator>,
    },
    /// Solutions supplied directly rather than computed, such as a window's
    /// content arriving from the stream engine.
    Buffer {
        /// The solutions.
        content: Bindings,
        /// Where they came from, for diagnostics.
        origin: String,
    },
    /// A nested query evaluated independently, then joined with the enclosing
    /// pattern on its projected variables.
    Subquery {
        /// The nested plan.
        inner: Box<LogicalOperator>,
        /// What it projects and how its modifiers apply.
        spec: SubquerySpec,
    },
    /// Compute a new binding from existing ones by calling a registered function.
    Bind {
        /// The input being extended.
        input: Box<LogicalOperator>,
        /// Function to call; resolved against the store's registered UDFs at
        /// execution time, not at planning time.
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
        /// Rows; `None` is `UNDEF`, which leaves that variable unbound rather than
        /// binding it, so it does not constrain the join.
        values: Vec<Vec<Option<u32>>>,
    },
    /// Bind a variable from a model's prediction over each input solution.
    MLPredict {
        /// Solutions to predict over.
        input: Box<LogicalOperator>,
        /// Model to invoke; must be approved by the host's ML policy.
        model_name: String,
        /// Variables forming the feature vector, in model input order.
        input_variables: Vec<String>,
        /// Variable the prediction binds to.
        output_variable: String,
    },
}

impl LogicalOperator {
    /// Whether this plan is worth running the join-ordering search on.
    ///
    /// True when it joins at least two reorderable base patterns, or contains
    /// operators the counting cannot see through. A single pattern has no ordering
    /// to choose, so the search would only cost time.
    pub fn requires_cost_based_join_ordering(&self) -> bool {
        self.reorderable_scan_count()
            .is_none_or(|patterns| patterns >= 2)
    }

    /// Number of joinable base patterns, or `None` if the plan contains other operators
    fn reorderable_scan_count(&self) -> Option<usize> {
        match self {
            Self::Unit => Some(0),
            Self::Scan { .. } => Some(1),
            Self::Graph { input, .. }
            | Self::Selection {
                predicate: input, ..
            }
            | Self::Projection {
                predicate: input, ..
            }
            | Self::Bind { input, .. } => input.reorderable_scan_count(),
            Self::Join { left, right } => Some(
                left.reorderable_scan_count()?
                    .saturating_add(right.reorderable_scan_count()?),
            ),
            // Conservatively require statistics
            Self::Union { .. }
            | Self::Subquery { .. }
            | Self::MLPredict { .. }
            | Self::Values { .. }
            | Self::Buffer { .. } => None,
        }
    }

    /// Creates the SPARQL unit table.
    pub fn unit() -> Self {
        Self::Unit
    }

    /// Creates a new scan logical operator
    pub fn scan(pattern: TriplePattern) -> Self {
        Self::quad_scan(QuadPattern {
            subject: pattern.0,
            predicate: pattern.1,
            object: pattern.2,
            graph: GraphTerm::Default,
        })
    }

    /// Creates a graph-aware scan logical operator.
    pub fn quad_scan(pattern: QuadPattern) -> Self {
        Self::Scan { pattern }
    }

    /// Creates a multiset UNION logical operator.
    pub fn union(branches: Vec<LogicalOperator>) -> Self {
        Self::Union { branches }
    }

    /// Creates a GRAPH logical operator.
    pub fn graph(input: LogicalOperator, graph: GraphTerm) -> Self {
        Self::Graph {
            input: Box::new(input),
            graph,
        }
    }

    /// Creates a new selection logical operator
    pub fn selection(predicate: LogicalOperator, condition: Condition) -> Self {
        Self::Selection {
            predicate: Box::new(predicate),
            condition,
        }
    }

    /// Creates a new projection logical operator
    pub fn projection(predicate: LogicalOperator, variables: Vec<String>) -> Self {
        Self::Projection {
            predicate: Box::new(predicate),
            variables,
        }
    }

    /// Creates a new join logical operator
    pub fn join(left: LogicalOperator, right: LogicalOperator) -> Self {
        Self::Join {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// A plan node supplying solutions directly, such as a window's content.
    pub fn buffer(content: Bindings, origin: String) -> Self {
        Self::Buffer { content, origin }
    }

    /// Creates a new subquery logical operator
    pub fn subquery(inner: LogicalOperator, spec: SubquerySpec) -> Self {
        Self::Subquery {
            inner: Box::new(inner),
            spec,
        }
    }

    /// Creates a new bind logical operator
    pub fn bind(
        input: LogicalOperator,
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

    /// Creates a new values logical operator
    pub fn values(variables: Vec<String>, values: Vec<Vec<Option<u32>>>) -> Self {
        Self::Values { variables, values }
    }

    /// Creates a new ML.PREDICT logical operator
    pub fn ml_predict(
        input: LogicalOperator,
        model_name: String,
        input_variables: Vec<String>,
        output_variable: String,
    ) -> Self {
        Self::MLPredict {
            input: Box::new(input),
            model_name,
            input_variables,
            output_variable,
        }
    }
}
