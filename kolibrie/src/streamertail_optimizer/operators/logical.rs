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

/// Logical operators represent the high-level query structure before optimization
#[derive(Debug, Clone)]
pub enum LogicalOperator {
    /// The SPARQL unit table: one empty solution mapping.
    Unit,
    Scan {
        pattern: QuadPattern,
    },
    /// Multiset union. Branches are optimizer boundaries and retain duplicates.
    Union {
        branches: Vec<LogicalOperator>,
    },
    /// Evaluate an input pattern with the given active named graph.
    Graph {
        input: Box<LogicalOperator>,
        graph: GraphTerm,
    },
    Selection {
        predicate: Box<LogicalOperator>,
        condition: Condition,
    },
    Projection {
        predicate: Box<LogicalOperator>,
        variables: Vec<String>,
    },
    Join {
        left: Box<LogicalOperator>,
        right: Box<LogicalOperator>,
    },
    Buffer {
        content: Bindings,
        origin: String,
    },
    Subquery {
        inner: Box<LogicalOperator>,
        spec: SubquerySpec,
    },
    Bind {
        input: Box<LogicalOperator>,
        function_name: String,
        arguments: Vec<String>,
        output_variable: String,
    },
    Values {
        variables: Vec<String>,
        values: Vec<Vec<Option<u32>>>, // Each row can have an encoded term or UNDEF
    },
    MLPredict {
        input: Box<LogicalOperator>,
        model_name: String,
        input_variables: Vec<String>,
        output_variable: String,
    },
}

impl LogicalOperator {
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
