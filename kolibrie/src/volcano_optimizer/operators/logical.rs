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
use shared::terms::TriplePattern;

/// Logical operators represent the high-level query structure before optimization
#[derive(Debug, Clone)]
pub enum LogicalOperator {
    Scan {
        pattern: TriplePattern,
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
    Subquery {
        inner: Box<LogicalOperator>,
        projected_vars: Vec<String>,
    },
    Bind {
        input: Box<LogicalOperator>,
        function_name: String,
        arguments: Vec<String>,
        output_variable: String,
    },
    Values {
        variables: Vec<String>,
        values: Vec<Vec<Option<String>>>, // Each row can have Some(value) or None (UNDEF)
    },
}

impl LogicalOperator {
    /// Creates a new scan logical operator
    pub fn scan(pattern: TriplePattern) -> Self {
        Self::Scan { pattern }
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

    /// Creates a new subquery logical operator
    pub fn subquery(inner: LogicalOperator, projected_vars: Vec<String>) -> Self {
        Self::Subquery {
            inner: Box:: new(inner),
            projected_vars,
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
    pub fn values(variables: Vec<String>, values: Vec<Vec<Option<String>>>) -> Self {
        Self::Values { variables, values }
    }
}
