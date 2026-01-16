/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::{dictionary::Dictionary, query::FilterExpression};
use std::collections::HashMap;

/// Represents a condition for filtering operations
#[derive(Debug, Clone)]
pub struct Condition {
    pub expression: FilterExpression<'static>,
}

/// ID-based result type for performance optimization
#[derive(Debug, Clone)]
pub struct IdResult {
    pub bindings: HashMap<String, u32>, // Variable -> ID mapping
}

impl Condition {
    /// Creates a new condition
    pub fn new(variable: String, operator: String, value: String) -> Self {
        // Leak strings to get 'static lifetime
        let var_static: &'static str = Box::leak(variable.into_boxed_str());
        let op_static: &'static str = Box::leak(operator.into_boxed_str());
        let val_static: &'static str = Box::leak(value.into_boxed_str());

        Self {
            expression: FilterExpression::Comparison(var_static, op_static, val_static),
        }
    }

    /// Creates a new condition from a filter expression
    pub fn from_filter(filter: FilterExpression<'static>) -> Self {
        Self { expression: filter }
    }

    /// Evaluates the condition against string-based results
    pub fn evaluate(&self, result: &HashMap<String, String>) -> bool {
        self.evaluate_filter(&self.expression, result)
    }

    /// Evaluates a filter expression recursively
    fn evaluate_filter(&self, expr: &FilterExpression, result: &HashMap<String, String>) -> bool {
        match expr {
            FilterExpression::Comparison(var, op, value) => {
                let var_name = var.strip_prefix('?').unwrap_or(var);
                if let Some(result_value) = result.get(var_name) {
                    match *op {
                        "=" => result_value == value,
                        "!=" => result_value != value,
                        ">" => result_value.parse::<f64>().unwrap_or(0.0) 
                            > value.parse::<f64>().unwrap_or(0.0),
                        ">=" => result_value.parse::<f64>().unwrap_or(0.0) 
                            >= value.parse::<f64>().unwrap_or(0.0),
                        "<" => result_value.parse::<f64>().unwrap_or(0.0) 
                            < value.parse::<f64>().unwrap_or(0.0),
                        "<=" => result_value.parse::<f64>().unwrap_or(0.0) 
                            <= value.parse::<f64>().unwrap_or(0.0),
                        _ => false,
                    }
                } else {
                    false
                }
            }
            FilterExpression::And(left, right) => {
                self.evaluate_filter(left, result) && self.evaluate_filter(right, result)
            }
            FilterExpression::Or(left, right) => {
                self.evaluate_filter(left, result) || self.evaluate_filter(right, result)
            }
            FilterExpression::Not(inner) => {
                !self.evaluate_filter(inner, result)
            }
            FilterExpression::ArithmeticExpr(_expr) => {
                // For now, arithmetic expressions in filters return false
                // TODO: Implement full arithmetic expression evaluation
                false
            }
        }
    }

    /// Evaluates the condition against ID-based results for performance
    pub fn evaluate_with_ids(
        &self,
        result: &HashMap<String, u32>,
        dictionary: &Dictionary,
    ) -> bool {
        self.evaluate_filter_with_ids(&self.expression, result, dictionary)
    }

    /// Evaluates a filter expression with IDs recursively
    fn evaluate_filter_with_ids(
        &self,
        expr: &FilterExpression,
        result: &HashMap<String, u32>,
        dictionary: &Dictionary,
    ) -> bool {
        match expr {
            FilterExpression::Comparison(var, op, value) => {
                let var_name = var.strip_prefix('?').unwrap_or(var);
                if let Some(&id) = result.get(var_name) {
                    let decoded_value = dictionary.decode(id).unwrap();
                    match *op {
                        "=" => decoded_value == *value,
                        "!=" => decoded_value != *value,
                        ">" => decoded_value.parse::<f64>().unwrap_or(0.0) 
                            > value.parse::<f64>().unwrap_or(0.0),
                        ">=" => decoded_value.parse::<f64>().unwrap_or(0.0) 
                            >= value.parse::<f64>().unwrap_or(0.0),
                        "<" => decoded_value.parse::<f64>().unwrap_or(0.0) 
                            < value.parse::<f64>().unwrap_or(0.0),
                        "<=" => decoded_value.parse::<f64>().unwrap_or(0.0) 
                            <= value.parse::<f64>().unwrap_or(0.0),
                        _ => false,
                    }
                } else {
                    false
                }
            }
            FilterExpression::And(left, right) => {
                self.evaluate_filter_with_ids(left, result, dictionary) 
                    && self.evaluate_filter_with_ids(right, result, dictionary)
            }
            FilterExpression::Or(left, right) => {
                self.evaluate_filter_with_ids(left, result, dictionary) 
                    || self.evaluate_filter_with_ids(right, result, dictionary)
            }
            FilterExpression::Not(inner) => {
                !self.evaluate_filter_with_ids(inner, result, dictionary)
            }
            FilterExpression::ArithmeticExpr(_expr) => {
                // TODO: Implement arithmetic expression evaluation
                false
            }
        }
    }
}

impl IdResult {
    /// Creates a new empty IdResult
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    /// Creates a new IdResult with the given bindings
    pub fn with_bindings(bindings: HashMap<String, u32>) -> Self {
        Self { bindings }
    }

    /// Inserts a new binding
    pub fn insert(&mut self, variable: String, id: u32) {
        self.bindings.insert(variable, id);
    }

    /// Gets the ID for a variable
    pub fn get(&self, variable: &str) -> Option<&u32> {
        self.bindings.get(variable)
    }

    /// Checks if the result contains a binding for the given variable
    pub fn contains(&self, variable: &str) -> bool {
        self.bindings.contains_key(variable)
    }

    /// Returns the number of bindings
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Checks if the result is empty
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

impl Default for IdResult {
    fn default() -> Self {
        Self::new()
    }
}
