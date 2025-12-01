/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::dictionary::Dictionary;
use std::collections::HashMap;

/// Represents a condition for filtering operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    pub variable: String,
    pub operator: String,
    pub value: String,
}

/// ID-based result type for performance optimization
#[derive(Debug, Clone)]
pub struct IdResult {
    pub bindings: HashMap<String, u32>, // Variable -> ID mapping
}

impl Condition {
    /// Creates a new condition
    pub fn new(variable: String, operator: String, value: String) -> Self {
        Self {
            variable,
            operator,
            value,
        }
    }

    /// Evaluates the condition against string-based results
    pub fn evaluate(&self, result: &HashMap<String, String>) -> bool {
        if let Some(value) = result.get(&self.variable) {
            match self.operator.as_str() {
                "=" => value == &self.value,
                "!=" => value != &self.value,
                ">" => value.parse::<i32>().unwrap_or(0) > self.value.parse::<i32>().unwrap_or(0),
                ">=" => value.parse::<i32>().unwrap_or(0) >= self.value.parse::<i32>().unwrap_or(0),
                "<" => value.parse::<i32>().unwrap_or(0) < self.value.parse::<i32>().unwrap_or(0),
                "<=" => value.parse::<i32>().unwrap_or(0) <= self.value.parse::<i32>().unwrap_or(0),
                _ => false,
            }
        } else {
            false
        }
    }

    /// Evaluates the condition against ID-based results for performance
    pub fn evaluate_with_ids(
        &self,
        result: &HashMap<String, u32>,
        dictionary: &Dictionary,
    ) -> bool {
        if let Some(&id) = result.get(&self.variable) {
            // Only decode when necessary for comparison
            let decoded_value = dictionary.decode(id).unwrap();
            match self.operator.as_str() {
                "=" => decoded_value == self.value,
                "!=" => decoded_value != self.value,
                ">" => {
                    decoded_value.parse::<i32>().unwrap_or(0)
                        > self.value.parse::<i32>().unwrap_or(0)
                }
                ">=" => {
                    decoded_value.parse::<i32>().unwrap_or(0)
                        >= self.value.parse::<i32>().unwrap_or(0)
                }
                "<" => {
                    decoded_value.parse::<i32>().unwrap_or(0)
                        < self.value.parse::<i32>().unwrap_or(0)
                }
                "<=" => {
                    decoded_value.parse::<i32>().unwrap_or(0)
                        <= self.value.parse::<i32>().unwrap_or(0)
                }
                _ => false,
            }
        } else {
            false
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
