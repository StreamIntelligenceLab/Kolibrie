/*
 * Copyright Â© 2024 Volodymyr Kadzhaia
 * Copyright Â© 2024 Pieter Bonte
 * KU Leuven â€” Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::{
    dictionary::Dictionary,
    query::{ArithmeticExpression, FilterExpression, SortDirection},
};
use std::collections::HashMap;

/// Owned execution representation of Kolibrie's existing arithmetic syntax.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionArithmetic {
    Operand(String),
    Add(Box<ConditionArithmetic>, Box<ConditionArithmetic>),
    Subtract(Box<ConditionArithmetic>, Box<ConditionArithmetic>),
    Multiply(Box<ConditionArithmetic>, Box<ConditionArithmetic>),
    Divide(Box<ConditionArithmetic>, Box<ConditionArithmetic>),
}

/// Owned execution representation of the existing parsed `FilterExpression`.
///
/// The parser remains source-borrowed; lowering copies each expression once
/// into the physical plan. This avoids leaking query strings to manufacture a
/// `'static` parser lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionExpression {
    Comparison(String, String, String),
    ArithmeticComparison(ConditionArithmetic, String, ConditionArithmetic),
    And(Box<ConditionExpression>, Box<ConditionExpression>),
    Or(Box<ConditionExpression>, Box<ConditionExpression>),
    Not(Box<ConditionExpression>),
    ArithmeticExpr(Box<ConditionArithmetic>),
    FunctionCall(String, Vec<String>),
}

/// Represents a condition for filtering operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    pub expression: ConditionExpression,
}

/// ID-based result type for performance optimization.
#[derive(Debug, Clone)]
pub struct IdResult {
    pub bindings: HashMap<String, u32>,
}

/// One owned projection item for a subquery.
///
/// The parser keeps source-borrowed strings, while logical and physical plans
/// must own the query metadata that survives optimization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubqueryProjection {
    pub kind: String,
    pub variable: String,
    pub alias: Option<String>,
}

/// SELECT modifiers that must be applied inside a subquery before its
/// solutions are joined back into the enclosing group graph pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubquerySpec {
    /// `None` represents `SELECT *`; `Some` is an explicit projection list.
    pub projection: Option<Vec<SubqueryProjection>>,
    pub distinct: bool,
    pub group_vars: Vec<String>,
    pub order_conditions: Vec<(String, SortDirection)>,
    pub limit: Option<usize>,
}

impl Condition {
    fn normalize_variable(variable: &str) -> &str {
        variable
            .strip_prefix('?')
            .or_else(|| variable.strip_prefix('$'))
            .unwrap_or(variable)
    }

    fn is_variable(value: &str) -> bool {
        value.starts_with('?') || value.starts_with('$')
    }

    fn normalize_lexical(value: &str) -> &str {
        let value = value.trim();
        if value.starts_with('<') && value.ends_with('>') && value.len() >= 2 {
            return &value[1..value.len() - 1];
        }
        if let Some(rest) = value.strip_prefix('"') {
            let mut escaped = false;
            for (index, character) in rest.char_indices() {
                if escaped {
                    escaped = false;
                    continue;
                }
                match character {
                    '\\' => escaped = true,
                    '"' => return &rest[..index],
                    _ => {}
                }
            }
        }
        value
    }

    fn own_arithmetic(expression: &ArithmeticExpression<'_>) -> ConditionArithmetic {
        match expression {
            ArithmeticExpression::Operand(value) => {
                ConditionArithmetic::Operand((*value).to_string())
            }
            ArithmeticExpression::Add(left, right) => ConditionArithmetic::Add(
                Box::new(Self::own_arithmetic(left)),
                Box::new(Self::own_arithmetic(right)),
            ),
            ArithmeticExpression::Subtract(left, right) => ConditionArithmetic::Subtract(
                Box::new(Self::own_arithmetic(left)),
                Box::new(Self::own_arithmetic(right)),
            ),
            ArithmeticExpression::Multiply(left, right) => ConditionArithmetic::Multiply(
                Box::new(Self::own_arithmetic(left)),
                Box::new(Self::own_arithmetic(right)),
            ),
            ArithmeticExpression::Divide(left, right) => ConditionArithmetic::Divide(
                Box::new(Self::own_arithmetic(left)),
                Box::new(Self::own_arithmetic(right)),
            ),
        }
    }

    fn own_raw_arithmetic(expression: &str) -> Option<ConditionArithmetic> {
        let (remaining, expression) =
            crate::parser::parse_arithmetic_expression(expression).ok()?;
        if !remaining.trim().is_empty() {
            return None;
        }
        Some(Self::own_arithmetic(&expression))
    }

    fn is_compound_arithmetic(expression: &ConditionArithmetic) -> bool {
        !matches!(expression, ConditionArithmetic::Operand(_))
    }

    fn own_filter<F>(filter: &FilterExpression<'_>, resolve_constant: &F) -> ConditionExpression
    where
        F: Fn(&str) -> String,
    {
        match filter {
            FilterExpression::Comparison(variable, operator, value) => {
                let left_arithmetic = Self::own_raw_arithmetic(variable);
                let right_arithmetic = Self::own_raw_arithmetic(value);
                if left_arithmetic
                    .as_ref()
                    .is_some_and(Self::is_compound_arithmetic)
                    || right_arithmetic
                        .as_ref()
                        .is_some_and(Self::is_compound_arithmetic)
                {
                    if let (Some(left), Some(right)) = (left_arithmetic, right_arithmetic) {
                        return ConditionExpression::ArithmeticComparison(
                            left,
                            (*operator).to_string(),
                            right,
                        );
                    }
                }

                let value = if Self::is_variable(value) {
                    (*value).to_string()
                } else {
                    resolve_constant(value)
                };
                ConditionExpression::Comparison(
                    (*variable).to_string(),
                    (*operator).to_string(),
                    value,
                )
            }
            FilterExpression::And(left, right) => ConditionExpression::And(
                Box::new(Self::own_filter(left, resolve_constant)),
                Box::new(Self::own_filter(right, resolve_constant)),
            ),
            FilterExpression::Or(left, right) => ConditionExpression::Or(
                Box::new(Self::own_filter(left, resolve_constant)),
                Box::new(Self::own_filter(right, resolve_constant)),
            ),
            FilterExpression::Not(inner) => {
                ConditionExpression::Not(Box::new(Self::own_filter(inner, resolve_constant)))
            }
            FilterExpression::ArithmeticExpr(expression) => {
                ConditionExpression::ArithmeticExpr(Box::new(Self::own_arithmetic(expression)))
            }
            FilterExpression::FunctionCall(name, arguments) => {
                let arguments = arguments
                    .iter()
                    .map(|argument| {
                        if Self::is_variable(argument) {
                            (*argument).to_string()
                        } else {
                            resolve_constant(argument)
                        }
                    })
                    .collect();
                ConditionExpression::FunctionCall((*name).to_string(), arguments)
            }
        }
    }

    fn evaluate_arithmetic<F: Fn(&str) -> Option<f64>>(
        expression: &ConditionArithmetic,
        resolve: &F,
    ) -> Result<f64, String> {
        match expression {
            ConditionArithmetic::Operand(value) => {
                if Self::is_variable(value) {
                    resolve(value)
                        .ok_or_else(|| format!("variable '{value}' is unbound or non-numeric"))
                } else {
                    Self::normalize_lexical(value)
                        .parse::<f64>()
                        .map_err(|_| format!("cannot parse '{value}' as a number"))
                }
            }
            ConditionArithmetic::Add(left, right) => Ok(Self::evaluate_arithmetic(left, resolve)?
                + Self::evaluate_arithmetic(right, resolve)?),
            ConditionArithmetic::Subtract(left, right) => {
                Ok(Self::evaluate_arithmetic(left, resolve)?
                    - Self::evaluate_arithmetic(right, resolve)?)
            }
            ConditionArithmetic::Multiply(left, right) => {
                Ok(Self::evaluate_arithmetic(left, resolve)?
                    * Self::evaluate_arithmetic(right, resolve)?)
            }
            ConditionArithmetic::Divide(left, right) => {
                let right = Self::evaluate_arithmetic(right, resolve)?;
                if right == 0.0 {
                    Err("division by zero".to_string())
                } else {
                    Ok(Self::evaluate_arithmetic(left, resolve)? / right)
                }
            }
        }
    }

    pub fn new(variable: String, operator: String, value: String) -> Self {
        Self {
            expression: ConditionExpression::Comparison(variable, operator, value),
        }
    }

    pub fn from_filter(filter: FilterExpression<'_>) -> Self {
        Self::from_filter_with_resolver(&filter, |value| value.to_string())
    }

    pub fn from_filter_with_resolver<F>(filter: &FilterExpression<'_>, resolve_constant: F) -> Self
    where
        F: Fn(&str) -> String,
    {
        Self {
            expression: Self::own_filter(filter, &resolve_constant),
        }
    }

    pub fn evaluate(&self, result: &HashMap<String, String>) -> bool {
        self.evaluate_filter(&self.expression, result)
    }

    fn evaluate_filter(
        &self,
        expression: &ConditionExpression,
        result: &HashMap<String, String>,
    ) -> bool {
        match expression {
            ConditionExpression::Comparison(variable, operator, value) => {
                let Some(result_value) = result.get(Self::normalize_variable(variable)) else {
                    return false;
                };
                let rhs = if Self::is_variable(value) {
                    result
                        .get(Self::normalize_variable(value))
                        .map(String::as_str)
                } else {
                    Some(Self::normalize_lexical(value))
                };
                let Some(rhs) = rhs else {
                    return false;
                };
                let lhs = Self::normalize_lexical(result_value);
                Self::compare_lexical(lhs, operator, rhs)
            }
            ConditionExpression::ArithmeticComparison(left, operator, right) => {
                let resolver = |variable: &str| {
                    result
                        .get(Self::normalize_variable(variable))?
                        .parse::<f64>()
                        .ok()
                };
                let Ok(left) = Self::evaluate_arithmetic(left, &resolver) else {
                    return false;
                };
                let Ok(right) = Self::evaluate_arithmetic(right, &resolver) else {
                    return false;
                };
                Self::compare_numeric(left, operator, right)
            }
            ConditionExpression::And(left, right) => {
                self.evaluate_filter(left, result) && self.evaluate_filter(right, result)
            }
            ConditionExpression::Or(left, right) => {
                self.evaluate_filter(left, result) || self.evaluate_filter(right, result)
            }
            ConditionExpression::Not(inner) => !self.evaluate_filter(inner, result),
            ConditionExpression::ArithmeticExpr(expression) => {
                let resolver = |variable: &str| {
                    result
                        .get(Self::normalize_variable(variable))?
                        .parse::<f64>()
                        .ok()
                };
                Self::evaluate_arithmetic(expression, &resolver)
                    .map(|value| value != 0.0)
                    .unwrap_or(false)
            }
            ConditionExpression::FunctionCall(name, arguments) => {
                if name != "isTRIPLE" {
                    return false;
                }
                let Some(argument) = arguments.first() else {
                    return false;
                };
                let value = if Self::is_variable(argument) {
                    result
                        .get(Self::normalize_variable(argument))
                        .map(String::as_str)
                        .unwrap_or("")
                } else {
                    argument
                };
                value.starts_with("<<") && value.ends_with(">>")
            }
        }
    }

    fn compare_lexical(lhs: &str, operator: &str, rhs: &str) -> bool {
        match operator {
            "=" => lhs == rhs,
            "!=" => lhs != rhs,
            ">" => lhs.parse::<f64>().unwrap_or(0.0) > rhs.parse::<f64>().unwrap_or(0.0),
            ">=" => lhs.parse::<f64>().unwrap_or(0.0) >= rhs.parse::<f64>().unwrap_or(0.0),
            "<" => lhs.parse::<f64>().unwrap_or(0.0) < rhs.parse::<f64>().unwrap_or(0.0),
            "<=" => lhs.parse::<f64>().unwrap_or(0.0) <= rhs.parse::<f64>().unwrap_or(0.0),
            _ => false,
        }
    }

    fn compare_numeric(lhs: f64, operator: &str, rhs: f64) -> bool {
        match operator {
            "=" => lhs == rhs,
            "!=" => lhs != rhs,
            ">" => lhs > rhs,
            ">=" => lhs >= rhs,
            "<" => lhs < rhs,
            "<=" => lhs <= rhs,
            _ => false,
        }
    }

    pub fn evaluate_with_ids(
        &self,
        result: &HashMap<String, u32>,
        dictionary: &Dictionary,
    ) -> bool {
        self.evaluate_filter_with_ids(&self.expression, result, dictionary)
    }

    fn evaluate_filter_with_ids(
        &self,
        expression: &ConditionExpression,
        result: &HashMap<String, u32>,
        dictionary: &Dictionary,
    ) -> bool {
        match expression {
            ConditionExpression::Comparison(variable, operator, value) => {
                let Some(&id) = result.get(Self::normalize_variable(variable)) else {
                    return false;
                };
                if Self::is_variable(value) {
                    let Some(&rhs) = result.get(Self::normalize_variable(value)) else {
                        return false;
                    };
                    return match operator.as_str() {
                        "=" => id == rhs,
                        "!=" => id != rhs,
                        _ => {
                            let lhs = dictionary.decode(id).unwrap_or("");
                            let rhs = dictionary.decode(rhs).unwrap_or("");
                            Self::compare_lexical(lhs, operator, rhs)
                        }
                    };
                }

                let lhs = Self::normalize_lexical(dictionary.decode(id).unwrap_or(""));
                let rhs = Self::normalize_lexical(value);
                Self::compare_lexical(lhs, operator, rhs)
            }
            ConditionExpression::ArithmeticComparison(left, operator, right) => {
                let resolver = |variable: &str| {
                    let &id = result.get(Self::normalize_variable(variable))?;
                    dictionary.decode(id)?.parse::<f64>().ok()
                };
                let Ok(left) = Self::evaluate_arithmetic(left, &resolver) else {
                    return false;
                };
                let Ok(right) = Self::evaluate_arithmetic(right, &resolver) else {
                    return false;
                };
                Self::compare_numeric(left, operator, right)
            }
            ConditionExpression::And(left, right) => {
                self.evaluate_filter_with_ids(left, result, dictionary)
                    && self.evaluate_filter_with_ids(right, result, dictionary)
            }
            ConditionExpression::Or(left, right) => {
                self.evaluate_filter_with_ids(left, result, dictionary)
                    || self.evaluate_filter_with_ids(right, result, dictionary)
            }
            ConditionExpression::Not(inner) => {
                !self.evaluate_filter_with_ids(inner, result, dictionary)
            }
            ConditionExpression::ArithmeticExpr(expression) => {
                let resolver = |variable: &str| {
                    let &id = result.get(Self::normalize_variable(variable))?;
                    dictionary.decode(id)?.parse::<f64>().ok()
                };
                Self::evaluate_arithmetic(expression, &resolver)
                    .map(|value| value != 0.0)
                    .unwrap_or(false)
            }
            ConditionExpression::FunctionCall(name, arguments) => {
                use shared::quoted_triple_store::is_quoted_triple_id;
                if name != "isTRIPLE" {
                    return false;
                }
                let Some(argument) = arguments.first() else {
                    return false;
                };
                result
                    .get(Self::normalize_variable(argument))
                    .is_some_and(|id| is_quoted_triple_id(*id))
            }
        }
    }
}

impl IdResult {
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    pub fn with_bindings(bindings: HashMap<String, u32>) -> Self {
        Self { bindings }
    }

    pub fn insert(&mut self, variable: String, id: u32) {
        self.bindings.insert(variable, id);
    }

    pub fn get(&self, variable: &str) -> Option<&u32> {
        self.bindings.get(variable)
    }

    pub fn contains(&self, variable: &str) -> bool {
        self.bindings.contains_key(variable)
    }

    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

impl Default for IdResult {
    fn default() -> Self {
        Self::new()
    }
}
