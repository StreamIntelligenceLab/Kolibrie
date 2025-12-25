/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::operators::{LogicalOperator, PhysicalOperator};
use super::types::Condition;
use crate::sparql_database::SparqlDatabase;
use shared::query::{FilterExpression, SubQuery, ValuesClause};
use shared::terms::{Term, TriplePattern};
use std::collections::{HashMap, HashSet};

/// Extracts a triple pattern from a physical operator if it's a scan operation
pub fn extract_pattern(op: &PhysicalOperator) -> Option<&TriplePattern> {
    match op {
        PhysicalOperator::TableScan { pattern } | PhysicalOperator::IndexScan { pattern } => {
            Some(pattern)
        }
        // If it's a Filter, keep searching in its child
        PhysicalOperator::Filter { input, .. } => extract_pattern(input),
        _ => None,
    }
}

/// Checks if a pattern contains a specific variable
pub fn pattern_contains_variable(pattern: &TriplePattern, var: &str) -> bool {
    matches!(&pattern.0, Term::Variable(v) if v == var)
        || matches!(&pattern.1, Term::Variable(v) if v == var)
        || matches!(&pattern.2, Term::Variable(v) if v == var)
}

/// Estimates the selectivity of an operator for optimization purposes
pub fn estimate_operator_selectivity(op: &LogicalOperator, _database: &SparqlDatabase) -> u64 {
    match op {
        LogicalOperator::Scan { pattern } => {
            let bound_count = count_bound_terms(pattern);

            match bound_count {
                3 => 1, // Highest priority - fully bound
                2 => 2, // High priority - two bounds
                1 => 3, // Medium priority - one bound
                0 => 4, // Lowest priority - no bounds
                _ => 5,
            }
        }
        LogicalOperator::Selection { predicate, .. } => {
            // Selections are generally high priority due to filtering
            estimate_operator_selectivity(predicate, _database) + 10
        }
        LogicalOperator::Join { left, right } => {
            // Join cost depends on both sides
            let left_sel = estimate_operator_selectivity(left, _database);
            let right_sel = estimate_operator_selectivity(right, _database);
            left_sel + right_sel + 5
        }
        LogicalOperator::Projection { predicate, .. } => {
            // Projection doesn't change selectivity much
            estimate_operator_selectivity(predicate, _database) + 1
        }
        LogicalOperator::Subquery { inner, .. } => {
            estimate_operator_selectivity(inner, _database) + 15
        }
        LogicalOperator::Bind { input, .. } => {
            estimate_operator_selectivity(input, _database) + 2
        }
        LogicalOperator::Values { values, .. } => {
            values.len() as u64
        }
    }
}

/// Counts the number of bound terms (constants) in a triple pattern
fn count_bound_terms(pattern: &TriplePattern) -> usize {
    let mut count = 0;

    if matches!(&pattern.0, Term::Constant(_)) {
        count += 1;
    }
    if matches!(&pattern.1, Term::Constant(_)) {
        count += 1;
    }
    if matches!(&pattern.2, Term::Constant(_)) {
        count += 1;
    }

    count
}

/// Builds an optimized logical plan from query components
pub fn build_logical_plan(
    variables: Vec<(&str, &str)>,
    patterns: Vec<(&str, &str, &str)>,
    filters: Vec<FilterExpression>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
    binds: Vec<(&str, Vec<&str>, &str)>,
    values_clause: Option<&ValuesClause>,
) -> LogicalOperator {
    // Create base operator from VALUES if present, otherwise empty join base
    let mut result = if let Some(values_clause) = values_clause {
        // Convert ValuesClause to LogicalOperator::Values
        let variables: Vec<String> = values_clause
            .variables
            .iter()
            .map(|v| v.to_string())
            .collect();

        let values: Vec<Vec<Option<String>>> = values_clause
            .values
            .iter()
            .map(|row| {
                row.iter()
                .map(|value| match value {
                    shared::query::Value::Term(term) => Some(term.clone()),
                    shared::query::Value::Undef => None,
                })
                .collect()
            })
            .collect();

        LogicalOperator::values(variables, values)
    } else {
        // Start with first pattern as before
        let first_pattern = if patterns.is_empty() {
            // Empty query - return a minimal scan
            let pattern = (
                Term::Variable("?s".to_string()),
                Term::Variable("?p".to_string()),
                Term::Variable("?o".to_string()),
            );
            LogicalOperator::scan(pattern)
        } else {
            let (subject_str, predicate_str, object_str) = patterns[0];
            let pattern = convert_pattern_to_triple(
                subject_str,
                predicate_str,
                object_str,
                prefixes,
                database
            );
            LogicalOperator::scan(pattern)
        };
        first_pattern
    };

    // If we have VALUES, join it with all patterns
    // Otherwise, join patterns together as before
    let start_idx = if values_clause.is_some() { 0 } else { 1 };

    for (subject_str, predicate_str, object_str) in patterns.iter().skip(start_idx) {
        let pattern = convert_pattern_to_triple(
            subject_str,
            predicate_str,
            object_str,
            prefixes,
            database,
        );
        let scan_op = LogicalOperator::scan(pattern);
        result = LogicalOperator::join(result, scan_op);
    }

    // Apply filters that couldn't be pushed down
    for filter in filters {
        let condition = convert_filter_to_condition(&filter);
        result = LogicalOperator::selection(result, condition);
    }

    // Apply BIND clauses
    for (func_name, args, output_var) in binds {
        let function_name = func_name.to_string();
        let arguments: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let output_variable = output_var.to_string();

        result = LogicalOperator::bind(result, function_name, arguments, output_variable);
    }

    // Apply projection if specific variables were requested
    if !variables.is_empty() {
        let var_names: Vec<String> = variables.into_iter().map(|(_, v)| v.to_string()).collect();
        result = LogicalOperator::projection(result, var_names);
    }

    result
}

// Helper function to convert pattern strings to TriplePattern
fn convert_pattern_to_triple(
    subject_str: &str,
    predicate_str: &str,
    object_str: &str,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> TriplePattern {
    let subject = if subject_str.starts_with('?') {
        Term::Variable(subject_str.to_string())
    } else {
        let resolved = resolve_with_prefixes(subject_str, prefixes);
        Term::Constant(database.dictionary.encode(&resolved))
    };

    let predicate = if predicate_str.starts_with('?') {
        Term::Variable(predicate_str.to_string())
    } else {
        let resolved = resolve_with_prefixes(predicate_str, prefixes);
        Term::Constant(database.dictionary.encode(&resolved))
    };

    let object = if object_str.starts_with('?') {
        Term::Variable(object_str.to_string())
    } else {
        let resolved = resolve_with_prefixes(object_str, prefixes);
        Term::Constant(database.dictionary.encode(&resolved))
    };

    (subject, predicate, object)
}

/// Builds a logical operator from a SubQuery structure
pub fn build_logical_plan_from_subquery(
    subquery: &SubQuery,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> LogicalOperator {
    // Build the inner logical plan from the subquery patterns
    let variables:  Vec<(&str, &str)> = subquery
        .variables
        .iter()
        .map(|(var_type, var_name, _aggregation)| (*var_type, *var_name))
        .collect();
    
    let inner_plan = build_logical_plan(
        variables.clone(),
        subquery.patterns.clone(),
        subquery.filters.clone(),
        prefixes,
        database,
        subquery.binds.clone(),
        None,
    );
    
    // Extract variable names for projection
    let projected_vars: Vec<String> = variables
        .iter()
        .map(|(_, var_name)| var_name.to_string())
        .collect();
    
    // Wrap in a subquery operator
    LogicalOperator::subquery(inner_plan, projected_vars)
}

/// Resolves a URI with prefixes
fn resolve_with_prefixes(uri: &str, prefixes: &HashMap<String, String>) -> String {
    if let Some(colon_pos) = uri.find(':') {
        let (prefix, suffix) = uri.split_at(colon_pos);
        if let Some(base_uri) = prefixes.get(prefix) {
            format!("{}{}", base_uri, &suffix[1..]) // Skip the ':'
        } else {
            uri.to_string()
        }
    } else {
        uri.to_string()
    }
}

/// Converts a FilterExpression with any lifetime to 'static lifetime
fn make_filter_static(filter: &FilterExpression) -> FilterExpression<'static> {
    match filter {
        FilterExpression::Comparison(var, op, value) => {
            let var_static: &'static str = Box::leak(var.to_string().into_boxed_str());
            let op_static: &'static str = Box::leak(op.to_string().into_boxed_str());
            let val_static: &'static str = Box::leak(value.to_string().into_boxed_str());
            FilterExpression::Comparison(var_static, op_static, val_static)
        }
        FilterExpression::And(left, right) => {
            FilterExpression::And(
                Box::new(make_filter_static(left)),
                Box::new(make_filter_static(right)),
            )
        }
        FilterExpression::Or(left, right) => {
            FilterExpression::Or(
                Box::new(make_filter_static(left)),
                Box::new(make_filter_static(right)),
            )
        }
        FilterExpression::Not(inner) => {
            FilterExpression::Not(Box::new(make_filter_static(inner)))
        }
        FilterExpression::ArithmeticExpr(expr) => {
            let expr_static: &'static str = Box::leak(expr.to_string().into_boxed_str());
            FilterExpression::ArithmeticExpr(expr_static)
        }
    }
}

/// Checks if a filter can be pushed down to a specific pattern
fn can_push_filter_to_pattern(op: &LogicalOperator, filter: &FilterExpression) -> bool {
    // Don't push down complex filters (AND/OR/NOT) - apply them after joins
    if matches!(filter, FilterExpression::And(_,_) | FilterExpression::Or(_,_) | FilterExpression::Not(_)) {
        return false;
    }

    if let LogicalOperator::Scan { pattern } = op {
        // Extract variables from the filter
        let filter_vars = extract_filter_variables(filter);
        
        // Extract variables from the pattern
        let pattern_vars = extract_pattern_variables(pattern);
        
        // Filter can be pushed down if all its variables are in the pattern
        filter_vars.iter().all(|fv| pattern_vars.contains(fv))
    } else {
        false
    }
}

/// Extracts all variables from a filter expression
fn extract_filter_variables(filter: &FilterExpression) -> HashSet<String> {
    let mut vars = HashSet::new();
    
    match filter {
        FilterExpression::Comparison(var, _, _) => {
            let var_name = var.strip_prefix('?').unwrap_or(var).to_string();
            vars.insert(var_name);
        }
        FilterExpression::And(left, right) | FilterExpression::Or(left, right) => {
            vars.extend(extract_filter_variables(left));
            vars.extend(extract_filter_variables(right));
        }
        FilterExpression::Not(inner) => {
            vars.extend(extract_filter_variables(inner));
        }
        FilterExpression::ArithmeticExpr(_) => {
            // TODO: Parse arithmetic expressions to extract variables
        }
    }
    
    vars
}

/// Extracts all variables from a triple pattern
fn extract_pattern_variables(pattern: &TriplePattern) -> HashSet<String> {
    let mut vars = HashSet::new();
    
    if let Term::Variable(v) = &pattern.0 {
        vars.insert(v.strip_prefix('?').unwrap_or(v).to_string());
    }
    if let Term::Variable(v) = &pattern.1 {
        vars.insert(v.strip_prefix('?').unwrap_or(v).to_string());
    }
    if let Term::Variable(v) = &pattern.2 {
        vars.insert(v.strip_prefix('?').unwrap_or(v).to_string());
    }
    
    vars
}

/// Converts a FilterExpression to a Condition
fn convert_filter_to_condition(filter: &FilterExpression) -> Condition {
    // Convert the filter to have 'static lifetime by leaking strings
    let static_filter = make_filter_static(filter);
    Condition::from_filter(static_filter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::terms::Term;

    #[test]
    fn test_pattern_contains_variable() {
        let pattern = (
            Term::Variable("s".to_string()),
            Term::Constant(1),
            Term::Variable("o".to_string()),
        );

        assert!(pattern_contains_variable(&pattern, "s"));
        assert!(pattern_contains_variable(&pattern, "o"));
        assert!(!pattern_contains_variable(&pattern, "p"));
    }

    #[test]
    fn test_count_bound_terms() {
        let pattern1 = (
            Term::Variable("s".to_string()),
            Term::Variable("p".to_string()),
            Term::Variable("o".to_string()),
        );
        assert_eq!(count_bound_terms(&pattern1), 0);

        let pattern2 = (
            Term::Constant(1),
            Term::Variable("p".to_string()),
            Term::Constant(2),
        );
        assert_eq!(count_bound_terms(&pattern2), 2);

        let pattern3 = (Term::Constant(1), Term::Constant(2), Term::Constant(3));
        assert_eq!(count_bound_terms(&pattern3), 3);
    }

    #[test]
    fn test_resolve_with_prefixes() {
        let mut prefixes = HashMap::new();
        prefixes.insert("ex".to_string(), "http://example.org/".to_string());

        let resolved = resolve_with_prefixes("ex:test", &prefixes);
        assert_eq!(resolved, "http://example.org/test");

        let unresolved = resolve_with_prefixes("http://other.org/test", &prefixes);
        assert_eq!(unresolved, "http://other.org/test");
    }
}
