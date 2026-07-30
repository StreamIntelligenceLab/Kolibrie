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
use super::types::{Condition, SubqueryProjection, SubquerySpec};
use crate::sparql_database::SparqlDatabase;
use shared::dataset_index::{GraphTerm, QuadPattern};
use shared::query::{FilterExpression, GroupGraphPattern, SubQuery, ValuesClause};
use shared::terms::{Term, TriplePattern};
use std::collections::HashMap;

/// Extracts a quad pattern from a physical operator if it's a scan operation.
pub fn extract_pattern(op: &PhysicalOperator) -> Option<&QuadPattern> {
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
        LogicalOperator::Unit => 0,
        LogicalOperator::Scan { pattern } => {
            let triple = (
                pattern.subject.clone(),
                pattern.predicate.clone(),
                pattern.object.clone(),
            );
            let bound_count = count_bound_terms(&triple);

            match bound_count {
                3 => 1, // Highest priority - fully bound
                2 => 2, // High priority - two bounds
                1 => 3, // Medium priority - one bound
                0 => 4, // Lowest priority - no bounds
                _ => 5,
            }
        }
        LogicalOperator::Union { branches } => branches
            .iter()
            .map(|branch| estimate_operator_selectivity(branch, _database))
            .sum(),
        LogicalOperator::Graph { input, .. } => estimate_operator_selectivity(input, _database),
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
        LogicalOperator::Buffer { .. } => 10000,
        LogicalOperator::Subquery { inner, .. } => {
            estimate_operator_selectivity(inner, _database) + 15
        }
        LogicalOperator::Bind { input, .. } => estimate_operator_selectivity(input, _database) + 2,
        LogicalOperator::Values { values, .. } => values.len() as u64,
        LogicalOperator::MLPredict {
            input,
            input_variables,
            ..
        } => {
            let base_selectivity = estimate_operator_selectivity(input, _database);
            let ml_overhead = 50 + (input_variables.len() as u64 * 10);
            base_selectivity + ml_overhead
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
    binds: &[(&str, Vec<&str>, &str)],
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

        let values = compile_values_rows(values_clause, prefixes, database).unwrap_or_default();

        LogicalOperator::values(variables, values)
    } else {
        // The empty group pattern is the SPARQL unit table.
        let first_pattern = if patterns.is_empty() {
            LogicalOperator::unit()
        } else {
            let (subject_str, predicate_str, object_str) = patterns[0];
            let pattern = convert_pattern_to_triple(
                subject_str,
                predicate_str,
                object_str,
                prefixes,
                database,
            );
            LogicalOperator::scan(pattern)
        };
        first_pattern
    };

    // If we have VALUES, join it with all patterns
    // Otherwise, join patterns together as before
    let start_idx = if values_clause.is_some() { 0 } else { 1 };

    for (subject_str, predicate_str, object_str) in patterns.iter().skip(start_idx) {
        let pattern =
            convert_pattern_to_triple(subject_str, predicate_str, object_str, prefixes, database);
        let scan_op = LogicalOperator::scan(pattern);
        result = LogicalOperator::join(result, scan_op);
    }

    // Apply filters that couldn't be pushed down
    for filter in filters {
        let condition = convert_filter_to_condition(&filter, prefixes, database);
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

/// Compiles one lexical SPARQL term into Kolibrie's existing execution term.
///
/// This is the single lowering entry point used by ordinary triple patterns,
/// GRAPH patterns and update templates. It preserves the historical prefix,
/// dictionary and RDF-star behavior without introducing a parallel term AST.
pub fn compile_term(
    term_str: &str,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Term {
    let trimmed = term_str.trim();
    if trimmed.starts_with("<<") && trimmed.ends_with(">>") {
        let inner = trimmed[2..trimmed.len() - 2].trim();
        let (s_str, p_str, o_str) = SparqlDatabase::split_quoted_triple_content(inner);
        let s_term = compile_term(&s_str, prefixes, database);
        let p_term = if p_str.trim() == "a" {
            compile_term(
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                prefixes,
                database,
            )
        } else {
            compile_term(&p_str, prefixes, database)
        };
        let o_term = compile_term(&o_str, prefixes, database);

        // If all components are constants, resolve to a single Constant ID
        if let (Term::Constant(s_id), Term::Constant(p_id), Term::Constant(o_id)) =
            (&s_term, &p_term, &o_term)
        {
            let mut qt = database.quoted_triple_store.write().unwrap();
            let qt_id = qt.encode(*s_id, *p_id, *o_id);
            Term::Constant(qt_id)
        } else {
            // Contains variables — use QuotedTriple variant for pattern matching
            Term::QuotedTriple(Box::new((s_term, p_term, o_term)))
        }
    } else if trimmed.starts_with('?') || trimmed.starts_with('$') {
        Term::Variable(trimmed.to_string())
    } else {
        let resolved = resolve_sparql_lexical_value(trimmed, prefixes, database);
        let mut dict = database.dictionary.write().unwrap();
        Term::Constant(dict.encode(&resolved))
    }
}

fn resolve_sparql_lexical_value(
    value: &str,
    prefixes: &HashMap<String, String>,
    database: &SparqlDatabase,
) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with('<') && trimmed.ends_with('>') && !trimmed.starts_with("<<") {
        unescape_sparql_iri(&trimmed[1..trimmed.len() - 1])
    } else if trimmed.starts_with(['"', '\'']) {
        // Literal lexical values are never prefix names, even when their
        // content contains a colon.
        literal_lexical_value(trimmed)
    } else if trimmed.starts_with("_:") {
        trimmed.to_string()
    } else {
        let expanded = database.resolve_query_term(trimmed, prefixes);
        // PN_LOCAL_ESC contributes the escaped character itself to the
        // expanded IRI. Percent escapes intentionally remain percent encoded.
        unescape_sparql_iri(&expanded)
    }
}

fn unescape_sparql_iri(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut index = 0usize;
    while index < value.len() {
        let tail = &value[index..];
        let character = tail.chars().next().expect("valid UTF-8 boundary");
        if character != '\\' {
            result.push(character);
            index += character.len_utf8();
            continue;
        }

        let escape = &tail[1..];
        let Some(escaped) = escape.chars().next() else {
            result.push('\\');
            break;
        };
        if matches!(escaped, 'u' | 'U') {
            let digits = if escaped == 'u' { 4 } else { 8 };
            let hexadecimal = &escape[escaped.len_utf8()..];
            if hexadecimal.len() >= digits {
                if let Ok(codepoint) = u32::from_str_radix(&hexadecimal[..digits], 16) {
                    if let Some(decoded) = char::from_u32(codepoint) {
                        result.push(decoded);
                        index += 1 + escaped.len_utf8() + digits;
                        continue;
                    }
                }
            }
        }

        result.push(escaped);
        index += 1 + escaped.len_utf8();
    }
    result
}

/// Returns the lexical content of a quoted literal, honoring escaped quote and
/// backslash boundaries. Language/datatype suffixes retain Kolibrie's
/// historical string-dictionary behavior and are not part of the stored value.
fn literal_lexical_value(literal: &str) -> String {
    let Some(quote) = literal
        .chars()
        .next()
        .filter(|quote| matches!(quote, '"' | '\''))
    else {
        return literal.to_string();
    };
    let delimiter_len = if literal.starts_with(&quote.to_string().repeat(3)) {
        3
    } else {
        1
    };
    let delimiter = quote.to_string().repeat(delimiter_len);
    let mut value = String::new();
    let mut index = delimiter_len;
    while index < literal.len() {
        let tail = &literal[index..];
        if tail.starts_with(&delimiter) {
            break;
        }
        let character = tail.chars().next().expect("valid UTF-8 boundary");
        match character {
            '\\' => {
                let escape = &tail[1..];
                let Some(escaped) = escape.chars().next() else {
                    value.push('\\');
                    break;
                };
                match escaped {
                    't' => value.push('\t'),
                    'b' => value.push('\u{8}'),
                    'n' => value.push('\n'),
                    'r' => value.push('\r'),
                    'f' => value.push('\u{C}'),
                    '"' => value.push('"'),
                    '\'' => value.push('\''),
                    '\\' => value.push('\\'),
                    'u' | 'U' => {
                        let digits = if escaped == 'u' { 4 } else { 8 };
                        let hexadecimal = &escape[escaped.len_utf8()..];
                        if hexadecimal.len() >= digits {
                            if let Ok(codepoint) = u32::from_str_radix(&hexadecimal[..digits], 16) {
                                if let Some(decoded) = char::from_u32(codepoint) {
                                    value.push(decoded);
                                    index += 1 + escaped.len_utf8() + digits;
                                    continue;
                                }
                            }
                        }
                        value.push('\\');
                        value.push(escaped);
                    }
                    other => {
                        value.push('\\');
                        value.push(other);
                    }
                }
                index += 1 + escaped.len_utf8();
            }
            other => {
                value.push(other);
                index += other.len_utf8();
            }
        }
    }
    value
}

/// Compiles a lexical triple through the same lowering path as SELECT and
/// update execution.
pub fn compile_triple(
    pattern: (&str, &str, &str),
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> TriplePattern {
    (
        compile_term(pattern.0, prefixes, database),
        if pattern.1.trim() == "a" {
            compile_term(
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                prefixes,
                database,
            )
        } else {
            compile_term(pattern.1, prefixes, database)
        },
        compile_term(pattern.2, prefixes, database),
    )
}

/// Compiles a graph selector using the same prefix and dictionary lowering as
/// triple terms.
pub fn compile_graph_term(
    graph: &str,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<GraphTerm, String> {
    match compile_term(graph, prefixes, database) {
        Term::Variable(variable) => Ok(GraphTerm::Variable(variable)),
        Term::Constant(graph) => Ok(GraphTerm::Named(graph)),
        Term::QuotedTriple(_) => Err("a GRAPH name must be an IRI or variable".to_string()),
    }
}

/// Lowers the unified recursive graph-pattern AST into the existing logical
/// optimizer algebra.
pub fn build_logical_plan_from_group(
    pattern: &GroupGraphPattern<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<LogicalOperator, String> {
    build_logical_plan_from_group_in_scope(pattern, prefixes, database, &GraphTerm::Default)
}

/// Lowers a graph pattern while retaining the graph scope on every scan.
///
/// The enclosing `Graph` operator is still required for unit/empty patterns
/// and to bind a graph variable before FILTER, BIND, VALUES, and subqueries.
/// Carrying the scope on scans additionally lets the optimizer estimate and
/// group scans without accidentally treating named-graph data as default data.
fn build_logical_plan_from_group_in_scope(
    pattern: &GroupGraphPattern<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
    graph_scope: &GraphTerm,
) -> Result<LogicalOperator, String> {
    match pattern {
        GroupGraphPattern::Unit => Ok(LogicalOperator::unit()),
        GroupGraphPattern::Bgp(patterns) => {
            let mut plan = LogicalOperator::unit();
            for pattern in patterns {
                let triple = compile_triple(*pattern, prefixes, database);
                let scan = LogicalOperator::quad_scan(QuadPattern {
                    subject: triple.0,
                    predicate: triple.1,
                    object: triple.2,
                    graph: graph_scope.clone(),
                });
                plan = append_join(plan, scan);
            }
            Ok(plan)
        }
        GroupGraphPattern::Join(patterns) => {
            let mut plan = LogicalOperator::unit();
            let mut filters = Vec::new();
            for pattern in patterns {
                match pattern {
                    GroupGraphPattern::Filter(filter) => {
                        // SPARQL FILTER scope is the containing group graph
                        // pattern, not the portion of the group that precedes
                        // the FILTER lexically. Defer direct filters until the
                        // rest of this group has been lowered so they can see
                        // variables introduced by later triples and BINDs.
                        //
                        // Recursive groups collect their own filters, keeping
                        // nested GRAPH and UNION branch scopes intact.
                        filters.push(filter);
                    }
                    GroupGraphPattern::Bind((function, arguments, output)) => {
                        plan = LogicalOperator::bind(
                            plan,
                            (*function).to_string(),
                            arguments
                                .iter()
                                .map(|argument| (*argument).to_string())
                                .collect(),
                            (*output).to_string(),
                        );
                    }
                    _ => {
                        let next = build_logical_plan_from_group_in_scope(
                            pattern,
                            prefixes,
                            database,
                            graph_scope,
                        )?;
                        plan = append_join(plan, next);
                    }
                }
            }
            for filter in filters {
                plan = LogicalOperator::selection(
                    plan,
                    convert_filter_to_condition(filter, prefixes, database),
                );
            }
            Ok(plan)
        }
        GroupGraphPattern::Union(branches) => {
            let branches = branches
                .iter()
                .map(|branch| {
                    build_logical_plan_from_group_in_scope(branch, prefixes, database, graph_scope)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalOperator::union(branches))
        }
        GroupGraphPattern::Graph { name, pattern } => {
            let graph = compile_graph_term(name, prefixes, database)?;
            let input =
                build_logical_plan_from_group_in_scope(pattern, prefixes, database, &graph)?;
            Ok(LogicalOperator::graph(input, graph))
        }
        GroupGraphPattern::Filter(filter) => Ok(LogicalOperator::selection(
            LogicalOperator::unit(),
            convert_filter_to_condition(filter, prefixes, database),
        )),
        GroupGraphPattern::Bind((function, arguments, output)) => Ok(LogicalOperator::bind(
            LogicalOperator::unit(),
            (*function).to_string(),
            arguments
                .iter()
                .map(|argument| (*argument).to_string())
                .collect(),
            (*output).to_string(),
        )),
        GroupGraphPattern::Values(values) => values_operator(values, prefixes, database),
        GroupGraphPattern::SubQuery(subquery) => {
            build_logical_plan_from_subquery_in_scope(subquery, prefixes, database, graph_scope)
        }
    }
}

fn append_join(left: LogicalOperator, right: LogicalOperator) -> LogicalOperator {
    if matches!(left, LogicalOperator::Unit) {
        right
    } else if matches!(right, LogicalOperator::Unit) {
        left
    } else {
        LogicalOperator::join(left, right)
    }
}

fn values_operator(
    values_clause: &ValuesClause<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<LogicalOperator, String> {
    let variables = values_clause
        .variables
        .iter()
        .map(|variable| (*variable).to_string())
        .collect();
    let values = compile_values_rows(values_clause, prefixes, database)?;
    Ok(LogicalOperator::values(variables, values))
}

fn compile_values_rows(
    values_clause: &ValuesClause<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<Vec<Vec<Option<u32>>>, String> {
    let mut rows = Vec::with_capacity(values_clause.values.len());
    for row in &values_clause.values {
        let mut compiled = Vec::with_capacity(row.len());
        for value in row {
            match value {
                shared::query::Value::Undef => compiled.push(None),
                shared::query::Value::Term(term) => match compile_term(term, prefixes, database) {
                    Term::Constant(id) => compiled.push(Some(id)),
                    Term::Variable(_) | Term::QuotedTriple(_) => {
                        return Err(format!("VALUES term must be a constant RDF term: {term}"));
                    }
                },
            }
        }
        rows.push(compiled);
    }
    Ok(rows)
}

// Compatibility helper for existing callers in this module.
fn convert_pattern_to_triple(
    subject_str: &str,
    predicate_str: &str,
    object_str: &str,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> TriplePattern {
    compile_triple((subject_str, predicate_str, object_str), prefixes, database)
}

/// Builds a logical operator from a SubQuery structure
pub fn build_logical_plan_from_subquery(
    subquery: &SubQuery,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<LogicalOperator, String> {
    build_logical_plan_from_subquery_in_scope(subquery, prefixes, database, &GraphTerm::Default)
}

fn build_logical_plan_from_subquery_in_scope(
    subquery: &SubQuery,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
    graph_scope: &GraphTerm,
) -> Result<LogicalOperator, String> {
    let inner_plan = build_logical_plan_from_group_in_scope(
        &subquery.query.pattern,
        prefixes,
        database,
        graph_scope,
    )?;
    let projection = if subquery.query.variables == vec![("*", "*", None)] {
        None
    } else {
        Some(
            subquery
                .query
                .variables
                .iter()
                .map(|(kind, variable, alias)| SubqueryProjection {
                    kind: (*kind).to_string(),
                    variable: (*variable).to_string(),
                    alias: alias.map(str::to_string),
                })
                .collect(),
        )
    };
    let spec = SubquerySpec {
        projection,
        distinct: subquery.query.distinct,
        group_vars: subquery
            .query
            .group_vars
            .iter()
            .map(|variable| (*variable).to_string())
            .collect(),
        order_conditions: subquery
            .query
            .order_conditions
            .iter()
            .map(|condition| (condition.variable.to_string(), condition.direction.clone()))
            .collect(),
        limit: subquery.query.limit,
    };

    Ok(LogicalOperator::subquery(inner_plan, spec))
}

/// Resolves a URI with prefixes
#[cfg(test)]
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

/// Converts a FilterExpression to a Condition
fn convert_filter_to_condition(
    filter: &FilterExpression,
    prefixes: &HashMap<String, String>,
    database: &SparqlDatabase,
) -> Condition {
    Condition::from_filter_with_resolver(filter, |value| {
        resolve_sparql_lexical_value(value, prefixes, database)
    })
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

    #[test]
    fn graph_scope_is_lowered_onto_child_scans() {
        let mut database = SparqlDatabase::new();
        let prefixes = HashMap::new();
        let pattern = GroupGraphPattern::Graph {
            name: "?g",
            pattern: Box::new(GroupGraphPattern::Bgp(vec![("?s", "?p", "?o")])),
        };

        let plan = build_logical_plan_from_group(&pattern, &prefixes, &mut database).unwrap();

        let LogicalOperator::Graph { input, graph } = plan else {
            panic!("expected GRAPH logical operator");
        };
        assert_eq!(graph, GraphTerm::Variable("?g".to_string()));
        let LogicalOperator::Scan { pattern } = *input else {
            panic!("expected graph-scoped scan");
        };
        assert_eq!(
            pattern.graph,
            GraphTerm::Variable("?g".to_string()),
            "the optimizer must see the same graph scope as execution"
        );
    }

    #[test]
    fn nested_graph_scope_overrides_the_outer_scan_scope() {
        let mut database = SparqlDatabase::new();
        let prefixes = HashMap::new();
        let pattern = GroupGraphPattern::Graph {
            name: "?outer",
            pattern: Box::new(GroupGraphPattern::Graph {
                name: "<http://example.com/inner>",
                pattern: Box::new(GroupGraphPattern::Bgp(vec![("?s", "?p", "?o")])),
            }),
        };

        let plan = build_logical_plan_from_group(&pattern, &prefixes, &mut database).unwrap();

        let LogicalOperator::Graph {
            input: outer_input, ..
        } = plan
        else {
            panic!("expected outer GRAPH");
        };
        let LogicalOperator::Graph {
            input: inner_input,
            graph: GraphTerm::Named(inner_graph),
        } = *outer_input
        else {
            panic!("expected nested fixed GRAPH");
        };
        let LogicalOperator::Scan { pattern } = *inner_input else {
            panic!("expected nested graph scan");
        };
        assert_eq!(pattern.graph, GraphTerm::Named(inner_graph));
    }
}
