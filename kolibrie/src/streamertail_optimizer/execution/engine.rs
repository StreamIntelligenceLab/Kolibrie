/*
 * Copyright Â© 2024 Volodymyr Kadzhaia
 * Copyright Â© 2024 Pieter Bonte
 * KU Leuven â€” Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::super::operators::PhysicalOperator;
use super::exec_stats::exec_count;
use super::super::types::{SubqueryProjection, SubquerySpec};

use crate::sparql_database::SparqlDatabase;
use ml::MLPredictionResult;
use rayon::prelude::*;

use shared::dataset_index::{GraphId, GraphTerm, QuadPattern};
use shared::query::SortDirection;
use shared::quoted_triple_store::is_quoted_triple_id;
use shared::terms::{Bindings, Term};

use std::collections::{BTreeMap, HashMap, HashSet};

/// Below this many rows a bind join runs on one thread, since splitting costs more in plan re-entry than it saves
const BIND_JOIN_MIN_CHUNK: usize = 64;

/// The RDF dataset visible to one query execution.
///
/// `default_graphs` contains the physical graphs whose triples form the query
/// default graph. More than one graph implements SPARQL's merged `FROM`
/// default; duplicate triples are removed while scanning it. `named_graphs`
/// is the complete set visible to `GRAPH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetView {
    pub default_graphs: Vec<GraphId>,
    pub named_graphs: HashSet<GraphId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streamertail_optimizer::Condition;
    use shared::dataset_index::Quad;

    fn encode(database: &SparqlDatabase, value: &str) -> u32 {
        database.dictionary.write().unwrap().encode(value)
    }

    fn row(pairs: &[(&str, u32)]) -> HashMap<String, u32> {
        pairs
            .iter()
            .map(|(variable, value)| ((*variable).to_string(), *value))
            .collect()
    }

    fn sorted(bindings: Bindings) -> Vec<Vec<(String, u32)>> {
        let mut rows: Vec<Vec<(String, u32)>> = bindings
            .into_iter()
            .map(|row| {
                let mut pairs: Vec<(String, u32)> = row.into_iter().collect();
                pairs.sort();
                pairs
            })
            .collect();
        rows.sort();
        rows
    }

    #[test]
    fn hash_join_agrees_with_the_nested_loop_on_every_row_shape() {
        let cases: Vec<(Bindings, Bindings)> = vec![
            // Shared variable, several matches and one dangling value
            (
                vec![row(&[("x", 1), ("a", 10)]), row(&[("x", 2), ("a", 20)])],
                vec![
                    row(&[("x", 1), ("b", 11)]),
                    row(&[("x", 1), ("b", 12)]),
                    row(&[("x", 3), ("b", 13)]),
                ],
            ),
            // Right side leaves the key variable unbound
            (
                vec![row(&[("x", 1), ("a", 10)])],
                vec![row(&[("b", 11)]), row(&[("x", 1), ("b", 12)])],
            ),
            // Left side leaves the key variable unbound
            (
                vec![row(&[("a", 10)]), row(&[("x", 1), ("a", 11)])],
                vec![row(&[("x", 1), ("b", 12)]), row(&[("x", 2), ("b", 13)])],
            ),
            // Conflicting values on the shared variable
            (
                vec![row(&[("x", 1)])],
                vec![row(&[("x", 2)])],
            ),
            // No shared variable at all: a Cartesian product
            (
                vec![row(&[("a", 1)]), row(&[("a", 2)])],
                vec![row(&[("b", 3)]), row(&[("b", 4)])],
            ),
        ];

        for (index, (left, right)) in cases.into_iter().enumerate() {
            let expected =
                sorted(ExecutionEngine::join_solution_sequences(left.clone(), right.clone()));
            let actual = sorted(ExecutionEngine::hash_join_solution_sequences(left, right));
            assert_eq!(actual, expected, "case {} disagreed with the nested loop", index);
        }
    }

    #[test]
    fn unit_and_union_preserve_solution_multiplicity() {
        let mut database = SparqlDatabase::new();
        let plan =
            PhysicalOperator::union(vec![PhysicalOperator::unit(), PhysicalOperator::unit()]);

        let results = ExecutionEngine::execute_with_ids(&plan, &mut database);
        assert_eq!(results, vec![HashMap::new(), HashMap::new()]);
    }

    #[test]
    fn graph_variable_binds_before_filter_and_includes_empty_graphs() {
        let mut database = SparqlDatabase::new();
        let first = encode(&database, "http://example.com/first");
        let second = encode(&database, "http://example.com/second");
        database.dataset_index.create_graph(GraphId::Named(first));
        database.dataset_index.create_graph(GraphId::Named(second));

        let plan = PhysicalOperator::graph(
            PhysicalOperator::filter(
                PhysicalOperator::unit(),
                Condition::new("$g".to_string(), "=".to_string(), "?expected".to_string()),
            ),
            GraphTerm::Variable("$g".to_string()),
        );
        let context = ExecutionContext::new(DatasetView::from_database(&database));
        let mut input = HashMap::new();
        input.insert("expected".to_string(), second);

        let results = ExecutionEngine::execute_with_ids_and_input(
            &plan,
            &mut database,
            &context,
            vec![input],
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("g"), Some(&second));
    }

    #[test]
    fn graph_scan_checks_repeated_variables_against_incoming_bindings() {
        let mut database = SparqlDatabase::new();
        let graph = encode(&database, "http://example.com/graph");
        let subject = encode(&database, "http://example.com/subject");
        let predicate = encode(&database, "http://example.com/predicate");
        let other = encode(&database, "http://example.com/other");
        database.add_quad(Quad {
            subject,
            predicate,
            object: subject,
            graph: GraphId::Named(graph),
        });
        database.add_quad(Quad {
            subject,
            predicate,
            object: other,
            graph: GraphId::Named(graph),
        });

        let scan = PhysicalOperator::quad_index_scan(QuadPattern {
            subject: Term::Variable("?value".to_string()),
            predicate: Term::Constant(predicate),
            object: Term::Variable("$value".to_string()),
            graph: GraphTerm::Default,
        });
        let plan = PhysicalOperator::graph(scan, GraphTerm::Named(graph));

        let results = ExecutionEngine::execute_with_ids(&plan, &mut database);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("value"), Some(&subject));
    }

    #[test]
    fn scan_matches_a_variable_repeated_within_one_pattern() {
        let mut database = SparqlDatabase::new();
        let predicate = encode(&database, "http://example.com/self");
        let looping = encode(&database, "http://example.com/looping");
        let other = encode(&database, "http://example.com/other");
        database.add_quad(Quad {
            subject: looping,
            predicate,
            object: looping,
            graph: GraphId::Default,
        });
        database.add_quad(Quad {
            subject: looping,
            predicate,
            object: other,
            graph: GraphId::Default,
        });

        let plan = PhysicalOperator::quad_index_scan(QuadPattern {
            subject: Term::Variable("?x".to_string()),
            predicate: Term::Constant(predicate),
            object: Term::Variable("?x".to_string()),
            graph: GraphTerm::Default,
        });

        let results = ExecutionEngine::execute_with_ids(&plan, &mut database);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("x"), Some(&looping));
    }

    #[test]
    fn merged_default_suppresses_duplicate_triples() {
        let mut database = SparqlDatabase::new();
        let graph = encode(&database, "http://example.com/graph");
        let subject = encode(&database, "http://example.com/subject");
        let predicate = encode(&database, "http://example.com/predicate");
        let object = encode(&database, "value");
        database.add_quad(Quad {
            subject,
            predicate,
            object,
            graph: GraphId::Default,
        });
        database.add_quad(Quad {
            subject,
            predicate,
            object,
            graph: GraphId::Named(graph),
        });

        let plan = PhysicalOperator::table_scan((
            Term::Variable("?s".to_string()),
            Term::Variable("?p".to_string()),
            Term::Variable("?o".to_string()),
        ));
        let dataset = DatasetView::new(
            [GraphId::Default, GraphId::Named(graph)],
            std::iter::empty(),
        );

        let results = ExecutionEngine::execute_with_ids_and_dataset(&plan, &mut database, &dataset);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn values_keeps_all_undef_and_heterogeneous_union_rows() {
        let mut database = SparqlDatabase::new();
        let value = encode(&database, "value");
        let plan = PhysicalOperator::union(vec![
            PhysicalOperator::values(vec!["?x".to_string()], vec![vec![Some(value)]]),
            PhysicalOperator::values(vec!["?y".to_string()], vec![vec![None]]),
        ]);

        let results = ExecutionEngine::execute_with_ids(&plan, &mut database);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("x"), Some(&value));
        assert!(results[1].is_empty());
    }
}

impl DatasetView {
    /// The normal database dataset: the physical default graph plus every
    /// catalogued named graph.
    pub fn from_database(database: &SparqlDatabase) -> Self {
        Self {
            default_graphs: vec![GraphId::Default],
            named_graphs: database.dataset_index.named_graphs().into_iter().collect(),
        }
    }

    /// Creates an explicit SPARQL dataset view.
    pub fn new(
        default_graphs: impl IntoIterator<Item = GraphId>,
        named_graphs: impl IntoIterator<Item = GraphId>,
    ) -> Self {
        let mut seen_defaults = HashSet::new();
        let default_graphs = default_graphs
            .into_iter()
            .filter(|graph| seen_defaults.insert(*graph))
            .collect();
        let named_graphs = named_graphs
            .into_iter()
            .filter(|graph| matches!(graph, GraphId::Named(_)))
            .collect();
        Self {
            default_graphs,
            named_graphs,
        }
    }

    /// Creates a dataset with an empty query default and explicit named
    /// visibility, as required by `FROM NAMED` without `FROM`.
    pub fn empty_default(named_graphs: impl IntoIterator<Item = GraphId>) -> Self {
        Self::new(std::iter::empty(), named_graphs)
    }

    pub fn is_named_visible(&self, graph: GraphId) -> bool {
        matches!(graph, GraphId::Named(_)) && self.named_graphs.contains(&graph)
    }
}

/// Recursive execution state. An active graph is established by a `GRAPH`
/// operator and overrides only default-scoped scans in its child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionContext {
    pub dataset: DatasetView,
    pub active_graph: Option<GraphId>,
}

impl ExecutionContext {
    pub fn new(dataset: DatasetView) -> Self {
        Self {
            dataset,
            active_graph: None,
        }
    }

    fn with_active_graph(&self, graph: GraphId) -> Self {
        Self {
            dataset: self.dataset.clone(),
            active_graph: Some(graph),
        }
    }
}

/// Execution engine for physical operators
pub struct ExecutionEngine;

impl ExecutionEngine {
    /// Executes a physical operator and returns string results
    pub fn execute(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<HashMap<String, String>> {
        let dataset = DatasetView::from_database(database);
        Self::execute_with_dataset(operator, database, &dataset)
    }

    /// Executes a plan against an explicit SPARQL dataset and decodes the
    /// final solution sequence through the database dictionary.
    pub fn execute_with_dataset(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
        dataset: &DatasetView,
    ) -> Vec<HashMap<String, String>> {
        let id_results = Self::execute_with_ids_and_dataset(operator, database, dataset);

        // Convert ID results to string results only at the final step
        id_results
            .into_par_iter()
            .map(|id_result| {
                let dict = database.dictionary.read().unwrap();
                let qt_store = database.quoted_triple_store.read().unwrap();
                let result: HashMap<String, String> = id_result
                    .into_iter()
                    .map(|(var, id)| {
                        let decoded = dict
                            .decode_term(id, &qt_store)
                            .unwrap_or_else(|| "unknown".to_string());
                        (var, decoded)
                    })
                    .collect();
                drop(dict);
                result
            })
            .collect()
    }

    /// Executes a physical operator and returns ID-based results for performance
    pub fn execute_with_ids(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<HashMap<String, u32>> {
        let dataset = DatasetView::from_database(database);
        Self::execute_with_ids_and_dataset(operator, database, &dataset)
    }

    /// Executes a plan against an explicit SPARQL dataset.
    pub fn execute_with_ids_and_dataset(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
        dataset: &DatasetView,
    ) -> Bindings {
        let context = ExecutionContext::new(dataset.clone());
        Self::execute_with_ids_and_context(operator, database, &context)
    }

    /// Executes a plan with an explicit recursive execution context.
    pub fn execute_with_ids_and_context(
        operator: &PhysicalOperator,
        database: &SparqlDatabase,
        context: &ExecutionContext,
    ) -> Bindings {
        Self::execute_with_ids_and_input(operator, database, context, vec![HashMap::new()])
    }

    /// Executes a plan using the supplied solution sequence as input.
    ///
    /// This is the common execution contract for joins, GRAPH, FILTER, BIND,
    /// VALUES and update WHERE evaluation.
    pub fn execute_with_ids_and_input(
        operator: &PhysicalOperator,
        database: &SparqlDatabase,
        context: &ExecutionContext,
        incoming: Bindings,
    ) -> Bindings {
        if incoming.is_empty() {
            return Vec::new();
        }

        match operator {
            PhysicalOperator::Unit => incoming,
            PhysicalOperator::TableScan { pattern } => {
                Self::execute_quad_scan_with_ids(database, pattern, context, incoming)
            }
            PhysicalOperator::IndexScan { pattern } => {
                Self::execute_quad_scan_with_ids(database, pattern, context, incoming)
            }
            PhysicalOperator::Union { branches } => {
                let mut results = Vec::new();
                for branch in branches {
                    results.extend(Self::execute_with_ids_and_input(
                        branch,
                        database,
                        context,
                        incoming.clone(),
                    ));
                }
                results
            }
            PhysicalOperator::Graph { input, graph } => {
                Self::execute_graph_with_ids(database, input, graph, context, incoming)
            }
            PhysicalOperator::Filter { input, condition } => {
                let input_results =
                    Self::execute_with_ids_and_input(input, database, context, incoming);
                // Taken per row on purpose: holding a read guard across the parallel section can deadlock a work-stolen writer
                input_results
                    .into_par_iter()
                    .filter(|result| {
                        let dict = database.dictionary.read().unwrap();
                        let result = condition.evaluate_with_ids(result, &*dict);
                        drop(dict);
                        result
                    })
                    .collect()
            }
            PhysicalOperator::Projection { input, variables } => {
                let input_results =
                    Self::execute_with_ids_and_input(input, database, context, incoming);

                // Strip '?' prefix from projection variables for matching
                let stripped_vars: Vec<String> = variables
                    .iter()
                    .map(|v| Self::normalize_variable(v).to_string())
                    .collect();

                let projected: Vec<HashMap<String, u32>> = input_results
                    .into_par_iter()
                    .map(|mut result| {
                        result.retain(|k, _| stripped_vars.iter().any(|var| var == k));
                        result
                    })
                    .collect();
                projected
            }
            PhysicalOperator::BindJoin { left, right } => {
                let left_results =
                    Self::execute_with_ids_and_input(left, database, context, incoming);
                Self::execute_bind_join(right, database, context, left_results)
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_results =
                    Self::execute_with_ids_and_input(left, database, context, incoming);
                if left_results.is_empty() {
                    return Vec::new();
                }
                let right_results = Self::execute_with_ids_and_context(right, database, context);
                Self::hash_join_solution_sequences(left_results, right_results)
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_results =
                    Self::execute_with_ids_and_input(left, database, context, incoming);
                if left_results.is_empty() {
                    return Vec::new();
                }
                let right_results = Self::execute_with_ids_and_context(right, database, context);
                Self::join_solution_sequences(left_results, right_results)
            }
            PhysicalOperator::StarJoin { join_var, patterns } => {
                let _ = join_var;
                let mut results = incoming;
                for pattern in patterns {
                    let quad = QuadPattern {
                        subject: pattern.0.clone(),
                        predicate: pattern.1.clone(),
                        object: pattern.2.clone(),
                        graph: GraphTerm::Default,
                    };
                    results = Self::execute_quad_scan_with_ids(database, &quad, context, results);
                    if results.is_empty() {
                        break;
                    }
                }
                results
            }
            PhysicalOperator::InMemoryBuffer { content, origin: _ } => {
                Self::join_solution_sequences(incoming, content.clone())
            }
            PhysicalOperator::Subquery { inner, spec } => {
                // A subquery has its own variable scope and is evaluated once.
                let inner_results = Self::execute_with_ids_and_input(
                    inner,
                    database,
                    context,
                    vec![HashMap::new()],
                );
                let finalized = Self::finalize_subquery(inner_results, spec, database);
                Self::join_solution_sequences(incoming, finalized)
            }
            PhysicalOperator::Bind {
                input,
                function_name,
                arguments,
                output_variable,
            } => {
                let mut input_results =
                    Self::execute_with_ids_and_input(input, database, context, incoming);
                let output_var = Self::normalize_variable(output_variable);

                if function_name == "CONCAT" {
                    // Decode all needed values first
                    let dict = database.dictionary.read().unwrap();
                    let decoded_values: Vec<Vec<String>> = input_results
                        .iter()
                        .map(|row| {
                            arguments
                                .iter()
                                .map(|arg| {
                                    let arg_stripped = Self::normalize_variable(arg);
                                    if Self::is_variable(arg) {
                                        if let Some(&id) = row.get(arg_stripped) {
                                            dict.decode(id).unwrap_or("").to_string()
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        arg.trim_matches('"').to_string()
                                    }
                                })
                                .collect()
                        })
                        .collect();
                    drop(dict);

                    // Now encode the concatenated results
                    let mut dict_write = database.dictionary.write().unwrap();
                    for (row, decoded_row) in input_results.iter_mut().zip(decoded_values.iter()) {
                        let concatenated = decoded_row.join("");
                        let result_id = dict_write.encode(&concatenated);
                        row.insert(output_var.to_string(), result_id);
                    }
                    drop(dict_write);

                    input_results
                } else if let Some(func) = database.udfs.get(function_name.as_str()) {
                    // Similar fix for UDF
                    let dict = database.dictionary.read().unwrap();
                    let decoded_args: Vec<Vec<String>> = input_results
                        .iter()
                        .map(|row| {
                            arguments
                                .iter()
                                .map(|arg| {
                                    let arg_stripped = Self::normalize_variable(arg);
                                    if Self::is_variable(arg) {
                                        if let Some(&id) = row.get(arg_stripped) {
                                            dict.decode(id).unwrap_or("").to_string()
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        arg.trim_matches('"').to_string()
                                    }
                                })
                                .collect()
                        })
                        .collect();
                    drop(dict);

                    let mut dict_write = database.dictionary.write().unwrap();
                    for (row, decoded_row) in input_results.iter_mut().zip(decoded_args.iter()) {
                        let resolved_args: Vec<&str> =
                            decoded_row.iter().map(|s| s.as_str()).collect();
                        let result = func.call(resolved_args);
                        let result_id = dict_write.encode(&result);
                        row.insert(output_var.to_string(), result_id);
                    }
                    drop(dict_write);

                    input_results
                } else if function_name == "SUBJECT"
                    || function_name == "PREDICATE"
                    || function_name == "OBJECT"
                {
                    let qt_store = database.quoted_triple_store.read().unwrap();
                    for row in &mut input_results {
                        if let Some(arg) = arguments.first() {
                            let arg_stripped = Self::normalize_variable(arg);
                            if let Some(&id) = row.get(arg_stripped) {
                                if is_quoted_triple_id(id) {
                                    if let Some((s, p, o)) = qt_store.decode(id) {
                                        let component = match function_name.as_str() {
                                            "SUBJECT" => s,
                                            "PREDICATE" => p,
                                            "OBJECT" => o,
                                            _ => unreachable!(),
                                        };
                                        row.insert(output_var.to_string(), component);
                                    }
                                }
                            }
                        }
                    }
                    drop(qt_store);
                    input_results
                } else if function_name == "TRIPLE" {
                    if arguments.len() == 3 {
                        for row in &mut input_results {
                            let args: Vec<Option<u32>> = arguments
                                .iter()
                                .map(|arg| {
                                    let arg_stripped = Self::normalize_variable(arg);
                                    if Self::is_variable(arg) {
                                        row.get(arg_stripped).copied()
                                    } else {
                                        let mut dict = database.dictionary.write().unwrap();
                                        Some(dict.encode(arg_stripped))
                                    }
                                })
                                .collect();
                            if let (Some(s), Some(p), Some(o)) = (args[0], args[1], args[2]) {
                                let mut qt_store = database.quoted_triple_store.write().unwrap();
                                let qt_id = qt_store.encode(s, p, o);
                                row.insert(output_var.to_string(), qt_id);
                            }
                        }
                    }
                    input_results
                } else if function_name == "isTRIPLE" {
                    let mut dict_write = database.dictionary.write().unwrap();
                    for row in &mut input_results {
                        if let Some(arg) = arguments.first() {
                            let arg_stripped = Self::normalize_variable(arg);
                            if let Some(&id) = row.get(arg_stripped) {
                                let result_str = if is_quoted_triple_id(id) {
                                    "true"
                                } else {
                                    "false"
                                };
                                let result_id = dict_write.encode(result_str);
                                row.insert(output_var.to_string(), result_id);
                            }
                        }
                    }
                    drop(dict_write);
                    input_results
                } else {
                    eprintln!("Function {} not found", function_name);
                    input_results
                }
            }
            PhysicalOperator::Values { variables, values } => {
                let stripped_vars: Vec<String> = variables
                    .iter()
                    .map(|v| Self::normalize_variable(v).to_string())
                    .collect();

                // Convert VALUES data to result rows
                let mut results = Vec::new();

                for value_row in values {
                    let mut row = HashMap::new();

                    for (i, var) in stripped_vars.iter().enumerate() {
                        if let Some(Some(value_id)) = value_row.get(i) {
                            row.insert(var.clone(), *value_id);
                        }
                    }

                    // An all-UNDEF row is still the unit solution mapping.
                    results.push(row);
                }

                Self::join_solution_sequences(incoming, results)
            }
            PhysicalOperator::MLPredict {
                input,
                model_name,
                model_path,
                input_variables,
                output_variable,
            } => {
                // Execute the input operator first
                let input_results =
                    Self::execute_with_ids_and_input(input, database, context, incoming);

                if input_results.is_empty() {
                    return input_results;
                }

                println!(
                    "[ML.PREDICT] Executing prediction with model: {}",
                    model_name
                );
                println!("[ML.PREDICT] Model path: {}", model_path);
                println!("[ML.PREDICT] Input variables: {:?}", input_variables);
                println!("[ML.PREDICT] Output variable: {}", output_variable);
                println!("[ML.PREDICT] Input rows: {}", input_results.len());

                // Try Candle first: when the model name maps to exactly one registered
                // NEURAL RELATION, run the trained Candle MLP. Otherwise fall back to
                // the legacy Python/sklearn path.
                match crate::ml_predict_candle::try_candle_predict_by_model_name(
                    database,
                    model_name,
                    &input_results,
                ) {
                    Ok(Some(dispatch)) => {
                        println!("[ML.PREDICT] Dispatched to Candle (model={})", model_name);
                        return Self::merge_candle_predictions(
                            input_results,
                            dispatch.predictions,
                            output_variable,
                            database,
                        );
                    }
                    Ok(None) => {
                        println!("[ML.PREDICT] No Candle registration for model '{}', falling back to Python", model_name);
                    }
                    Err(e) => {
                        eprintln!("[ML.PREDICT] Candle dispatch error: {}", e);
                        return input_results;
                    }
                }

                // Extract input data for ML prediction
                let input_data =
                    Self::extract_ml_input_data(&input_results, input_variables, database);

                // Call the existing ML handler infrastructure
                match Self::invoke_ml_handler(model_path, model_name, input_data) {
                    Ok(predictions) => Self::merge_ml_predictions(
                        input_results,
                        predictions,
                        output_variable,
                        database,
                    ),
                    Err(e) => {
                        eprintln!("[ML.PREDICT] Error executing ML model: {}", e);
                        input_results
                    }
                }
            }
        }
    }

    fn normalize_variable(variable: &str) -> &str {
        variable
            .strip_prefix('?')
            .or_else(|| variable.strip_prefix('$'))
            .unwrap_or(variable)
    }

    fn is_variable(value: &str) -> bool {
        value.starts_with('?') || value.starts_with('$')
    }

    fn finalize_subquery(
        rows: Bindings,
        spec: &SubquerySpec,
        database: &SparqlDatabase,
    ) -> Bindings {
        let mut rows = Self::aggregate_subquery_rows(rows, spec, database);
        Self::apply_subquery_order(&mut rows, &spec.order_conditions, database);

        if let Some(projection) = &spec.projection {
            let projected_variables: HashSet<String> = projection
                .iter()
                .map(Self::subquery_output_variable)
                .collect();
            for row in &mut rows {
                row.retain(|variable, _| projected_variables.contains(variable));
            }
        }

        if spec.distinct {
            let mut seen = HashSet::new();
            rows.retain(|row| {
                let mut key: Vec<_> = row
                    .iter()
                    .map(|(variable, value)| (variable.clone(), *value))
                    .collect();
                key.sort_unstable();
                seen.insert(key)
            });
        }

        if let Some(limit) = spec.limit {
            rows.truncate(limit);
        }
        rows
    }

    fn aggregate_subquery_rows(
        rows: Bindings,
        spec: &SubquerySpec,
        database: &SparqlDatabase,
    ) -> Bindings {
        let aggregates: Vec<&SubqueryProjection> = spec
            .projection
            .iter()
            .flatten()
            .filter(|projection| projection.kind != "VAR" && projection.kind != "*")
            .collect();
        if aggregates.is_empty() && spec.group_vars.is_empty() {
            return rows;
        }

        let mut groups: BTreeMap<Vec<Option<u32>>, Bindings> = BTreeMap::new();
        for row in rows {
            let key = spec
                .group_vars
                .iter()
                .map(|variable| row.get(Self::normalize_variable(variable)).copied())
                .collect();
            groups.entry(key).or_default().push(row);
        }
        if groups.is_empty() && spec.group_vars.is_empty() {
            groups.insert(Vec::new(), Vec::new());
        }

        groups
            .into_values()
            .map(|group| {
                let mut result = group.first().cloned().unwrap_or_default();
                for aggregate in &aggregates {
                    let input = Self::normalize_variable(&aggregate.variable);
                    let values = group
                        .iter()
                        .filter_map(|row| row.get(input).copied())
                        .filter_map(|id| database.decode_any(id))
                        .collect::<Vec<_>>();
                    let value = match aggregate.kind.to_ascii_uppercase().as_str() {
                        "COUNT" => Some(values.len().to_string()),
                        "SUM" => Some(
                            values
                                .iter()
                                .filter_map(|value| value.parse::<f64>().ok())
                                .sum::<f64>()
                                .to_string(),
                        ),
                        "AVG" => {
                            let numbers = values
                                .iter()
                                .filter_map(|value| value.parse::<f64>().ok())
                                .collect::<Vec<_>>();
                            (!numbers.is_empty()).then(|| {
                                (numbers.iter().sum::<f64>() / numbers.len() as f64).to_string()
                            })
                        }
                        "MIN" => values
                            .iter()
                            .filter_map(|value| value.parse::<f64>().ok())
                            .min_by(|left, right| {
                                left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
                            })
                            .map(|value| value.to_string()),
                        "MAX" => values
                            .iter()
                            .filter_map(|value| value.parse::<f64>().ok())
                            .max_by(|left, right| {
                                left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
                            })
                            .map(|value| value.to_string()),
                        _ => None,
                    };
                    let output = Self::subquery_output_variable(aggregate);
                    if let Some(value) = value {
                        let id = database.dictionary.write().unwrap().encode(&value);
                        result.insert(output, id);
                    } else {
                        result.remove(&output);
                    }
                }
                result
            })
            .collect()
    }

    fn apply_subquery_order(
        rows: &mut [HashMap<String, u32>],
        conditions: &[(String, SortDirection)],
        database: &SparqlDatabase,
    ) {
        rows.sort_by(|left, right| {
            for (variable, direction) in conditions {
                let variable = Self::normalize_variable(variable);
                let left_value = left
                    .get(variable)
                    .and_then(|id| database.decode_any(*id))
                    .unwrap_or_default();
                let right_value = right
                    .get(variable)
                    .and_then(|id| database.decode_any(*id))
                    .unwrap_or_default();
                let comparison = match (left_value.parse::<f64>(), right_value.parse::<f64>()) {
                    (Ok(left), Ok(right)) => left
                        .partial_cmp(&right)
                        .unwrap_or(std::cmp::Ordering::Equal),
                    _ => left_value.cmp(&right_value),
                };
                let comparison = match direction {
                    SortDirection::Asc => comparison,
                    SortDirection::Desc => comparison.reverse(),
                };
                if comparison != std::cmp::Ordering::Equal {
                    return comparison;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn subquery_output_variable(projection: &SubqueryProjection) -> String {
        Self::normalize_variable(projection.alias.as_deref().unwrap_or(&projection.variable))
            .to_string()
    }

    fn execute_graph_with_ids(
        database: &SparqlDatabase,
        input: &PhysicalOperator,
        graph: &GraphTerm,
        context: &ExecutionContext,
        incoming: Bindings,
    ) -> Bindings {
        match graph {
            GraphTerm::Default => {
                let default_context = ExecutionContext {
                    dataset: context.dataset.clone(),
                    active_graph: None,
                };
                Self::execute_with_ids_and_input(input, database, &default_context, incoming)
            }
            GraphTerm::Named(graph_id) => {
                let graph_id = GraphId::Named(*graph_id);
                if !context.dataset.is_named_visible(graph_id)
                    || !database.dataset_index.graph_exists(graph_id)
                {
                    return Vec::new();
                }
                let graph_context = context.with_active_graph(graph_id);
                Self::execute_with_ids_and_input(input, database, &graph_context, incoming)
            }
            GraphTerm::Variable(variable) => {
                let variable = Self::normalize_variable(variable).to_string();
                let mut visible_graphs: Vec<_> =
                    context.dataset.named_graphs.iter().copied().collect();
                visible_graphs.sort_unstable();

                let mut results = Vec::new();
                for row in incoming {
                    if let Some(&bound_graph) = row.get(&variable) {
                        let graph = GraphId::Named(bound_graph);
                        if context.dataset.is_named_visible(graph)
                            && database.dataset_index.graph_exists(graph)
                        {
                            let graph_context = context.with_active_graph(graph);
                            results.extend(Self::execute_with_ids_and_input(
                                input,
                                database,
                                &graph_context,
                                vec![row],
                            ));
                        }
                        continue;
                    }

                    for graph in &visible_graphs {
                        if !database.dataset_index.graph_exists(*graph) {
                            continue;
                        }
                        let GraphId::Named(graph_id) = *graph else {
                            continue;
                        };
                        let mut graph_row = row.clone();
                        graph_row.insert(variable.clone(), graph_id);
                        let graph_context = context.with_active_graph(*graph);
                        results.extend(Self::execute_with_ids_and_input(
                            input,
                            database,
                            &graph_context,
                            vec![graph_row],
                        ));
                    }
                }
                results
            }
        }
    }

    fn execute_quad_scan_with_ids(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        context: &ExecutionContext,
        incoming: Bindings,
    ) -> Bindings {
        let mut results = Vec::new();

        for row in incoming {
            match &pattern.graph {
                GraphTerm::Default => {
                    if let Some(active_graph) = context.active_graph {
                        Self::scan_one_graph(
                            database,
                            pattern,
                            active_graph,
                            None,
                            &row,
                            &mut results,
                        );
                    } else {
                        Self::scan_query_default(database, pattern, context, &row, &mut results);
                    }
                }
                GraphTerm::Named(graph_id) => {
                    let graph = GraphId::Named(*graph_id);
                    if context.dataset.is_named_visible(graph)
                        && database.dataset_index.graph_exists(graph)
                    {
                        Self::scan_one_graph(database, pattern, graph, None, &row, &mut results);
                    }
                }
                GraphTerm::Variable(variable) => {
                    let variable = Self::normalize_variable(variable);
                    if let Some(&bound_graph) = row.get(variable) {
                        let graph = GraphId::Named(bound_graph);
                        if context.dataset.is_named_visible(graph)
                            && database.dataset_index.graph_exists(graph)
                        {
                            Self::scan_one_graph(
                                database,
                                pattern,
                                graph,
                                Some((variable, bound_graph)),
                                &row,
                                &mut results,
                            );
                        }
                    } else {
                        let mut visible_graphs: Vec<_> =
                            context.dataset.named_graphs.iter().copied().collect();
                        visible_graphs.sort_unstable();
                        for graph in visible_graphs {
                            if !database.dataset_index.graph_exists(graph) {
                                continue;
                            }
                            let GraphId::Named(graph_id) = graph else {
                                continue;
                            };
                            Self::scan_one_graph(
                                database,
                                pattern,
                                graph,
                                Some((variable, graph_id)),
                                &row,
                                &mut results,
                            );
                        }
                    }
                }
            }
        }

        results
    }

    fn scan_query_default(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        context: &ExecutionContext,
        row: &HashMap<String, u32>,
        results: &mut Bindings,
    ) {
        let (subject, predicate, object) = Self::bound_scan_keys(pattern, row);
        let mut seen = HashSet::new();

        for graph in &context.dataset.default_graphs {
            exec_count!(SCAN_PROBES);
            for quad in database
                .dataset_index
                .query_graph(*graph, subject, predicate, object)
            {
                exec_count!(QUADS_EXAMINED);
                if !seen.insert((quad.subject, quad.predicate, quad.object)) {
                    continue;
                }
                Self::match_quad(
                    database,
                    pattern,
                    quad.subject,
                    quad.predicate,
                    quad.object,
                    row,
                    results,
                );
            }
        }
    }

    fn scan_one_graph(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        graph: GraphId,
        graph_binding: Option<(&str, u32)>,
        row: &HashMap<String, u32>,
        results: &mut Bindings,
    ) {
        let (subject, predicate, object) = Self::bound_scan_keys(pattern, row);
        exec_count!(SCAN_PROBES);
        for quad in database
            .dataset_index
            .query_graph(graph, subject, predicate, object)
        {
            exec_count!(QUADS_EXAMINED);
            let mut seed = row.clone();
            if let Some((variable, graph_id)) = graph_binding {
                if let Some(&existing) = seed.get(variable) {
                    if existing != graph_id {
                        continue;
                    }
                } else {
                    seed.insert(variable.to_string(), graph_id);
                }
            }
            Self::match_quad(
                database,
                pattern,
                quad.subject,
                quad.predicate,
                quad.object,
                &seed,
                results,
            );
        }
    }

    fn bound_scan_keys(
        pattern: &QuadPattern,
        row: &HashMap<String, u32>,
    ) -> (Option<u32>, Option<u32>, Option<u32>) {
        (
            Self::bound_term_value(&pattern.subject, row),
            Self::bound_term_value(&pattern.predicate, row),
            Self::bound_term_value(&pattern.object, row),
        )
    }

    fn bound_term_value(term: &Term, row: &HashMap<String, u32>) -> Option<u32> {
        match term {
            Term::Constant(value) => Some(*value),
            Term::Variable(variable) => row.get(Self::normalize_variable(variable)).copied(),
            Term::QuotedTriple(_) => None,
        }
    }

    fn match_quad(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        subject: u32,
        predicate: u32,
        object: u32,
        seed: &HashMap<String, u32>,
        results: &mut Bindings,
    ) {
        let values = [
            (&pattern.subject, subject),
            (&pattern.predicate, predicate),
            (&pattern.object, object),
        ];

        // Quoted triples bind a variable number of nested variables; every other pattern binds at most three and fits a stack buffer
        if values
            .iter()
            .any(|(term, _)| matches!(term, Term::QuotedTriple(_)))
        {
            let mut bindings = seed.clone();
            for (term, value) in values {
                if !Self::match_term_with_store(database, term, value, &mut bindings) {
                    return;
                }
            }
            exec_count!(ROWS_EMITTED);
            results.push(bindings);
            return;
        }

        let mut fresh: [(&str, u32); 3] = [("", 0); 3];
        let mut fresh_len = 0;
        for (term, value) in values {
            match term {
                Term::Constant(constant) => {
                    if *constant != value {
                        return;
                    }
                }
                Term::Variable(variable) => {
                    let variable = Self::normalize_variable(variable);
                    let existing = seed.get(variable).copied().or_else(|| {
                        fresh[..fresh_len]
                            .iter()
                            .find(|(name, _)| *name == variable)
                            .map(|(_, bound)| *bound)
                    });
                    match existing {
                        Some(bound) if bound != value => return,
                        Some(_) => {}
                        None => {
                            fresh[fresh_len] = (variable, value);
                            fresh_len += 1;
                        }
                    }
                }
                Term::QuotedTriple(_) => unreachable!("quoted triples take the general path"),
            }
        }

        let mut bindings = seed.clone();
        bindings.reserve(fresh_len);
        for (variable, value) in &fresh[..fresh_len] {
            bindings.insert((*variable).to_string(), *value);
        }
        exec_count!(ROWS_EMITTED);
        results.push(bindings);
    }

    fn match_term_with_store(
        database: &SparqlDatabase,
        term: &Term,
        value: u32,
        bindings: &mut HashMap<String, u32>,
    ) -> bool {
        match term {
            Term::Constant(constant) => *constant == value,
            Term::Variable(variable) => {
                let variable = Self::normalize_variable(variable);
                if let Some(&existing) = bindings.get(variable) {
                    existing == value
                } else {
                    bindings.insert(variable.to_string(), value);
                    true
                }
            }
            Term::QuotedTriple(pattern) => {
                if !is_quoted_triple_id(value) {
                    return false;
                }
                let components = {
                    let store = database.quoted_triple_store.read().unwrap();
                    store.decode(value)
                };
                let Some((subject, predicate, object)) = components else {
                    return false;
                };
                Self::match_term_with_store(database, &pattern.0, subject, bindings)
                    && Self::match_term_with_store(database, &pattern.1, predicate, bindings)
                    && Self::match_term_with_store(database, &pattern.2, object, bindings)
            }
        }
    }

    /// Feeds a solution sequence into a dependent plan in parallel chunks, concatenated in chunk order to match a sequential pass
    fn execute_bind_join(
        right: &PhysicalOperator,
        database: &SparqlDatabase,
        context: &ExecutionContext,
        left_results: Bindings,
    ) -> Bindings {
        if left_results.is_empty() {
            return Vec::new();
        }

        let threads = rayon::current_num_threads();
        let chunk_size = (left_results.len() / threads.max(1)).max(BIND_JOIN_MIN_CHUNK);
        if chunk_size >= left_results.len() {
            return Self::execute_with_ids_and_input(right, database, context, left_results);
        }

        let chunks: Vec<Bindings> = left_results
            .par_chunks(chunk_size)
            .map(|chunk| {
                Self::execute_with_ids_and_input(right, database, context, chunk.to_vec())
            })
            .collect();
        chunks.into_iter().flatten().collect()
    }

    /// Merges two solution mappings if they agree on every shared variable
    fn merge_rows(
        left_row: &HashMap<String, u32>,
        right_row: &HashMap<String, u32>,
    ) -> Option<HashMap<String, u32>> {
        for (variable, left_value) in left_row {
            if let Some(right_value) = right_row.get(variable) {
                if right_value != left_value {
                    return None;
                }
            }
        }
        let mut joined = left_row.clone();
        for (variable, value) in right_row {
            joined.entry(variable.clone()).or_insert(*value);
        }
        Some(joined)
    }

    /// Variables bound by some row on both sides, in deterministic order
    fn shared_variables(left: &Bindings, right: &Bindings) -> Vec<String> {
        let left_variables: HashSet<&str> =
            left.iter().flat_map(|row| row.keys().map(String::as_str)).collect();
        let mut shared: Vec<&str> = right
            .iter()
            .flat_map(|row| row.keys().map(String::as_str))
            .filter(|variable| left_variables.contains(variable))
            .collect();
        shared.sort_unstable();
        shared.dedup();
        shared.into_iter().map(str::to_string).collect()
    }

    /// The join key of a row, or `None` when it leaves a key variable unbound
    fn join_key(row: &HashMap<String, u32>, variables: &[String]) -> Option<Vec<u32>> {
        variables.iter().map(|variable| row.get(variable).copied()).collect()
    }

    fn join_solution_sequences(left: Bindings, right: Bindings) -> Bindings {
        if left.is_empty() || right.is_empty() {
            return Vec::new();
        }

        left.par_iter()
            .flat_map_iter(|left_row| {
                right
                    .iter()
                    .filter_map(move |right_row| Self::merge_rows(left_row, right_row))
            })
            .collect()
    }

    /// Build/probe join that builds on the right side and probes in parallel with the left, so emitted order matches the nested loop
    fn hash_join_solution_sequences(left: Bindings, right: Bindings) -> Bindings {
        if left.is_empty() || right.is_empty() {
            return Vec::new();
        }

        let key_variables = Self::shared_variables(&left, &right);
        if key_variables.is_empty() {
            return Self::join_solution_sequences(left, right);
        }

        let all_right: Vec<&HashMap<String, u32>> = right.iter().collect();
        let mut table: HashMap<Vec<u32>, Vec<&HashMap<String, u32>>> = HashMap::new();
        let mut unkeyed: Vec<&HashMap<String, u32>> = Vec::new();
        for right_row in &right {
            match Self::join_key(right_row, &key_variables) {
                Some(key) => table.entry(key).or_default().push(right_row),
                None => unkeyed.push(right_row),
            }
        }

        left.par_iter()
            .flat_map_iter(|left_row| {
                // A fully bound left row probes the table plus the unhashable rows; a partially bound one can match anything
                let (probed, residual) = match Self::join_key(left_row, &key_variables) {
                    Some(key) => (
                        table.get(&key).map(Vec::as_slice).unwrap_or(&[]),
                        unkeyed.as_slice(),
                    ),
                    None => (all_right.as_slice(), &[][..]),
                };
                probed
                    .iter()
                    .chain(residual.iter())
                    .filter_map(move |right_row| Self::merge_rows(left_row, right_row))
            })
            .collect()
    }

    /// Extracts input data for ML prediction from query results
    fn extract_ml_input_data(
        input_results: &[HashMap<String, u32>],
        input_variables: &[String],
        database: &SparqlDatabase,
    ) -> Vec<Vec<f64>> {
        if let Some(first_row) = input_results.first() {
            println!(
                "[ML.PREDICT DEBUG] First row keys: {:?}",
                first_row.keys().collect::<Vec<_>>()
            );
            println!(
                "[ML.PREDICT DEBUG] Input variables to check: {:?}",
                input_variables
            );

            // Show what values decode to
            let dict = database.dictionary.read().unwrap();
            for (key, &id) in first_row {
                if let Some(value) = dict.decode(id) {
                    println!(
                        "[ML.PREDICT DEBUG]   {} -> {} (parses as f64: {})",
                        key,
                        value,
                        value.parse::<f64>().is_ok()
                    );
                }
            }
            drop(dict);
        }

        // Identify which variables are actually numeric by checking the first row
        let numeric_vars: Vec<String> = if let Some(first_row) = input_results.first() {
            let dict = database.dictionary.read().unwrap();
            let vars: Vec<String> = input_variables
                .iter()
                .filter(|var| {
                    let var_stripped = var.strip_prefix('?').unwrap_or(var);
                    if let Some(&id) = first_row.get(var_stripped) {
                        if let Some(value_str) = dict.decode(id) {
                            value_str.parse::<f64>().is_ok()
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();
            drop(dict);
            vars
        } else {
            return Vec::new();
        };

        println!("[ML.PREDICT] Numeric feature variables: {:?}", numeric_vars);

        // Now extract only numeric features
        let dict = database.dictionary.read().unwrap();
        let result: Vec<Vec<f64>> = input_results
            .iter()
            .map(|row| {
                numeric_vars
                    .iter()
                    .filter_map(|var| {
                        let var_stripped = var.strip_prefix('?').unwrap_or(var);

                        if let Some(&id) = row.get(var_stripped) {
                            if let Some(value_str) = dict.decode(id) {
                                value_str.parse::<f64>().ok()
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .collect();
        drop(dict);
        result
    }

    /// Invokes the ML handler to make predictions
    fn invoke_ml_handler(
        model_dir: &str,
        model_name: &str,
        input_data: Vec<Vec<f64>>,
    ) -> Result<MLPredictionResult, Box<dyn std::error::Error>> {
        use ml::generate_ml_models;
        use ml::MLHandler;

        println!("[ML.PREDICT] Initializing ML handler...");
        let mut ml_handler = MLHandler::new()?;

        println!("[ML.PREDICT] Looking for models in: {}", model_dir);

        let model_dir_path = std::path::PathBuf::from(model_dir);
        std::fs::create_dir_all(&model_dir_path)?;

        // Check if a matching .pkl model exists
        let models_exist = std::fs::read_dir(&model_dir_path)?
            .filter_map(Result::ok)
            .filter(|entry| {
                let path = entry.path();
                path.is_file()
                    && path.extension().map_or(false, |ext| ext == "pkl")
                    && path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .map_or(false, |stem| stem.ends_with("_predictor"))
            })
            .count()
            >= 1;

        if !models_exist {
            println!("[ML.PREDICT] Models not found. Generating models...");
            // Derive script name from model_name: "fraud_predictor" -> "fraud_predictor.py"
            let script_name = format!("{}.py", model_name);
            let predictor_script = model_dir_path
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.join(&script_name))
                .unwrap_or_else(|| std::path::PathBuf::from(&script_name));

            if let Some(script_path) = predictor_script.to_str() {
                generate_ml_models(&model_dir_path, script_path)?;
            }
        }

        println!("[ML.PREDICT] Discovering models and analyzing schemas...");
        let model_ids = ml_handler.discover_and_load_models(&model_dir_path, model_name)?;

        if model_ids.is_empty() {
            return Err("No valid models found with TTL schemas".into());
        }

        let best_model_name = ml_handler.best_model.as_deref().unwrap_or(&model_ids[0]);
        println!("[ML.PREDICT] Using best model: {}", best_model_name);

        println!(
            "[ML.PREDICT] Running predictions on {} samples...",
            input_data.len()
        );
        let start = std::time::Instant::now();

        let result = ml_handler.predict(best_model_name, input_data)?;

        let elapsed = start.elapsed();
        println!(
            "[ML.PREDICT] Prediction completed in {:.3}s",
            elapsed.as_secs_f64()
        );
        println!(
            "[ML.PREDICT] Throughput: {:.1} predictions/sec",
            result.predictions.len() as f64 / elapsed.as_secs_f64()
        );

        Ok(result)
    }

    /// Merges string-valued Candle predictions back into id-encoded query rows.
    fn merge_candle_predictions(
        mut input_results: Vec<HashMap<String, u32>>,
        predictions: Vec<String>,
        output_variable: &str,
        database: &SparqlDatabase,
    ) -> Vec<HashMap<String, u32>> {
        let output_var = output_variable.strip_prefix('?').unwrap_or(output_variable);

        let mut dict = database.dictionary.write().unwrap();
        for (i, prediction) in predictions.iter().enumerate() {
            if i < input_results.len() {
                let prediction_id = dict.encode(prediction);
                input_results[i].insert(output_var.to_string(), prediction_id);
            }
        }
        drop(dict);

        println!(
            "[ML.PREDICT] Candle: merged {} predictions",
            predictions.len()
        );
        input_results
    }

    /// Merges ML predictions back into query results
    fn merge_ml_predictions(
        mut input_results: Vec<HashMap<String, u32>>,
        predictions: MLPredictionResult,
        output_variable: &str,
        database: &SparqlDatabase,
    ) -> Vec<HashMap<String, u32>> {
        let output_var = output_variable.strip_prefix('?').unwrap_or(output_variable);

        let mut dict = database.dictionary.write().unwrap();
        for (i, prediction) in predictions.predictions.iter().enumerate() {
            if i < input_results.len() {
                let prediction_str = prediction.to_string();
                let prediction_id = dict.encode(&prediction_str);
                input_results[i].insert(output_var.to_string(), prediction_id);
            }
        }
        drop(dict);

        println!(
            "[ML.PREDICT] Successfully added {} predictions",
            predictions.predictions.len()
        );
        input_results
    }
}
