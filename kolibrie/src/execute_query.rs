/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Unified SPARQL query and update execution.
//!
//! Standard SELECT and the supported Update forms are parsed into the same
//! lexical AST, lowered once into Kolibrie's existing logical algebra,
//! optimized by Streamertail, and evaluated by the physical execution engine.

use crate::error_handler::format_parse_error;
use crate::neural_relations::{
    execute_train_decl, materialize_neural_relations_for_patterns, register_neural_declarations,
};
use crate::parser::{parse_combined_query, parse_combined_query_with_options};
use crate::sparql_database::SparqlDatabase;
use crate::streamertail_optimizer::{
    build_logical_plan_from_group, compile_graph_term, compile_term, DatasetView, ExecutionEngine,
    Streamertail,
};
use shared::dataset_index::{GraphId, GraphTerm, Quad};
use shared::query::{
    CombinedQuery, DeleteClause, GroupGraphPattern, InsertClause, LexicalQuadPattern,
    OrderCondition, SelectQuery, SortDirection, SparqlOperation, UpdateOperation,
};
use shared::quoted_triple_store::is_quoted_triple_id;
use shared::terms::{Bindings, Term};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

type StringBinding = HashMap<String, String>;

/// Summary returned by the error-preserving update entry point.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdateSummary {
    pub inserted_quads: usize,
    pub deleted_quads: usize,
}

/// Execute SELECT or a compatibility Update request through the unified
/// parser → logical plan → optimizer → physical executor pipeline.
///
/// This historical adapter accepts standalone `INSERT { ... }` and
/// `DELETE { ... }` as DATA aliases. The error-preserving Update API below
/// intentionally accepts only standard syntax.
pub fn execute_query_rayon_parallel2_volcano(
    sparql: &str,
    database: &mut SparqlDatabase,
) -> Vec<Vec<String>> {
    match execute_request(sparql, database, true) {
        Ok(results) => results,
        Err(error) => {
            eprintln!("SPARQL execution failed: {error}");
            Vec::new()
        }
    }
}

/// Execute a query request without accepting Update syntax.
///
/// HTTP query endpoints use this entry point so a request submitted with
/// `application/sparql-query` (or a `query=` parameter) cannot mutate the
/// dataset. Kolibrie's explicitly dispatched RULE/RSP/ML extensions remain
/// available when no standard Update operation is present.
pub fn execute_sparql_query(
    sparql: &str,
    database: &mut SparqlDatabase,
) -> Result<Vec<Vec<String>>, String> {
    let combined = parse_request(sparql, false)?;
    match combined.sparql.as_ref() {
        Some(SparqlOperation::Update(_)) => {
            Err("expected a SPARQL query, found an Update operation".to_string())
        }
        Some(SparqlOperation::Select(query)) => {
            let prefixes = prepare_extensions(&combined, database)?;
            execute_select(query, &prefixes, database)
        }
        None => {
            prepare_extensions(&combined, database)?;
            Ok(Vec::new())
        }
    }
}

/// Execute one of the six supported standard SPARQL Update forms.
pub fn execute_sparql_update(
    sparql: &str,
    database: &mut SparqlDatabase,
) -> Result<UpdateSummary, String> {
    execute_update_request(sparql, database, false)
}

/// Compatibility entry point used by legacy adapters. It differs from
/// `execute_sparql_update` only by accepting standalone INSERT/DELETE aliases;
/// both paths produce and execute the same `UpdateOperation`.
pub(crate) fn execute_sparql_update_compat(
    sparql: &str,
    database: &mut SparqlDatabase,
) -> Result<UpdateSummary, String> {
    execute_update_request(sparql, database, true)
}

fn execute_request(
    sparql: &str,
    database: &mut SparqlDatabase,
    allow_data_aliases: bool,
) -> Result<Vec<Vec<String>>, String> {
    let combined = parse_request(sparql, allow_data_aliases)?;
    let prefixes = prepare_extensions(&combined, database)?;

    match combined.sparql.as_ref() {
        Some(SparqlOperation::Select(query)) => execute_select(query, &prefixes, database),
        Some(SparqlOperation::Update(update)) => {
            execute_update_operation(update, &prefixes, database)?;
            Ok(Vec::new())
        }
        None => Ok(Vec::new()),
    }
}

fn execute_update_request(
    sparql: &str,
    database: &mut SparqlDatabase,
    allow_data_aliases: bool,
) -> Result<UpdateSummary, String> {
    let combined = parse_request(sparql, allow_data_aliases)?;
    let prefixes = prepare_extensions(&combined, database)?;
    match combined.sparql.as_ref() {
        Some(SparqlOperation::Update(update)) => {
            execute_update_operation(update, &prefixes, database)
        }
        Some(SparqlOperation::Select(_)) => {
            Err("expected a SPARQL Update operation, found SELECT".to_string())
        }
        None => Err("expected a SPARQL Update operation".to_string()),
    }
}

fn parse_request(input: &str, allow_data_aliases: bool) -> Result<CombinedQuery<'_>, String> {
    let parsed = if allow_data_aliases {
        parse_combined_query_with_options(input, true)
    } else {
        parse_combined_query(input)
    };
    match parsed {
        Ok((remaining, combined)) if remaining.trim().is_empty() => Ok(combined),
        Ok((remaining, _)) => {
            let offset = input.len().saturating_sub(remaining.len());
            Err(format!(
                "unexpected trailing SPARQL input at byte {offset}: {}",
                remaining.trim()
            ))
        }
        Err(error) => Err(format_parse_error(input, error)),
    }
}

fn prepare_extensions(
    combined: &CombinedQuery<'_>,
    database: &mut SparqlDatabase,
) -> Result<HashMap<String, String>, String> {
    // Database prefixes remain available, while a query-local declaration
    // takes precedence for this request.
    let mut prefixes = database.prefixes.clone();
    prefixes.extend(combined.prefixes.clone());
    database.prefixes.extend(combined.prefixes.clone());

    register_neural_declarations(
        database,
        &prefixes,
        &combined.model_decls,
        &combined.neural_relation_decls,
        &combined.train_neural_relation_decls,
    );

    let normalized_trains = combined
        .train_neural_relation_decls
        .iter()
        .filter_map(|declaration| {
            let predicate = database.resolve_query_term(&declaration.predicate, &prefixes);
            database
                .train_neural_relation_decls
                .get(&predicate)
                .cloned()
        })
        .collect::<Vec<_>>();
    for declaration in &normalized_trains {
        execute_train_decl(database, declaration)
            .map_err(|error| format!("failed to execute TRAIN NEURAL RELATION: {error}"))?;
    }

    Ok(prefixes)
}

fn execute_select(
    query: &SelectQuery<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<Vec<Vec<String>>, String> {
    let mut lexical_patterns = Vec::new();
    collect_triple_patterns(&query.pattern, &mut lexical_patterns);
    materialize_neural_relations_for_patterns(database, &lexical_patterns, prefixes)?;

    let dataset = build_dataset_view(query, prefixes, database)?;
    let logical_plan = build_logical_plan_from_group(&query.pattern, prefixes, database)?;
    let bindings = optimize_and_execute(logical_plan, &dataset, database);
    let rows = decode_bindings(bindings, database);
    Ok(finalize_select(rows, query))
}

fn optimize_and_execute(
    logical_plan: crate::streamertail_optimizer::LogicalOperator,
    dataset: &DatasetView,
    database: &mut SparqlDatabase,
) -> Bindings {
    let stats = database.get_or_build_stats();
    let mut optimizer = Streamertail::with_cached_stats_and_dataset(stats, dataset.clone());
    let physical_plan = optimizer.find_best_plan(&logical_plan);
    ExecutionEngine::execute_with_ids_and_dataset(&physical_plan, database, dataset)
}

fn build_dataset_view(
    query: &SelectQuery<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<DatasetView, String> {
    if query.from.is_empty() && query.from_named.is_empty() {
        return Ok(DatasetView::from_database(database));
    }

    let mut default_graphs = Vec::new();
    for graph in &query.from {
        default_graphs.push(compile_dataset_graph(graph, prefixes, database)?);
    }

    let mut named_graphs = Vec::new();
    for graph in &query.from_named {
        named_graphs.push(compile_dataset_graph(graph, prefixes, database)?);
    }

    Ok(DatasetView::new(default_graphs, named_graphs))
}

fn compile_dataset_graph(
    graph: &str,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<GraphId, String> {
    match compile_graph_term(graph, prefixes, database)? {
        GraphTerm::Named(graph) => Ok(GraphId::Named(graph)),
        GraphTerm::Variable(_) => Err("dataset graph names cannot be variables".to_string()),
        GraphTerm::Default => Err("dataset graph names must be IRIs".to_string()),
    }
}

fn decode_bindings(bindings: Bindings, database: &SparqlDatabase) -> Vec<StringBinding> {
    bindings
        .into_iter()
        .map(|binding| {
            binding
                .into_iter()
                .map(|(variable, id)| {
                    (
                        normalize_variable(&variable).to_string(),
                        database.decode_any(id).unwrap_or_default(),
                    )
                })
                .collect()
        })
        .collect()
}

fn finalize_select(mut rows: Vec<StringBinding>, query: &SelectQuery<'_>) -> Vec<Vec<String>> {
    let columns = projection_columns(query);
    if query
        .variables
        .iter()
        .any(|(kind, _, _)| *kind != "VAR" && *kind != "*")
    {
        rows = aggregate_rows(rows, query);
    }

    apply_order_by(&mut rows, &query.order_conditions);

    if query.distinct {
        let mut seen = HashSet::new();
        rows.retain(|row| {
            let key = columns
                .iter()
                .map(|column| row.get(normalize_variable(column)).cloned())
                .collect::<Vec<_>>();
            seen.insert(key)
        });
    }

    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }

    rows.into_iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| {
                    row.get(normalize_variable(column))
                        .cloned()
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect()
}

fn projection_columns(query: &SelectQuery<'_>) -> Vec<String> {
    if query.variables == vec![("*", "*", None)] {
        let mut columns = Vec::new();
        collect_pattern_variables(&query.pattern, &mut columns);
        return columns;
    }

    query
        .variables
        .iter()
        .map(|(kind, variable, alias)| {
            if *kind == "VAR" {
                (*variable).to_string()
            } else {
                alias.unwrap_or(variable).to_string()
            }
        })
        .collect()
}

fn collect_pattern_variables(pattern: &GroupGraphPattern<'_>, variables: &mut Vec<String>) {
    match pattern {
        GroupGraphPattern::Unit | GroupGraphPattern::Filter(_) => {}
        GroupGraphPattern::Bgp(patterns) => {
            for (subject, predicate, object) in patterns {
                collect_term_variables(subject, variables);
                collect_term_variables(predicate, variables);
                collect_term_variables(object, variables);
            }
        }
        GroupGraphPattern::Join(patterns) | GroupGraphPattern::Union(patterns) => {
            for pattern in patterns {
                collect_pattern_variables(pattern, variables);
            }
        }
        GroupGraphPattern::Graph { name, pattern } => {
            collect_term_variables(name, variables);
            collect_pattern_variables(pattern, variables);
        }
        GroupGraphPattern::Bind((_, _, output)) => push_variable(variables, output),
        GroupGraphPattern::Values(values) => {
            for variable in &values.variables {
                push_variable(variables, variable);
            }
        }
        GroupGraphPattern::SubQuery(subquery) => {
            if subquery.query.variables == vec![("*", "*", None)] {
                collect_pattern_variables(&subquery.query.pattern, variables);
            } else {
                for (kind, variable, alias) in &subquery.query.variables {
                    if *kind == "VAR" {
                        push_variable(variables, variable);
                    } else if let Some(alias) = alias {
                        push_variable(variables, alias);
                    }
                }
            }
        }
    }
}

fn collect_term_variables(term: &str, variables: &mut Vec<String>) {
    let term = term.trim();
    if is_variable(term) {
        push_variable(variables, term);
    } else if term.starts_with("<<") && term.ends_with(">>") {
        let (subject, predicate, object) =
            SparqlDatabase::split_quoted_triple_content(term[2..term.len() - 2].trim());
        collect_term_variables(&subject, variables);
        collect_term_variables(&predicate, variables);
        collect_term_variables(&object, variables);
    }
}

fn push_variable(variables: &mut Vec<String>, variable: &str) {
    let key = normalize_variable(variable);
    if !variables
        .iter()
        .any(|existing| normalize_variable(existing) == key)
    {
        variables.push(variable.to_string());
    }
}

fn aggregate_rows(rows: Vec<StringBinding>, query: &SelectQuery<'_>) -> Vec<StringBinding> {
    let mut groups: BTreeMap<Vec<Option<String>>, Vec<StringBinding>> = BTreeMap::new();
    for row in rows {
        let key = query
            .group_vars
            .iter()
            .map(|variable| row.get(normalize_variable(variable)).cloned())
            .collect();
        groups.entry(key).or_default().push(row);
    }
    if groups.is_empty() && query.group_vars.is_empty() {
        groups.insert(Vec::new(), Vec::new());
    }

    groups
        .into_values()
        .map(|group| {
            let mut result = group.first().cloned().unwrap_or_default();
            for (kind, variable, alias) in &query.variables {
                if *kind == "VAR" || *kind == "*" {
                    continue;
                }
                let output = normalize_variable(alias.unwrap_or(variable)).to_string();
                let input = normalize_variable(variable);
                let values = group
                    .iter()
                    .filter_map(|row| row.get(input))
                    .collect::<Vec<_>>();
                let value = match kind.to_ascii_uppercase().as_str() {
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
                if let Some(value) = value {
                    result.insert(output, value);
                } else {
                    result.remove(&output);
                }
            }
            result
        })
        .collect()
}

fn apply_order_by(rows: &mut [StringBinding], conditions: &[OrderCondition<'_>]) {
    rows.sort_by(|left, right| {
        for condition in conditions {
            let variable = normalize_variable(condition.variable);
            let left_value = left.get(variable).map(String::as_str).unwrap_or("");
            let right_value = right.get(variable).map(String::as_str).unwrap_or("");
            let comparison = match (left_value.parse::<f64>(), right_value.parse::<f64>()) {
                (Ok(left), Ok(right)) => left
                    .partial_cmp(&right)
                    .unwrap_or(std::cmp::Ordering::Equal),
                _ => left_value.cmp(right_value),
            };
            let comparison = match condition.direction {
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

fn collect_triple_patterns<'a>(
    pattern: &'a GroupGraphPattern<'a>,
    output: &mut Vec<(&'a str, &'a str, &'a str)>,
) {
    match pattern {
        GroupGraphPattern::Bgp(patterns) => output.extend(patterns.iter().copied()),
        GroupGraphPattern::Join(patterns) | GroupGraphPattern::Union(patterns) => {
            for pattern in patterns {
                collect_triple_patterns(pattern, output);
            }
        }
        GroupGraphPattern::Graph { pattern, .. } => collect_triple_patterns(pattern, output),
        GroupGraphPattern::SubQuery(subquery) => {
            collect_triple_patterns(&subquery.query.pattern, output)
        }
        GroupGraphPattern::Unit
        | GroupGraphPattern::Filter(_)
        | GroupGraphPattern::Bind(_)
        | GroupGraphPattern::Values(_) => {}
    }
}

fn execute_update_operation(
    operation: &UpdateOperation<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<UpdateSummary, String> {
    let summary = match operation {
        UpdateOperation::InsertData(insert) => {
            let insertions = instantiate_data(&insert.quads, prefixes, database, true)?;
            apply_mutations(BTreeSet::new(), insertions, database)
        }
        UpdateOperation::DeleteData(delete) => {
            let deletions = instantiate_data(&delete.quads, prefixes, database, false)?;
            apply_mutations(deletions, BTreeSet::new(), database)
        }
        UpdateOperation::InsertWhere {
            insert,
            where_pattern,
        } => execute_modify(None, Some(insert), where_pattern, prefixes, database)?,
        UpdateOperation::DeleteWhere {
            delete,
            where_pattern,
        }
        | UpdateOperation::DeleteWhereShorthand {
            delete,
            where_pattern,
        } => execute_modify(Some(delete), None, where_pattern, prefixes, database)?,
        UpdateOperation::DeleteInsertWhere {
            delete,
            insert,
            where_pattern,
        } => execute_modify(
            Some(delete),
            Some(insert),
            where_pattern,
            prefixes,
            database,
        )?,
    };
    database.invalidate_stats_cache();
    Ok(summary)
}

fn execute_modify(
    delete: Option<&DeleteClause<'_>>,
    insert: Option<&InsertClause<'_>>,
    where_pattern: &GroupGraphPattern<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<UpdateSummary, String> {
    let mut lexical_patterns = Vec::new();
    collect_triple_patterns(where_pattern, &mut lexical_patterns);
    materialize_neural_relations_for_patterns(database, &lexical_patterns, prefixes)?;

    let logical_plan = build_logical_plan_from_group(where_pattern, prefixes, database)?;
    let dataset = DatasetView::from_database(database);

    // The WHERE is evaluated once. Both templates are instantiated completely
    // from this same pre-operation solution sequence before any quad mutation.
    let bindings = optimize_and_execute(logical_plan, &dataset, database);
    let deletions = match delete {
        Some(delete) => instantiate_templates(&delete.quads, &bindings, prefixes, database, false)?,
        None => BTreeSet::new(),
    };
    let insertions = match insert {
        Some(insert) => instantiate_templates(&insert.quads, &bindings, prefixes, database, true)?,
        None => BTreeSet::new(),
    };

    Ok(apply_mutations(deletions, insertions, database))
}

fn instantiate_data(
    quads: &[LexicalQuadPattern<'_>],
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
    insert: bool,
) -> Result<BTreeSet<Quad>, String> {
    instantiate_templates(quads, &[HashMap::new()], prefixes, database, insert)
}

fn instantiate_templates(
    templates: &[LexicalQuadPattern<'_>],
    bindings: &[HashMap<String, u32>],
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
    insert: bool,
) -> Result<BTreeSet<Quad>, String> {
    let mut quads = BTreeSet::new();
    for binding in bindings {
        // SPARQL Update gives every solution its own blank-node allocation,
        // while repeated labels within that solution share the same node.
        let mut blank_nodes = HashMap::new();
        for template in templates {
            if let Some(quad) = instantiate_quad(
                template,
                binding,
                prefixes,
                database,
                insert,
                &mut blank_nodes,
            )? {
                quads.insert(quad);
            }
        }
    }
    Ok(quads)
}

fn instantiate_quad(
    template: &LexicalQuadPattern<'_>,
    binding: &HashMap<String, u32>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
    insert: bool,
    blank_nodes: &mut HashMap<String, u32>,
) -> Result<Option<Quad>, String> {
    let Some(subject) = instantiate_term(
        template.triple.0,
        binding,
        prefixes,
        database,
        insert,
        blank_nodes,
    )?
    else {
        return Ok(None);
    };
    if (is_variable(template.triple.0) || is_quoted_triple_id(subject))
        && !is_legal_subject(subject, database)
    {
        return Ok(None);
    }
    let Some(predicate) = instantiate_term(
        template.triple.1,
        binding,
        prefixes,
        database,
        insert,
        blank_nodes,
    )?
    else {
        return Ok(None);
    };
    if is_variable(template.triple.1) && !is_legal_predicate(predicate, database) {
        return Ok(None);
    }
    let Some(object) = instantiate_term(
        template.triple.2,
        binding,
        prefixes,
        database,
        insert,
        blank_nodes,
    )?
    else {
        return Ok(None);
    };
    if is_quoted_triple_id(object) && !is_legal_object(object, database) {
        return Ok(None);
    }
    let graph = match template.graph {
        Some(graph_name) => {
            let Some(graph) = instantiate_graph(graph_name, binding, prefixes, database)? else {
                return Ok(None);
            };
            if is_variable(graph_name) {
                let GraphId::Named(graph_id) = graph else {
                    return Ok(None);
                };
                if !is_legal_graph_name(graph_id, database) {
                    return Ok(None);
                }
            }
            graph
        }
        None => GraphId::Default,
    };
    Ok(Some(Quad {
        subject,
        predicate,
        object,
        graph,
    }))
}

fn instantiate_graph(
    graph: &str,
    binding: &HashMap<String, u32>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
) -> Result<Option<GraphId>, String> {
    if is_variable(graph) {
        return Ok(binding
            .get(normalize_variable(graph))
            .copied()
            .map(GraphId::Named));
    }
    match compile_graph_term(graph, prefixes, database)? {
        GraphTerm::Named(graph) => Ok(Some(GraphId::Named(graph))),
        GraphTerm::Variable(_) => unreachable!("variables handled above"),
        GraphTerm::Default => Err("update GRAPH name must be an IRI".to_string()),
    }
}

fn is_legal_subject(id: u32, database: &SparqlDatabase) -> bool {
    if is_quoted_triple_id(id) {
        return is_legal_quoted_triple(id, database);
    }
    let Some(value) = database.decode_any(id) else {
        return false;
    };
    value.starts_with("_:")
        || is_probable_absolute_iri(&value)
        || database.dataset_index.graph_exists(GraphId::Named(id))
        || !database
            .dataset_index
            .query_quads(Some(id), None, None, None)
            .is_empty()
}

fn is_legal_predicate(id: u32, database: &SparqlDatabase) -> bool {
    if is_quoted_triple_id(id) {
        return false;
    }
    let Some(value) = database.decode_any(id) else {
        return false;
    };
    !value.starts_with("_:")
        && (is_probable_absolute_iri(&value)
            || database.dataset_index.graph_exists(GraphId::Named(id))
            || !database
                .dataset_index
                .query_quads(None, Some(id), None, None)
                .is_empty())
}

fn is_legal_object(id: u32, database: &SparqlDatabase) -> bool {
    !is_quoted_triple_id(id) || is_legal_quoted_triple(id, database)
}

fn is_legal_graph_name(id: u32, database: &SparqlDatabase) -> bool {
    if is_quoted_triple_id(id) {
        return false;
    }
    let Some(value) = database.decode_any(id) else {
        return false;
    };
    !value.starts_with("_:")
        && (is_probable_absolute_iri(&value)
            || database.dataset_index.graph_exists(GraphId::Named(id)))
}

fn is_legal_quoted_triple(id: u32, database: &SparqlDatabase) -> bool {
    let components = database.quoted_triple_store.read().unwrap().decode(id);
    let Some((subject, predicate, object)) = components else {
        return false;
    };
    is_legal_subject(subject, database)
        && is_legal_predicate(predicate, database)
        && is_legal_object(object, database)
}

fn is_probable_absolute_iri(value: &str) -> bool {
    let Some((scheme, _)) = value.split_once(':') else {
        return false;
    };
    let mut characters = scheme.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
}

fn instantiate_term(
    term: &str,
    binding: &HashMap<String, u32>,
    prefixes: &HashMap<String, String>,
    database: &mut SparqlDatabase,
    insert: bool,
    blank_nodes: &mut HashMap<String, u32>,
) -> Result<Option<u32>, String> {
    let term = term.trim();
    if is_variable(term) {
        return Ok(binding.get(normalize_variable(term)).copied());
    }
    if term.starts_with("_:") {
        if !insert {
            return Err("blank nodes are not allowed in DELETE templates".to_string());
        }
        if let Some(id) = blank_nodes.get(term) {
            return Ok(Some(*id));
        }
        let id = allocate_blank_node(term, database);
        blank_nodes.insert(term.to_string(), id);
        return Ok(Some(id));
    }
    if term.starts_with("<<") && term.ends_with(">>") {
        let (subject, predicate, object) =
            SparqlDatabase::split_quoted_triple_content(term[2..term.len() - 2].trim());
        let Some(subject) =
            instantiate_term(&subject, binding, prefixes, database, insert, blank_nodes)?
        else {
            return Ok(None);
        };
        let Some(predicate) =
            instantiate_term(&predicate, binding, prefixes, database, insert, blank_nodes)?
        else {
            return Ok(None);
        };
        let Some(object) =
            instantiate_term(&object, binding, prefixes, database, insert, blank_nodes)?
        else {
            return Ok(None);
        };
        let id = database
            .quoted_triple_store
            .write()
            .unwrap()
            .encode(subject, predicate, object);
        return Ok(Some(id));
    }

    match compile_term(term, prefixes, database) {
        Term::Constant(id) => Ok(Some(id)),
        Term::Variable(_) => Ok(None),
        Term::QuotedTriple(_) => Err("unresolved variable in quoted update triple".to_string()),
    }
}

fn allocate_blank_node(label: &str, database: &mut SparqlDatabase) -> u32 {
    static NEXT_UPDATE_BLANK: AtomicU64 = AtomicU64::new(1);
    loop {
        let allocation = NEXT_UPDATE_BLANK.fetch_add(1, AtomicOrdering::Relaxed);
        let lexical = format!("_:kolibrie-update-{allocation}-{}", &label[2..]);
        let mut dictionary = database.dictionary.write().unwrap();
        if dictionary.string_to_id.contains_key(&lexical) {
            continue;
        }
        return dictionary.encode(&lexical);
    }
}

fn apply_mutations(
    deletions: BTreeSet<Quad>,
    insertions: BTreeSet<Quad>,
    database: &mut SparqlDatabase,
) -> UpdateSummary {
    let deleted_quads = deletions
        .iter()
        .filter(|quad| database.dataset_index.delete_quad(quad))
        .count();
    let inserted_quads = insertions
        .iter()
        .filter(|quad| database.dataset_index.insert_quad(quad))
        .count();
    UpdateSummary {
        inserted_quads,
        deleted_quads,
    }
}

fn normalize_variable(variable: &str) -> &str {
    variable
        .strip_prefix('?')
        .or_else(|| variable.strip_prefix('$'))
        .unwrap_or(variable)
}

fn is_variable(term: &str) -> bool {
    term.starts_with('?') || term.starts_with('$')
}
