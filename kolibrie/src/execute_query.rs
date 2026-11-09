/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::database::KolibrieDB;
use crate::error_handler::format_parse_error;
use crate::parser::{parse_combined_query, parse_combined_query_with_options};
pub use crate::query_error::QueryError;
use crate::streamertail_optimizer::{
    build_logical_plan_from_group, compile_graph_term, compile_term, DatasetView, ExecutionEngine,
    Streamertail,
};
use crate::term_order;
use rayon::prelude::*;
use shared::dataset_index::{GraphId, GraphTerm, Quad};
use shared::query::{
    CombinedQuery, DeleteClause, GroupGraphPattern, InsertClause, LexicalQuadPattern,
    OrderCondition, SelectQuery, SortDirection, SparqlOperation, UpdateOperation,
};
use shared::quoted_triple_store::is_quoted_triple_id;
use shared::terms::{BindingRef, Bindings, Term, UNBOUND};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Rows the decoder hands to projection: a schema once, and values per row
struct StringTable {
    variables: Vec<String>,
    terms: Vec<String>,
    rows: Vec<Vec<Option<u32>>>,
}

impl StringTable {
    /// The column index of an already-normalised variable name
    fn column(&self, name: &str) -> Option<usize> {
        self.variables.iter().position(|v| v == name)
    }

    /// The value an interned cell refers to
    fn term(&self, cell: Option<u32>) -> Option<&str> {
        self.terms.get(cell? as usize).map(String::as_str)
    }
}

/// Interns computed values, for the aggregates that invent strings no term table has
struct TermInterner {
    terms: Vec<String>,
    indices: HashMap<String, u32>,
}

impl TermInterner {
    fn new() -> Self {
        Self {
            terms: Vec::new(),
            indices: HashMap::new(),
        }
    }

    fn intern(&mut self, value: String) -> u32 {
        if let Some(&index) = self.indices.get(&value) {
            return index;
        }
        let index = self.terms.len() as u32;
        self.indices.insert(value.clone(), index);
        self.terms.push(value);
        index
    }
}

/// Rows decoded per dictionary lock acquisition
const DECODE_LOCK_CHUNK: usize = 1024;

/// The rows a request answered with, and the variables they are rows of
#[derive(Clone, Default)]
pub struct Solutions {
    variables: Vec<String>,
    terms: Vec<String>,
    rows: Vec<Vec<Option<u32>>>,
}

impl Solutions {
    /// The value an interned cell refers to
    fn term(&self, cell: Option<u32>) -> Option<&str> {
        self.terms.get(cell? as usize).map(String::as_str)
    }
}

/// Compared by what the solutions say, not by how they are interned
impl PartialEq for Solutions {
    fn eq(&self, other: &Self) -> bool {
        self.variables == other.variables
            && self.rows.len() == other.rows.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(left, right)| left.values().eq(right.values()))
    }
}

impl Eq for Solutions {}

/// Prints the values, not the indices behind them
impl std::fmt::Debug for Solutions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Solutions")
            .field("variables", &self.variables)
            .field(
                "rows",
                &self
                    .iter()
                    .map(|solution| solution.values().collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Solutions {
    /// The projected variables, in column order, without the leading `?` or `$`
    pub fn variables(&self) -> &[String] {
        &self.variables
    }

    /// Number of solutions
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether there are no solutions
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Whether `variable` is one of the projected variables, with or without its sigil
    pub fn has_variable(&self, variable: &str) -> bool {
        let name = normalize_variable(variable);
        self.variables.iter().any(|v| v == name)
    }

    /// The rows as plain strings, dropping the variable names and rendering an unbound value as
    /// `""`
    pub fn into_rows(self) -> Vec<Vec<String>> {
        self.into()
    }

    /// The solutions, in result order
    pub fn iter(&self) -> impl Iterator<Item = Solution<'_>> {
        self.rows.iter().map(|values| Solution {
            variables: &self.variables,
            terms: &self.terms,
            values,
        })
    }
}

impl<'a> IntoIterator for &'a Solutions {
    type Item = Solution<'a>;
    type IntoIter = Box<dyn Iterator<Item = Solution<'a>> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl From<Solutions> for Vec<Vec<String>> {
    /// The shape the older entry points return
    fn from(solutions: Solutions) -> Self {
        solutions
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| solutions.term(*cell).unwrap_or_default().to_string())
                    .collect()
            })
            .collect()
    }
}

/// One solution: a value per projected variable
#[derive(Debug, Clone, Copy)]
pub struct Solution<'a> {
    variables: &'a [String],
    terms: &'a [String],
    values: &'a [Option<u32>],
}

impl<'a> Solution<'a> {
    /// The value bound to `variable`, with or without its leading `?` or `$`
    pub fn get(&self, variable: &str) -> Option<&'a str> {
        let name = normalize_variable(variable);
        let column = self.variables.iter().position(|v| v == name)?;
        self.term(*self.values.get(column)?)
    }

    /// The values in column order, matching [`Solutions::variables`]
    pub fn values(&self) -> impl Iterator<Item = Option<&'a str>> + 'a {
        let terms = self.terms;
        self.values
            .iter()
            .map(move |cell| cell.and_then(|cell| terms.get(cell as usize).map(String::as_str)))
    }

    /// The value an interned cell refers to
    fn term(&self, cell: Option<u32>) -> Option<&'a str> {
        self.terms.get(cell? as usize).map(String::as_str)
    }
}

/// Runs a SPARQL request and returns its solution rows
pub fn execute(sparql: &str, database: &mut KolibrieDB) -> Result<Solutions, QueryError> {
    execute_request(sparql, database, RequestKind::Any)
}

/// Runs a training-data `SELECT` and returns its rows keyed by variable name
#[cfg(feature = "ml")]
pub(crate) fn query_training_rows(
    db: &mut KolibrieDB,
    select_query: &str,
) -> Result<Vec<HashMap<String, crate::ml_feature_loader::RdfTerm>>, Box<dyn std::error::Error>> {
    let solutions = execute(select_query, db)?;
    let variables: Vec<String> = solutions.variables().to_vec();
    if variables.is_empty() {
        return Err("training data query must SELECT at least one variable".into());
    }
    Ok(solutions
        .into_rows()
        .into_iter()
        .map(|row| variables.iter().cloned().zip(row).collect())
        .collect())
}

/// Parses the input of every `ML.PREDICT` clause in `query`, recursively
fn predict_inputs<'a>(query: &CombinedQuery<'a>) -> crate::ml_policy::PredictInputs<'a> {
    let entries = query
        .ml_predict
        .iter()
        .chain(
            query
                .rules
                .iter()
                .filter_map(|rule| rule.ml_predict.as_ref()),
        )
        .map(|predict| match parse_combined_query(predict.input_raw) {
            Ok((remaining, nested)) if remaining.trim().is_empty() && nested.sparql.is_some() => {
                let deeper = predict_inputs(&nested);
                Ok((nested, deeper))
            }
            _ => Err(crate::ml_policy::MlError::Forbidden),
        })
        .collect();
    crate::ml_policy::PredictInputs::new(entries)
}

/// Checks whether a request would be permitted, without running it or touching the database
pub fn validate_query_policy(sparql: &str, database: &KolibrieDB) -> Result<(), QueryError> {
    let combined = parse_request(sparql, true).map_err(QueryError::Parse)?;
    let predicts = predict_inputs(&combined);
    database
        .ml_context
        .validate_request(
            &combined,
            &predicts,
            !database.neural_relation_decls.is_empty(),
        )
        .map_err(QueryError::Ml)
}

/// Execute with temporary host-selected ML authority
pub fn execute_sparql_with_ml_context(
    sparql: &str,
    database: &mut KolibrieDB,
    context: &crate::ml_policy::MlExecutionContext,
) -> Result<Vec<Vec<String>>, QueryError> {
    with_ml_context(database, context, |database| {
        execute_request(sparql, database, RequestKind::Any).map(Vec::from)
    })
}

pub(crate) fn with_ml_context<R>(
    database: &mut KolibrieDB,
    context: &crate::ml_policy::MlExecutionContext,
    execute: impl FnOnce(&mut KolibrieDB) -> R,
) -> R {
    let previous = std::mem::replace(&mut database.ml_context, context.clone());
    struct ContextScope<'a> {
        database: &'a mut KolibrieDB,
        previous: crate::ml_policy::MlExecutionContext,
    }
    impl Drop for ContextScope<'_> {
        fn drop(&mut self) {
            self.database.ml_context = std::mem::take(&mut self.previous);
        }
    }
    let scope = ContextScope { database, previous };
    execute(scope.database)
}

/// Summary returned by the error-preserving update entry point
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdateSummary {
    /// Quads added
    pub inserted_quads: usize,
    /// Quads removed
    pub deleted_quads: usize,
}

/// Executes a query, reporting failure by returning no rows
#[deprecated(
    note = "use `execute`, which returns Result<Solutions, QueryError>: this reports \
            failure by returning no rows, which a caller cannot tell from a query \
            that matched nothing"
)]
pub fn execute_query_rayon_parallel2_volcano(
    sparql: &str,
    database: &mut KolibrieDB,
) -> Vec<Vec<String>> {
    match execute(sparql, database) {
        Ok(results) => results.into(),
        Err(error) => {
            match error {
                QueryError::Ml(code) => eprintln!("{code}"),
                _ => eprintln!("SPARQL_EXECUTION_FAILED"),
            }
            Vec::new()
        }
    }
}

/// Execute a query request without accepting Update syntax
pub fn execute_sparql_query(
    sparql: &str,
    database: &mut KolibrieDB,
) -> Result<Vec<Vec<String>>, QueryError> {
    execute_request(sparql, database, RequestKind::QueryOnly).map(Solutions::into_rows)
}

/// Execute one of the six supported standard SPARQL Update forms
pub fn execute_sparql_update(
    sparql: &str,
    database: &mut KolibrieDB,
) -> Result<UpdateSummary, String> {
    execute_update_request(sparql, database, false)
}

/// Compatibility entry point that also accepts standalone INSERT/DELETE aliases
pub(crate) fn execute_sparql_update_compat(
    sparql: &str,
    database: &mut KolibrieDB,
) -> Result<UpdateSummary, String> {
    execute_update_request(sparql, database, true)
}

/// What forms a request may take, which is the whole of what the query entry points disagree about
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestKind {
    /// A query or an Update, with the standalone `INSERT`/`DELETE` aliases accepted
    Any,
    /// A query only
    QueryOnly,
}

fn execute_request(
    sparql: &str,
    database: &mut KolibrieDB,
    kind: RequestKind,
) -> Result<Solutions, QueryError> {
    let combined = parse_request(sparql, kind == RequestKind::Any).map_err(QueryError::Parse)?;
    combined.single_rule().map_err(QueryError::Unsupported)?;
    let predicts = predict_inputs(&combined);
    database
        .ml_context
        .validate_request(
            &combined,
            &predicts,
            !database.neural_relation_decls.is_empty(),
        )
        .map_err(QueryError::Ml)?;
    if let Some(predict) = &combined.ml_predict {
        #[cfg(feature = "ml")]
        if database.ml_context.require_local().is_ok() {
            crate::ml_runtime_for_build().execute_program(database, sparql)?;
            return Ok(Solutions::default());
        }
        return execute_approved_prediction(predict, &combined.prefixes, database)
            .map_err(QueryError::Execution);
    }
    if kind == RequestKind::QueryOnly
        && matches!(combined.sparql.as_ref(), Some(SparqlOperation::Update(_)))
    {
        return Err(QueryError::Unsupported(
            "expected a SPARQL query, found an Update operation".to_string(),
        ));
    }

    let prefixes = prepare_extensions(&combined, database).map_err(QueryError::Execution)?;

    match combined.sparql.as_ref() {
        Some(SparqlOperation::Select(query)) => {
            execute_select(query, &prefixes, database).map_err(QueryError::Execution)
        }
        Some(SparqlOperation::Update(update)) => {
            execute_update_operation(update, &prefixes, database).map_err(QueryError::Execution)?;
            Ok(Solutions::default())
        }
        None => Ok(Solutions::default()),
    }
}

fn execute_update_request(
    sparql: &str,
    database: &mut KolibrieDB,
    allow_data_aliases: bool,
) -> Result<UpdateSummary, String> {
    let combined = parse_request(sparql, allow_data_aliases)?;
    combined.single_rule()?;
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

fn execute_approved_prediction(
    predict: &shared::query::MLPredictClause<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut KolibrieDB,
) -> Result<Solutions, String> {
    if predict.distribution {
        return Err(
            "ML.PREDICT ... OUTPUT ?v DISTRIBUTION runs only through kolibrie::program".to_string(),
        );
    }
    database
        .ml_context
        .approved_model(predict.model)
        .map_err(|e| e.to_string())?;
    let mut input = String::new();
    for (prefix, iri) in prefixes {
        input.push_str(&format!("PREFIX {prefix}: <{iri}>\n"));
    }
    input.push_str(predict.input_raw);
    let rows = execute_sparql_query(&input, database).map_err(|error| error.to_string())?;
    let features = rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| {
                    crate::ml_feature_loader::rdf_term_to_f64(v)
                        .map_err(|_| "ML_INVALID_INPUT".to_string())
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let predictions = database
        .ml_context
        .predict(predict.model, &features)
        .map_err(|e| e.to_string())?;
    let mut variables = predict_input_columns(predict);
    variables.push(normalize_variable(predict.output).to_string());
    let mut interner = TermInterner::new();
    let rows = rows
        .into_iter()
        .zip(predictions)
        .map(|(row, prediction)| {
            let mut values: Vec<Option<u32>> = row
                .into_iter()
                .map(|value| Some(interner.intern(value)))
                .collect();
            values.push(Some(interner.intern(prediction)));
            values
        })
        .collect();
    Ok(Solutions {
        variables,
        terms: interner.terms,
        rows,
    })
}

fn prepare_extensions(
    combined: &CombinedQuery<'_>,
    database: &mut KolibrieDB,
) -> Result<HashMap<String, String>, String> {
    let predicts = predict_inputs(combined);
    database
        .ml_context
        .validate_request(
            combined,
            &predicts,
            !database.neural_relation_decls.is_empty(),
        )
        .map_err(|e| e.to_string())?;
    let mut prefixes = database.prefixes.clone();
    prefixes.extend(combined.prefixes.clone());
    database.prefixes.extend(combined.prefixes.clone());

    crate::neural_registry::register_neural_declarations_checked(
        database,
        &prefixes,
        &combined.model_decls,
        &combined.neural_relation_decls,
        &combined.train_neural_relation_decls,
    )?;

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
        crate::ml_runtime_for_build()
            .execute_train_decl(database, declaration)
            .map_err(|error| format!("failed to execute TRAIN NEURAL RELATION: {error}"))?;
    }

    Ok(prefixes)
}

fn execute_select(
    query: &SelectQuery<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut KolibrieDB,
) -> Result<Solutions, String> {
    let mut lexical_patterns = Vec::new();
    collect_triple_patterns(&query.pattern, &mut lexical_patterns);
    crate::ml_runtime_for_build().materialize_for_patterns(
        database,
        &lexical_patterns,
        prefixes,
    )?;

    let dataset = build_dataset_view(query, prefixes, database)?;
    let logical_plan = build_logical_plan_from_group(&query.pattern, prefixes, database)?;
    let bindings = optimize_and_execute(logical_plan, &dataset, database);
    let rows = decode_bindings(bindings, database);
    Ok(finalize_select(rows, query))
}

fn optimize_and_execute(
    logical_plan: crate::streamertail_optimizer::LogicalOperator,
    dataset: &DatasetView,
    database: &mut KolibrieDB,
) -> Bindings {
    let stats = if logical_plan.requires_cost_based_join_ordering() {
        database.get_or_build_planning_stats()
    } else {
        std::sync::Arc::new(crate::stats::DatabaseStats::new())
    };
    let mut optimizer = Streamertail::with_cached_stats_and_dataset(stats, dataset.clone());
    let physical_plan = optimizer.find_best_plan(&logical_plan);
    ExecutionEngine::execute_with_ids_and_dataset(&physical_plan, database, dataset)
}

fn build_dataset_view(
    query: &SelectQuery<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut KolibrieDB,
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
    database: &mut KolibrieDB,
) -> Result<GraphId, String> {
    match compile_graph_term(graph, prefixes, database)? {
        GraphTerm::Named(graph) => Ok(GraphId::Named(graph)),
        GraphTerm::Variable(_) => Err("dataset graph names cannot be variables".to_string()),
        GraphTerm::Default => Err("dataset graph names must be IRIs".to_string()),
    }
}

fn decode_bindings(bindings: Bindings, database: &KolibrieDB) -> StringTable {
    let (variables, cells) = bindings.into_parts();
    let mut rows: Vec<Vec<Option<u32>>> = cells
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|id| if id == UNBOUND { None } else { Some(id) })
                .collect()
        })
        .collect();

    let chunk_terms: Vec<Vec<String>> = rows
        .par_chunks_mut(DECODE_LOCK_CHUNK)
        .map(|chunk| {
            let dictionary = database.dictionary.read();
            let quoted = database.quoted_triple_store.read().unwrap();
            let mut local: HashMap<u32, u32> = HashMap::new();
            let mut terms: Vec<String> = Vec::new();
            for row in chunk.iter_mut() {
                for cell in row.iter_mut() {
                    if let Some(id) = *cell {
                        let index = *local.entry(id).or_insert_with(|| {
                            terms.push(dictionary.decode_term(id, &quoted).unwrap_or_default());
                            (terms.len() - 1) as u32
                        });
                        *cell = Some(index);
                    }
                }
            }
            terms
        })
        .collect();

    let mut offsets: Vec<u32> = Vec::with_capacity(chunk_terms.len());
    let mut total = 0u32;
    for terms in &chunk_terms {
        offsets.push(total);
        total += terms.len() as u32;
    }

    rows.par_chunks_mut(DECODE_LOCK_CHUNK)
        .zip(offsets.par_iter())
        .for_each(|(chunk, &offset)| {
            if offset == 0 {
                return;
            }
            for row in chunk.iter_mut() {
                for cell in row.iter_mut() {
                    if let Some(index) = cell.as_mut() {
                        *index += offset;
                    }
                }
            }
        });

    let terms: Vec<String> = chunk_terms.into_iter().flatten().collect();

    StringTable {
        variables,
        terms,
        rows,
    }
}

fn finalize_select(mut table: StringTable, query: &SelectQuery<'_>) -> Solutions {
    let columns = projection_columns(query);
    if query
        .variables
        .iter()
        .any(|(kind, _, _)| *kind != "VAR" && *kind != "*")
    {
        table = aggregate_rows(table, query);
    }

    apply_order_by(&mut table, &query.order_conditions);

    let projected: Vec<Option<usize>> = columns
        .iter()
        .map(|column| table.column(normalize_variable(column)))
        .collect();

    if query.distinct {
        let mut seen = HashSet::new();
        table.rows.retain(|row| {
            let key: Vec<Option<u32>> = projected
                .iter()
                .map(|column| column.and_then(|index| row[index]))
                .collect();
            seen.insert(key)
        });
    }

    if let Some(limit) = query.limit {
        table.rows.truncate(limit);
    }

    let identity = projected.len() == table.variables.len()
        && projected
            .iter()
            .enumerate()
            .all(|(position, column)| *column == Some(position));
    let rows = if identity {
        table.rows
    } else {
        table
            .rows
            .iter()
            .map(|row| {
                projected
                    .iter()
                    .map(|column| column.and_then(|index| row[index]))
                    .collect()
            })
            .collect()
    };

    Solutions {
        variables: columns
            .iter()
            .map(|column| normalize_variable(column).to_string())
            .collect(),
        terms: table.terms,
        rows,
    }
}

/// Column names for an `ML.PREDICT` input query, mirroring [`projection_columns`]
fn predict_input_columns(predict: &shared::query::MLPredictClause<'_>) -> Vec<String> {
    if predict.input_select == vec![("*", "*", None)] {
        let mut columns = Vec::new();
        for (subject, predicate, object) in &predict.input_where {
            for term in [subject, predicate, object] {
                if is_variable(term) {
                    let name = normalize_variable(term).to_string();
                    if !columns.contains(&name) {
                        columns.push(name);
                    }
                }
            }
        }
        return columns;
    }

    predict
        .input_select
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
            KolibrieDB::split_quoted_triple_content(term[2..term.len() - 2].trim());
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

fn aggregate_rows(table: StringTable, query: &SelectQuery<'_>) -> StringTable {
    let group_columns: Vec<Option<usize>> = query
        .group_vars
        .iter()
        .map(|variable| table.column(normalize_variable(variable)))
        .collect();

    let mut groups: BTreeMap<Vec<Option<u32>>, Vec<usize>> = BTreeMap::new();
    for (index, row) in table.rows.iter().enumerate() {
        let key: Vec<Option<u32>> = group_columns
            .iter()
            .map(|column| column.and_then(|column| row[column]))
            .collect();
        groups.entry(key).or_default().push(index);
    }
    if groups.is_empty() && query.group_vars.is_empty() {
        groups.insert(Vec::new(), Vec::new());
    }

    let mut variables: Vec<String> = query
        .group_vars
        .iter()
        .map(|variable| normalize_variable(variable).to_string())
        .collect();
    let mut aggregates: Vec<(usize, &str, &str)> = Vec::new();
    for (kind, variable, alias) in &query.variables {
        if *kind == "VAR" || *kind == "*" {
            continue;
        }
        let output = normalize_variable(alias.unwrap_or(variable)).to_string();
        let column = match variables.iter().position(|v| *v == output) {
            Some(column) => column,
            None => {
                variables.push(output);
                variables.len() - 1
            }
        };
        aggregates.push((column, kind, normalize_variable(variable)));
    }

    let mut interner = TermInterner::new();
    let width = variables.len();
    let rows: Vec<Vec<Option<u32>>> = groups
        .into_iter()
        .map(|(key, members)| {
            let mut row = vec![None; width];
            for (column, value) in key.into_iter().enumerate() {
                row[column] = table
                    .term(value)
                    .map(|value| interner.intern(value.to_string()));
            }

            for &(column, kind, input) in &aggregates {
                let normalized_kind = kind.to_ascii_uppercase();
                let descriptor = crate::aggregate::Aggregate::parse(&normalized_kind);
                if descriptor.kind == "COUNT" && input == "*" {
                    let count = if descriptor.distinct {
                        let unique: BTreeSet<&Vec<Option<u32>>> =
                            members.iter().map(|&member| &table.rows[member]).collect();
                        unique.len().to_string()
                    } else {
                        members.len().to_string()
                    };
                    row[column] = Some(interner.intern(count));
                    continue;
                }
                let value = match table.column(input) {
                    Some(input) => {
                        let values: Vec<&str> = members
                            .iter()
                            .filter_map(|&member| table.term(table.rows[member][input]))
                            .collect();
                        crate::aggregate::value(kind, &values)
                    }
                    None => crate::aggregate::value(kind, &[]),
                };
                row[column] = value.map(|value| interner.intern(value));
            }
            row
        })
        .collect();

    StringTable {
        variables,
        terms: interner.terms,
        rows,
    }
}

fn apply_order_by(table: &mut StringTable, conditions: &[OrderCondition<'_>]) {
    if conditions.is_empty() {
        return;
    }

    let keys: Vec<(Option<usize>, SortDirection)> = conditions
        .iter()
        .map(|condition| {
            (
                table.column(normalize_variable(condition.variable)),
                condition.direction.clone(),
            )
        })
        .collect();

    let mut rows = std::mem::take(&mut table.rows);
    let terms = &table.terms;
    let term =
        |cell: Option<u32>| cell.and_then(|cell| terms.get(cell as usize).map(String::as_str));
    rows.sort_by(|left, right| {
        for (column, direction) in &keys {
            let Some(column) = *column else { continue };
            let comparison = term_order::compare(term(left[column]), term(right[column]));
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
    table.rows = rows;
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
    database: &mut KolibrieDB,
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
    Ok(summary)
}

fn execute_modify(
    delete: Option<&DeleteClause<'_>>,
    insert: Option<&InsertClause<'_>>,
    where_pattern: &GroupGraphPattern<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut KolibrieDB,
) -> Result<UpdateSummary, String> {
    let mut lexical_patterns = Vec::new();
    collect_triple_patterns(where_pattern, &mut lexical_patterns);
    crate::ml_runtime_for_build().materialize_for_patterns(
        database,
        &lexical_patterns,
        prefixes,
    )?;

    let logical_plan = build_logical_plan_from_group(where_pattern, prefixes, database)?;
    let dataset = DatasetView::from_database(database);

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
    database: &mut KolibrieDB,
    insert: bool,
) -> Result<BTreeSet<Quad>, String> {
    instantiate_templates(quads, &Bindings::unit(), prefixes, database, insert)
}

fn instantiate_templates(
    templates: &[LexicalQuadPattern<'_>],
    bindings: &Bindings,
    prefixes: &HashMap<String, String>,
    database: &mut KolibrieDB,
    insert: bool,
) -> Result<BTreeSet<Quad>, String> {
    let mut quads = BTreeSet::new();
    for binding in bindings {
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
    binding: BindingRef<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut KolibrieDB,
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
    binding: BindingRef<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut KolibrieDB,
) -> Result<Option<GraphId>, String> {
    if is_variable(graph) {
        return Ok(binding.get(normalize_variable(graph)).map(GraphId::Named));
    }
    match compile_graph_term(graph, prefixes, database)? {
        GraphTerm::Named(graph) => Ok(Some(GraphId::Named(graph))),
        GraphTerm::Variable(_) => unreachable!("variables handled above"),
        GraphTerm::Default => Err("update GRAPH name must be an IRI".to_string()),
    }
}

fn is_legal_subject(id: u32, database: &KolibrieDB) -> bool {
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

fn is_legal_predicate(id: u32, database: &KolibrieDB) -> bool {
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

fn is_legal_object(id: u32, database: &KolibrieDB) -> bool {
    !is_quoted_triple_id(id) || is_legal_quoted_triple(id, database)
}

fn is_legal_graph_name(id: u32, database: &KolibrieDB) -> bool {
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

fn is_legal_quoted_triple(id: u32, database: &KolibrieDB) -> bool {
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
    binding: BindingRef<'_>,
    prefixes: &HashMap<String, String>,
    database: &mut KolibrieDB,
    insert: bool,
    blank_nodes: &mut HashMap<String, u32>,
) -> Result<Option<u32>, String> {
    let term = term.trim();
    if is_variable(term) {
        return Ok(binding.get(normalize_variable(term)));
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
            KolibrieDB::split_quoted_triple_content(term[2..term.len() - 2].trim());
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

fn allocate_blank_node(label: &str, database: &mut KolibrieDB) -> u32 {
    static NEXT_UPDATE_BLANK: AtomicU64 = AtomicU64::new(1);
    loop {
        let allocation = NEXT_UPDATE_BLANK.fetch_add(1, AtomicOrdering::Relaxed);
        let lexical = format!("_:kolibrie-update-{allocation}-{}", &label[2..]);
        let mut dictionary = database.dictionary.write();
        if dictionary.string_to_id.contains_key(&lexical) {
            continue;
        }
        return dictionary.encode(&lexical);
    }
}

fn apply_mutations(
    deletions: BTreeSet<Quad>,
    insertions: BTreeSet<Quad>,
    database: &mut KolibrieDB,
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
