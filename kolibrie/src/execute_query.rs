/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::error_handler::format_parse_error;
use crate::parser::{parse_combined_query, parse_combined_query_with_options};
// Re-exported, not defined here: see `query_error` for why it moved.
pub use crate::query_error::QueryError;
use crate::sparql_database::SparqlDatabase;
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
use shared::terms::{Bindings, Term};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Rows the decoder hands to projection: a schema once, and values per row.
///
/// This used to be `Vec<HashMap<String, String>>`, one map per solution. That shape
/// existed only to be taken apart again one step later, because [`Solutions`] is
/// already a schema and rows -- so every result cell paid for a variable name it
/// shared with every other row, a hash to store it, and a second hash and a second
/// copy to read it back out.
///
/// Profiling five end-to-end queries over a 20,000 subject fixture put 54.5% of
/// working time in this path, against 12.8% in the engine's own row handling, which
/// is why this was converted first.
struct StringTable {
    /// Normalised variable names, in column order, allocated once for the result.
    variables: Vec<String>,
    /// One row per solution, one cell per column. `None` is an unbound variable.
    rows: Vec<Vec<Option<String>>>,
}

impl StringTable {
    /// The column index of an already-normalised variable name.
    ///
    /// Resolve once per result and index rows with it. Looking a name up per row is
    /// what the map shape did and what this type exists to stop.
    fn column(&self, name: &str) -> Option<usize> {
        self.variables.iter().position(|v| v == name)
    }
}

/// Rows decoded per dictionary lock acquisition.
///
/// The decode used to call `SparqlDatabase::decode_any` per cell, and that takes the
/// dictionary read lock per call -- 60,000 acquisitions for a 20,000 row, three
/// column result, all sequential. Mirrors `FILTER_LOCK_CHUNK` in the execution
/// engine, which already decoded this way.
const DECODE_LOCK_CHUNK: usize = 1024;

/// The rows a request answered with, and the variables they are rows of.
///
/// Replaces the bare `Vec<Vec<String>>` the older entry points return, which loses
/// two things the engine knew: which variable each column belongs to, and whether a
/// value is absent because it was unbound.
///
/// # Why the values are owned
///
/// [`Solution::get`] returns `Option<&str>`, so reading a result already borrows.
/// Whether the rows *behind* it should own their `String`s is a measured trade-off,
/// and which way it falls depends on how the caller reads the result.
///
/// Interning -- holding each distinct value once, with rows indexing it -- was built
/// and benchmarked. `optimizer_hotpath` has both arms:
///
/// | read through | owned | interned |
/// |---|---|---|
/// | [`into_rows`](Self::into_rows) | **9.53ms** | 10.65ms |
/// | [`iter`](Self::iter) and [`Solution::get`] | 10.09ms | **9.11ms** |
///
/// So interning is about 1.11x faster for a caller that borrows, and about 1.12x
/// slower for one that materialises, because `into_rows` allocates a `String` per
/// cell whatever the result holds and so pays interning's indirection for nothing.
/// Every consumer in this workspace materialises -- the CLI, the Python bindings, the
/// HTTP server, the examples, and this crate's own doc example -- so owning is the
/// right default today. Revisit it when something reads results through `iter`.
///
/// Note the reason is *not* that hashing costs more than copying: a `u32` hash
/// lookup measured 5.6ns against 24.4ns to clone a short IRI. An earlier version of
/// this note claimed otherwise. The interned build pays a sequential pass over every
/// cell to assign indices, and that pass cannot join the parallel chunked decode
/// around it.
///
/// Borrowing from the dictionary instead was not built: the result would hold a read
/// guard for its whole lifetime, blocking every writer, and since the database is
/// taken by `&mut` a caller could not touch it at all while results were alive.
///
/// # Unbound is not empty
///
/// A value is [`None`] when the solution did not bind that variable. SPARQL
/// distinguishes that from a variable bound to the empty string, and
/// `Vec<Vec<String>>` cannot: it renders both as `""`. [`From<Solutions>`] keeps that
/// old flattening for callers that want the previous shape.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Solutions {
    variables: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

impl Solutions {
    /// The projected variables, in column order, without the leading `?` or `$`.
    ///
    /// Empty for a request that binds nothing, such as an Update.
    pub fn variables(&self) -> &[String] {
        &self.variables
    }

    /// Number of solutions.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether there are no solutions.
    ///
    /// For a request that ran successfully this means the pattern matched nothing —
    /// a failure is an `Err` and never an empty `Solutions`.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Whether `variable` is one of the projected variables, with or without its
    /// sigil. Distinguishes "not projected" from "projected but unbound", which
    /// [`Solution::get`] cannot.
    pub fn has_variable(&self, variable: &str) -> bool {
        let name = normalize_variable(variable);
        self.variables.iter().any(|v| v == name)
    }

    /// The rows as plain strings, dropping the variable names and rendering an
    /// unbound value as `""`.
    ///
    /// The shape the older entry points return, for a caller that indexes by column
    /// and does not need either. It is lossy in a way `Vec<Vec<String>>` cannot
    /// express — an unbound variable and one bound to the empty string come out the
    /// same — so prefer [`iter`](Self::iter) in new code.
    pub fn into_rows(self) -> Vec<Vec<String>> {
        self.into()
    }

    /// The solutions, in result order.
    pub fn iter(&self) -> impl Iterator<Item = Solution<'_>> {
        self.rows.iter().map(|values| Solution {
            variables: &self.variables,
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
    /// The shape the older entry points return. Renders an unbound value as `""`,
    /// which is why it is lossy and why [`Solutions`] exists.
    fn from(solutions: Solutions) -> Self {
        solutions
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(Option::unwrap_or_default).collect())
            .collect()
    }
}

/// One solution: a value per projected variable.
#[derive(Debug, Clone, Copy)]
pub struct Solution<'a> {
    variables: &'a [String],
    values: &'a [Option<String>],
}

impl<'a> Solution<'a> {
    /// The value bound to `variable`, with or without its leading `?` or `$`.
    ///
    /// [`None`] both when the variable is unbound in this solution and when it is not
    /// projected at all; [`Solutions::has_variable`] separates those.
    pub fn get(&self, variable: &str) -> Option<&'a str> {
        let name = normalize_variable(variable);
        let column = self.variables.iter().position(|v| v == name)?;
        self.values.get(column)?.as_deref()
    }

    /// The values in column order, matching [`Solutions::variables`].
    pub fn values(&self) -> &'a [Option<String>] {
        self.values
    }
}

/// Runs a SPARQL request and returns its solution rows.
///
/// The entry point to prefer. One row per solution, one string per projected
/// variable, in the order the `SELECT` names them. An Update returns no rows, and so
/// does a request that only declares prefixes or rules.
///
/// # Errors
///
/// A [`QueryError`] saying which stage refused it. Note the contrast with
/// [`execute_query_rayon_parallel2_volcano`], which reports failure by returning no
/// rows — indistinguishable from a query that correctly matched nothing.
///
/// # Accepts Update syntax
///
/// For compatibility with the older entry point this accepts Update forms as well as
/// queries. [`execute_sparql_query`] rejects them, and
/// [`execute_sparql_update`] returns an [`UpdateSummary`] rather than rows.
pub fn execute(sparql: &str, database: &mut SparqlDatabase) -> Result<Solutions, QueryError> {
    execute_request(sparql, database, RequestKind::Any)
}

/// Runs a training-data `SELECT` and returns its rows keyed by variable name.
///
/// Lived in [`crate::ml_feature_loader`] until that module was made to depend on
/// nothing in the crate; it needs to execute a query, so it belongs here. It used to
/// parse `select_query` a second time purely to recover the projected variable names,
/// which [`Solutions::variables`] already carries, so the second parse is gone.
///
/// An unbound value is rendered as the empty string, as [`Solutions::into_rows`]
/// does, and reaches the caller as a term that fails to convert to a number.
///
/// # Errors
///
/// The query's own [`QueryError`] if it does not parse, is refused by the policy or
/// fails to evaluate, and a message of its own if it projects no variables. The
/// query comes from a `TRAIN` clause and is therefore caller-supplied, so none of
/// those is a bug in this crate: this used to `expect`, which aborted the process on
/// a malformed training query rather than returning the `Err` the signature already
/// allowed.
#[cfg(feature = "ml")]
pub(crate) fn query_training_rows(
    db: &mut SparqlDatabase,
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

/// Parses the input of every `ML.PREDICT` clause in `query`, recursively.
///
/// The ML policy has to look inside these inputs to validate the models they name,
/// but parsing is this layer's job, not the policy's; see
/// [`PredictInputs`](crate::ml_policy::PredictInputs). An input that does not parse
/// as one complete query becomes an `Err` entry rather than failing here, so the
/// policy still reports it in the order it would have found it.
///
/// Terminates because each nested input is a strict substring of the text enclosing
/// it, so the depth is bounded by the length of the request.
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

/// Checks whether a request would be permitted, without running it or touching the
/// database.
///
/// Parses and applies the ML policy, and nothing else. A request this accepts can
/// still fail when executed.
///
/// # Errors
///
/// [`QueryError::Parse`] if it does not parse, or [`QueryError::Ml`] if the policy
/// refuses it. The parse message used to be flattened to the literal
/// `"SPARQL_PARSE_ERROR"`, which discarded the snippet naming the offending token and
/// was consumed by nothing: the one caller that reported it mapped every
/// non-ML error to the same code regardless.
pub fn validate_query_policy(sparql: &str, database: &SparqlDatabase) -> Result<(), QueryError> {
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
    database: &mut SparqlDatabase,
    context: &crate::ml_policy::MlExecutionContext,
) -> Result<Vec<Vec<String>>, QueryError> {
    with_ml_context(database, context, |database| {
        execute_request(sparql, database, RequestKind::Any).map(Vec::from)
    })
}

pub(crate) fn with_ml_context<R>(
    database: &mut SparqlDatabase,
    context: &crate::ml_policy::MlExecutionContext,
    execute: impl FnOnce(&mut SparqlDatabase) -> R,
) -> R {
    let previous = std::mem::replace(&mut database.ml_context, context.clone());
    struct ContextScope<'a> {
        database: &'a mut SparqlDatabase,
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
    /// Quads added. Counts only genuinely new ones: re-inserting an existing quad
    /// does not increment this.
    pub inserted_quads: usize,
    /// Quads removed. Counts only ones that were present.
    pub deleted_quads: usize,
}

/// Executes a query, reporting failure by returning no rows.
///
/// Superseded by [`execute`]. Deprecated rather than deleted so code outside this
/// repository keeps compiling with a warning; every call site inside it has moved.
///
/// Execute SELECT or a compatibility Update request through the unified
#[deprecated(
    note = "use `execute`, which returns Result<Solutions, QueryError>: this reports \
            failure by returning no rows, which a caller cannot tell from a query \
            that matched nothing"
)]
pub fn execute_query_rayon_parallel2_volcano(
    sparql: &str,
    database: &mut SparqlDatabase,
) -> Vec<Vec<String>> {
    match execute(sparql, database) {
        Ok(results) => results.into(),
        Err(error) => {
            // Behaviour preserved exactly: an ML refusal prints its code, anything
            // else prints `SPARQL_EXECUTION_FAILED` and the real message is lost.
            // That is the reason to prefer `execute`, and the reason this wrapper is
            // kept only until its callers move.
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
    database: &mut SparqlDatabase,
) -> Result<Vec<Vec<String>>, QueryError> {
    // This repeated `execute_request`'s whole pipeline — parse, single_rule,
    // validate_request, the ML branch, dispatch — for the sake of two differences,
    // which are now what `RequestKind::QueryOnly` means. Keeping two copies of that
    // sequence is how the entry points drift apart.
    //
    // Still returns `String` rather than `QueryError`, and still flattens to
    // `Vec<Vec<String>>`, because the HTTP surface passes these errors to clients.
    // `execute` is the one that keeps the variable names and the unbound distinction.
    execute_request(sparql, database, RequestKind::QueryOnly).map(Solutions::into_rows)
}

/// Execute one of the six supported standard SPARQL Update forms
pub fn execute_sparql_update(
    sparql: &str,
    database: &mut SparqlDatabase,
) -> Result<UpdateSummary, String> {
    execute_update_request(sparql, database, false)
}

/// Compatibility entry point that also accepts standalone INSERT/DELETE aliases
pub(crate) fn execute_sparql_update_compat(
    sparql: &str,
    database: &mut SparqlDatabase,
) -> Result<UpdateSummary, String> {
    execute_update_request(sparql, database, true)
}

// Each stage's failure is classified here, where it is known which stage failed.
// Everything this calls still reports failure as a `String`; threading `QueryError`
// further down is a separate change, and doing it here keeps the distinction a caller
// can act on — a request that did not parse, versus one refused by policy, versus one
// that failed while running — without rewriting the pipeline underneath.
/// What forms a request may take, which is the whole of what the query entry points
/// disagree about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestKind {
    /// A query or an Update, with the standalone `INSERT`/`DELETE` aliases accepted.
    Any,
    /// A query only. An Update is refused, and the aliases do not parse.
    QueryOnly,
}

fn execute_request(
    sparql: &str,
    database: &mut SparqlDatabase,
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
    // After the ML branch and before `prepare_extensions`, which is where the
    // query-only entry point used to refuse: `prepare_extensions` registers prefixes
    // and neural declarations on the database, so a refused request must not reach it
    // and leave those behind. An `ML.PREDICT` request still takes the branch above,
    // as it did when this check lived in the `match` below.
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
    database: &mut SparqlDatabase,
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
    database: &mut SparqlDatabase,
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
    // The prediction is one more column, so it gets one more name: the variable the
    // clause binds it to. Everything before it keeps the input query's own names.
    let mut variables = predict_input_columns(predict);
    variables.push(normalize_variable(predict.output).to_string());
    Ok(Solutions {
        variables,
        rows: rows
            .into_iter()
            .zip(predictions)
            .map(|(row, prediction)| {
                let mut values: Vec<Option<String>> = row.into_iter().map(Some).collect();
                values.push(Some(prediction));
                values
            })
            .collect(),
    })
}

fn prepare_extensions(
    combined: &CombinedQuery<'_>,
    database: &mut SparqlDatabase,
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
    // Database prefixes remain available, while a query-local declaration
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
    database: &mut SparqlDatabase,
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
    database: &mut SparqlDatabase,
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

fn decode_bindings(bindings: Bindings, database: &SparqlDatabase) -> StringTable {
    // The schema is the union of every row's variables, not the first row's:
    // solution mappings are partial, so a variable may be bound only in a later row.
    let mut variables: Vec<String> = Vec::new();
    for row in &bindings {
        let mut names: Vec<&str> = row.keys().map(|key| normalize_variable(key)).collect();
        // Sorted so the column order does not depend on HashMap iteration order.
        names.sort_unstable();
        for name in names {
            if !variables.iter().any(|v| v == name) {
                variables.push(name.to_string());
            }
        }
    }

    // Lay the ids out per row first, so decoding is one pass over a fixed width and
    // the variable names are never touched again.
    let width = variables.len();
    let id_rows: Vec<Vec<Option<u32>>> = bindings
        .into_iter()
        .map(|row| {
            let mut cells = vec![None; width];
            for (name, id) in row {
                if let Some(index) = variables.iter().position(|v| v == normalize_variable(&name)) {
                    cells[index] = Some(id);
                }
            }
            cells
        })
        .collect();

    let rows = id_rows
        .into_par_iter()
        .chunks(DECODE_LOCK_CHUNK)
        .flat_map_iter(|chunk| {
            // One lock acquisition per chunk rather than per cell, and one decode per
            // distinct id rather than per occurrence -- a result that repeats a term
            // across rows, which is the normal case for a joined column, decodes it once.
            let dictionary = database.dictionary.read().unwrap();
            let quoted = database.quoted_triple_store.read().unwrap();
            let mut decoded_ids: HashMap<u32, Option<String>> = HashMap::new();
            let decoded: Vec<Vec<Option<String>>> = chunk
                .into_iter()
                .map(|cells| {
                    cells
                        .into_iter()
                        .map(|cell| {
                            // A bound id that will not decode stays `Some("")`, which is
                            // what unwrap_or_default gave before. Only an absent variable
                            // is `None`, and Solutions documents that difference.
                            cell.map(|id| {
                                decoded_ids
                                    .entry(id)
                                    .or_insert_with(|| dictionary.decode_term(id, &quoted))
                                    .clone()
                                    .unwrap_or_default()
                            })
                        })
                        .collect()
                })
                .collect();
            decoded.into_iter()
        })
        .collect();

    StringTable { variables, rows }
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

    // Every projected column resolved to an index once for the whole result. `None`
    // is a column the solutions never bound, which stays unbound in every row.
    let projected: Vec<Option<usize>> = columns
        .iter()
        .map(|column| table.column(normalize_variable(column)))
        .collect();

    if query.distinct {
        let mut seen = HashSet::new();
        table.rows.retain(|row| {
            let key: Vec<Option<String>> = projected
                .iter()
                .map(|column| column.and_then(|index| row[index].clone()))
                .collect();
            seen.insert(key)
        });
    }

    if let Some(limit) = query.limit {
        table.rows.truncate(limit);
    }

    // `None` where the solution did not bind the column: an unbound variable, which
    // SPARQL distinguishes from one bound to the empty string.
    let identity = projected.len() == table.variables.len()
        && projected
            .iter()
            .enumerate()
            .all(|(position, column)| *column == Some(position));
    let rows = if identity {
        // Projecting every column in its own order, so the rows move as they are
        // rather than being rebuilt a cell at a time.
        table.rows
    } else {
        table
            .rows
            .iter()
            .map(|row| {
                projected
                    .iter()
                    .map(|column| column.and_then(|index| row[index].clone()))
                    .collect()
            })
            .collect()
    };

    Solutions {
        variables: columns
            .iter()
            .map(|column| normalize_variable(column).to_string())
            .collect(),
        rows,
    }
}

/// Column names for an `ML.PREDICT` input query, mirroring [`projection_columns`].
///
/// The clause keeps the input query's projection and WHERE patterns as borrowed text
/// rather than a parsed `SelectQuery`, so `*` is expanded from the pattern triples
/// here instead of from a `GroupGraphPattern`.
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

fn aggregate_rows(table: StringTable, query: &SelectQuery<'_>) -> StringTable {
    // Grouping columns resolved once, then every row keyed by index.
    let group_columns: Vec<Option<usize>> = query
        .group_vars
        .iter()
        .map(|variable| table.column(normalize_variable(variable)))
        .collect();

    let mut groups: BTreeMap<Vec<Option<String>>, Vec<usize>> = BTreeMap::new();
    for (index, row) in table.rows.iter().enumerate() {
        let key: Vec<Option<String>> = group_columns
            .iter()
            .map(|column| column.and_then(|column| row[column].clone()))
            .collect();
        groups.entry(key).or_default().push(index);
    }
    if groups.is_empty() && query.group_vars.is_empty() {
        groups.insert(Vec::new(), Vec::new());
    }

    // The output schema is the grouping variables, then one column per aggregate.
    // An aggregate aliased onto a grouping variable reuses its column, which is what
    // inserting into one map per row did.
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

    let width = variables.len();
    let rows = groups
        .into_iter()
        .map(|(key, members)| {
            let mut row = vec![None; width];
            for (column, value) in key.into_iter().enumerate() {
                row[column] = value;
            }

            for &(column, kind, input) in &aggregates {
                let normalized_kind = kind.to_ascii_uppercase();
                let descriptor = crate::aggregate::Aggregate::parse(&normalized_kind);
                if descriptor.kind == "COUNT" && input == "*" {
                    // COUNT(*) counts solutions, and COUNT(DISTINCT *) counts the
                    // distinct ones over every decoded column, which is what
                    // deduplicating whole maps did.
                    row[column] = Some(if descriptor.distinct {
                        let unique: BTreeSet<&Vec<Option<String>>> =
                            members.iter().map(|&member| &table.rows[member]).collect();
                        unique.len().to_string()
                    } else {
                        members.len().to_string()
                    });
                    continue;
                }
                // `None` where the aggregate has no value, matching the removal the
                // map shape did, so the column reads as unbound rather than empty.
                row[column] = match table.column(input) {
                    Some(input) => {
                        let values: Vec<&str> = members
                            .iter()
                            .filter_map(|&member| table.rows[member][input].as_deref())
                            .collect();
                        crate::aggregate::value(kind, &values)
                    }
                    None => crate::aggregate::value(kind, &[]),
                };
            }
            row
        })
        .collect();

    StringTable { variables, rows }
}

fn apply_order_by(table: &mut StringTable, conditions: &[OrderCondition<'_>]) {
    if conditions.is_empty() {
        return;
    }

    // Resolved once for the sort, not per comparison: a sort does O(n log n)
    // comparisons, so a name lookup inside the comparator is paid that many times.
    let keys: Vec<(Option<usize>, SortDirection)> = conditions
        .iter()
        .map(|condition| {
            (
                table.column(normalize_variable(condition.variable)),
                condition.direction.clone(),
            )
        })
        .collect();

    table.rows.sort_by(|left, right| {
        for (column, direction) in &keys {
            let Some(column) = *column else { continue };
            let comparison = term_order::compare(
                left[column].as_deref(),
                right[column].as_deref(),
            );
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
    crate::ml_runtime_for_build().materialize_for_patterns(
        database,
        &lexical_patterns,
        prefixes,
    )?;

    let logical_plan = build_logical_plan_from_group(where_pattern, prefixes, database)?;
    let dataset = DatasetView::from_database(database);

    // The WHERE is evaluated once. Both templates are instantiated completely
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
        // SPARQL Update gives every solution its own blank-node allocation
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
