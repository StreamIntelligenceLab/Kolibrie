/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie_core::database::KolibrieDB;
use kolibrie_core::execute_query::execute;
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

enum LoadFormat {
    Turtle,
    NTriples,
    RdfXml,
}

fn normalize_load_format(path: &str, format: Option<&str>) -> PyResult<LoadFormat> {
    let normalized = if let Some(format) = format {
        format.trim().to_ascii_lowercase()
    } else {
        Path::new(path)
            .extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| extension.to_ascii_lowercase())
            .ok_or_else(|| {
                PyValueError::new_err(
                    "Could not infer RDF format from path; pass format='turtle', 'ntriples', or 'rdfxml'",
                )
            })?
    };

    match normalized.as_str() {
        "ttl" | "turtle" => Ok(LoadFormat::Turtle),
        "nt" | "ntriples" | "n-triples" => Ok(LoadFormat::NTriples),
        "rdf" | "xml" | "rdfxml" | "rdf+xml" => Ok(LoadFormat::RdfXml),
        other => Err(PyValueError::new_err(format!(
            "Unsupported RDF format '{other}'; expected turtle, ntriples, or rdfxml"
        ))),
    }
}

/// Locks a mutex, turning a poisoned lock into a Python exception
fn locked<'a, T>(mutex: &'a Mutex<T>, what: &str) -> PyResult<MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| PyRuntimeError::new_err(format!("{what} lock poisoned")))
}

#[pyclass(name = "KolibrieDB")]
pub struct PyKolibrieDB {
    db: Arc<Mutex<KolibrieDB>>,
}

impl std::fmt::Debug for PyKolibrieDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("KolibrieDB");
        match self.db.try_lock() {
            Ok(db) => s.field("quads", &db.quad_count()),
            Err(_) => s.field("quads", &"<locked>"),
        }
        .finish()
    }
}

#[pymethods]
impl PyKolibrieDB {
    #[new]
    fn new() -> Self {
        PyKolibrieDB {
            db: Arc::new(Mutex::new(KolibrieDB::new())),
        }
    }

    #[pyo3(signature = (subject, predicate, object))]
    pub fn add_triple(&self, subject: &str, predicate: &str, object: &str) -> PyResult<()> {
        locked(&self.db, "database")?.add_triple_parts(subject, predicate, object);
        Ok(())
    }

    fn parse_turtle(&self, turtle: &str) -> PyResult<()> {
        locked(&self.db, "database")?.parse_turtle(turtle);
        Ok(())
    }

    /// Load N-Triples from a string
    fn load_ntriples(&self, ntriples: &str) -> PyResult<()> {
        let mut db = locked(&self.db, "database")?;
        db.parse_ntriples_and_add(ntriples);
        Ok(())
    }

    /// Serialise the default graph as N-Triples
    fn to_ntriples(&self) -> PyResult<String> {
        let db = locked(&self.db, "database")?;
        Ok(db.generate_ntriples())
    }

    /// Serialise every graph as N-Quads, which unlike N-Triples keeps named graphs
    fn to_nquads(&self) -> PyResult<String> {
        let db = locked(&self.db, "database")?;
        Ok(db.generate_nquads())
    }

    /// Serialise as N-Triples, replacing blank nodes with Skolem IRIs under `base`
    fn to_ntriples_skolemised(&self, base: &str) -> PyResult<String> {
        let db = locked(&self.db, "database")?;
        Ok(db.generate_ntriples_skolemised(base))
    }

    /// Serialise as N-Quads, replacing blank nodes with Skolem IRIs under `base`
    fn to_nquads_skolemised(&self, base: &str) -> PyResult<String> {
        let db = locked(&self.db, "database")?;
        Ok(db.generate_nquads_skolemised(base))
    }

    #[pyo3(signature = (path, format=None))]
    fn load_file(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        format: Option<&str>,
    ) -> PyResult<()> {
        let os = py.import("os")?;
        let path_obj = os.getattr("fspath")?.call1((path,))?;
        let path_str: String = path_obj.extract()?;
        let load_format = normalize_load_format(&path_str, format)?;

        let mut db = locked(&self.db, "database")?;

        match load_format {
            LoadFormat::Turtle => {
                let data = std::fs::read_to_string(&path_str).map_err(|err| {
                    PyOSError::new_err(format!("Failed to read '{path_str}': {err}"))
                })?;
                db.parse_turtle(&data);
            }
            LoadFormat::NTriples => {
                let data = std::fs::read_to_string(&path_str).map_err(|err| {
                    PyOSError::new_err(format!("Failed to read '{path_str}': {err}"))
                })?;
                db.parse_ntriples_and_add(&data);
            }
            LoadFormat::RdfXml => {
                std::fs::File::open(&path_str).map_err(|err| {
                    PyOSError::new_err(format!("Failed to read '{path_str}': {err}"))
                })?;
                db.parse_rdf_from_file(&path_str);
            }
        }

        Ok(())
    }

    /// Execute a SELECT and return its rows
    fn exec_query(&self, query: &str) -> PyResult<Vec<Vec<String>>> {
        let mut db = locked(&self.db, "database")?;
        execute(query, &mut db)
            .map(|solutions| solutions.into_rows())
            .map_err(|error| PyErr::new::<PyRuntimeError, _>(error.to_string()))
    }

    /// Parses a Kolibrie `RULE` block, runs it, and returns the triples it derived
    fn add_rule(&self, rule: &str) -> PyResult<Vec<(String, String, String)>> {
        let mut db = locked(&self.db, "database")?;
        let (_rule, derived) =
            kolibrie_core::rule_execution::process_rule_definition(rule, &mut db)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let dictionary = db.dictionary();
        Ok(derived
            .into_iter()
            .map(|triple| {
                let term = |id| dictionary.decode(id).unwrap_or("").to_string();
                (
                    term(triple.subject),
                    term(triple.predicate),
                    term(triple.object),
                )
            })
            .collect())
    }

    /// Execute INSERT/DELETE DATA or a template-based INSERT/DELETE WHERE
    fn update(&self, update: &str) -> PyResult<()> {
        let mut db = locked(&self.db, "database")?;
        kolibrie_core::execute_query::execute_sparql_update(update, &mut db)
            .map(|_| ())
            .map_err(PyValueError::new_err)
    }

    /// Start building a query
    fn query(&self) -> PyQueryBuilder {
        PyQueryBuilder {
            db: Arc::clone(&self.db),
            subject: None,
            subject_like: None,
            subject_starting: None,
            subject_ending: None,
            predicate: None,
            predicate_like: None,
            predicate_starting: None,
            predicate_ending: None,
            object: None,
            object_like: None,
            object_starting: None,
            object_ending: None,
            distinct: false,
            limit: None,
            offset: None,
            sort_direction: PySortDirection::Ascending,

            window_width: None,
            window_slide: None,
            report_strategies: Vec::new(),
            tick_strategy: PyTick::TimeDriven,
            stream_operator: None,
            periodic_periods: Vec::new(),
        }
    }
}

#[pyclass(name = "SortDirection")]
#[derive(Clone, Debug)]
pub enum PySortDirection {
    Ascending,
    Descending,
}

#[pyclass(name = "StreamOperator")]
#[derive(Clone, Debug)]
pub enum PyStreamOperator {
    RSTREAM,
    ISTREAM,
    DSTREAM,
}

#[pyclass(name = "ReportStrategy")]
#[derive(Clone, Debug)]
pub enum PyReportStrategy {
    NonEmptyContent(),
    OnContentChange(),
    OnWindowClose(),
    Periodic(),
}

#[pyclass(name = "PeriodicReportStrategy")]
#[derive(Clone, Debug)]
pub struct PyPeriodicReportStrategy {
    period: usize,
}

#[pymethods]
impl PyPeriodicReportStrategy {
    #[new]
    fn new(period: usize) -> Self {
        Self { period }
    }

    #[getter]
    fn period(&self) -> usize {
        self.period
    }
}

#[pyclass(name = "Tick")]
#[derive(Clone, Debug)]
pub enum PyTick {
    TimeDriven,
    TupleDriven,
    BatchDriven,
}

#[pyclass(name = "StreamingQuery")]
pub struct PyStreamingQuery {
    config: StreamingConfig,
    results: Arc<Mutex<Vec<Vec<(String, String, String)>>>>,
}

impl std::fmt::Debug for PyStreamingQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("StreamingQuery");
        match self.results.try_lock() {
            Ok(r) => s.field("results", &r.len()),
            Err(_) => s.field("results", &"<locked>"),
        }
        .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct StreamingConfig {
    subject: Option<String>,
    subject_like: Option<String>,
    subject_starting: Option<String>,
    subject_ending: Option<String>,
    predicate: Option<String>,
    predicate_like: Option<String>,
    predicate_starting: Option<String>,
    predicate_ending: Option<String>,
    object: Option<String>,
    object_like: Option<String>,
    object_starting: Option<String>,
    object_ending: Option<String>,
    distinct: bool,
    limit: Option<usize>,
    offset: Option<usize>,

    window_width: Option<usize>,
    window_slide: Option<usize>,
    report_strategies: Vec<String>,
    tick_strategy: PyTick,
    stream_operator: Option<PyStreamOperator>,
    periodic_periods: Vec<usize>,
}

impl PyStreamingQuery {
    fn new(config: StreamingConfig) -> Self {
        Self {
            config,
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[pymethods]
impl PyStreamingQuery {
    /// Add a triple to the stream
    fn add_stream_triple(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        _timestamp: usize,
    ) -> PyResult<()> {
        let mut matches = true;

        if let Some(ref s) = self.config.subject {
            matches &= subject == s;
        }
        if let Some(ref s) = self.config.subject_like {
            matches &= subject.contains(s);
        }
        if let Some(ref s) = self.config.subject_starting {
            matches &= subject.starts_with(s);
        }
        if let Some(ref s) = self.config.subject_ending {
            matches &= subject.ends_with(s);
        }

        if matches {
            if let Some(ref p) = self.config.predicate {
                matches &= predicate == p;
            }
            if let Some(ref p) = self.config.predicate_like {
                matches &= predicate.contains(p);
            }
            if let Some(ref p) = self.config.predicate_starting {
                matches &= predicate.starts_with(p);
            }
            if let Some(ref p) = self.config.predicate_ending {
                matches &= predicate.ends_with(p);
            }
        }

        if matches {
            if let Some(ref o) = self.config.object {
                matches &= object == o;
            }
            if let Some(ref o) = self.config.object_like {
                matches &= object.contains(o);
            }
            if let Some(ref o) = self.config.object_starting {
                matches &= object.starts_with(o);
            }
            if let Some(ref o) = self.config.object_ending {
                matches &= object.ends_with(o);
            }
        }

        if matches {
            let triple = (
                subject.to_string(),
                predicate.to_string(),
                object.to_string(),
            );

            {
                let mut results = locked(&self.results, "results")?;
                if self.config.distinct {
                    let mut already_exists = false;
                    for batch in results.iter() {
                        if batch.contains(&triple) {
                            already_exists = true;
                            break;
                        }
                    }
                    if already_exists {
                        return Ok(());
                    }
                }

                let mut batch = vec![triple];

                if let Some(limit) = self.config.limit {
                    let total_count: usize = results.iter().map(|batch| batch.len()).sum();
                    if total_count >= limit {
                        return Ok(());
                    }
                    if batch.len() > limit {
                        batch.truncate(limit);
                    }
                }

                results.push(batch);
            }
        }

        Ok(())
    }

    /// Get streaming results
    fn get_stream_results(&self) -> PyResult<Vec<Vec<(String, String, String)>>> {
        {
            let results = locked(&self.results, "results")?;
            let mut current_results = results.clone();

            if let Some(offset) = self.config.offset {
                let total_triples: usize = current_results.iter().map(|batch| batch.len()).sum();
                if offset < total_triples {
                    let mut skipped = 0;
                    current_results.retain(|batch| {
                        if skipped < offset {
                            skipped += batch.len();
                            false
                        } else {
                            true
                        }
                    });
                } else {
                    current_results.clear();
                }
            }

            Ok(current_results)
        }
    }

    /// Get all accumulated streaming results
    fn get_all_stream_results(&self) -> PyResult<Vec<Vec<(String, String, String)>>> {
        self.get_stream_results()
    }

    /// Clear accumulated streaming results
    fn clear_stream_results(&self) -> PyResult<()> {
        locked(&self.results, "results")?.clear();
        Ok(())
    }

    /// Stop the streaming query
    fn stop_stream(&self) -> PyResult<()> {
        Ok(())
    }

    /// Check if currently in streaming mode
    fn is_streaming(&self) -> PyResult<bool> {
        Ok(true)
    }

    /// Get window configuration
    fn get_window_config(&self) -> PyResult<Option<(usize, usize)>> {
        Ok(match (self.config.window_width, self.config.window_slide) {
            (Some(width), Some(slide)) => Some((width, slide)),
            _ => None,
        })
    }

    /// Get configured stream operator
    fn get_stream_operator(&self) -> PyResult<Option<PyStreamOperator>> {
        Ok(self.config.stream_operator.clone())
    }

    /// Get report strategies as strings
    fn get_report_strategies(&self) -> PyResult<Vec<String>> {
        Ok(self.config.report_strategies.clone())
    }

    /// Get periodic periods
    fn get_periodic_periods(&self) -> PyResult<Vec<usize>> {
        Ok(self.config.periodic_periods.clone())
    }

    /// Get tick strategy
    fn get_tick_strategy(&self) -> PyResult<PyTick> {
        Ok(self.config.tick_strategy.clone())
    }
}

#[pyclass(name = "QueryBuilder")]
pub struct PyQueryBuilder {
    db: Arc<Mutex<KolibrieDB>>,
    subject: Option<String>,
    subject_like: Option<String>,
    subject_starting: Option<String>,
    subject_ending: Option<String>,
    predicate: Option<String>,
    predicate_like: Option<String>,
    predicate_starting: Option<String>,
    predicate_ending: Option<String>,
    object: Option<String>,
    object_like: Option<String>,
    object_starting: Option<String>,
    object_ending: Option<String>,
    distinct: bool,
    limit: Option<usize>,
    offset: Option<usize>,
    sort_direction: PySortDirection,

    window_width: Option<usize>,
    window_slide: Option<usize>,
    report_strategies: Vec<String>,
    tick_strategy: PyTick,
    stream_operator: Option<PyStreamOperator>,
    periodic_periods: Vec<usize>,
}

impl std::fmt::Debug for PyQueryBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set: Vec<&str> = [
            ("subject", self.subject.is_some()),
            ("subject_like", self.subject_like.is_some()),
            ("subject_starting", self.subject_starting.is_some()),
            ("subject_ending", self.subject_ending.is_some()),
            ("predicate", self.predicate.is_some()),
            ("predicate_like", self.predicate_like.is_some()),
            ("predicate_starting", self.predicate_starting.is_some()),
            ("predicate_ending", self.predicate_ending.is_some()),
            ("object", self.object.is_some()),
            ("object_like", self.object_like.is_some()),
            ("object_starting", self.object_starting.is_some()),
            ("object_ending", self.object_ending.is_some()),
        ]
        .iter()
        .filter(|(_, present)| *present)
        .map(|(name, _)| *name)
        .collect();
        f.debug_struct("QueryBuilder")
            .field("filters", &set)
            .field("distinct", &self.distinct)
            .field("limit", &self.limit)
            .field("offset", &self.offset)
            .field("sort_direction", &self.sort_direction)
            .finish_non_exhaustive()
    }
}

#[pymethods]
impl PyQueryBuilder {
    /// Set an exact subject filter
    fn with_subject(&self, subj: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.subject = Some(subj.to_owned());
        new_builder
    }

    /// Set an exact predicate filter
    fn with_predicate(&self, pred: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.predicate = Some(pred.to_owned());
        new_builder
    }

    /// Set an exact object filter
    fn with_object(&self, obj: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.object = Some(obj.to_owned());
        new_builder
    }

    /// Filter subjects containing a substring
    fn with_subject_like(&self, pattern: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.subject_like = Some(pattern.to_owned());
        new_builder
    }

    /// Filter subjects starting with a prefix
    fn with_subject_starting(&self, prefix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.subject_starting = Some(prefix.to_owned());
        new_builder
    }

    /// Filter subjects ending with a suffix
    fn with_subject_ending(&self, suffix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.subject_ending = Some(suffix.to_owned());
        new_builder
    }

    /// Filter predicates containing a substring
    fn with_predicate_like(&self, pattern: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.predicate_like = Some(pattern.to_owned());
        new_builder
    }

    /// Filter predicates starting with a prefix
    fn with_predicate_starting(&self, prefix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.predicate_starting = Some(prefix.to_owned());
        new_builder
    }

    /// Filter predicates ending with a suffix
    fn with_predicate_ending(&self, suffix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.predicate_ending = Some(suffix.to_owned());
        new_builder
    }

    /// Filter objects containing a substring
    fn with_object_like(&self, pattern: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.object_like = Some(pattern.to_owned());
        new_builder
    }

    /// Filter objects starting with a prefix
    fn with_object_starting(&self, prefix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.object_starting = Some(prefix.to_owned());
        new_builder
    }

    /// Filter objects ending with a suffix
    fn with_object_ending(&self, suffix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.object_ending = Some(suffix.to_owned());
        new_builder
    }

    /// Only return distinct results
    fn distinct(&self) -> Self {
        let mut new_builder = self.clone();
        new_builder.distinct = true;
        new_builder
    }

    /// Limit the number of results
    fn limit(&self, n: usize) -> Self {
        let mut new_builder = self.clone();
        new_builder.limit = Some(n);
        new_builder
    }

    /// Skip the first n results
    fn offset(&self, n: usize) -> Self {
        let mut new_builder = self.clone();
        new_builder.offset = Some(n);
        new_builder
    }

    /// Set sort direction to ascending
    fn asc(&self) -> Self {
        let mut new_builder = self.clone();
        new_builder.sort_direction = PySortDirection::Ascending;
        new_builder
    }

    /// Set sort direction to descending
    fn desc(&self) -> Self {
        let mut new_builder = self.clone();
        new_builder.sort_direction = PySortDirection::Descending;
        new_builder
    }

    /// Configure windowing for stream processing
    fn window(&self, width: usize, slide: usize) -> Self {
        let mut new_builder = self.clone();
        new_builder.window_width = Some(width);
        new_builder.window_slide = Some(slide);
        new_builder
    }

    /// Add a report strategy for window processing
    fn with_report_strategy(&self, strategy: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.report_strategies.push(strategy.to_string());
        new_builder
    }

    /// Add a periodic report strategy with a specific period
    fn with_periodic_report(&self, period: usize) -> Self {
        let mut new_builder = self.clone();
        new_builder.report_strategies.push("periodic".to_string());
        new_builder.periodic_periods.push(period);
        new_builder
    }

    /// Set the tick strategy for window processing
    fn with_tick_strategy(&self, tick: PyTick) -> Self {
        let mut new_builder = self.clone();
        new_builder.tick_strategy = tick;
        new_builder
    }

    /// Configure stream operator (RSTREAM, ISTREAM, DSTREAM)
    fn with_stream_operator(&self, operator: PyStreamOperator) -> Self {
        let mut new_builder = self.clone();
        new_builder.stream_operator = Some(operator);
        new_builder
    }

    /// Initialize streaming mode and return a streaming query handle
    fn as_stream(&self) -> PyResult<PyStreamingQuery> {
        let config = StreamingConfig {
            subject: self.subject.clone(),
            subject_like: self.subject_like.clone(),
            subject_starting: self.subject_starting.clone(),
            subject_ending: self.subject_ending.clone(),
            predicate: self.predicate.clone(),
            predicate_like: self.predicate_like.clone(),
            predicate_starting: self.predicate_starting.clone(),
            predicate_ending: self.predicate_ending.clone(),
            object: self.object.clone(),
            object_like: self.object_like.clone(),
            object_starting: self.object_starting.clone(),
            object_ending: self.object_ending.clone(),
            distinct: self.distinct,
            limit: self.limit,
            offset: self.offset,

            window_width: self.window_width,
            window_slide: self.window_slide,
            report_strategies: self.report_strategies.clone(),
            tick_strategy: self.tick_strategy.clone(),
            stream_operator: self.stream_operator.clone(),
            periodic_periods: self.periodic_periods.clone(),
        };

        Ok(PyStreamingQuery::new(config))
    }

    /// Build and execute the query, returning decoded (s,p,o) tuples
    fn get_decoded_triples(&self) -> PyResult<Vec<(String, String, String)>> {
        let db = locked(&self.db, "database")?;
        let qb = self.build_rust_query_builder(&*db);
        Ok(qb.get_decoded_triples())
    }

    /// Return just the decoded subjects
    fn get_subjects(&self) -> PyResult<Vec<String>> {
        let db = locked(&self.db, "database")?;
        let qb = self.build_rust_query_builder(&*db);
        Ok(qb.get_subjects())
    }

    /// Return just the decoded predicates
    fn get_predicates(&self) -> PyResult<Vec<String>> {
        let db = locked(&self.db, "database")?;
        let qb = self.build_rust_query_builder(&*db);
        Ok(qb.get_predicates())
    }

    /// Return just the decoded objects
    fn get_objects(&self) -> PyResult<Vec<String>> {
        let db = locked(&self.db, "database")?;
        let qb = self.build_rust_query_builder(&*db);
        Ok(qb.get_objects())
    }

    /// Count the number of matches
    fn count(&self) -> PyResult<usize> {
        let db = locked(&self.db, "database")?;
        let qb = self.build_rust_query_builder(&*db);
        Ok(qb.count())
    }

    /// Group results by subject, returning a dictionary
    fn group_by_subject(&self) -> PyResult<HashMap<String, Vec<(String, String, String)>>> {
        let results = self.get_decoded_triples()?;
        let mut groups = HashMap::new();

        for (s, p, o) in results {
            groups
                .entry(s.clone())
                .or_insert_with(Vec::new)
                .push((s, p, o));
        }
        Ok(groups)
    }

    /// Group results by predicate, returning a dictionary
    fn group_by_predicate(&self) -> PyResult<HashMap<String, Vec<(String, String, String)>>> {
        let results = self.get_decoded_triples()?;
        let mut groups = HashMap::new();

        for (s, p, o) in results {
            groups
                .entry(p.clone())
                .or_insert_with(Vec::new)
                .push((s, p, o));
        }
        Ok(groups)
    }

    /// Group results by object, returning a dictionary
    fn group_by_object(&self) -> PyResult<HashMap<String, Vec<(String, String, String)>>> {
        let results = self.get_decoded_triples()?;
        let mut groups = HashMap::new();

        for (s, p, o) in results {
            groups
                .entry(o.clone())
                .or_insert_with(Vec::new)
                .push((s, p, o));
        }
        Ok(groups)
    }
}

impl Clone for PyQueryBuilder {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            subject: self.subject.clone(),
            subject_like: self.subject_like.clone(),
            subject_starting: self.subject_starting.clone(),
            subject_ending: self.subject_ending.clone(),
            predicate: self.predicate.clone(),
            predicate_like: self.predicate_like.clone(),
            predicate_starting: self.predicate_starting.clone(),
            predicate_ending: self.predicate_ending.clone(),
            object: self.object.clone(),
            object_like: self.object_like.clone(),
            object_starting: self.object_starting.clone(),
            object_ending: self.object_ending.clone(),
            distinct: self.distinct,
            limit: self.limit,
            offset: self.offset,
            sort_direction: self.sort_direction.clone(),

            window_width: self.window_width,
            window_slide: self.window_slide,
            report_strategies: self.report_strategies.clone(),
            tick_strategy: self.tick_strategy.clone(),
            stream_operator: self.stream_operator.clone(),
            periodic_periods: self.periodic_periods.clone(),
        }
    }
}

impl PyQueryBuilder {
    /// Helper method to build the Rust QueryBuilder with all configured filters
    fn build_rust_query_builder<'a>(
        &self,
        db: &'a KolibrieDB,
    ) -> kolibrie_core::query_builder::QueryBuilder<'a> {
        use kolibrie_core::query_builder::QueryBuilder;

        let mut qb = QueryBuilder::new(db);

        if let Some(ref s) = self.subject {
            qb = qb.with_subject(s);
        }
        if let Some(ref p) = self.predicate {
            qb = qb.with_predicate(p);
        }
        if let Some(ref o) = self.object {
            qb = qb.with_object(o);
        }

        if let Some(ref s) = self.subject_like {
            qb = qb.with_subject_like(s);
        }
        if let Some(ref s) = self.subject_starting {
            qb = qb.with_subject_starting(s);
        }
        if let Some(ref s) = self.subject_ending {
            qb = qb.with_subject_ending(s);
        }

        if let Some(ref p) = self.predicate_like {
            qb = qb.with_predicate_like(p);
        }
        if let Some(ref p) = self.predicate_starting {
            qb = qb.with_predicate_starting(p);
        }
        if let Some(ref p) = self.predicate_ending {
            qb = qb.with_predicate_ending(p);
        }

        if let Some(ref o) = self.object_like {
            qb = qb.with_object_like(o);
        }
        if let Some(ref o) = self.object_starting {
            qb = qb.with_object_starting(o);
        }
        if let Some(ref o) = self.object_ending {
            qb = qb.with_object_ending(o);
        }

        if self.distinct {
            qb = qb.distinct();
        }
        if let Some(n) = self.limit {
            qb = qb.limit(n);
        }
        if let Some(n) = self.offset {
            qb = qb.offset(n);
        }

        match self.sort_direction {
            PySortDirection::Ascending => qb = qb.asc(),
            PySortDirection::Descending => qb = qb.desc(),
        }

        qb
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyKolibrieDB>()?;
    m.add_class::<PyQueryBuilder>()?;
    m.add_class::<PyStreamingQuery>()?;
    m.add_class::<PyPeriodicReportStrategy>()?;

    m.add_class::<PySortDirection>()?;
    m.add_class::<PyStreamOperator>()?;
    m.add_class::<PyReportStrategy>()?;
    m.add_class::<PyTick>()?;

    Ok(())
}
