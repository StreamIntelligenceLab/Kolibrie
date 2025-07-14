/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use pyo3::prelude::*;
use kolibrie::sparql_database::SparqlDatabase;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

#[pyclass]
pub struct PySparqlDatabase {
    db: Arc<Mutex<SparqlDatabase>>,
}

#[pymethods]
impl PySparqlDatabase {
    #[new]
    fn new() -> Self {
        PySparqlDatabase {
            db: Arc::new(Mutex::new(SparqlDatabase::new())),
        }
    }

    #[pyo3(signature = (subject, predicate, object))]
    pub fn add_triple(&self, subject: &str, predicate: &str, object: &str) {
        if let Ok(mut db) = self.db.lock() {
            db.add_triple_parts(subject, predicate, object);
        }
    }

    /// Start building a query.
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
            
            // RSP fields
            window_width: None,
            window_slide: None,
            report_strategies: Vec::new(),
            tick_strategy: PyTick::TimeDriven,
            stream_operator: None,
            periodic_periods: Vec::new(), // <- This was missing!
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub enum PySortDirection {
    Ascending,
    Descending,
}

#[pyclass]
#[derive(Clone)]
pub enum PyStreamOperator {
    RSTREAM,
    ISTREAM,
    DSTREAM,
}

// Simple enum for report strategies
#[pyclass]
#[derive(Clone)]
pub enum PyReportStrategy {
    NonEmptyContent(),
    OnContentChange(),
    OnWindowClose(),
    Periodic(),
}

#[pyclass]
#[derive(Clone)]
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

#[pyclass]
#[derive(Clone)]
pub enum PyTick {
    TimeDriven,
    TupleDriven,
    BatchDriven,
}

// Simplified streaming interface
#[pyclass]
pub struct PyStreamingQuery {
    config: StreamingConfig,
    // Store accumulated results
    results: Arc<Mutex<Vec<Vec<(String, String, String)>>>>,
}

#[derive(Clone)]
struct StreamingConfig {
    // Query filters
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
    
    // RSP config
    window_width: Option<usize>,
    window_slide: Option<usize>,
    report_strategies: Vec<String>, // Store as strings instead
    tick_strategy: PyTick,
    stream_operator: Option<PyStreamOperator>,
    periodic_periods: Vec<usize>, // Store periodic periods separately
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
    fn add_stream_triple(&self, subject: &str, predicate: &str, object: &str, _timestamp: usize) -> PyResult<()> {
        let mut matches = true;
        
        // Apply subject filters
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
        
        // Apply predicate filters
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
        
        // Apply object filters
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
            let triple = (subject.to_string(), predicate.to_string(), object.to_string());
            
            if let Ok(mut results) = self.results.lock() {
                // Apply distinct filter if configured
                if self.config.distinct {
                    // Check if this triple already exists in any result batch
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
                
                // Create a new result batch with this triple
                let mut batch = vec![triple];
                
                // Apply limit if configured
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
        if let Ok(results) = self.results.lock() {
            let mut current_results = results.clone();
            
            // Apply offset if configured
            if let Some(offset) = self.config.offset {
                let total_triples: usize = current_results.iter().map(|batch| batch.len()).sum();
                if offset < total_triples {
                    // For simplicity, we'll skip entire batches if needed
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
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to acquire results lock"))
        }
    }

    /// Get all accumulated streaming results
    fn get_all_stream_results(&self) -> PyResult<Vec<Vec<(String, String, String)>>> {
        self.get_stream_results()
    }

    /// Clear accumulated streaming results
    fn clear_stream_results(&self) -> PyResult<()> {
        if let Ok(mut results) = self.results.lock() {
            results.clear();
            Ok(())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to acquire results lock"))
        }
    }

    /// Stop the streaming query
    fn stop_stream(&self) -> PyResult<()> {
        // In a full implementation, this would stop background processing
        Ok(())
    }

    /// Check if currently in streaming mode
    fn is_streaming(&self) -> PyResult<bool> {
        Ok(true) // Always true for streaming queries
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

#[pyclass]
pub struct PyQueryBuilder {
    db: Arc<Mutex<SparqlDatabase>>,
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
    
    // RSP fields (simplified)
    window_width: Option<usize>,
    window_slide: Option<usize>,
    report_strategies: Vec<String>, // Store as strings
    tick_strategy: PyTick,
    stream_operator: Option<PyStreamOperator>,
    periodic_periods: Vec<usize>, // Store periodic periods separately
}

#[pymethods]
impl PyQueryBuilder {
    /// Set an exact subject filter.
    fn with_subject(&self, subj: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.subject = Some(subj.to_owned());
        new_builder
    }

    /// Set an exact predicate filter.
    fn with_predicate(&self, pred: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.predicate = Some(pred.to_owned());
        new_builder
    }

    /// Set an exact object filter.
    fn with_object(&self, obj: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.object = Some(obj.to_owned());
        new_builder
    }
    
    /// Filter subjects containing a substring.
    fn with_subject_like(&self, pattern: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.subject_like = Some(pattern.to_owned());
        new_builder
    }

    /// Filter subjects starting with a prefix.
    fn with_subject_starting(&self, prefix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.subject_starting = Some(prefix.to_owned());
        new_builder
    }

    /// Filter subjects ending with a suffix.
    fn with_subject_ending(&self, suffix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.subject_ending = Some(suffix.to_owned());
        new_builder
    }

    /// Filter predicates containing a substring.
    fn with_predicate_like(&self, pattern: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.predicate_like = Some(pattern.to_owned());
        new_builder
    }

    /// Filter predicates starting with a prefix.
    fn with_predicate_starting(&self, prefix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.predicate_starting = Some(prefix.to_owned());
        new_builder
    }

    /// Filter predicates ending with a suffix.
    fn with_predicate_ending(&self, suffix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.predicate_ending = Some(suffix.to_owned());
        new_builder
    }

    /// Filter objects containing a substring.
    fn with_object_like(&self, pattern: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.object_like = Some(pattern.to_owned());
        new_builder
    }

    /// Filter objects starting with a prefix.
    fn with_object_starting(&self, prefix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.object_starting = Some(prefix.to_owned());
        new_builder
    }

    /// Filter objects ending with a suffix.
    fn with_object_ending(&self, suffix: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.object_ending = Some(suffix.to_owned());
        new_builder
    }
    
    /// Only return distinct results.
    fn distinct(&self) -> Self {
        let mut new_builder = self.clone();
        new_builder.distinct = true;
        new_builder
    }

    /// Limit the number of results.
    fn limit(&self, n: usize) -> Self {
        let mut new_builder = self.clone();
        new_builder.limit = Some(n);
        new_builder
    }

    /// Skip the first n results.
    fn offset(&self, n: usize) -> Self {
        let mut new_builder = self.clone();
        new_builder.offset = Some(n);
        new_builder
    }

    /// Set sort direction to ascending.
    fn asc(&self) -> Self {
        let mut new_builder = self.clone();
        new_builder.sort_direction = PySortDirection::Ascending;
        new_builder
    }

    /// Set sort direction to descending.
    fn desc(&self) -> Self {
        let mut new_builder = self.clone();
        new_builder.sort_direction = PySortDirection::Descending;
        new_builder
    }
    
    /// Configure windowing for stream processing.
    fn window(&self, width: usize, slide: usize) -> Self {
        let mut new_builder = self.clone();
        new_builder.window_width = Some(width);
        new_builder.window_slide = Some(slide);
        new_builder
    }

    /// Add a report strategy for window processing.
    fn with_report_strategy(&self, strategy: &str) -> Self {
        let mut new_builder = self.clone();
        new_builder.report_strategies.push(strategy.to_string());
        new_builder
    }

    /// Add a periodic report strategy with a specific period.
    fn with_periodic_report(&self, period: usize) -> Self {
        let mut new_builder = self.clone();
        new_builder.report_strategies.push("periodic".to_string());
        new_builder.periodic_periods.push(period);
        new_builder
    }

    /// Set the tick strategy for window processing.
    fn with_tick_strategy(&self, tick: PyTick) -> Self {
        let mut new_builder = self.clone();
        new_builder.tick_strategy = tick;
        new_builder
    }

    /// Configure stream operator (RSTREAM, ISTREAM, DSTREAM).
    fn with_stream_operator(&self, operator: PyStreamOperator) -> Self {
        let mut new_builder = self.clone();
        new_builder.stream_operator = Some(operator);
        new_builder
    }

    /// Initialize streaming mode and return a streaming query handle.
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
    
    /// Build and execute the query, returning decoded (s,p,o) tuples.
    fn get_decoded_triples(&self) -> Vec<(String,String,String)> {
        if let Ok(db) = self.db.lock() {
            let qb = self.build_rust_query_builder(&*db);
            qb.get_decoded_triples()
        } else {
            Vec::new() // Return empty vector if lock fails
        }
    }

    /// Return just the decoded subjects.
    fn get_subjects(&self) -> Vec<String> {
        if let Ok(db) = self.db.lock() {
            let qb = self.build_rust_query_builder(&*db);
            qb.get_subjects()
        } else {
            Vec::new()
        }
    }

    /// Return just the decoded predicates.
    fn get_predicates(&self) -> Vec<String> {
        if let Ok(db) = self.db.lock() {
            let qb = self.build_rust_query_builder(&*db);
            qb.get_predicates()
        } else {
            Vec::new()
        }
    }

    /// Return just the decoded objects.
    fn get_objects(&self) -> Vec<String> {
        if let Ok(db) = self.db.lock() {
            let qb = self.build_rust_query_builder(&*db);
            qb.get_objects()
        } else {
            Vec::new()
        }
    }

    /// Count the number of matches.
    fn count(&self) -> usize {
        if let Ok(db) = self.db.lock() {
            let qb = self.build_rust_query_builder(&*db);
            qb.count()
        } else {
            0 // Return 0 if lock fails
        }
    }

    /// Group results by subject, returning a dictionary.
    fn group_by_subject(&self) -> HashMap<String, Vec<(String, String, String)>> {
        let results = self.get_decoded_triples();
        let mut groups = HashMap::new();
        
        for (s, p, o) in results {
            groups.entry(s.clone()).or_insert_with(Vec::new).push((s, p, o));
        }
        groups
    }

    /// Group results by predicate, returning a dictionary.
    fn group_by_predicate(&self) -> HashMap<String, Vec<(String, String, String)>> {
        let results = self.get_decoded_triples();
        let mut groups = HashMap::new();
        
        for (s, p, o) in results {
            groups.entry(p.clone()).or_insert_with(Vec::new).push((s, p, o));
        }
        groups
    }

    /// Group results by object, returning a dictionary.
    fn group_by_object(&self) -> HashMap<String, Vec<(String, String, String)>> {
        let results = self.get_decoded_triples();
        let mut groups = HashMap::new();
        
        for (s, p, o) in results {
            groups.entry(o.clone()).or_insert_with(Vec::new).push((s, p, o));
        }
        groups
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
            
            // RSP fields
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
    fn build_rust_query_builder<'a>(&self, db: &'a SparqlDatabase) -> kolibrie::query_builder::QueryBuilder<'a> {
        use kolibrie::query_builder::QueryBuilder;
        
        let mut qb = QueryBuilder::new(db);
        
        // Apply exact filters
        if let Some(ref s) = self.subject { qb = qb.with_subject(s); }
        if let Some(ref p) = self.predicate { qb = qb.with_predicate(p); }
        if let Some(ref o) = self.object { qb = qb.with_object(o); }
        
        // Apply pattern filters
        if let Some(ref s) = self.subject_like { qb = qb.with_subject_like(s); }
        if let Some(ref s) = self.subject_starting { qb = qb.with_subject_starting(s); }
        if let Some(ref s) = self.subject_ending { qb = qb.with_subject_ending(s); }
        
        if let Some(ref p) = self.predicate_like { qb = qb.with_predicate_like(p); }
        if let Some(ref p) = self.predicate_starting { qb = qb.with_predicate_starting(p); }
        if let Some(ref p) = self.predicate_ending { qb = qb.with_predicate_ending(p); }
        
        if let Some(ref o) = self.object_like { qb = qb.with_object_like(o); }
        if let Some(ref o) = self.object_starting { qb = qb.with_object_starting(o); }
        if let Some(ref o) = self.object_ending { qb = qb.with_object_ending(o); }
        
        // Apply modifiers
        if self.distinct { qb = qb.distinct(); }
        if let Some(n) = self.limit { qb = qb.limit(n); }
        if let Some(n) = self.offset { qb = qb.offset(n); }
        
        // Apply sort direction
        match self.sort_direction {
            PySortDirection::Ascending => qb = qb.asc(),
            PySortDirection::Descending => qb = qb.desc(),
        }
        
        qb
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register classes
    m.add_class::<PySparqlDatabase>()?;
    m.add_class::<PyQueryBuilder>()?;
    m.add_class::<PyStreamingQuery>()?;
    m.add_class::<PyPeriodicReportStrategy>()?;
    
    // Register enums
    m.add_class::<PySortDirection>()?;
    m.add_class::<PyStreamOperator>()?;
    m.add_class::<PyReportStrategy>()?;
    m.add_class::<PyTick>()?;
    
    Ok(())
}