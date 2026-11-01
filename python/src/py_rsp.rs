/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! RDF Stream Processing, for Python.
//!
//! The engine Python had was not this one. `QueryBuilder.as_stream` builds a
//! `StreamingQuery` that accumulates triples in a list and applies limit, offset and
//! distinct to them: no windows, no RSP-QL, no joining across streams. It is a filter
//! with a stream-shaped name.
//!
//! This exposes the real [`RSPEngine`]: an RSP-QL query naming its windows, a stream
//! per window, and results joined across them as the windows fire.
//!
//! # Results are collected, not called back
//!
//! The Rust engine delivers results to a consumer it invokes from its own threads.
//! Handing that a Python callable would mean taking the GIL on a thread Python does
//! not know about, for every solution. Instead the consumer appends to a buffer and
//! Python reads it, which is the same shape `StreamingQuery` already had and costs
//! nothing in a language that has to stop the world to run a callback anyway.
//!
//! Results are complete once [`stop`](PyRspEngine::stop) returns: it flushes every
//! window and waits for the coordinator, so a caller does not have to sleep and hope.

use kolibrie_core::rsp::r2r::R2ROperator;
use kolibrie_core::rsp::simple_r2r::SimpleR2R;
use kolibrie_core::rsp_engine::{RSPBuilder, RSPEngine, ResultConsumer};
use kolibrie_core::rsp::simple_r2r::QueryExecutionMode;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use shared::triple::Triple;
use std::sync::{Arc, Mutex};

/// A solution, as the pairs of variable name and term the engine produced.
type Solution = Vec<(String, String)>;

/// An RSP-QL query over one or more streams.
///
/// `unsendable` because the engine owns the window threads it started and is not
/// meant to cross between Python threads; using it from the thread that built it is
/// enforced by pyo3 rather than left to go wrong at runtime.
#[pyclass(name = "RspEngine", unsendable)]
pub struct PyRspEngine {
    /// `None` once stopped, which is what makes a second `stop` harmless and using a
    /// stopped engine an error rather than a silent no-op.
    engine: Option<RSPEngine<Triple, Solution>>,
    results: Arc<Mutex<Vec<Solution>>>,
}

#[pymethods]
impl PyRspEngine {
    /// Builds an engine from an RSP-QL query.
    ///
    /// The query names its windows and the streams they read, for example:
    ///
    /// ```text
    /// REGISTER RSTREAM <http://out/stream> AS
    /// SELECT *
    /// FROM NAMED WINDOW :w ON :temperature [RANGE 10 STEP 2]
    /// WHERE { WINDOW :w { ?s a <http://example/Reading> . } }
    /// ```
    #[new]
    fn new(rsp_ql_query: &str) -> PyResult<Self> {
        let results: Arc<Mutex<Vec<Solution>>> = Arc::new(Mutex::new(Vec::new()));
        let collected = Arc::clone(&results);
        let consumer = ResultConsumer {
            function: Arc::new(move |solution: Solution| {
                // A poisoned buffer loses this solution rather than poisoning the
                // engine's thread in turn; the caller sees a short result list, and
                // there is no Python to raise into from here.
                if let Ok(mut buffer) = collected.lock() {
                    buffer.push(solution);
                }
            }),
        };

        let engine: RSPEngine<Triple, Solution> = RSPBuilder::new()
            .add_rsp_ql_query(rsp_ql_query)
            .add_consumer(consumer)
            .add_r2r(Box::new(SimpleR2R::with_execution_mode(
                QueryExecutionMode::Volcano,
            )) as Box<dyn R2ROperator<Triple, _, Solution>>)
            .build()
            .map_err(PyValueError::new_err)?;

        Ok(PyRspEngine {
            engine: Some(engine),
            results,
        })
    }

    /// Adds N-Triples to one stream, timestamped.
    ///
    /// `timestamp` is the stream's own clock, not wall time: a window of
    /// `[RANGE 10 STEP 2]` counts in whatever unit the caller is stamping with.
    ///
    /// Returns how many triples were added, which is worth checking: text that does
    /// not parse yields none rather than raising, matching the readers elsewhere.
    fn add(&mut self, stream_iri: &str, ntriples: &str, timestamp: usize) -> PyResult<usize> {
        let engine = self.running()?;
        let triples = engine.parse_data(ntriples);
        let added = triples.len();
        for triple in triples {
            engine.add_to_stream(stream_iri, triple, timestamp);
        }
        Ok(added)
    }

    /// Flushes every window and waits for the results.
    ///
    /// Every solution the query will produce has been delivered once this returns,
    /// so there is no need to sleep before reading [`results`](Self::results).
    /// Calling it twice is harmless.
    fn stop(&mut self) -> PyResult<()> {
        if let Some(mut engine) = self.engine.take() {
            engine.stop();
        }
        Ok(())
    }

    /// The solutions produced so far, each a list of `(variable, term)` pairs.
    ///
    /// Terms are strings in their canonical spelling, so the helpers in this module
    /// -- `term_kind`, `numeric_value` and the rest -- read them.
    fn results(&self) -> PyResult<Vec<Solution>> {
        Ok(self
            .results
            .lock()
            .map_err(|_| PyRuntimeError::new_err("results lock poisoned"))?
            .clone())
    }

    /// How many solutions have been produced.
    fn result_count(&self) -> PyResult<usize> {
        Ok(self
            .results
            .lock()
            .map_err(|_| PyRuntimeError::new_err("results lock poisoned"))?
            .len())
    }

    /// Whether the engine is still accepting data.
    fn is_running(&self) -> bool {
        self.engine.is_some()
    }
}

impl PyRspEngine {
    /// The engine, or an error saying it has been stopped.
    fn running(&mut self) -> PyResult<&mut RSPEngine<Triple, Solution>> {
        self.engine
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("the engine has been stopped"))
    }
}

// Summarised rather than derived, as `PyStreamingQuery` is: the engine owns window
// threads and a store, none of which is useful in a debug line, and its buffer is
// behind a lock this must not block on.
impl std::fmt::Debug for PyRspEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut shown = formatter.debug_struct("RspEngine");
        shown.field("running", &self.engine.is_some());
        match self.results.try_lock() {
            Ok(results) => shown.field("results", &results.len()),
            Err(_) => shown.field("results", &"<locked>"),
        }
        .finish_non_exhaustive()
    }
}

/// Registers the RSP engine on the extension module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRspEngine>()?;
    Ok(())
}
