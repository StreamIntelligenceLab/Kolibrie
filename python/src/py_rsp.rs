/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! RDF Stream Processing, for Python

use kolibrie_core::rsp::r2r::R2ROperator;
use kolibrie_core::rsp::simple_r2r::QueryExecutionMode;
use kolibrie_core::rsp::simple_r2r::SimpleR2R;
use kolibrie_core::rsp_engine::{RSPBuilder, RSPEngine, ResultConsumer};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use shared::triple::Triple;
use std::sync::{Arc, Mutex};

/// A solution, as the pairs of variable name and term the engine produced
type Solution = Vec<(String, String)>;

/// An RSP-QL query over one or more streams
#[pyclass(name = "RspEngine", unsendable)]
pub struct PyRspEngine {
    engine: Option<RSPEngine<Triple, Solution>>,
    results: Arc<Mutex<Vec<Solution>>>,
}

#[pymethods]
impl PyRspEngine {
    /// Builds an engine from an RSP-QL query
    #[new]
    fn new(rsp_ql_query: &str) -> PyResult<Self> {
        let results: Arc<Mutex<Vec<Solution>>> = Arc::new(Mutex::new(Vec::new()));
        let collected = Arc::clone(&results);
        let consumer = ResultConsumer {
            function: Arc::new(move |solution: Solution| {
                if let Ok(mut buffer) = collected.lock() {
                    buffer.push(solution);
                }
            }),
        };

        let engine: RSPEngine<Triple, Solution> = RSPBuilder::new()
            .add_rsp_ql_query(rsp_ql_query)
            .add_consumer(consumer)
            .add_r2r(
                Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano))
                    as Box<dyn R2ROperator<Triple, _, Solution>>,
            )
            .build()
            .map_err(PyValueError::new_err)?;

        Ok(PyRspEngine {
            engine: Some(engine),
            results,
        })
    }

    /// Adds N-Triples to one stream, timestamped
    fn add(&mut self, stream_iri: &str, ntriples: &str, timestamp: usize) -> PyResult<usize> {
        let engine = self.running()?;
        let triples = engine.parse_data(ntriples);
        let added = triples.len();
        for triple in triples {
            engine.add_to_stream(stream_iri, triple, timestamp);
        }
        Ok(added)
    }

    /// Flushes every window and waits for the results
    fn stop(&mut self) -> PyResult<()> {
        if let Some(mut engine) = self.engine.take() {
            engine.stop();
        }
        Ok(())
    }

    /// The solutions produced so far, each a list of `(variable, term)` pairs
    fn results(&self) -> PyResult<Vec<Solution>> {
        Ok(self
            .results
            .lock()
            .map_err(|_| PyRuntimeError::new_err("results lock poisoned"))?
            .clone())
    }

    /// How many solutions have been produced
    fn result_count(&self) -> PyResult<usize> {
        Ok(self
            .results
            .lock()
            .map_err(|_| PyRuntimeError::new_err("results lock poisoned"))?
            .len())
    }

    /// Whether the engine is still accepting data
    fn is_running(&self) -> bool {
        self.engine.is_some()
    }
}

impl PyRspEngine {
    /// The engine, or an error saying it has been stopped
    fn running(&mut self) -> PyResult<&mut RSPEngine<Triple, Solution>> {
        self.engine
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("the engine has been stopped"))
    }
}

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

/// Registers the RSP engine on the extension module
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRspEngine>()?;
    Ok(())
}
