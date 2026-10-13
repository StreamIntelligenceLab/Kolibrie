/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Model execution for Kolibrie: a Python bridge and a native MLP.
//!
//! Two unrelated paths live here, and which one a query takes is decided in
//! `kolibrie`, not here:
//!
//! - [`MLHandler`] hosts predictors written in Python — scikit-learn and the like
//!   — over PyO3. The model is a Python object; this crate marshals rows across
//!   and marshals predictions back.
//! - [`candle_model`] is a small feed-forward network implemented in Rust, used
//!   by neural-relation training where the gradient comes from outside the
//!   network rather than from a loss over labels.
//!
//! # This crate grants no authority
//!
//! Nothing here checks whether a model is allowed to run. Loading a model by path
//! loads it, and the path is taken at face value. The trust decisions — which
//! models are approved, which artifacts match their pinned digest, whether
//! training may write at all — belong to
//! `kolibrie`'s `ml_policy`, and every caller is expected to have been through
//! it. Treat the entry points here as the mechanism, not the gate.

use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict, PyList},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

fn validate_py_module_name(name: &str) -> PyResult<()> {
    let mut bytes = name.bytes();
    if name.len() > 64
        || !bytes
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        || !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "invalid model name",
        ));
    }
    Ok(())
}

/// A small feed-forward network, for gradients that arrive from outside it.
pub mod candle_model;
pub use candle_model::{MlpNeuralPredicate, OutputType};

#[derive(Debug, Serialize, Deserialize)]
/// What one Python-hosted prediction call returned.
pub struct MLPredictionResult {
    /// One prediction per input row, in input order.
    pub predictions: Vec<f64>,
    /// Class probabilities, when the model exposes them. A model without a
    /// `predict_proba` equivalent yields `None`, which is not an error.
    pub probabilities: Option<Vec<f64>>,
    /// Per-feature importances, when the model exposes them.
    pub feature_importance: Option<Vec<f64>>,
    /// Metrics for the model used, read from its schema file rather than
    /// measured here — so they describe how it was trained, not this call.
    pub performance_metrics: ModelPerformanceMetrics,
    /// Where this call's time went.
    pub timing: PredictionTiming,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
/// Where one prediction call spent its time, in seconds.
///
/// Split around the model call because marshalling rows into Python and results
/// back out often costs more than the inference on a small batch.
pub struct PredictionTiming {
    /// Preparing the input, up to the model call.
    pub preprocessing_time: f64,
    /// The model call itself.
    pub actual_prediction_time: f64,
    /// Converting the results back.
    pub postprocessing_time: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
/// A model's own account of itself, as declared in its schema file.
///
/// These are read from the model's Turtle sidecar, not measured by this crate, so
/// they are claims about past training runs. [`Default`] is all zeros and no
/// scores, which is also what a model whose schema could not be parsed ends up
/// with — see [`MLHandler::compare_models`] for why that matters.
pub struct ModelPerformanceMetrics {
    /// Seconds spent training.
    pub training_time: f64,
    /// Seconds per prediction call.
    pub prediction_time: f64,
    /// Resident memory in megabytes.
    pub memory_usage_mb: f64,
    /// CPU utilisation as a percentage.
    pub cpu_usage_percent: f64,
    /// Classification accuracy, if the schema declares one.
    pub accuracy: Option<f64>,
    /// Coefficient of determination, for regressors.
    pub r2_score: Option<f64>,
    /// Mean squared error, for regressors.
    pub mse: Option<f64>,
}

/// Python-hosted predictors, loaded once and kept.
///
/// The two caches are populated independently, which is the point:
/// [`load_model_with_schema`](Self::load_model_with_schema) records a model's
/// declared metrics without importing it, so several candidates can be compared
/// before any Python is executed. Only [`load_model`](Self::load_model) imports.
pub struct MLHandler {
    /// Imported Python predictor objects, by model name. Holding a `PyObject`
    /// means this cache keeps the interpreter's objects alive.
    pub model_cache: BTreeMap<String, PyObject>,
    /// Declared metrics by model name, including for models never imported.
    pub schema_cache: BTreeMap<String, ModelPerformanceMetrics>,
    /// Model chosen by the last [`compare_models`](Self::compare_models) call.
    ///
    /// Once set, this **overrides the model name passed to**
    /// [`predict`](Self::predict). Nothing clears it.
    pub best_model: Option<String>,
}

impl MLHandler {
    /// An empty handler. Does not initialise Python; the first call that needs
    /// the interpreter acquires the GIL itself.
    pub fn new() -> PyResult<Self> {
        Ok(MLHandler {
            model_cache: BTreeMap::new(),
            schema_cache: BTreeMap::new(),
            best_model: None,
        })
    }

    fn parse_schema_file(&self, schema_file_path: &str) -> PyResult<ModelPerformanceMetrics> {
        let mut metrics = ModelPerformanceMetrics::default();

        Python::with_gil(|py| {
            let rdflib = py.import("rdflib")?;
            let graph = rdflib.call_method0("Graph")?;

            // Parse the TTL schema file
            graph.call_method1("parse", (schema_file_path, "turtle"))?;

            // Create a SPARQL query to extract performance metrics
            let query = r#"
                PREFIX mls: <http://www.w3.org/ns/mls#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                SELECT ?label (xsd:float(?rawValue) as ?value)
                WHERE {
                    ?eval a mls:ModelEvaluation ;
                        mls:specifiedBy ?measure ;
                        mls:hasValue ?rawValue .
                    ?measure rdfs:label ?label .
                }
            "#;

            let results = graph.call_method1("query", (query,))?;

            for row in results.try_iter()? {
                let row = row?;
                let label: String = row.get_item(0)?.extract()?;
                // Convert the value to a string first and then parse it as f64
                let value_obj = row.get_item(1)?;
                let value_str: String = value_obj.str()?.extract()?;
                // Try to parse it as a float
                if let Ok(value) = value_str.parse::<f64>() {
                    match label.as_str() {
                        "training_time" => metrics.training_time = value,
                        "prediction_time" => metrics.prediction_time = value,
                        "memory_usage_mb" => metrics.memory_usage_mb = value,
                        "cpu_usage_percent" => metrics.cpu_usage_percent = value,
                        "mse" => metrics.mse = Some(value),
                        "r2" => metrics.r2_score = Some(value),
                        _ => {}
                    }
                }
            }

            // Extract the CPU time from the run quality
            let cpu_query = r#"
                PREFIX mls: <http://www.w3.org/ns/mls#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT (xsd:float(?rawValue) as ?value)
                WHERE {
                    ?run a mls:Run ;
                        mls:hasQuality ?quality .
                    ?quality rdfs:label "CPU Time Used" ;
                            mls:hasValue ?rawValue .
                }
            "#;

            let cpu_results = graph.call_method1("query", (cpu_query,))?;

            for row in cpu_results.try_iter()? {
                let row = row?;
                // Convert the value to a string first and then parse it as f64
                let value_obj = row.get_item(0)?;
                let value_str: String = value_obj.str()?.extract()?;
                if let Ok(value) = value_str.parse::<f64>() {
                    metrics.training_time = value; // Use CPU time as training time if not already set
                }
            }

            Ok(metrics)
        })
    }

    /// Records a model's declared metrics without importing it.
    ///
    /// The schema path is derived from `model_path` by replacing `.pkl` with
    /// `.ttl` — every occurrence, so a path with `.pkl` in a directory name is
    /// rewritten there too.
    ///
    /// A schema that cannot be parsed is **not** an error: it logs
    /// `KOLIBRIE_OPERATION_FAILED` to stderr and caches
    /// [`ModelPerformanceMetrics::default`] instead. The model is then recorded as
    /// costing nothing, which [`compare_models`](Self::compare_models) reads as
    /// the cheapest possible candidate.
    pub fn load_model_with_schema(
        &mut self,
        model_name: &str,
        model_path: &str,
    ) -> PyResult<ModelPerformanceMetrics> {
        // Get the TTL file path by replacing .pkl extension with .ttl
        let schema_file_path = model_path.replace(".pkl", ".ttl");

        // Parse the schema file to get performance metrics directly from TTL
        let metrics = match self.parse_schema_file(&schema_file_path) {
            Ok(m) => m,
            Err(_e) => {
                eprintln!("KOLIBRIE_OPERATION_FAILED");
                // Create default metrics if TTL parsing fails
                ModelPerformanceMetrics::default()
            }
        };

        // Store metrics in cache without loading the model yet
        self.schema_cache
            .insert(model_name.to_string(), metrics.clone());

        Ok(metrics)
    }

    /// Imports a Python predictor and caches the resulting object.
    ///
    /// Both `model_name` and `module_name` must be ASCII identifiers of at most
    /// 64 characters, because they are interpolated into a Python import: the
    /// model directory's parent is prepended to `sys.path`, so an unchecked name
    /// would choose what gets imported.
    ///
    /// Grants no authority of its own — see the crate documentation. A caller that
    /// has not been through `kolibrie`'s `ml_policy` is importing whatever the
    /// path points at.
    pub fn load_model(
        &mut self,
        model_name: &str,
        model_path: &str,
        module_name: Option<&str>,
    ) -> PyResult<()> {
        validate_py_module_name(model_name)?;
        validate_py_module_name(module_name.unwrap_or("predictor"))?;
        Python::with_gil(|py| {
            let sys = py.import("sys")?;
            let paths = sys.getattr("path")?;
            let model_path = Path::new(model_path);
            let src_dir = model_path.parent().and_then(Path::parent).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("invalid local artifact location")
            })?;
            let current_path: Vec<String> = paths.extract()?;
            let src_dir_str = src_dir.to_str().ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("invalid local artifact location")
            })?;

            if !current_path.contains(&src_dir_str.to_string()) {
                paths.call_method1("insert", (0, src_dir_str))?;
            }

            let builtins = py.import("builtins")?;
            let pickle = py.import("pickle")?;
            let importlib = py.import("importlib")?;
            let globals = PyDict::new(py);

            globals.set_item("__builtins__", builtins.clone())?;
            globals.set_item("pickle", pickle)?;
            globals.set_item("importlib", &importlib)?;
            globals.set_item("__name__", "__main__")?;

            let actual_module_name = module_name.unwrap_or("predictor").trim_end_matches(".py");
            let imported = importlib.call_method1("import_module", (actual_module_name,))?;
            for name in builtins.call_method1("dir", (&imported,))?.try_iter()? {
                let name: String = name?.extract()?;
                if !name.starts_with('_') {
                    globals.set_item(&name, imported.getattr(name.as_str())?)?;
                }
            }
            let bytes = std::fs::read(model_path)
                .map_err(|_| pyo3::exceptions::PyIOError::new_err("model read failed"))?;
            let model = py
                .import("pickle")?
                .call_method1("loads", (PyBytes::new(py, &bytes),))?;

            self.model_cache
                .insert(model_name.to_string(), model.into());
            Ok(())
        })
    }

    /// Predict from host-verified module and artifact bytes
    pub fn predict_approved(
        module_name: &str,
        module_path: &str,
        module_bytes: &[u8],
        artifact_bytes: &[u8],
        input: &[Vec<f64>],
    ) -> PyResult<Vec<f64>> {
        validate_py_module_name(module_name)?;
        Python::with_gil(|py| {
            let sys = py.import("sys")?;
            let modules = sys.getattr("modules")?.downcast_into::<PyDict>()?;
            if let Some(existing) = modules.get_item(module_name)? {
                let origin: String = existing
                    .getattr("__file__")
                    .and_then(|v| v.extract())
                    .map_err(|_| {
                        pyo3::exceptions::PyPermissionError::new_err("model module conflict")
                    })?;
                let source: Vec<u8> = existing
                    .getattr("__kolibrie_verified_source__")
                    .and_then(|v| v.extract())
                    .map_err(|_| {
                        pyo3::exceptions::PyPermissionError::new_err("model module conflict")
                    })?;
                if origin != module_path || source != module_bytes {
                    return Err(pyo3::exceptions::PyPermissionError::new_err(
                        "model module conflict",
                    ));
                }
            } else {
                let module = py
                    .import("types")?
                    .call_method1("ModuleType", (module_name,))?;
                module.setattr("__file__", module_path)?;
                module.setattr("__package__", "")?;
                let namespace = module.getattr("__dict__")?;
                modules.set_item(module_name, &module)?;
                let result = (|| -> PyResult<()> {
                    let builtins = py.import("builtins")?;
                    let code = builtins.call_method1(
                        "compile",
                        (PyBytes::new(py, module_bytes), module_path, "exec"),
                    )?;
                    builtins.call_method1("exec", (code, &namespace, &namespace))?;
                    module.setattr(
                        "__kolibrie_verified_source__",
                        PyBytes::new(py, module_bytes),
                    )?;
                    Ok(())
                })();
                if let Err(error) = result {
                    modules.del_item(module_name)?;
                    return Err(error);
                }
            }
            let registered = modules.get_item(module_name)?.ok_or_else(|| {
                pyo3::exceptions::PyPermissionError::new_err("model module conflict")
            })?;
            let actual_origin: String = registered
                .getattr("__file__")
                .and_then(|v| v.extract())
                .map_err(|_| {
                    pyo3::exceptions::PyPermissionError::new_err("model module conflict")
                })?;
            if actual_origin != module_path {
                return Err(pyo3::exceptions::PyPermissionError::new_err(
                    "model module conflict",
                ));
            }
            let model = py
                .import("pickle")?
                .call_method1("loads", (PyBytes::new(py, artifact_bytes),))?;
            let rows = PyList::new(py, input.iter())?;
            model.call_method1("predict", (rows,))?.extract()
        })
    }

    // Modified to prioritize lowest resource usage
    /// Picks the cheapest of `model_names` and remembers it.
    ///
    /// "Best" means least resource-hungry, not most accurate: the score is
    /// `0.5 * cpu_usage_percent + 0.4 * memory_usage_mb + 0.1 * prediction_time`,
    /// and no accuracy field enters it. Note also that the two terms that dominate
    /// are in different units, so the weights are not comparable across models
    /// measured on different machines.
    ///
    /// Two consequences worth knowing before relying on it:
    ///
    /// - A model whose schema failed to parse scores zero on every term and
    ///   therefore always wins. See
    ///   [`load_model_with_schema`](Self::load_model_with_schema).
    /// - The winner is stored in [`best_model`](Self::best_model) and from then on
    ///   overrides the model name given to [`predict`](Self::predict). Calling
    ///   this is not a read-only comparison.
    ///
    /// Models absent from the schema cache are skipped; if none of them is
    /// present, the first name is returned unchanged. `None` only for an empty
    /// slice.
    pub fn compare_models<'a>(&mut self, model_names: &[&'a str]) -> Option<&'a str> {
        if model_names.is_empty() {
            return None;
        }

        // Default to first model
        let mut best_model = model_names[0];
        let mut best_score = std::f64::MAX;

        for &model_name in model_names {
            if let Some(metrics) = self.schema_cache.get(model_name) {
                // Prioritize CPU and memory usage
                let cpu_weight = 0.5;
                let memory_weight = 0.4;
                let time_weight = 0.1;

                let resource_score = cpu_weight * metrics.cpu_usage_percent
                    + memory_weight * metrics.memory_usage_mb
                    + time_weight * metrics.prediction_time;

                if resource_score < best_score {
                    best_score = resource_score;
                    best_model = model_name;
                }
            }
        }

        // Store the best model name for future use
        self.best_model = Some(best_model.to_string());

        Some(best_model)
    }

    /// Runs a cached predictor over `input_data`.
    ///
    /// `model_name` is **ignored** when
    /// [`best_model`](Self::best_model) is set, which
    /// [`compare_models`](Self::compare_models) does as a side effect. On a
    /// handler that has compared models, this always predicts with the winner.
    ///
    /// # Errors
    ///
    /// If the model that would be used has not been imported by
    /// [`load_model`](Self::load_model). A cached schema is not enough.
    pub fn predict(
        &self,
        model_name: &str,
        input_data: Vec<Vec<f64>>,
    ) -> PyResult<MLPredictionResult> {
        let actual_model_name = if let Some(ref best_model) = self.best_model {
            best_model
        } else {
            model_name
        };

        if !self.model_cache.contains_key(actual_model_name) {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Model {} not found in cache. Call load_model first.",
                actual_model_name
            )));
        }

        let python_start = Instant::now();

        let result = Python::with_gil(|py| {
            let preprocessing_start = Instant::now();

            let model = self.model_cache.get(actual_model_name).unwrap();

            // Convert input data to Python list
            let rows: PyResult<Vec<PyObject>> = input_data
                .iter()
                .map(|row| {
                    let py_row = PyList::new(py, row.iter())?;
                    Ok(py_row.into())
                })
                .collect();
            let rows = rows?;
            let py_input = PyList::new(py, rows)?;

            let preprocessing_time = preprocessing_start.elapsed().as_secs_f64();

            // Actual ML prediction
            let prediction_start = Instant::now();
            let predictions = model.call_method1(py, "predict", (py_input.clone(),))?;
            let predictions: Vec<f64> = predictions.extract(py)?;
            let prediction_time = prediction_start.elapsed().as_secs_f64();

            // Postprocessing
            let postprocessing_start = Instant::now();

            let probabilities = model
                .call_method1(py, "predict_proba", (py_input,))
                .and_then(|probs| probs.extract::<Vec<f64>>(py))
                .ok();

            let feature_importance = PyResult::Ok(())
                .and_then(|_| model.getattr(py, "model"))
                .and_then(|model_obj| model_obj.getattr(py, "feature_importances_"))
                .and_then(|fi| fi.extract::<Vec<f64>>(py))
                .ok();

            let performance_metrics = match self.schema_cache.get(actual_model_name) {
                Some(metrics) => metrics.clone(),
                None => ModelPerformanceMetrics::default(),
            };

            let postprocessing_time = postprocessing_start.elapsed().as_secs_f64();

            let _total_python_time = python_start.elapsed().as_secs_f64();

            Ok(MLPredictionResult {
                predictions,
                probabilities,
                feature_importance,
                performance_metrics,
                timing: PredictionTiming {
                    preprocessing_time,
                    actual_prediction_time: prediction_time,
                    postprocessing_time,
                },
            })
        });

        result
    }

    // Utility function to discover and load all models and their TTL schemas at once
    /// Scans `model_dir` for predictors and records their schemas.
    ///
    /// Picks up files named `<prefix>_predictor.pkl` only, and names each model
    /// `<prefix>_model` after the text before the first underscore — so
    /// `rf_predictor.pkl` becomes `rf_model`, and two files sharing a prefix
    /// collide on one name.
    ///
    /// Schemas only: nothing is imported, and a directory that cannot be read
    /// yields an empty list rather than an error.
    pub fn discover_and_load_models(
        &mut self,
        model_dir: &Path,
        model_module: &str,
    ) -> PyResult<Vec<String>> {
        let mut model_ids = Vec::new();

        if let Ok(entries) = std::fs::read_dir(model_dir) {
            // First pass: Only load schemas from TTL files without loading models
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.is_file() && path.extension().map_or(false, |ext| ext == "pkl") {
                    if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if file_stem.ends_with("_predictor") {
                            // Get model type prefix from filename (rf_, gb_, lr_, etc.)
                            let model_type = file_stem.split('_').next().unwrap_or("unknown");
                            let model_id = format!("{}_model", model_type);

                            match self.load_model_with_schema(&model_id, path.to_str().unwrap()) {
                                Ok(_) => {
                                    model_ids.push(model_id);
                                }
                                Err(_e) => {
                                    eprintln!("KOLIBRIE_OPERATION_FAILED");
                                }
                            }
                        }
                    }
                }
            }

            // Compare models to find the one with lowest resource usage
            let model_id_refs: Vec<&str> = model_ids.iter().map(|s| s.as_str()).collect();
            if let Some(best_model) = self.compare_models(&model_id_refs) {
                // Second pass: Only load the best model to save resources
                for entry in std::fs::read_dir(model_dir).unwrap().filter_map(Result::ok) {
                    let path = entry.path();
                    if path.is_file() && path.extension().map_or(false, |ext| ext == "pkl") {
                        if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                            let model_type = file_stem.split('_').next().unwrap_or("unknown");
                            let model_id = format!("{}_model", model_type);

                            // Only load the best model
                            if model_id == best_model {
                                match self.load_model(
                                    &model_id,
                                    path.to_str().unwrap(),
                                    Some(model_module),
                                ) {
                                    Ok(_) => {
                                        self.best_model = Some(model_id.clone());
                                    }
                                    Err(_e) => {
                                        eprintln!("KOLIBRIE_OPERATION_FAILED");
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(model_ids)
    }
}

/// Generate models only for trusted-local execution
pub fn generate_ml_models(model_dir: &Path, model: &str) -> Result<(), Box<dyn std::error::Error>> {
    validate_py_module_name(model)?;
    let output = model_dir.canonicalize()?;
    let script = output
        .parent()
        .ok_or("missing script directory")?
        .join(format!("{model}.py"))
        .canonicalize()?;
    let status = std::process::Command::new(if cfg!(windows) { "python" } else { "python3" })
        .arg(script)
        .current_dir(&output)
        .env("KOLIBRIE_TRAINING_OUTPUT", &output)
        .stdin(std::process::Stdio::null())
        .status()?;
    if !status.success() {
        return Err("local model generation failed".into());
    }
    Ok(())
}

#[cfg(test)]
mod policy_tests {
    use super::*;

    #[test]
    fn invalid_module_names_are_rejected_at_the_crate_boundary() {
        for name in [
            "",
            "1model",
            "m.py",
            "../m",
            "m');raise RuntimeError()#",
            "with space",
        ] {
            assert!(validate_py_module_name(name).is_err());
        }
        assert!(validate_py_module_name("valid_model_1").is_ok());
    }

    #[test]
    fn approved_module_uses_supplied_bytes_and_rejects_origin_conflicts() {
        let source = b"class Model:\n    def predict(self, rows):\n        return [row[0]+1 for row in rows]\n";
        let pickle = b"ckolibrie_byte_loading_test\nModel\n)R.";
        assert_eq!(
            MLHandler::predict_approved(
                "kolibrie_byte_loading_test",
                "/approved/test.py",
                source,
                pickle,
                &[vec![2.0]]
            )
            .unwrap(),
            vec![3.0]
        );
        assert!(MLHandler::predict_approved(
            "kolibrie_byte_loading_test",
            "/unapproved/test.py",
            source,
            pickle,
            &[vec![2.0]]
        )
        .is_err());
        assert!(MLHandler::predict_approved(
            "kolibrie_byte_loading_test",
            "/approved/test.py",
            b"raise RuntimeError('CANARY')",
            pickle,
            &[vec![2.0]]
        )
        .is_err());
    }
}
