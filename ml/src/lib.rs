/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::ffi::CString;
use std::path::Path;
use std::time::Instant;
use serde::{Serialize, Deserialize};
use pyo3::{prelude::*, types::{PyDict, PyList}};

#[derive(Debug, Serialize, Deserialize)]
pub struct MLPredictionResult {
    pub predictions: Vec<f64>,
    pub probabilities: Option<Vec<f64>>,
    pub feature_importance: Option<Vec<f64>>,
    pub performance_metrics: ModelPerformanceMetrics,
    pub timing: PredictionTiming,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PredictionTiming {
    pub preprocessing_time: f64,
    pub actual_prediction_time: f64,
    pub postprocessing_time: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ModelPerformanceMetrics {
    pub training_time: f64,
    pub prediction_time: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub accuracy: Option<f64>,
    pub r2_score: Option<f64>,
    pub mse: Option<f64>,
}

pub struct MLHandler {
    pub model_cache: BTreeMap<String, PyObject>,
    pub schema_cache: BTreeMap<String, ModelPerformanceMetrics>,
    pub best_model: Option<String>,
}

impl MLHandler {
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

    pub fn load_model_with_schema(&mut self, model_name: &str, model_path: &str) -> PyResult<ModelPerformanceMetrics> {
        // Get the TTL file path by replacing .pkl extension with .ttl
        let schema_file_path = model_path.replace(".pkl", ".ttl");
        
        // Parse the schema file to get performance metrics directly from TTL
        let metrics = match self.parse_schema_file(&schema_file_path) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Error parsing schema file {}: {}", schema_file_path, e);
                // Create default metrics if TTL parsing fails
                ModelPerformanceMetrics::default()
            }
        };
        
        // Store metrics in cache without loading the model yet
        self.schema_cache.insert(model_name.to_string(), metrics.clone());
        
        Ok(metrics)
    }

    pub fn load_model(&mut self, model_name: &str, model_path: &str, module_name: Option<&str>) -> PyResult<()> {
        Python::with_gil(|py| {
            let sys = py.import("sys")?;
            let paths = sys.getattr("path")?;
            let model_path = Path::new(model_path);
            let src_dir = model_path.parent().unwrap().parent().unwrap();
            let current_path: Vec<String> = paths.extract()?;
            let src_dir_str = src_dir.to_str().unwrap();
            
            if !current_path.contains(&src_dir_str.to_string()) {
                paths.call_method1("insert", (0, src_dir_str))?;
            }

            let builtins = py.import("builtins")?;
            let pickle = py.import("pickle")?;
            let importlib = py.import("importlib")?;
            let globals = PyDict::new(py);
            
            globals.set_item("__builtins__", builtins.clone())?;
            globals.set_item("pickle", pickle)?;
            globals.set_item("importlib", importlib)?;
            globals.set_item("__name__", "__main__")?;
            
            let actual_module_name = module_name.unwrap_or("predictor").trim_end_matches(".py");
            let import_code = format!(
                r#"
try:
    imported_module = importlib.import_module('{}')
    for attr_name in dir(imported_module):
        attr = getattr(imported_module, attr_name)
        if not attr_name.startswith('_'):
            globals()[attr_name] = attr
    print("Successfully imported module: {{'{}'}}")
except ImportError as e:
    print(f"Error importing module {{'{}'}}")
    raise
                "#,
                actual_module_name, actual_module_name, actual_module_name
            );
            
            let import_cstring = CString::new(import_code).expect("Failed to convert import code to CString");
            py.run(import_cstring.as_c_str(), Some(&globals), None)?;
            
            let model_path_str = model_path.to_str().unwrap().replace('\\', "/");
            let code = format!(
                r#"
import pickle
with open(r'{}', 'rb') as f:
    model = pickle.load(f)
                "#,
                model_path_str
            );
            
            let code_cstring = CString::new(code).expect("Failed to convert code to CString");
            py.run(code_cstring.as_c_str(), Some(&globals), None)?;
            let model_option = globals.get_item("model")?;
            let model = model_option.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to load model")
            })?;
            
            self.model_cache.insert(model_name.to_string(), model.into());
            println!("Successfully loaded model '{}' from module '{}'", model_name, actual_module_name);
            Ok(())
        })
    }

    // Modified to prioritize lowest resource usage
    pub fn compare_models<'a>(&mut self, model_names: &[&'a str]) -> Option<&'a str> {
        if model_names.is_empty() {
            return None;
        }

        // Default to first model
        let mut best_model = model_names[0];
        let mut best_score = std::f64::MAX;

        println!("\nComparing models based on resource usage (lower is better):");
        for &model_name in model_names {
            if let Some(metrics) = self.schema_cache.get(model_name) {
                // Prioritize CPU and memory usage
                let cpu_weight = 0.5;
                let memory_weight = 0.4;
                let time_weight = 0.1;
                
                let resource_score = 
                    cpu_weight * metrics.cpu_usage_percent + 
                    memory_weight * metrics.memory_usage_mb +
                    time_weight * metrics.prediction_time;
                
                println!("{} Model:", model_name);
                println!("  CPU Usage: {:.2}%", metrics.cpu_usage_percent);
                println!("  Memory Usage: {:.2} MB", metrics.memory_usage_mb);
                println!("  Prediction Time: {:.4} seconds", metrics.prediction_time);
                println!("  Combined Resource Score: {:.2}", resource_score);
                
                if resource_score < best_score {
                    best_score = resource_score;
                    best_model = model_name;
                }
            }
        }
        
        // Store the best model name for future use
        self.best_model = Some(best_model.to_string());
        println!("\nSelected model with lowest resource usage: {}", best_model);
        
        Some(best_model)
    }

    pub fn predict(&self, model_name: &str, input_data: Vec<Vec<f64>>) -> PyResult<MLPredictionResult> {
        let actual_model_name = if let Some(ref best_model) = self.best_model {
            best_model
        } else {
            model_name
        };
        
        if !self.model_cache.contains_key(actual_model_name) {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Model {} not found in cache. Call load_model first.", actual_model_name)
            ));
        }
        
        println!("\n[PYTHON TIMING] Starting prediction with model '{}'", actual_model_name);
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
            println!("[PYTHON TIMING] Preprocessing completed: {:.6} seconds", preprocessing_time);

            // Actual ML prediction
            let prediction_start = Instant::now();
            let predictions = model.call_method1(py, "predict", (py_input.clone(),))?;
            let predictions: Vec<f64> = predictions.extract(py)?;
            let prediction_time = prediction_start.elapsed().as_secs_f64();
            println!("[PYTHON TIMING] Actual prediction completed: {:.6} seconds", prediction_time);

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
            println!("[PYTHON TIMING] Postprocessing completed: {:.6} seconds", postprocessing_time);
            
            let total_python_time = python_start.elapsed().as_secs_f64();
            println!("[PYTHON TIMING] Total Python execution: {:.6} seconds\n", total_python_time);

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
    pub fn discover_and_load_models(&mut self, model_dir: &Path, model_module: &str) -> PyResult<Vec<String>> {
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
                            
                            println!("Loading schema for model: {} from {}", model_id, path.display());
                            match self.load_model_with_schema(&model_id, path.to_str().unwrap()) {
                                Ok(_) => {
                                    model_ids.push(model_id);
                                },
                                Err(e) => {
                                    eprintln!("Error loading schema for {}: {}", model_id, e);
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
                                println!("Loading only the best model: {} from {}", model_id, path.display());
                                match self.load_model(&model_id, path.to_str().unwrap(), Some(model_module)) {
                                    Ok(_) => {
                                        self.best_model = Some(model_id.clone());
                                    },
                                    Err(e) => {
                                        eprintln!("Error loading best model {}: {}", model_id, e);
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

pub fn generate_ml_models(model_dir: &std::path::Path, model: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating ML models...");
    
    // Get the path to the predictor.py script
    let src_dir = model_dir.parent().unwrap_or_else(|| std::path::Path::new("."));
    let predictor_script = src_dir.join(model);
    
    if !predictor_script.exists() {
        return Err(format!("Predictor script not found at {}", predictor_script.display()).into());
    }
    
    // Run the Python script using Python's C API through pyo3
    Python::with_gil(|py| {
        // Add src_dir to Python path
        let sys = py.import("sys")?;
        let path = sys.getattr("path")?;
        path.call_method1("insert", (0, src_dir.to_str().unwrap()))?;
        
        // Get the current working directory
        let os = py.import("os")?;
        let cwd = os.call_method0("getcwd")?;
        println!("Current working directory: {}", cwd);

        // Import and run the predictor module
        println!("Running predictor.py to generate models...");
        
        // Import the module and execute it
        let result = std::panic::catch_unwind(|| {
            let module_name = model.trim_end_matches(".py");
            let _predictor = py.import(module_name)?;
            println!("Successfully imported predictor module");
            Ok::<_, PyErr>(())
        });
        
        if result.is_err() {
            println!("Failed to import predictor module directly, trying alternate method...");
            
            // Execute the script directly
            let subprocess = py.import("subprocess")?;
            let python_exe = sys.getattr("executable")?;
            let args = (python_exe.clone(), predictor_script.to_str().unwrap());
            
            println!("Executing: {} {}", python_exe, predictor_script.display());
            let result = subprocess.call_method1("run", args)?;
            
            let return_code = result.getattr("returncode")?;
            if !return_code.is_truthy()? {
                println!("Successfully generated models using subprocess");
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to generate models, return code: {}", return_code)
                ));
            }
        }
        
        Ok(())
    })?;
    
    // Verify that models were created
    let model_count = std::fs::read_dir(model_dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let path = entry.path();
            path.is_file() && path.extension().map_or(false, |ext| ext == "pkl") &&
            path.file_stem().and_then(|s| s.to_str()).map_or(false, |stem| stem.ends_with("_predictor"))
        })
        .count();
    
    if model_count < 3 {
        return Err(format!("Expected at least 3 models to be generated, but found {}", model_count).into());
    }
    
    println!("Successfully generated {} ML models", model_count);
    Ok(())
}