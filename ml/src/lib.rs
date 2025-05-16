/*
 * Copyright © 2024 ladroid
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

#[allow(dead_code)]
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
            // Add the Python path
            let sys = py.import("sys")?;
            let paths = sys.getattr("path")?;
            
            // Get the absolute path to the model file and src directory
            let model_path = Path::new(model_path);
            let src_dir = model_path.parent().unwrap().parent().unwrap();
            
            let current_path: Vec<String> = paths.extract()?;
            
            // Add src directory to Python path at the beginning if not already there
            let src_dir_str = src_dir.to_str().unwrap();
            if !current_path.contains(&src_dir_str.to_string()) {
                paths.call_method1("insert", (0, src_dir_str))?;
            }

            // Import required modules
            let builtins = py.import("builtins")?;
            let pickle = py.import("pickle")?;
            let importlib = py.import("importlib")?;
            
            // Create globals dictionary and populate it
            let globals = PyDict::new(py);
            globals.set_item("__builtins__", builtins.clone())?;
            globals.set_item("pickle", pickle)?;
            globals.set_item("importlib", importlib)?;
            globals.set_item("__name__", "__main__")?;
            
            // First load the model to inspect its class
            let model_path_str = model_path.to_str().unwrap().replace('\\', "/");
            let code = format!(
                r#"
import pickle
with open(r'{}', 'rb') as f:
    model = pickle.load(f)
                "#,
                model_path_str
            );
            
            // Execute the code to load the model
            let code_cstring = CString::new(code).expect("Failed to convert code to CString");
            py.run(code_cstring.as_c_str(), Some(&globals), None)?;
            
            // Get the model from globals
            let model_option = globals.get_item("model")?;
            let model = model_option.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to load model")
            })?;
            
            // Determine module name based on provided value, model class name, or model attributes
            let actual_module_name = match module_name {
                // Use explicitly provided module name if available
                Some(name) => name.to_string(),
                None => {
                    // Try to determine module from model class
                    let get_module_code = r#"
module_name = getattr(model, '__module__', '').split('.')[0]
if not module_name or module_name == '__main__' or module_name == 'builtins':
    # Try to get algorithm type from model attributes
    if hasattr(model, 'algorithm_type'):
        module_name = model.algorithm_type
    # Look for common model attributes that might indicate the type
    elif hasattr(model, 'feature_importances_'):
        module_name = 'predictor'  # Tree-based models
    elif hasattr(model, 'coef_'):
        module_name = 'linear_models'  # Linear models
    else:
        # Inspect the class name for clues
        class_name = model.__class__.__name__.lower()
        if 'forest' in class_name or 'tree' in class_name or 'boost' in class_name:
            module_name = 'predictor'
        elif 'linear' in class_name or 'regress' in class_name:
            module_name = 'linear_models'
        else:
            module_name = 'predictor'  # Default
                    "#;
                    
                    let code_cstring = CString::new(get_module_code).expect("Failed to convert module detection code to CString");
                    py.run(code_cstring.as_c_str(), Some(&globals), None)?;
                    
                    let module_name = globals.get_item("module_name")?;
                    let module_name_obj = module_name.ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to determine module name")
                    })?;
                    
                    module_name_obj.extract::<String>()?
                }
            };
            
            // Dynamically import the determined module
            let import_code = format!(
                r#"
try:
    # Try to use importlib for a cleaner import
    imported_module = importlib.import_module('{}')
    # Get all the classes from the module
    for attr_name in dir(imported_module):
        if not attr_name.startswith('_') and attr_name[0].isupper():
            # Add class attributes to globals
            globals()[attr_name] = getattr(imported_module, attr_name)
except ImportError:
    print(f"Warning: Could not import module {{'{}'}}. Using model as is.")
                "#,
                actual_module_name, actual_module_name
            );
            
            // Execute the import code
            let import_cstring = CString::new(import_code).expect("Failed to convert import code to CString");
            let _ = py.run(import_cstring.as_c_str(), Some(&globals), None);
            
            // Store the model in cache
            self.model_cache.insert(model_name.to_string(), model.into());
            
            println!("Loaded model '{}' from module '{}'", model_name, actual_module_name);
            Ok(())
        })
    }

    pub fn get_model_performance(&self, model_name: &str) -> Option<ModelPerformanceMetrics> {
        self.schema_cache.get(model_name).cloned()
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
        // If best_model is set, use that instead of the provided model_name
        let actual_model_name = if let Some(ref best_model) = self.best_model {
            best_model
        } else {
            model_name
        };
        
        // Check if the model is loaded, if not, return an error
        if !self.model_cache.contains_key(actual_model_name) {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Model {} not found in cache. Call load_model first.", actual_model_name)
            ));
        }
        
        let start = Instant::now();
        
        let mut result = Python::with_gil(|py| {
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

            // Make prediction
            let predictions = model.call_method1(py, "predict", (py_input.clone(),))?;
            let predictions: Vec<f64> = predictions.extract(py)?;

            // Try to get probabilities if available
            let probabilities = model
                .call_method1(py, "predict_proba", (py_input,))
                .and_then(|probs| probs.extract::<Vec<f64>>(py))
                .ok();

            // Try to get feature importance if available
            let feature_importance = PyResult::Ok(())
                .and_then(|_| model.getattr(py, "model"))
                .and_then(|model_obj| model_obj.getattr(py, "feature_importances_"))
                .and_then(|fi| fi.extract::<Vec<f64>>(py))
                .ok();

            // Use performance metrics from TTL instead of measuring them at runtime
            let performance_metrics = match self.schema_cache.get(actual_model_name) {
                Some(metrics) => metrics.clone(),
                None => ModelPerformanceMetrics::default(),
            };

            Ok(MLPredictionResult {
                predictions,
                probabilities,
                feature_importance,
                performance_metrics,
            })
        });
        
        // Only track prediction time for logging, but don't use it for model selection
        let elapsed = start.elapsed();
        if let Ok(ref mut _res) = result {
            println!("Actual prediction time: {:.4} seconds", elapsed.as_secs_f64());
        }
        
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