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
    model_cache: BTreeMap<String, PyObject>,
    schema_cache: BTreeMap<String, ModelPerformanceMetrics>,
}

#[allow(dead_code)]
impl MLHandler {
    pub fn new() -> PyResult<Self> {
        Ok(MLHandler {
            model_cache: BTreeMap::new(),
            schema_cache: BTreeMap::new(),
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
    // Load the model
    self.load_model(model_name, model_path)?;
    
    // Get performance metrics directly from the model
    let metrics = Python::with_gil(|py| -> PyResult<ModelPerformanceMetrics> {
        let model = self.model_cache.get(model_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Model {} not found in cache", model_name)
            )
        })?;
        
        // Call get_performance_metrics directly on the model
        let metrics = model.call_method0(py, "get_performance_metrics")?;
        let perf_dict: BTreeMap<String, f64> = metrics.extract(py)?;
        
        let mut performance_metrics = ModelPerformanceMetrics::default();
        
        if let Some(&val) = perf_dict.get("training_time") {
            performance_metrics.training_time = val;
        }
        if let Some(&val) = perf_dict.get("prediction_time") {
            performance_metrics.prediction_time = val;
        }
        if let Some(&val) = perf_dict.get("memory_usage_mb") {
            performance_metrics.memory_usage_mb = val;
        }
        if let Some(&val) = perf_dict.get("cpu_usage_percent") {
            performance_metrics.cpu_usage_percent = val;
        }
        
        // Try to get R2 and MSE if they exist in the model's attributes
        if let Ok(metrics_dict) = model.call_method1(py, "get", ("evaluation_metrics",)) {
            let metrics_dict: PyResult<BTreeMap<String, f64>> = metrics_dict.extract(py);
            if let Ok(eval_metrics) = metrics_dict {
                if let Some(&r2) = eval_metrics.get("r2") {
                    performance_metrics.r2_score = Some(r2);
                }
                if let Some(&mse) = eval_metrics.get("mse") {
                    performance_metrics.mse = Some(mse);
                }
            }
        }
        
        Ok(performance_metrics)
    })?;
    
    // Store metrics in cache
    self.schema_cache.insert(model_name.to_string(), metrics.clone());
    
    Ok(metrics)
}

    pub fn load_model(&mut self, model_name: &str, model_path: &str) -> PyResult<()> {
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
            let predictor = py.import("predictor")?;
            
            // Create globals dictionary and populate it
            let globals = PyDict::new(py);
            globals.set_item("__builtins__", builtins)?;
            globals.set_item("pickle", pickle)?;
            globals.set_item("predictor", &predictor)?;
            globals.set_item("__name__", "__main__")?;
            
            // Add predictor classes to globals
            let rf_predictor = predictor.getattr("RandomForestPredictor")?;
            globals.set_item("RandomForestPredictor", rf_predictor)?;
            
            let gb_predictor = predictor.getattr("GradientBoostingPredictor")?;
            globals.set_item("GradientBoostingPredictor", gb_predictor)?;
            
            let base_predictor = predictor.getattr("BasePredictor")?;
            globals.set_item("BasePredictor", base_predictor)?;
            
            // Create and execute the Python code to load the model
            let model_path_str = model_path.to_str().unwrap().replace('\\', "/");
            let code = format!(
                r#"
import pickle
with open(r'{}', 'rb') as f:
    model = pickle.load(f)
                "#,
                model_path_str
            );
            
            // Execute the code
            let code_cstring = CString::new(code).expect("Failed to convert code to CString");
            py.run(code_cstring.as_c_str(), Some(&globals), None)?;
            
            // Get the model from globals
            let model_option = globals.get_item("model")?;
            let model = model_option.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to load model")
            })?;
            
            // Store the model in cache
            self.model_cache.insert(model_name.to_string(), model.into());
            Ok(())
        })
    }

    pub fn get_model_performance(&self, model_name: &str) -> Option<ModelPerformanceMetrics> {
        self.schema_cache.get(model_name).cloned()
    }

    pub fn compare_models<'a>(&self, model_names: &[&'a str]) -> Option<&'a str> {
        if model_names.is_empty() {
            return None;
        }

        // Default to first model
        let mut best_model = model_names[0];
        let mut best_score = std::f64::MAX;

        for &model_name in model_names {
            if let Some(metrics) = self.schema_cache.get(model_name) {
                // Create a combined score (lower is better)
                // Weighted combination of time, memory usage, and error metric (MSE)
                let time_weight = 0.3;
                let memory_weight = 0.3;
                let mse_weight = 0.4;
                
                let mut score = 
                    time_weight * metrics.prediction_time + 
                    memory_weight * metrics.memory_usage_mb;
                
                // Add MSE if available (lower is better)
                if let Some(mse) = metrics.mse {
                    score += mse_weight * mse;
                }
                
                // If we have R2 score (higher is better), subtract it to make lower score better
                if let Some(r2) = metrics.r2_score {
                    score -= mse_weight * r2;
                }
                
                if score < best_score {
                    best_score = score;
                    best_model = model_name;
                }
            }
        }

        Some(best_model)
    }

    pub fn predict(&self, model_name: &str, input_data: Vec<Vec<f64>>) -> PyResult<MLPredictionResult> {
        let start = Instant::now();
        
        let mut result = Python::with_gil(|py| {
            let model = self.model_cache.get(model_name).ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Model {} not found in cache", model_name)
                )
            })?;

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

            // Get performance metrics
            let metrics = model.call_method0(py, "get_performance_metrics")?;
            let py_metrics: PyObject = metrics.into();
            let perf_dict: BTreeMap<String, PyObject> = py_metrics.extract(py)?;
            
            let mut performance_metrics = ModelPerformanceMetrics::default();
            
            if let Some(val) = perf_dict.get("training_time") {
                performance_metrics.training_time = val.extract(py)?;
            }
            if let Some(val) = perf_dict.get("prediction_time") {
                performance_metrics.prediction_time = val.extract(py)?;
            }
            if let Some(val) = perf_dict.get("memory_usage_mb") {
                performance_metrics.memory_usage_mb = val.extract(py)?;
            }
            if let Some(val) = perf_dict.get("cpu_usage_percent") {
                performance_metrics.cpu_usage_percent = val.extract(py)?;
            }

            Ok(MLPredictionResult {
                predictions,
                probabilities,
                feature_importance,
                performance_metrics,
            })
        });
        
        // Track prediction time
        let elapsed = start.elapsed();
        if let Ok(ref mut res) = result {
            res.performance_metrics.prediction_time = elapsed.as_secs_f64();
        }
        
        result
    }
}