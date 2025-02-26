use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::types::PyDict;
use std::collections::BTreeMap;
use std::path::Path;
use std::ffi::CString;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct MLPredictionResult {
    pub predictions: Vec<f64>,
    pub probabilities: Option<Vec<f64>>,
    pub feature_importance: Option<Vec<f64>>,
}

pub struct MLHandler {
    model_cache: BTreeMap<String, PyObject>,
}

impl MLHandler {
    pub fn new() -> PyResult<Self> {
        Ok(MLHandler {
            model_cache: BTreeMap::new(),
        })
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
            
            // Get TemperaturePredictor class and add it to globals
            let temp_predictor = predictor.getattr("TemperaturePredictor")?;
            globals.set_item("TemperaturePredictor", temp_predictor)?;
            
            // Create and execute the Python code to load the model
            let model_path_str = model_path.to_str().unwrap().replace('\\', "/");
            let code = format!(
    r#"from predictor import TemperaturePredictor
with open(r'{}', 'rb') as f:
    model = pickle.load(f)"#,
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

    pub fn predict(&self, model_name: &str, input_data: Vec<Vec<f64>>) -> PyResult<MLPredictionResult> {
        Python::with_gil(|py| {
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
            let feature_importance = model
                .getattr(py, "feature_importances_")
                .and_then(|fi| fi.extract::<Vec<f64>>(py))
                .ok();

            Ok(MLPredictionResult {
                predictions,
                probabilities,
                feature_importance,
            })
        })
    }
}