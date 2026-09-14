/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::sparql_database::SparqlDatabase;
use ml::MLHandler;
use shared::query::MLPredictClause;
use std::error::Error;
use std::time::Instant;

#[derive(Debug)]
pub struct MLPredictTiming {
    pub total_time: f64,
    pub rust_to_python_time: f64,
    pub python_preprocessing_time: f64,
    pub actual_prediction_time: f64,
    pub python_postprocessing_time: f64,
    pub python_to_rust_time: f64,
}

impl MLPredictTiming {
/// Return local timing diagnostics without model or input details
    pub fn print_breakdown(&self) {
        println!(
            "ML_TIMING total_seconds={:.6} prediction_seconds={:.6}",
            self.total_time, self.actual_prediction_time
        );
    }
}

pub fn execute_ml_prediction_from_clause<F, G, T, U>(
    ml_predict: &MLPredictClause,
    database: &SparqlDatabase,
    model: &str,
    extract_data: F,
    predict: G,
) -> Result<(Vec<U>, MLPredictTiming), Box<dyn Error>>
where
    F: FnOnce(&SparqlDatabase) -> Result<Vec<T>, Box<dyn Error>>,
    G: FnOnce(
        &MLHandler,
        &str,
        &[T],
        &[String],
    ) -> Result<(Vec<U>, MLPredictTiming), Box<dyn Error>>,
    T: Clone,
    U: Clone,
{
    database.ml_context.require_local()?;
    crate::ml_policy::validate_model_name(model)?;
    let ml_predict_start = Instant::now();

    // Rust setup and model discovery
    let rust_setup_start = Instant::now();

    // Define model paths
    let model_dir = {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        loop {
            let ml_dir = path.join("ml");
            if ml_dir.exists() && ml_dir.is_dir() {
                break ml_dir.join("examples").join("models");
            }

            if !path.pop() {
                eprintln!("KOLIBRIE_OPERATION_FAILED");
                break std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("models");
            }
        }
    };

    let mut ml_handler = MLHandler::new()?;

    // Check if models need to be generated
    let models_exist = std::fs::read_dir(&model_dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let path = entry.path();
            path.is_file()
                && path.extension().map_or(false, |ext| ext == "pkl")
                && path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map_or(false, |stem| stem.ends_with("_predictor"))
        })
        .count()
        >= 1;

    if !models_exist {
        return Err("model is missing; generate it explicitly in a trusted local workflow".into());
    }

    let model_ids = ml_handler.discover_and_load_models(&model_dir, model)?;

    if model_ids.is_empty() {
        return Err("No valid models found with TTL schemas".into());
    }

    // Extract variable names
    let variable_names: Vec<String> = ml_predict
        .input_select
        .iter()
        .map(|(var, _, _)| var.trim_start_matches('?').to_string())
        .collect();

    // Extract data
    let data = extract_data(database)?;

    // Filter feature names
    let feature_names: Vec<String> = variable_names
        .iter()
        .filter(|&name| *name != "room" && *name != "road")
        .cloned()
        .collect();

    let best_model_name = ml_handler.best_model.as_deref().unwrap_or(&model_ids[0]);

    let rust_setup_time = rust_setup_start.elapsed().as_secs_f64();

    // Call Python prediction (this measures internal Python timing)
    let python_call_start = Instant::now();
    let (predictions, mut timing) = predict(&ml_handler, best_model_name, &data, &feature_names)?;
    let _python_call_time = python_call_start.elapsed().as_secs_f64();

    // Calculate total time
    let total_time = ml_predict_start.elapsed().as_secs_f64();

    // Update timing with Rust-side measurements
    timing.rust_to_python_time = rust_setup_time;
    timing.python_to_rust_time = total_time
        - rust_setup_time
        - timing.python_preprocessing_time
        - timing.actual_prediction_time
        - timing.python_postprocessing_time;
    timing.total_time = total_time;

    timing.print_breakdown();

    Ok((predictions, timing))
}

/// Execute with an already initialized ML handler
pub fn execute_ml_prediction_with_handler<F, G, T, U>(
    ml_predict: &MLPredictClause,
    database: &SparqlDatabase,
    ml_handler: &MLHandler,
    best_model_name: &str,
    extract_data: F,
    predict: G,
) -> Result<(Vec<U>, MLPredictTiming), Box<dyn Error>>
where
    F: FnOnce(&SparqlDatabase) -> Result<Vec<T>, Box<dyn Error>>,
    G: FnOnce(
        &MLHandler,
        &str,
        &[T],
        &[String],
    ) -> Result<(Vec<U>, MLPredictTiming), Box<dyn Error>>,
    T: Clone,
    U: Clone,
{
    database.ml_context.require_local()?;
    crate::ml_policy::validate_model_name(best_model_name)?;
    let variable_names: Vec<String> = ml_predict
        .input_select
        .iter()
        .map(|(var, _, _)| var.trim_start_matches('?').to_string())
        .collect();

    let data = extract_data(database)?;

    let feature_names: Vec<String> = variable_names
        .iter()
        .filter(|&name| *name != "room" && *name != "road")
        .cloned()
        .collect();

    let (predictions, timing) = predict(ml_handler, best_model_name, &data, &feature_names)?;
    Ok((predictions, timing))
}

/// Discover local artifacts with an initialized ML handler
pub fn setup_ml_handler(model: &str) -> Result<(MLHandler, String), Box<dyn Error>> {
    crate::ml_policy::validate_model_name(model)?;
    let model_dir = {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let ml_dir = path.join("ml");
            if ml_dir.exists() && ml_dir.is_dir() {
                break ml_dir.join("examples").join("models");
            }
            if !path.pop() {
                eprintln!("KOLIBRIE_OPERATION_FAILED");
                break std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("models");
            }
        }
    };

    let mut ml_handler = MLHandler::new()?;

    let models_exist = std::fs::read_dir(&model_dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let path = entry.path();
            path.is_file()
                && path.extension().map_or(false, |ext| ext == "pkl")
                && path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map_or(false, |stem| stem.ends_with("_predictor"))
        })
        .count()
        >= 1;

    if !models_exist {
        return Err("model is missing; generate it explicitly in a trusted local workflow".into());
    }

    let model_ids = ml_handler.discover_and_load_models(&model_dir, model)?;

    if model_ids.is_empty() {
        return Err("No valid models found with TTL schemas".into());
    }

    let best_model = ml_handler
        .best_model
        .clone()
        .unwrap_or_else(|| model_ids[0].clone());

    Ok((ml_handler, best_model))
}
