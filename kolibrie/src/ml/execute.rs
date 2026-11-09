/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Running an `ML.PREDICT` clause through the Python backend

use crate::database::KolibrieDB;
use ml::MLHandler;
use shared::query::MLPredictClause;
use std::error::Error;
use std::time::Instant;

#[derive(Debug)]
/// Where the time went in one `ML.PREDICT`, in seconds
pub struct MLPredictTiming {
    /// Wall-clock time for the whole clause
    pub total_time: f64,
    /// Handing the input across to Python
    pub rust_to_python_time: f64,
    /// Python-side preparation before inference
    pub python_preprocessing_time: f64,
    /// The model call itself
    pub actual_prediction_time: f64,
    /// Python-side work on the results
    pub python_postprocessing_time: f64,
    /// Bringing the results back
    pub python_to_rust_time: f64,
}

impl MLPredictTiming {
    /// Prints the timing to stdout
    pub fn print_breakdown(&self) {
        println!(
            "ML_TIMING total_seconds={:.6} prediction_seconds={:.6}",
            self.total_time, self.actual_prediction_time
        );
    }
}

/// Runs an `ML.PREDICT` clause and returns its predictions with timing
pub fn execute_ml_prediction_from_clause<F, G, T, U>(
    ml_predict: &MLPredictClause,
    database: &KolibrieDB,
    model: &str,
    extract_data: F,
    predict: G,
) -> Result<(Vec<U>, MLPredictTiming), Box<dyn Error>>
where
    F: FnOnce(&KolibrieDB) -> Result<Vec<T>, Box<dyn Error>>,
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

    let rust_setup_start = Instant::now();

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

    let best_model_name = ml_handler.best_model.as_deref().unwrap_or(&model_ids[0]);

    let rust_setup_time = rust_setup_start.elapsed().as_secs_f64();

    let python_call_start = Instant::now();
    let (predictions, mut timing) = predict(&ml_handler, best_model_name, &data, &feature_names)?;
    let _python_call_time = python_call_start.elapsed().as_secs_f64();

    let total_time = ml_predict_start.elapsed().as_secs_f64();

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
    database: &KolibrieDB,
    ml_handler: &MLHandler,
    best_model_name: &str,
    extract_data: F,
    predict: G,
) -> Result<(Vec<U>, MLPredictTiming), Box<dyn Error>>
where
    F: FnOnce(&KolibrieDB) -> Result<Vec<T>, Box<dyn Error>>,
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
