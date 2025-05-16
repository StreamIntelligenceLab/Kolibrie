/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::sparql_database::SparqlDatabase;
use ml::MLHandler;
use ml::generate_ml_models;
use shared::query::MLPredictClause;
use std::error::Error;

// Function to extract features based on the parsed SELECT variables from ML.PREDICT
pub fn execute_ml_prediction_from_clause<F, G, T, U>(
    ml_predict: &MLPredictClause,
    database: &SparqlDatabase,
    model: &str,
    extract_data: F,
    predict: G
) -> Result<Vec<U>, Box<dyn Error>> 
where
    F: FnOnce(&SparqlDatabase) -> Result<Vec<T>, Box<dyn Error>>,
    G: FnOnce(&MLHandler, &str, &[T], &[String]) -> Result<Vec<U>, Box<dyn Error>>,
    T: Clone, // Ensure T can be cloned (for slices)
    U: Clone, // Ensure U can be cloned
{
    // Define model paths
    let model_dir = {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        
        // Go up directories
        loop {
            let ml_dir = path.join("ml");
            if ml_dir.exists() && ml_dir.is_dir() {
                break ml_dir.join("examples").join("models");
            }
            
            if !path.pop() {
                // Couldn't find the ml directory in any parent - use a fallback path
                eprintln!("Warning: Could not locate 'ml' directory in any parent directory!");
                break std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("models");
            }
        }
    };
    
    // Initialize ML handler
    let mut ml_handler = MLHandler::new()?;
    
    // Ensure models directory exists
    std::fs::create_dir_all(&model_dir)?;
    
    println!("Looking for models in: {}", model_dir.display());
    
    // Check if models need to be generated
    let models_exist = std::fs::read_dir(&model_dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let path = entry.path();
            path.is_file() && path.extension().map_or(false, |ext| ext == "pkl") &&
            path.file_stem().and_then(|s| s.to_str()).map_or(false, |stem| stem.ends_with("_predictor"))
        })
        .count() >= 3; // We expect at least 3 models: rf, gb, and lr
    
    // If models don't exist or are outdated, generate them
    if !models_exist {
        println!("Models not found or outdated. Generating models...");
        generate_ml_models(&model_dir, model)?;
    }
    
    // Discover, load TTL schemas, and find the best model in one step
    println!("\nDiscovering models and analyzing schemas...");
    let model_ids = ml_handler.discover_and_load_models(&model_dir, model)?;
    
    if model_ids.is_empty() {
        return Err("No valid models found with TTL schemas".into());
    }
    
    // Extract variable names from SELECT clause (remove ? prefix)
    let variable_names: Vec<String> = ml_predict.input_select
        .iter()
        .map(|(var, _, _)| var.trim_start_matches('?').to_string())
        .collect();
    
    println!("SELECT variables: {:?}", variable_names);
    
    // Use the provided function to extract data
    let data = extract_data(database)?;
    
    // Filter out non-numeric variables
    let feature_names: Vec<String> = variable_names.iter()
        .filter(|&name| *name != "room")
        .cloned()
        .collect();
    
    println!("Feature names for prediction: {:?}", feature_names);
    
    // Get the name of the best model stored in the handler
    let best_model_name = ml_handler.best_model.as_deref().unwrap_or(&model_ids[0]);
    
    // Use the prediction function with the best model
    predict(&ml_handler, best_model_name, &data, &feature_names)
}