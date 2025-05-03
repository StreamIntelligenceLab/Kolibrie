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
                break ml_dir.join("src").join("models");
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
    
    // Continue with model discovery and loading as before
    let mut model_paths = Vec::new();
    let mut model_ids = Vec::new();
    
    if let Ok(entries) = std::fs::read_dir(&model_dir) {
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "pkl") {
                if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if file_stem.ends_with("_predictor") {
                        // Get model type prefix from filename (rf_, gb_, lr_, etc.)
                        let model_type = file_stem.split('_').next().unwrap_or("unknown");
                        let model_id = format!("{}_model", model_type);
                        
                        println!("Found model: {} at {}", model_id, path.display());
                        model_paths.push(path);
                        model_ids.push(model_id);
                    }
                }
            }
        }
    }
    
    if model_paths.is_empty() {
        return Err("No predictor models found in the models directory".into());
    }
    
    // Load all discovered models with their schemas
    println!("\nLoading {} models:", model_paths.len());
    
    let mut model_metrics = Vec::new();
    
    for (model_id, model_path) in model_ids.iter().zip(model_paths.iter()) {
        if !model_path.exists() {
            eprintln!("Warning: Model file not found at {}", model_path.display());
            continue;
        }
        
        match ml_handler.load_model_with_schema(model_id, model_path.to_str().unwrap(), Some("predictor")) {
            Ok(metrics) => {
                model_metrics.push((model_id.clone(), metrics));
                println!("Successfully loaded model: {}", model_id);
            },
            Err(e) => {
                eprintln!("Error loading model {}: {}", model_id, e);
            }
        }
    }
    
    if model_metrics.is_empty() {
        return Err("Failed to load any valid models".into());
    }
    
    // Print performance comparison for all loaded models
    println!("\nModel Performance Comparison:");
    for (model_id, metrics) in &model_metrics {
        println!("\n{} Model:", model_id);
        println!("  Training Time: {:.4} seconds", metrics.training_time);
        println!("  Prediction Time: {:.4} seconds", metrics.prediction_time);
        println!("  Memory Usage: {:.2} MB", metrics.memory_usage_mb);
        println!("  CPU Usage: {:.2}%", metrics.cpu_usage_percent);
        if let Some(r2) = metrics.r2_score {
            println!("  R² Score: {:.4}", r2);
        }
        if let Some(mse) = metrics.mse {
            println!("  MSE: {:.4}", mse);
        }
    }
    
    // Convert model_ids from Vec<String> to Vec<&str> to satisfy compare_models
    let model_id_refs: Vec<&str> = model_ids.iter().map(|s| s.as_str()).collect();
    
    // Compare all loaded models and select the best one
    let best_model = match ml_handler.compare_models(&model_id_refs) {
        Some(model) => model.to_string(),
        None => {
            // Default to first model if comparison fails
            model_ids.first().unwrap_or(&String::from("unknown")).clone()
        }
    };
    
    println!("\nSelected best model: {}", best_model);
    
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
    
    // Use the provided prediction function 
    predict(&ml_handler, &best_model, &data, &feature_names)
}