use datalog::knowledge_graph::KnowledgeGraph;
use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use ml::MLHandler;
use pyo3::prepare_freethreaded_python;
use serde::{Deserialize, Serialize};
use shared::terms::Term;
use shared::triple::Triple;
use std::error::Error;
use std::time::SystemTime;
use shared::query::MLPredictClause;

#[derive(Debug, Serialize, Deserialize)]
struct RoomData {
    room_id: String,
    temperature: f64,
    humidity: f64,
    occupancy: i32,
    timestamp: SystemTime,
}

#[derive(Debug, Serialize, Deserialize)]
struct Prediction {
    room_id: String,
    predicted_temperature: f64,
    confidence: f64,
    timestamp: SystemTime,
}

// Function to extract features based on the parsed SELECT variables from ML.PREDICT
fn execute_ml_prediction_from_clause(
    ml_predict: &MLPredictClause,
    database: &SparqlDatabase,
) -> Result<Vec<Prediction>, Box<dyn Error>> {
    // Initialize ML handler
    let mut ml_handler = MLHandler::new()?;
    
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
    
    // Load two models with schema
    let rf_model_path = model_dir.join("rf_temperature_predictor.pkl");
    let gb_model_path = model_dir.join("gb_temperature_predictor.pkl");
    
    if !rf_model_path.exists() || !gb_model_path.exists() {
        return Err(format!("Model files not found at {} or {}", 
            rf_model_path.display(), gb_model_path.display()).into());
    }
    
    // Load both models with schema
    let rf_metrics = ml_handler.load_model_with_schema("rf_model", rf_model_path.to_str().unwrap())?;
    let gb_metrics = ml_handler.load_model_with_schema("gb_model", gb_model_path.to_str().unwrap())?;
    
    // Print performance comparison
    println!("\nModel Performance Comparison:");
    println!("RandomForest Model:");
    println!("  Training Time: {:.4} seconds", rf_metrics.training_time);
    println!("  Prediction Time: {:.4} seconds", rf_metrics.prediction_time);
    println!("  Memory Usage: {:.2} MB", rf_metrics.memory_usage_mb);
    println!("  CPU Usage: {:.2}%", rf_metrics.cpu_usage_percent);
    if let Some(r2) = rf_metrics.r2_score {
        println!("  R² Score: {:.4}", r2);
    }
    if let Some(mse) = rf_metrics.mse {
        println!("  MSE: {:.4}", mse);
    }
    
    println!("\nGradientBoosting Model:");
    println!("  Training Time: {:.4} seconds", gb_metrics.training_time);
    println!("  Prediction Time: {:.4} seconds", gb_metrics.prediction_time);
    println!("  Memory Usage: {:.2} MB", gb_metrics.memory_usage_mb);
    println!("  CPU Usage: {:.2}%", gb_metrics.cpu_usage_percent);
    if let Some(r2) = gb_metrics.r2_score {
        println!("  R² Score: {:.4}", r2);
    }
    if let Some(mse) = gb_metrics.mse {
        println!("  MSE: {:.4}", mse);
    }
    
    // Compare models and select the best one
    let model_names = ["rf_model", "gb_model"];
    let best_model = ml_handler.compare_models(&model_names)
        .unwrap_or("rf_model"); // Default to RandomForest if comparison fails
    
    println!("\nSelected best model: {}", best_model);
    
    // Extract variable names from SELECT clause (remove ? prefix)
    let variable_names: Vec<String> = ml_predict.input_select
        .iter()
        .map(|(var, _, _)| var.trim_start_matches('?').to_string())
        .collect();
    
    println!("SELECT variables: {:?}", variable_names);
    
    // Extract data from database
    let room_data: Vec<RoomData> = database
        .triples
        .iter()
        .filter(|triple| {
            database
                .dictionary
                .decode(triple.predicate)
                .map_or(false, |pred| pred.ends_with("temperature"))
        })
        .map(|triple| {
            let room_id = database
                .dictionary
                .decode(triple.subject)
                .unwrap_or_default()
                .split('#')
                .last()
                .unwrap_or_default()
                .to_string();

            let temperature = database
                .dictionary
                .decode(triple.object)
                .unwrap_or_default()
                .parse()
                .unwrap_or(0.0);

            // Find humidity and occupancy
            let humidity = database
                .triples
                .iter()
                .find(|t| {
                    t.subject == triple.subject
                        && database
                            .dictionary
                            .decode(t.predicate)
                            .map_or(false, |p| p.ends_with("humidity"))
                })
                .and_then(|t| database.dictionary.decode(t.object))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);

            let occupancy = database
                .triples
                .iter()
                .find(|t| {
                    t.subject == triple.subject
                        && database
                            .dictionary
                            .decode(t.predicate)
                            .map_or(false, |p| p.ends_with("occupancy"))
                })
                .and_then(|t| database.dictionary.decode(t.object))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            RoomData {
                room_id,
                temperature,
                humidity,
                occupancy,
                timestamp: SystemTime::now(),
            }
        })
        .collect();

    println!("Found {} room data entries", room_data.len());
    for room in &room_data {
        println!(
            "Room: {}, Temp: {}, Humidity: {}, Occupancy: {}",
            room.room_id, room.temperature, room.humidity, room.occupancy
        );
    }
    
    // Filter out non-numeric variables
    let feature_names: Vec<String> = variable_names.iter()
        .filter(|&name| *name != "room")
        .cloned()
        .collect();
    
    println!("Feature names for prediction: {:?}", feature_names);
    
    // Ensure we have data before proceeding
    if room_data.is_empty() {
        return Err("No input data found for ML prediction".into());
    }
    
    // Dynamically build feature vectors based on selected variables
    let features: Vec<Vec<f64>> = room_data
        .iter()
        .map(|data| {
            let mut feature_vector = Vec::new();
            
            // Only include features that were specified in the SELECT clause
            for feature_name in &feature_names {
                match feature_name.as_str() {
                    "temp" => feature_vector.push(data.temperature),
                    "humidity" => feature_vector.push(data.humidity),
                    "occupancy" => feature_vector.push(data.occupancy as f64),
                    // Add other features as needed
                    _ => {} // Skip unknown features
                }
            }
            
            feature_vector
        })
        .collect();
    
    // Only proceed if we have features to process
    if features.is_empty() || features[0].is_empty() {
        return Err("No valid features found for prediction based on SELECT variables".into());
    }
    
    println!("Using features for prediction: {:?}", features);
    
    // Use the selected best model
    let prediction_results = ml_handler.predict(best_model, features)?;
    
    // Print performance of the selected model during this prediction
    println!("\nPerformance during prediction:");
    println!("  Prediction Time: {:.4} seconds", prediction_results.performance_metrics.prediction_time);
    println!("  Memory Usage: {:.2} MB", prediction_results.performance_metrics.memory_usage_mb);
    println!("  CPU Usage: {:.2}%", prediction_results.performance_metrics.cpu_usage_percent);
    
    // Create prediction objects
    let predictions: Vec<Prediction> = room_data
        .iter()
        .zip(prediction_results.predictions.iter())
        .zip(prediction_results.probabilities.unwrap_or_default().iter().chain(std::iter::repeat(&0.95)))
        .map(|((data, &pred), &conf)| Prediction {
            room_id: data.room_id.clone(),
            predicted_temperature: pred,
            confidence: conf,
            timestamp: SystemTime::now(),
        })
        .collect();

    Ok(predictions)
}

fn main() -> Result<(), Box<dyn Error>> {
    prepare_freethreaded_python();

    let rdf_xml_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org#"
                 xmlns:sensor="http://example.org/sensor#">
          <rdf:Description rdf:about="http://example.org#Room101">
            <sensor:temperature>22.5</sensor:temperature>
            <sensor:humidity>45.0</sensor:humidity>
            <sensor:occupancy>5</sensor:occupancy>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Room102">
            <sensor:temperature>23.8</sensor:temperature>
            <sensor:humidity>52.0</sensor:humidity>
            <sensor:occupancy>8</sensor:occupancy>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Room103">
            <sensor:temperature>27.2</sensor:temperature>
            <sensor:humidity>48.0</sensor:humidity>
            <sensor:occupancy>3</sensor:occupancy>
          </rdf:Description>
        </rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml_data);
    println!("Database RDF triples: {:#?}", database.triples);

    let mut kg = KnowledgeGraph::new();
    for triple in database.triples.iter() {
        let subject = database.dictionary.decode(triple.subject);
        let predicate = database.dictionary.decode(triple.predicate);
        let object = database.dictionary.decode(triple.object);
        kg.add_abox_triple(&subject.unwrap(), &predicate.unwrap(), &object.unwrap());
    }
    println!("KnowledgeGraph ABox loaded.");

    let combined_query_input = r#"PREFIX ex: <http://example.org#>
PREFIX sensor: <http://example.org/sensor#>
RULE :TemperatureAlert(?room) :- 
    WHERE { 
        ?room sensor:temperature ?temp ;
              sensor:humidity ?humidity
        FILTER (?temp > 25)
    } 
    => 
    { 
        ?room ex:temperatureAlert "High temperature detected" .
    }.
    ML.PREDICT(MODEL temperature_predictor,
        INPUT {
            SELECT ?room ?temp ?humidity ?occupancy
            WHERE {
                ?room sensor:temperature ?temp ;
                      sensor:humidity ?humidity ;
                      sensor:occupancy ?occupancy
            }
        },
        OUTPUT ?predicted_temp
    )
SELECT ?room ?alert
WHERE { 
    :TemperatureAlert(?room) .
    ?room ex:temperatureAlert ?alert
}"#;

    let (_rest, combined_query) =
        parse_combined_query(combined_query_input).expect("Failed to parse combined query");
    println!("Combined query parsed successfully.");
    println!("Parsed combined query: {:#?}", combined_query);

    // Process rules
    if let Some(rule) = combined_query.rule.clone() {
        let dynamic_rule =
            convert_combined_rule(rule.clone(), &mut database.dictionary, &combined_query.prefixes);
        println!("Dynamic rule: {:#?}", dynamic_rule);
        kg.add_rule(dynamic_rule.clone());
        println!("Rule added to KnowledgeGraph.");

        let expanded = match dynamic_rule.conclusion.1 {
            Term::Constant(code) => database.dictionary.decode(code).unwrap_or_else(|| ""),
            _ => "",
        };
        let local = if let Some(idx) = expanded.rfind('#') {
            &expanded[idx + 1..]
        } else if let Some(idx) = expanded.rfind(':') {
            &expanded[idx + 1..]
        } else {
            &expanded
        };
        let rule_key = local.to_lowercase();
        database.rule_map.insert(rule_key, expanded.to_string());
        
        // Check for ML.PREDICT clause and use the improved implementation
        if let Some(ml_predict) = &rule.ml_predict {
            println!("Using enhanced ML.PREDICT execution with model comparison...");
            match execute_ml_prediction_from_clause(ml_predict, &database) {
                Ok(predictions) => {
                    println!("\nML Predictions:");
                    for prediction in predictions {
                        println!(
                            "Room: {}, Predicted Temperature: {:.1}°C, Confidence: {:.2}",
                            prediction.room_id, prediction.predicted_temperature, prediction.confidence
                        );

                        // Add predictions to knowledge graph and database
                        let subject = format!("http://example.org#{}", prediction.room_id);
                        let predicate = "http://example.org/sensor#predictedTemperature";
                        kg.add_abox_triple(
                            &subject,
                            predicate,
                            &prediction.predicted_temperature.to_string(),
                        );

                        let subject_id = database.dictionary.encode(&subject);
                        let predicate_id = database.dictionary.encode(predicate);
                        let object_id = database
                            .dictionary
                            .encode(&prediction.predicted_temperature.to_string());
                        database.triples.insert(Triple {
                            subject: subject_id,
                            predicate: predicate_id,
                            object: object_id,
                        });
                    }
                }
                Err(e) => eprintln!("Error making ML predictions with dynamic execution: {}", e),
            }
        } else {
            // Fall back to original implementation if no ML.PREDICT clause
            println!("No ML.PREDICT clause found, using standard execution...");
        }
    }

    // Infer new facts
    let inferred_facts = kg.infer_new_facts_semi_naive();
    println!("\nInferred {} new fact(s):", inferred_facts.len());
    for triple in inferred_facts.iter() {
        println!(
            "{}",
            database.triple_to_string(triple, &database.dictionary)
        );
        database.triples.insert(triple.clone());
    }

    Ok(())
}
