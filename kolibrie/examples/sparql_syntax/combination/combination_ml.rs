/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalog::knowledge_graph::KnowledgeGraph;
use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::execute_ml::execute_ml_prediction_from_clause;
use ml::MLHandler;
use pyo3::prepare_freethreaded_python;
use serde::{Deserialize, Serialize};
use shared::terms::Term;
use shared::triple::Triple;
use std::error::Error;
use std::time::SystemTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RoomData {
    room_id: String,
    temperature: f64,
    humidity: f64,
    occupancy: i32,
    timestamp: SystemTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Prediction {
    room_id: String,
    predicted_temperature: f64,
    confidence: f64,
    timestamp: SystemTime,
}

// Function to extract room data from the database
fn extract_room_data_from_database(
    database: &SparqlDatabase,
) -> Result<Vec<RoomData>, Box<dyn Error>> {
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
    
    // Ensure we have data before proceeding
    if room_data.is_empty() {
        return Err("No input data found for ML prediction".into());
    }
    
    Ok(room_data)
}

// Function that handles temperature prediction logic
fn predict_temperatures(
    ml_handler: &MLHandler,
    best_model: &str,
    room_data: &[RoomData],
    feature_names: &[String],
) -> Result<Vec<Prediction>, Box<dyn Error>> {
    // Dynamically build feature vectors based on selected variables
    let features: Vec<Vec<f64>> = room_data
        .iter()
        .map(|data| {
            let mut feature_vector = Vec::new();
            
            // Only include features that were specified in the SELECT clause
            for feature_name in feature_names {
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
    
    // Use the selected model
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

        let expanded = if let Some(first_conclusion) = dynamic_rule.conclusion.first() {
            match first_conclusion.1 {
                Term::Constant(code) => database.dictionary.decode(code).unwrap_or_else(|| ""),
                _ => "",
            }
        } else {
            ""
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
            match execute_ml_prediction_from_clause(ml_predict, &database, "predictor.py", extract_room_data_from_database,
    predict_temperatures) {
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
