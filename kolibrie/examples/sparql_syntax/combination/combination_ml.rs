/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::execute_query::execute_query_rayon_parallel2_volcano;
use kolibrie::streamertail_optimizer::operators::LogicalOperator;
use kolibrie::streamertail_optimizer::optimizer::Streamertail;
use pyo3::prepare_freethreaded_python;
use serde::{Deserialize, Serialize};
use shared::triple::Triple;
use std::collections::HashMap;
use std::error::Error;
use std::time::SystemTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Prediction {
    room_id: String,
    predicted_temperature: f64,
    confidence: f64,
    timestamp: SystemTime,
}

fn main() -> Result<(), Box<dyn Error>> {
    prepare_freethreaded_python();

    let rdf_xml_data = r#"<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org#"
         xmlns:sensor="http://example.org/sensor#">

    <!-- Room 101 -->
    <rdf:Description rdf:about="http://example.org#room101">
        <rdf:type rdf:resource="http://example.org#Room"/>
        <sensor:temperature>22.5</sensor:temperature>
        <sensor:humidity>45.0</sensor:humidity>
        <sensor:occupancy>5</sensor:occupancy>
    </rdf:Description>

    <!-- Room 102 -->
    <rdf:Description rdf:about="http://example.org#room102">
        <rdf:type rdf:resource="http://example.org#Room"/>
        <sensor:temperature>23.8</sensor:temperature>
        <sensor:humidity>52.0</sensor:humidity>
        <sensor:occupancy>8</sensor:occupancy>
    </rdf:Description>

    <!-- Room 103 -->
    <rdf:Description rdf:about="http://example.org#room103">
        <rdf:type rdf:resource="http://example.org#Room"/>
        <sensor:temperature>27.2</sensor:temperature>
        <sensor:humidity>48.0</sensor:humidity>
        <sensor:occupancy>3</sensor:occupancy>
    </rdf:Description>
</rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml_data);
    database.get_or_build_stats();
    println!("Database RDF triples loaded.");

    let rule_definition = r#"PREFIX ex: <http://example.org#>
PREFIX sensor: <http://example.org/sensor#>
RULE :TemperatureAlert(?room) :- 
    CONSTRUCT { 
        ?room ex:temperatureAlert "High temperature detected" .
    }
    WHERE { 
        ?room sensor:temperature ?temp ;
            sensor:humidity ?humidity
        FILTER (?temp > 25)
    }
    ML.PREDICT(MODEL "temperature_predictor",
        INPUT {
            SELECT ?room ?temp ?humidity ?occupancy
            WHERE {
                ?room sensor:temperature ?temp ;
                    sensor:humidity ?humidity ;
                    sensor:occupancy ?occupancy
            }
        },
        OUTPUT ?predicted_temp
    )"#;

    match process_rule_definition(rule_definition, &mut database) {
        Ok((rule, inferred_facts)) => {
            println!("Rule processed successfully.");
            
            println!("Rule details:");
            println!("  Premise patterns: {:?}", rule.premise);
            println!("  Filters: {:?}", rule.filters);
            println!("  Conclusion: {:?}", rule.conclusion);

            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                let dict = database.dictionary.read().unwrap();
                println!("  {}", database.triple_to_string(triple, &*dict));
            }
            
            if let Ok((_rest, (parsed_rule, _))) = parse_standalone_rule(rule_definition) {
                if let Some(ml_predict) = &parsed_rule.ml_predict {
                    println!("Using optimizer-based ML.PREDICT execution...");
                    
                    let logical_plan = build_ml_predict_logical_plan(ml_predict, &mut database)?;
                    
                    let mut optimizer = Streamertail::new(&database);
                    let physical_plan = optimizer.find_best_plan(&logical_plan);
                    
                    println!("Physical plan optimized.");
                    
                    let ml_results = optimizer.execute_plan(&physical_plan, &mut database);
                    
                    let predictions = process_ml_results(ml_results);
                    
                    println!("\nML Predictions:");
                    for prediction in &predictions {
                        println!(
                            "Room: {}, Predicted Temperature: {:.1}°C, Confidence: {:.2}",
                            prediction.room_id, prediction.predicted_temperature, prediction.confidence
                        );

                        add_prediction_to_database(&prediction, &mut database);

                        database.build_all_indexes();
                        database.get_or_build_stats();
                    }
                }
            }
        }
        Err(error) => {
            eprintln!("Error processing rule definition: {}", error);
            return Err(error.into());
        }
    }

    let select_query = r#"PREFIX ex: <http://example.org#>
PREFIX sensor: <http://example.org/sensor#>

SELECT ?room ?alert
WHERE { 
    ?room ex:temperatureAlert ?alert .
}"#;

    let query_results = execute_query_rayon_parallel2_volcano(select_query, &mut database);
    println!("Final query results (rooms with temperature alerts): {:?}", query_results);
    
    let predictions_query = r#"PREFIX sensor: <http://example.org/sensor#>
SELECT ?room ?predicted_temp ?confidence
WHERE {
    ?room sensor:predictedTemperature ?predicted_temp ;
          sensor:predictionConfidence ?confidence
}"#;
    
    let predictions_results = execute_query_rayon_parallel2_volcano(predictions_query, &mut database);
    println!("ML Predictions in database: {:?}", predictions_results);
    
    let all_rooms_query = r#"PREFIX sensor: <http://example.org/sensor#>
SELECT ?room ?temp ?humidity ?occupancy
WHERE {
    ?room sensor:temperature ?temp ;
          sensor:humidity ?humidity ;
          sensor:occupancy ?occupancy
}"#;
    
    let all_rooms_results = execute_query_rayon_parallel2_volcano(all_rooms_query, &mut database);
    println!("All room data: {:?}", all_rooms_results);

    Ok(())
}

fn build_ml_predict_logical_plan(
    ml_predict: &shared::query::MLPredictClause,
    database: &mut SparqlDatabase,
) -> Result<LogicalOperator, Box<dyn Error>> {
    let room_data = extract_data_for_ml(database)?;
    
    let input_variables: Vec<String> = ml_predict.input_select
        .iter()
        .filter(|(var, _, _)| {
            let stripped = var.strip_prefix('?').unwrap_or(var);
            stripped != "room" && stripped != "road" && stripped != "id" && stripped != "entity"
        })
        .map(|(var, _, _)| var.to_string())
        .collect();
    
    let buffer_plan = LogicalOperator::buffer(room_data, "ML_INPUT".to_string());
    
    Ok(LogicalOperator::ml_predict(
        buffer_plan,
        ml_predict.model.to_string(),
        input_variables,
        ml_predict.output.to_string(),
    ))
}

fn extract_data_for_ml(database: &mut SparqlDatabase) -> Result<Vec<HashMap<String, u32>>, Box<dyn Error>> {
    let mut results = Vec::new();
    
    let dict = database.dictionary.read().unwrap();
    let temp_pred = dict.string_to_id.get("http://example.org/sensor#temperature").copied();
    let humidity_pred = dict.string_to_id.get("http://example.org/sensor#humidity").copied();
    let occupancy_pred = dict.string_to_id.get("http://example.org/sensor#occupancy").copied();
    drop(dict);
    
    let temp_pred = temp_pred.unwrap_or_else(|| {
        database.dictionary.write().unwrap().encode("http://example.org/sensor#temperature")
    });
    let humidity_pred = humidity_pred.unwrap_or_else(|| {
        database.dictionary.write().unwrap().encode("http://example.org/sensor#humidity")
    });
    let occupancy_pred = occupancy_pred.unwrap_or_else(|| {
        database.dictionary.write().unwrap().encode("http://example.org/sensor#occupancy")
    });
    
    let subjects: std::collections::HashSet<u32> = database.triples
        .iter()
        .filter(|t| t.predicate == temp_pred)
        .map(|t| t.subject)
        .collect();
    
    for &subject in &subjects {
        let mut row = HashMap::new();
        row.insert("room".to_string(), subject);
        
        if let Some(triple) = database.triples.iter().find(|t| t.subject == subject && t.predicate == temp_pred) {
            row.insert("temp".to_string(), triple.object);
        }
        
        if let Some(triple) = database.triples.iter().find(|t| t.subject == subject && t.predicate == humidity_pred) {
            row.insert("humidity".to_string(), triple.object);
        }
        
        if let Some(triple) = database.triples.iter().find(|t| t.subject == subject && t.predicate == occupancy_pred) {
            row.insert("occupancy".to_string(), triple.object);
        }
        
        if row.len() == 4 {
            results.push(row);
        }
    }
    
    Ok(results)
}

fn process_ml_results(ml_results: Vec<HashMap<String, String>>) -> Vec<Prediction> {
    ml_results
        .iter()
        .map(|row| {
            let room_id = row.get("room")
                .and_then(|s| s.split('#').last())
                .unwrap_or("unknown")
                .to_string();
            
            let predicted_temperature = row.get("predicted_temp")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            
            let confidence = 0.95;
            
            Prediction {
                room_id,
                predicted_temperature,
                confidence,
                timestamp: SystemTime::now(),
            }
        })
        .collect()
}

fn add_prediction_to_database(prediction: &Prediction, database: &mut SparqlDatabase) {
    let subject = format!("http://example.org#{}", prediction.room_id);
    let predicate = "http://example.org/sensor#predictedTemperature";
    
    let mut dict = database.dictionary.write().unwrap();
    let subject_id = dict.encode(&subject);
    let predicate_id = dict.encode(predicate);
    let object_id = dict.encode(&prediction.predicted_temperature.to_string());
    drop(dict);
    
    database.triples.insert(Triple {
        subject: subject_id,
        predicate: predicate_id,
        object: object_id,
    });
    
    let confidence_predicate = "http://example.org/sensor#predictionConfidence";
    let mut dict = database.dictionary.write().unwrap();
    let confidence_predicate_id = dict.encode(confidence_predicate);
    let confidence_object_id = dict.encode(&prediction.confidence.to_string());
    drop(dict);
    
    database.triples.insert(Triple {
        subject: subject_id,
        predicate: confidence_predicate_id,
        object: confidence_object_id,
    });
    
    let timestamp_predicate = "http://example.org/sensor#predictionTimestamp";
    let timestamp_str = format!("{}", 
        chrono::DateTime::<chrono::Utc>::from(prediction.timestamp)
            .format("%Y-%m-%d %H:%M:%S"));
    
    let mut dict = database.dictionary.write().unwrap();
    let timestamp_predicate_id = dict.encode(timestamp_predicate);
    let timestamp_object_id = dict.encode(&timestamp_str);
    drop(dict);
    
    database.triples.insert(Triple {
        subject: subject_id,
        predicate: timestamp_predicate_id,
        object: timestamp_object_id,
    });
}