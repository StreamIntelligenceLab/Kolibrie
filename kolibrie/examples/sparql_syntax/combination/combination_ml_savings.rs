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
use kolibrie::execute_ml::execute_ml_prediction_from_clause;
use kolibrie::execute_query::execute_query;
use ml::MLHandler;
use pyo3::prepare_freethreaded_python;
use serde::{Deserialize, Serialize};
use shared::triple::Triple;
use std::error::Error;
use std::time::SystemTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FinancialData {
    user_id: String,
    income: f64,
    spending: f64,
    savings_rate: f64,
    timestamp: SystemTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SavingsPrediction {
    user_id: String,
    predicted_savings: f64,
    confidence: f64,
    timestamp: SystemTime,
}

// Function to extract financial data from the database
fn extract_financial_data_from_database(
    database: &SparqlDatabase,
) -> Result<Vec<FinancialData>, Box<dyn Error>> {
    // Extract data from database
    let financial_data: Vec<FinancialData> = database
        .triples
        .iter()
        .filter(|triple| {
            database
                .dictionary
                .decode(triple.predicate)
                .map_or(false, |pred| pred.ends_with("income"))
        })
        .map(|triple| {
            let user_id = database
                .dictionary
                .decode(triple.subject)
                .unwrap_or_default()
                .split('#')
                .last()
                .unwrap_or_default()
                .to_string();

            let income = database
                .dictionary
                .decode(triple.object)
                .unwrap_or_default()
                .parse()
                .unwrap_or(0.0);

            // Find spending and savings_rate
            let spending = database
                .triples
                .iter()
                .find(|t| {
                    t.subject == triple.subject
                        && database
                            .dictionary
                            .decode(t.predicate)
                            .map_or(false, |p| p.ends_with("spending"))
                })
                .and_then(|t| database.dictionary.decode(t.object))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);

            let savings_rate = database
                .triples
                .iter()
                .find(|t| {
                    t.subject == triple.subject
                        && database
                            .dictionary
                            .decode(t.predicate)
                            .map_or(false, |p| p.ends_with("savings_rate"))
                })
                .and_then(|t| database.dictionary.decode(t.object))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.10); // Default to 10% savings rate if not specified

            FinancialData {
                user_id,
                income,
                spending,
                savings_rate,
                timestamp: SystemTime::now(),
            }
        })
        .collect();

    println!("Found {} financial data entries", financial_data.len());
    for data in &financial_data {
        println!(
            "User: {}, Income: ${:.2}, Spending: ${:.2}, Savings Rate: {:.2}%",
            data.user_id, data.income, data.spending, data.savings_rate * 100.0
        );
    }
    
    // Ensure we have data before proceeding
    if financial_data.is_empty() {
        return Err("No input data found for ML prediction".into());
    }
    
    Ok(financial_data)
}

// Function that handles savings prediction logic
fn predict_savings(
    ml_handler: &MLHandler,
    best_model: &str,
    financial_data: &[FinancialData],
    feature_names: &[String],
) -> Result<(Vec<SavingsPrediction>, kolibrie:: execute_ml::MLPredictTiming), Box<dyn Error>> {
    // Dynamically build feature vectors based on selected variables
    let features: Vec<Vec<f64>> = financial_data
        .iter()
        .map(|data| {
            let mut feature_vector = Vec::new();
            
            // Only include features that were specified in the SELECT clause
            for feature_name in feature_names {
                match feature_name.as_str() {
                    "income" => feature_vector.push(data.income),
                    "spending" => feature_vector.push(data.spending),
                    "savings_rate" => feature_vector.push(data.savings_rate),
                    "disposable_income" => feature_vector.push(data.income - data.spending),
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

    // Create timing structure
    let timing = kolibrie::execute_ml::MLPredictTiming {
        total_time: 0.0, // Will be updated by execute_ml_prediction_from_clause
        rust_to_python_time: 0.0, // Will be updated by execute_ml_prediction_from_clause
        python_preprocessing_time: prediction_results.timing.preprocessing_time,
        actual_prediction_time: prediction_results.timing.actual_prediction_time,
        python_postprocessing_time: prediction_results.timing.postprocessing_time,
        python_to_rust_time: 0.0, // Will be updated by execute_ml_prediction_from_clause
    };
    
    // Print performance of the selected model during this prediction
    println!("\nPerformance during prediction:");
    println!("  Prediction Time: {:.4} seconds", prediction_results.performance_metrics.prediction_time);
    println!("  Memory Usage: {:.2} MB", prediction_results.performance_metrics.memory_usage_mb);
    println!("  CPU Usage: {:.2}%", prediction_results.performance_metrics.cpu_usage_percent);
    
    // Create prediction objects
    let predictions: Vec<SavingsPrediction> = financial_data
        .iter()
        .zip(prediction_results.predictions.iter())
        .zip(prediction_results.probabilities.unwrap_or_default().iter().chain(std::iter::repeat(&0.95)))
        .map(|((data, &pred), &conf)| SavingsPrediction {
            user_id: data.user_id.clone(),
            predicted_savings: pred,
            confidence: conf,
            timestamp: SystemTime::now(),
        })
        .collect();

    // Print out predictions with explanations
    for (i, prediction) in predictions.iter().enumerate() {
        let data = &financial_data[i];
        let disposable_income = data.income - data.spending;
        
        println!("\nUser {} Savings Prediction:", data.user_id);
        println!("  Income: ${:.2}", data.income);
        println!("  Spending: ${:.2}", data.spending);
        println!("  Disposable Income: ${:.2}", disposable_income);
        println!("  Current Savings Rate: {:.2}%", data.savings_rate * 100.0);
        println!("  Predicted Future Savings: ${:.2}", prediction.predicted_savings);
        println!("  Confidence: {:.2}%", prediction.confidence * 100.0);
        
        // Provide some basic financial advice based on the prediction
        if prediction.predicted_savings < 0.0 {
            println!("  Warning: Negative savings predicted! Consider reducing expenses.");
        } else if prediction.predicted_savings < (data.income * 0.1) {
            println!("  Advice: Your savings are below 10% of your income. Consider budgeting for more savings.");
        } else if prediction.predicted_savings > (data.income * 0.2) {
            println!("  Great job! Your savings are above 20% of your income.");
        }
    }

    Ok((predictions, timing))
}

fn main() -> Result<(), Box<dyn Error>> {
    prepare_freethreaded_python();

    let rdf_xml_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:ex="http://example.org#"
                xmlns:finance="http://example.org/finance#">
        <rdf:Description rdf:about="http://example.org#User101">
            <finance:income>5200.00</finance:income>
            <finance:spending>3600.00</finance:spending>
            <finance:savings_rate>0.15</finance:savings_rate>
            <finance:last_updated>2025-05-27T17:43:10Z</finance:last_updated>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org#User102">
            <finance:income>4800.00</finance:income>
            <finance:spending>4100.00</finance:spending>
            <finance:savings_rate>0.08</finance:savings_rate>
            <finance:last_updated>2025-05-27T17:43:10Z</finance:last_updated>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org#User103">
            <finance:income>7200.00</finance:income>
            <finance:spending>3900.00</finance:spending>
            <finance:savings_rate>0.22</finance:savings_rate>
            <finance:last_updated>2025-05-27T17:43:10Z</finance:last_updated>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org#User104">
            <finance:income>3800.00</finance:income>
            <finance:spending>3200.00</finance:spending>
            <finance:savings_rate>0.12</finance:savings_rate>
            <finance:last_updated>2025-05-27T17:43:10Z</finance:last_updated>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org#User105">
            <finance:income>6500.00</finance:income>
            <finance:spending>5900.00</finance:spending>
            <finance:savings_rate>0.05</finance:savings_rate>
            <finance:last_updated>2025-05-27T17:43:10Z</finance:last_updated>
        </rdf:Description>
        </rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml_data);
    println!("Database RDF triples loaded.");

    // Define the rule separately with CONSTRUCT and WHERE clauses
    let rule_definition = r#"PREFIX finance: <http://example.org/finance#>
PREFIX ex: <http://example.org#>

RULE :SavingsAlert(?user) :- 
    CONSTRUCT { 
        ?user ex:savingsAlert "High spending detected - savings at risk" .
    }
    WHERE { 
        ?user finance:income ?income ;
            finance:spending ?spending .
        FILTER (?spending > 5200)
    }
    ML.PREDICT(MODEL "saving_predictor",
        INPUT {
            SELECT ?user ?income ?spending ?savings_rate
            WHERE {
                ?user finance:income ?income ;
                    finance:spending ?spending ;
                    finance:savings_rate ?savings_rate
            }
        },
        OUTPUT ?predicted_savings
    )"#;

    // Use process_rule_definition to process the rule
    match process_rule_definition(rule_definition, &mut database) {
        Ok((rule, inferred_facts)) => {
            println!("Rule processed successfully.");
            
            // Print rule details
            println!("Rule details:");
            println!("  Premise patterns: {:?}", rule.premise);
            println!("  Filters: {:?}", rule.filters);
            println!("  Conclusion: {:?}", rule.conclusion);

            println!("Inferred {} new fact(s):", inferred_facts.len());
            for triple in inferred_facts.iter() {
                println!("  {}", database.triple_to_string(triple, &database.dictionary));
            }
            
            // Parse the rule to get ML.PREDICT clause
            if let Ok((_rest, (parsed_rule, _))) = parse_standalone_rule(rule_definition) {
                if let Some(ml_predict) = &parsed_rule.ml_predict {
                    println!("Using enhanced ML.PREDICT execution with model comparison...");
                    match execute_ml_prediction_from_clause(
                        ml_predict, 
                        &database, 
                        "saving_predictor.py", 
                        extract_financial_data_from_database, 
                        predict_savings
                    ) {
                        Ok((predictions, timing)) => {
                            timing.print_breakdown();
                            println!("\nML Predictions:");
                            for prediction in predictions {
                                println!(
                                    "User: {}, Predicted Savings: ${:.2}, Confidence: {:.2}",
                                    prediction.user_id, prediction.predicted_savings, prediction.confidence
                                );

                                // Add predictions to database
                                let subject = format!("http://example.org#{}", prediction.user_id);
                                let predicate = "http://example.org/finance#predictedSavings";
                                let subject_id = database.dictionary.encode(&subject);
                                let predicate_id = database.dictionary.encode(predicate);
                                let object_id = database
                                    .dictionary
                                    .encode(&prediction.predicted_savings.to_string());
                                database.triples.insert(Triple {
                                    subject: subject_id,
                                    predicate: predicate_id,
                                    object: object_id,
                                });
                                
                                // Also add confidence score to the database
                                let confidence_predicate = "http://example.org/finance#predictionConfidence";
                                let confidence_predicate_id = database.dictionary.encode(confidence_predicate);
                                let confidence_object_id = database
                                    .dictionary
                                    .encode(&prediction.confidence.to_string());
                                database.triples.insert(Triple {
                                    subject: subject_id,
                                    predicate: confidence_predicate_id,
                                    object: confidence_object_id,
                                });
                                
                                // Add timestamp to the database
                                let timestamp_predicate = "http://example.org/finance#predictionTimestamp";
                                let timestamp_str = format!("{}", 
                                    chrono::DateTime::<chrono::Utc>::from(prediction.timestamp)
                                        .format("%Y-%m-%d %H:%M:%S"));
                                let timestamp_predicate_id = database.dictionary.encode(timestamp_predicate);
                                let timestamp_object_id = database
                                    .dictionary
                                    .encode(&timestamp_str);
                                database.triples.insert(Triple {
                                    subject: subject_id,
                                    predicate: timestamp_predicate_id,
                                    object: timestamp_object_id,
                                });
                            }
                            
                            // Add an execution metadata triple with current timestamp
                            let metadata_subject = "http://example.org#predictionExecution";
                            let timestamp_predicate = "http://example.org/metadata#executionTime";
                            let timestamp_value = "2025-05-30 14:49:43"; // Current UTC time
                            
                            let metadata_subject_id = database.dictionary.encode(metadata_subject);
                            let timestamp_predicate_id = database.dictionary.encode(timestamp_predicate);
                            let timestamp_value_id = database.dictionary.encode(timestamp_value);
                            
                            database.triples.insert(Triple {
                                subject: metadata_subject_id,
                                predicate: timestamp_predicate_id,
                                object: timestamp_value_id,
                            });
                        }
                        Err(e) => eprintln!("Error making ML predictions with dynamic execution: {}", e),
                    }
                }
            }
        }
        Err(error) => {
            eprintln!("Error processing rule definition: {}", error);
            return Err(error.into());
        }
    }

    // Define the SELECT query to get users with savings alerts
    let select_query = r#"PREFIX finance: <http://example.org/finance#>
PREFIX ex: <http://example.org#>

SELECT ?user ?alert
WHERE { 
    ?user ex:savingsAlert ?alert .
}"#;

    // Execute the SELECT query to get results
    let query_results = execute_query(select_query, &mut database);
    println!("Final query results (users with savings alerts): {:?}", query_results);
    
    // Execute a query to show predictions for comparison
    let predictions_query = r#"PREFIX finance: <http://example.org/finance#>
SELECT ?user ?predicted_savings ?confidence
WHERE {
    ?user finance:predictedSavings ?predicted_savings ;
          finance:predictionConfidence ?confidence
}"#;
    
    let predictions_results = execute_query(predictions_query, &mut database);
    println!("ML Predictions in database: {:?}", predictions_results);
    
    // Execute a query to show all user financial data for comparison
    let all_users_query = r#"PREFIX finance: <http://example.org/finance#>
SELECT ?user ?income ?spending ?savings_rate
WHERE {
    ?user finance:income ?income ;
          finance:spending ?spending ;
          finance:savings_rate ?savings_rate
}"#;
    
    let all_users_results = execute_query(all_users_query, &mut database);
    println!("All user financial data: {:?}", all_users_results);

    Ok(())
}
