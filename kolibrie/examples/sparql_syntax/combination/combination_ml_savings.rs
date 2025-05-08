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
) -> Result<Vec<SavingsPrediction>, Box<dyn Error>> {
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

    Ok(predictions)
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
            <finance:last_updated>2025-05-01T10:30:00Z</finance:last_updated>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org#User102">
            <finance:income>4800.00</finance:income>
            <finance:spending>4100.00</finance:spending>
            <finance:savings_rate>0.08</finance:savings_rate>
            <finance:last_updated>2025-05-03T15:45:00Z</finance:last_updated>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org#User103">
            <finance:income>7200.00</finance:income>
            <finance:spending>3900.00</finance:spending>
            <finance:savings_rate>0.22</finance:savings_rate>
            <finance:last_updated>2025-05-06T08:15:00Z</finance:last_updated>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org#User104">
            <finance:income>3800.00</finance:income>
            <finance:spending>3200.00</finance:spending>
            <finance:savings_rate>0.12</finance:savings_rate>
            <finance:last_updated>2025-05-07T16:20:00Z</finance:last_updated>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org#User105">
            <finance:income>6500.00</finance:income>
            <finance:spending>5900.00</finance:spending>
            <finance:savings_rate>0.05</finance:savings_rate>
            <finance:last_updated>2025-05-08T07:10:00Z</finance:last_updated>
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

    let combined_query_input = r#"PREFIX finance: <http://example.org/finance#>
PREFIX ex: <http://example.org/>

RULE :SavingsAlert(?user) :- 
    WHERE { 
        ?user finance:income ?income ;
              finance:spending ?spending
        FILTER (?spending > 5200)
    } 
    => 
    { 
        ?user ex:savingsAlert "High spending detected - savings at risk" .
    }.
    ML.PREDICT(MODEL saving_predictor,
        INPUT {
            SELECT ?user ?income ?spending ?savings_rate
            WHERE {
                ?user finance:income ?income ;
                      finance:spending ?spending ;
                      finance:savings_rate ?savings_rate
            }
        },
        OUTPUT ?predicted_savings
    )

SELECT ?user ?alert ?predicted_savings
WHERE { 
    :SavingsAlert(?user) .
    ?user ex:savingsAlert ?alert ;
          finance:predictedSavings ?predicted_savings
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
            match execute_ml_prediction_from_clause(ml_predict, &database, "saving_predictor.py", extract_financial_data_from_database, predict_savings) {
                Ok(predictions) => {
                    println!("\nML Predictions:");
                    for prediction in predictions {
                        println!(
                            "User: {}, Predicted Savings: ${:.2}, Confidence: {:.2}",
                            prediction.user_id, prediction.predicted_savings, prediction.confidence
                        );

                        // Add predictions to knowledge graph and database
                        let subject = format!("http://example.org#{}", prediction.user_id);
                        let predicate = "http://example.org/finance#predictedSavings";
                        kg.add_abox_triple(
                            &subject,
                            predicate,
                            &prediction.predicted_savings.to_string(),
                        );

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
                    
                    // Add an execution metadata triple with timestamp
                    let metadata_subject = "http://example.org#predictionExecution";
                    let timestamp_predicate = "http://example.org/metadata#executionTime";
                    let timestamp_value = "2025-05-08 08:57:25"; // Current UTC time
                    
                    kg.add_abox_triple(
                        metadata_subject,
                        timestamp_predicate,
                        timestamp_value,
                    );
                    
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
        } else {
            // Fall back to original implementation if no ML.PREDICT clause
            println!("No ML.PREDICT clause found, using standard execution...");
        }
    } else {
        println!("No rule found in the combined query.");
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
