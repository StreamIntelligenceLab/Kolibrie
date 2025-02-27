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

fn execute_ml_prediction(room_data: &[RoomData]) -> Result<Vec<Prediction>, Box<dyn Error>> {
    let mut ml_handler = MLHandler::new()?;

    let model_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("Stream_Reasoning")
        .join("ml")
        .join("src")
        .join("models")
        .join("temperature_predictor.pkl");

    if !model_path.exists() {
        return Err(format!("Model file not found at {}", model_path.display()).into());
    }

    ml_handler.load_model("temperature_predictor", model_path.to_str().unwrap())?;

    let features: Vec<Vec<f64>> = room_data
        .iter()
        .map(|data| vec![data.temperature, data.humidity, data.occupancy as f64])
        .collect();

    let prediction_results = ml_handler.predict("temperature_predictor", features)?;

    let predictions: Vec<Prediction> = room_data
        .iter()
        .zip(prediction_results.predictions.iter())
        .zip(prediction_results.probabilities.unwrap_or_default().iter())
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
    if let Some(rule) = combined_query.rule {
        let dynamic_rule =
            convert_combined_rule(rule, &mut database.dictionary, &combined_query.prefixes);
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
    }

    // Extract room data and execute ML predictions
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

    // Execute ML predictions
    match execute_ml_prediction(&room_data) {
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
        Err(e) => eprintln!("Error making predictions: {}", e),
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
