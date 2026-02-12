use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::rsp_engine::{RSPBuilder, SimpleR2R, ResultConsumer, QueryExecutionMode};
use ml::MLHandler;
use ml::generate_ml_models;
use datalog::reasoning::Reasoner;
use shared::triple::Triple;
use shared::rule::Rule;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup ML Model
    let mut ml_handler = setup_ml_model()?;
    
    // Setup database with initial ontology
    let mut database = setup_knowledge_base();
    
    // Define and load reasoning rule
    let rule_query = define_comfort_rule();
    let (rule, _inferred) = process_rule_definition(&rule_query, &mut database)?;
    
    // Setup RSP Engine with reasoning
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = result_container.clone();
    
    let result_consumer = ResultConsumer {
        function: Arc::new(Box::new(move |bindings: Vec<(String, String)>| {
            let mut results = result_container_clone.lock().unwrap();
            results.push(bindings.clone());
            
            // Print real-time alerts
            let binding_map: HashMap<_, _> = bindings.iter().cloned().collect();
            if let (Some(room), Some(temp)) = (binding_map.get("room"), binding_map.get("temp")) {
                println!("Stream Alert: Room {} at {}", room, temp);
            }
        }))
    };
    
    let rsp_query = r#"
        PREFIX ex: <http://example.org/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        
        REGISTER RSTREAM <http://out/comfort> AS
        SELECT ?room ?temp ?comfort
        FROM NAMED WINDOW :tempWindow ON :sensorStream [RANGE 60 STEP 10]
        WHERE {
            WINDOW :tempWindow {
                ?sensor ex:hasRoom ?room ;
                       ex:temperature ?temp ;
                       ex:comfortLevel ?comfort .
            }
        }
    "#;
    
    let mut engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>> = 
        RSPBuilder::new()
            .add_rsp_ql_query(rsp_query)
            .add_consumer(result_consumer)
            .add_r2r(Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano)))
            .build()?;
    
    // Run the combined workflow
    println!("Time | Room    | Temp  | Humidity | Occupancy | ML Predicted | Comfort Level | Action");
    println!("-----|---------|-------|----------|-----------|--------------|---------------|-------");
    
    run_combined_workflow(&mut engine, &mut ml_handler, &mut database, &rule)?;
    
    // Stop the engine
    engine.stop();
    thread::sleep(Duration::from_secs(1));

    Ok(())
}

fn setup_ml_model() -> Result<MLHandler, Box<dyn std::error::Error>> {
    let model_dir = {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let ml_dir = path.join("ml");
            if ml_dir.exists() && ml_dir.is_dir() {
                break ml_dir.join("examples").join("models");
            }
            if !path.pop() {
                break std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("models");
            }
        }
    };
    
    std::fs::create_dir_all(&model_dir)?;
    
    // Check if models exist
    let models_exist = std::fs::read_dir(&model_dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let path = entry.path();
            path.is_file() && path.extension().map_or(false, |ext| ext == "pkl")
        })
        .count() >= 3;
    
    if !models_exist {
        generate_ml_models(&model_dir, "predictor.py")?;
    }
    
    let mut ml_handler = MLHandler::new()?;
    let model_ids = ml_handler.discover_and_load_models(&model_dir, "predictor")?;
    
    println!("Loaded {} ML models", model_ids.len());
    println!("Selected best model: {}", ml_handler.best_model.as_ref().unwrap_or(&"unknown".to_string()));
    
    Ok(ml_handler)
}

fn setup_knowledge_base() -> SparqlDatabase {
    let mut database = SparqlDatabase::new();
    
    // Register prefixes
    database.prefixes.insert("ex".to_string(), "http://example.org/".to_string());
    database.prefixes.insert("rdf".to_string(), "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
    
    // Add initial ontology - room definitions
    database.add_triple_parts("http://example.org/Office1", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Room");
    database.add_triple_parts("http://example.org/Office2", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Room");
    
    database
}

fn define_comfort_rule() -> String {
    r#"PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :ComfortLevelRule :- 
CONSTRUCT {
    ?sensor ex:comfortLevel "uncomfortable" .
}
WHERE {
    ?sensor ex:temperature ?temp .
    FILTER(?temp > 25)
}
    "#.to_string()
}

fn run_combined_workflow(
    engine: &mut kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>>,
    ml_handler: &mut MLHandler,
    database: &mut SparqlDatabase,
    comfort_rule: &Rule,
) -> Result<(), Box<dyn std::error::Error>> {
    
    let rooms = vec!["Office1", "Office2"];
    let best_model = ml_handler.best_model.as_ref().unwrap().clone();
    
    for time in 0..8 {
        for (room_idx, room) in rooms.iter().enumerate() {
            let base_temp = 20.0 + (time as f64 * 1.5) + (room_idx as f64 * 2.0);
            let temp = base_temp + (time as f64 * 0.5);
            let humidity = 50.0 + (time as f64 * 2.0);
            let occupancy = 5 + time + room_idx;
            
            // ML Prediction
            let input_data = vec![vec![temp, humidity, occupancy as f64]];
            let ml_result = ml_handler.predict(&best_model, input_data)?;
            let predicted_temp = ml_result.predictions[0];
            
            // Create sensor and room URIs
            let sensor_uri = format!("http://example.org/Sensor_{}", room);
            let room_uri = format!("http://example.org/{}", room);
            
            // Add triples
            database.add_triple_parts(&sensor_uri, "http://example.org/hasRoom", &room_uri);
            database.add_triple_parts(&sensor_uri, "http://example.org/temperature", &temp.to_string());
            database.add_triple_parts(&sensor_uri, "http://example.org/humidity", &humidity.to_string());
            database.add_triple_parts(&sensor_uri, "http://example.org/occupancy", &occupancy.to_string());
            
            let triples_data = format!(
                "<{}> <http://example.org/hasRoom> <{}> .
                 <{}> <http://example.org/temperature> \"{}\" .
                 <{}> <http://example.org/humidity> \"{}\" .
                 <{}> <http://example.org/occupancy> \"{}\" .",
                sensor_uri, room_uri,
                sensor_uri, temp,
                sensor_uri, humidity,
                sensor_uri, occupancy
            );
            
            let triples = engine.parse_data(&triples_data);
            for triple in triples {
                engine.add_to_stream("sensorStream", triple.clone(), time);
            }
            
            let mut reasoner = Reasoner::new();
            
            // Share the same dictionary to maintain ID consistency
            reasoner.dictionary = database.dictionary.clone();
            
            // Copy all triples from database to reasoner
            for triple in database.triples.iter() {
                if let (Some(s), Some(p), Some(o)) = (
                    database.dictionary.decode(triple.subject),
                    database.dictionary.decode(triple.predicate),
                    database.dictionary.decode(triple.object)
                ) {
                    reasoner.add_abox_triple(s, p, o);
                }
            }
            
            // Add the rule
            reasoner.add_rule(comfort_rule.clone());
            
            // Perform inference
            let inferred_facts = reasoner.infer_new_facts_semi_naive();
            
            // Add inferred facts back to database
            for fact in &inferred_facts {
                database.add_triple(fact.clone());
            }
            
            // Sync dictionaries back
            database.dictionary = reasoner.dictionary.clone();
            
            // Query for the inferred comfort level
            let comfort_level = query_comfort_level(database, &sensor_uri);
            
            // Determine action
            let action = if temp > 25.0 || predicted_temp > 26.0 {
                "ACTIVATE COOLING"
            } else if predicted_temp > 24.0 {
                "PREPARE COOLING"
            } else {
                "NORMAL"
            };
            
            println!(
                "{:4} | {:7} | {:5.1} | {:8.1} | {:9} | {:12.1} | {:13} | {}",
                time, room, temp, humidity, occupancy,
                predicted_temp, comfort_level, action
            );
        }
        
        thread::sleep(Duration::from_millis(100));
    }
    
    Ok(())
}

// Helper function to query the inferred comfort level
fn query_comfort_level(database: &SparqlDatabase, sensor_uri: &str) -> String {
    if let (Some(&comfort_pred_id), Some(&sensor_id)) = (
        database.dictionary.string_to_id.get("http://example.org/comfortLevel"),
        database.dictionary.string_to_id.get(sensor_uri)
    ) {
        if let Some(triple) = database.triples.iter()
            .find(|t| t.subject == sensor_id && t.predicate == comfort_pred_id)
        {
            if let Some(value) = database.dictionary.decode(triple.object) {
                return value.to_string();
            }
        }
    }
    
    "comfortable".to_string()
}