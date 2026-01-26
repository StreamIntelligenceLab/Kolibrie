use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::rsp_engine::{RSPBuilder, SimpleR2R, ResultConsumer, QueryExecutionMode};
use ml::MLHandler;
use ml::generate_ml_models;
use shared::triple::Triple;
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
    let (_rule, _inferred) = process_rule_definition(&rule_query, &mut database)?;
    
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
    
    run_combined_workflow(&mut engine, &mut ml_handler, &mut database)?;
    
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
    ?sensor ex:comfortLevel ?level .
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
) -> Result<(), Box<dyn std::error::Error>> {
    
    let rooms = vec!["Office1", "Office2"];
    let best_model = ml_handler.best_model.as_ref().unwrap().clone();
    
    // Simulate 8 time steps
    for time in 0..8 {
        for (room_idx, room) in rooms.iter().enumerate() {
            // Generate sensor data
            let base_temp = 20.0 + (time as f64 * 1.5) + (room_idx as f64 * 2.0);
            let temp = base_temp + (time as f64 * 0.5);
            let humidity = 50.0 + (time as f64 * 2.0);
            let occupancy = 5 + time + room_idx;
            
            // PART A: ML Prediction
            let input_data = vec![vec![temp, humidity, occupancy as f64]];
            let ml_result = ml_handler.predict(&best_model, input_data)?;
            let predicted_temp = ml_result.predictions[0];
            
            // PART B: Create streaming triples
            let sensor_uri = format!("http://example.org/Sensor_{}", room);
            let room_uri = format!("http://example.org/{}", room);
            
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
            
            // PART C: Add to stream (RSP processing)
            for triple in triples {
                engine.add_to_stream("sensorStream", triple.clone(), time);
                
                // Also add to database for reasoning
                database.add_triple(triple);
            }
            
            // PART D: Apply reasoning
            let mut reasoning_db = database.clone();
            let comfort_level = if temp > 25.0 {
                // Add inferred comfort level
                reasoning_db.add_triple_parts(
                    &sensor_uri,
                    "http://example.org/comfortLevel",
                    "uncomfortable"
                );
                "uncomfortable"
            } else if temp > 22.0 {
                reasoning_db.add_triple_parts(
                    &sensor_uri,
                    "http://example.org/comfortLevel",
                    "slightly_warm"
                );
                "slightly_warm"
            } else {
                reasoning_db.add_triple_parts(
                    &sensor_uri,
                    "http://example.org/comfortLevel",
                    "comfortable"
                );
                "comfortable"
            };
            
            // Update main database
            *database = reasoning_db;
            
            // PART E: Determine action
            let action = if temp > 25.0 || predicted_temp > 26.0 {
                "ACTIVATE COOLING"
            } else if predicted_temp > 24.0 {
                "PREPARE COOLING"
            } else {
                "NORMAL"
            };
            
            // Print results
            println!(
                "{:4} | {:7} | {:5.1} | {:8.1} | {:9} | {:12.1} | {:13} | {}",
                time,
                room,
                temp,
                humidity,
                occupancy,
                predicted_temp,
                comfort_level,
                action
            );
        }
        
        // Small delay to simulate real-time
        thread::sleep(Duration::from_millis(100));
    }
    
    Ok(())
}