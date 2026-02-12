use kolibrie::execute_query::*;
use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use rand::Rng;


fn main() {
    println!("=== Continuous Real-Time Stream Testing with Windowing ===\n");
        
    let mut database = SparqlDatabase::new();
    let mut stream_counter = 0;
    let mut total_alerts = 0;
    
    // Set up the streaming simulation to run for 24 seconds
    // This simulates a real-world scenario where data comes in batches every few seconds
    let stream_duration = Duration::from_secs(24);
    let batch_interval = Duration::from_secs(3); // New sensor data arrives every 3 seconds
    let start_time = SystemTime::now();
    
    println!("Starting continuous stream simulation...");
    println!("Data will arrive every: {:?}", batch_interval);
    println!("Total test duration: {:?}\n", stream_duration);
    
    loop {
        let elapsed = start_time.elapsed().unwrap();
        if elapsed >= stream_duration {
            break;
        }
        
        stream_counter += 1;
        let current_timestamp = get_current_timestamp();
        
        println!("=== PROCESSING BATCH {} === [{}s elapsed]", stream_counter, elapsed.as_secs());
        
        // Create a new batch of sensor readings with realistic temperature variations
        let sensor_data = generate_dynamic_sensor_batch(stream_counter, current_timestamp);
        
        // Convert the sensor data to RDF format and load it into our database
        let rdf_xml = create_sensor_rdf_batch(sensor_data.clone(), stream_counter);
        database.parse_rdf(&rdf_xml);
        
        println!("Successfully loaded batch {} - Database now contains {} triples", 
                stream_counter, database.triples.len());
        
        // Display the current batch of sensor readings with their status
        for (room, temp, timestamp) in &sensor_data {
            let status = if *temp > 100 { "CRITICAL" } 
                        else if *temp > 85 { "HIGH" }
                        else if *temp < 30 { "LOW" }
                        else { "NORMAL" };
            println!("  {} -> {}°C at {} - Status: {}", room, temp, timestamp, status);
        }
        
        // Process the data through our windowing rules to detect anomalies and patterns
        let batch_alerts = apply_simplified_windowing_rules(&mut database, stream_counter);
        total_alerts += batch_alerts;
        
        // Show current streaming statistics
        println!("Current Stream Statistics:");
        println!("   - Batches processed so far: {}", stream_counter);
        println!("   - Total data points in database: {}", database.triples.len());
        println!("   - Total alerts generated: {}", total_alerts);
        println!("   - Time remaining in simulation: {}s\n", 
                (stream_duration.as_secs() as i64 - elapsed.as_secs() as i64).max(0));
        
        // Wait for the next batch of data to arrive
        thread::sleep(batch_interval);
    }
    
    // Show final results after all streaming is complete
    println!("=== STREAMING SIMULATION COMPLETE ===");
    println!("Final Statistics Summary:");
    println!("   - Total batches processed: {}", stream_counter);
    println!("   - Total data points stored: {}", database.triples.len());
    println!("   - Total alerts generated: {}", total_alerts);
    if stream_counter > 0 {
        println!("   - Average alerts per batch: {:.2}", total_alerts as f64 / stream_counter as f64);
    }
    
    // Run a comprehensive analysis of all the collected data
    println!("\nRunning Final Data Analysis:");
    run_final_analysis(&mut database);
}

fn generate_dynamic_sensor_batch(batch_num: u32, base_timestamp: u64) -> Vec<(String, i32, u64)> {
    let mut rng = rand::rng();
    let rooms = vec!["Room101", "Room102", "Room103", "Room104", "Room105"];
    let mut batch_data = Vec::new();
    
    // Each batch contains 2-3 sensor readings to simulate realistic data flow
    let reading_count = rng.random_range(2..=3);
    
    for i in 0..reading_count {
        let room = rooms[rng.random_range(0..rooms.len())].to_string();
        
        // Generate realistic temperature variations based on room characteristics
        // Each room has different baseline temperatures to simulate real environments
        let base_temp = match room.as_str() {
            "Room101" => 85,  // Equipment room - tends to run hot
            "Room102" => 72,  // Office space - normal temperature
            "Room103" => 95,  // Server room - very hot due to equipment
            "Room104" => 68,  // Well-ventilated room - cooler
            "Room105" => 78,  // Meeting room - moderate temperature
            _ => 75,
        };
        
        // Add random variation to simulate real sensor fluctuations
        // Later batches get additional heat boost to simulate system stress
        let variation = rng.random_range(-10..=25);
        let heat_boost = if batch_num > 5 { rng.random_range(0..=15) } else { 0 };
        let temperature = (base_temp + variation + heat_boost).max(10).min(140);
        
        // Stagger timestamps within the batch to simulate readings at different times
        let timestamp = base_timestamp + (i as u64 * 5);
        
        batch_data.push((room, temperature, timestamp));
    }
    
    batch_data
}

fn create_sensor_rdf_batch(sensor_data: Vec<(String, i32, u64)>, batch_num: u32) -> String {
    // Create the RDF/XML header with necessary namespace declarations
    let mut rdf = String::from(r#"<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org#"
         xmlns:stream="http://example.org/stream#">"#);
    
    // Convert each sensor reading into RDF format
    // Each reading gets a unique URI and contains room, temperature, timestamp, and batch info
    for (i, (room, temp, timestamp)) in sensor_data.iter().enumerate() {
        let reading_id = format!("http://example.org/stream#reading{}_{}", batch_num, i + 1);
        rdf.push_str(&format!(r#"
  <rdf:Description rdf:about="{}">
    <ex:room>{}</ex:room>
    <ex:temperature>{}</ex:temperature>
    <ex:timestamp>{}</ex:timestamp>
    <ex:batchNumber>{}</ex:batchNumber>
  </rdf:Description>"#, reading_id, room, temp, timestamp, batch_num));
    }
    
    rdf.push_str("\n</rdf:RDF>");
    rdf
}

fn apply_simplified_windowing_rules(database: &mut SparqlDatabase, _batch_num: u32) -> u32 {
    let mut alert_count = 0;
    
    // Apply different windowing rules to detect various types of anomalies
    // Each rule uses a different stream processing approach (RSTREAM, ISTREAM, DSTREAM)
    
    // Rule 1: RSTREAM - Detects high temperature readings using sliding window
    println!("Applying RSTREAM rule for high temperature detection...");
    let high_temp_rule = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :TemperatureAlert(?room) :- 
RSTREAM
FROM NAMED WINDOW <http://example.org/window1> ON <http://example.org/temperatureStream> [SLIDING 6 SLIDE 2 REPORT ON_WINDOW_CLOSE TICK TIME_DRIVEN] 
CONSTRUCT { 
    ?room ex:hasAlert "high_temperature" .
}
WHERE { 
    ?reading ex:room ?room ; 
             ex:temperature ?temp ;
             ex:timestamp ?time .
    FILTER (?temp > 90)
}"#;

    match process_rule_definition(high_temp_rule, database) {
        Ok((_, inferred_facts)) => {
            if !inferred_facts.is_empty() {
                println!("RSTREAM rule detected {} high temperature alerts", inferred_facts.len());
                alert_count += inferred_facts.len() as u32;
                
                // Show what alerts were generated
                for (i, triple) in inferred_facts.iter().enumerate() {
                    let s = database.dictionary.decode(triple.subject).unwrap_or("unknown");
                    let p = database.dictionary.decode(triple.predicate).unwrap_or("unknown");
                    let o = database.dictionary.decode(triple.object).unwrap_or("unknown");
                    println!("    Alert {}: {} -> {} -> {}", i + 1, s, p, o);
                }
            }
        }
        Err(e) => println!("RSTREAM rule processing failed: {}", e),
    }
    
    // Rule 2: ISTREAM - Detects new moderate temperature readings using tumbling window
    println!("Applying ISTREAM rule for moderate temperature detection...");
    let moderate_rule = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :NewHighTemp(?room) :- 
ISTREAM
FROM NAMED WINDOW <http://example.org/window2> ON <http://example.org/tempStream> [TUMBLING 4 REPORT NON_EMPTY_CONTENT TICK TUPLE_DRIVEN] 
CONSTRUCT { 
    ?room ex:newHighReading ?temp .
}
WHERE { 
    ?reading ex:room ?room ; 
             ex:temperature ?temp .
    FILTER (?temp > 85)
}"#;

    match process_rule_definition(moderate_rule, database) {
        Ok((_, inferred_facts)) => {
            if !inferred_facts.is_empty() {
                println!("ISTREAM rule found {} new high temperature readings", inferred_facts.len());
                alert_count += inferred_facts.len() as u32;
                
                // Show what new readings were detected
                for (i, triple) in inferred_facts.iter().enumerate() {
                    let s = database.dictionary.decode(triple.subject).unwrap_or("unknown");
                    let p = database.dictionary.decode(triple.predicate).unwrap_or("unknown");
                    let o = database.dictionary.decode(triple.object).unwrap_or("unknown");
                    println!("    New reading {}: {} -> {} -> {}", i + 1, s, p, o);
                }
            }
        }
        Err(e) => println!("ISTREAM rule processing failed: {}", e),
    }
    
    // Rule 3: DSTREAM - Detects extreme temperature readings using range-based window
    println!("Applying DSTREAM rule for extreme temperature detection...");
    let extreme_rule = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :ExtremeAlert(?room) :- 
DSTREAM
FROM NAMED WINDOW <http://example.org/window3> ON <http://example.org/sensorStream> [RANGE 8 REPORT PERIODIC TICK TIME_DRIVEN]
CONSTRUCT { 
    ?room ex:extremeLevel ?temp .
}
WHERE { 
    ?reading ex:room ?room ; 
             ex:temperature ?temp ;
             ex:timestamp ?time .
    FILTER (?temp > 115)
}"#;

    match process_rule_definition(extreme_rule, database) {
        Ok((_, inferred_facts)) => {
            if !inferred_facts.is_empty() {
                println!("DSTREAM rule detected {} extreme temperature conditions", inferred_facts.len());
                alert_count += inferred_facts.len() as u32;
                
                // Show what extreme conditions were found
                for (i, triple) in inferred_facts.iter().enumerate() {
                    let s = database.dictionary.decode(triple.subject).unwrap_or("unknown");
                    let p = database.dictionary.decode(triple.predicate).unwrap_or("unknown");
                    let o = database.dictionary.decode(triple.object).unwrap_or("unknown");
                    println!("    Extreme condition {}: {} -> {} -> {}", i + 1, s, p, o);
                }
            }
        }
        Err(e) => println!("DSTREAM rule processing failed: {}", e),
    }
    
    // Query the database to see cumulative results from all processed batches
    println!("Checking cumulative results in database...");
    
    // Count all high temperature alerts that have been generated
    let all_alerts_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?alert
WHERE { 
  ?room ex:hasAlert ?alert . 
}"#;
    
    let all_alerts = execute_query(all_alerts_query, database);
    if !all_alerts.is_empty() {
        println!("Total high temperature alerts in database: {}", all_alerts.len());
    }
    
    // Count all high temperature readings that have been flagged
    let all_readings_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { 
  ?room ex:newHighReading ?temp . 
}"#;
    
    let all_readings = execute_query(all_readings_query, database);
    if !all_readings.is_empty() {
        println!("Total flagged high readings in database: {}", all_readings.len());
    }
    
    // Count all extreme temperature conditions
    let extreme_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { 
  ?room ex:extremeLevel ?temp . 
}"#;
    
    let extreme_results = execute_query(extreme_query, database);
    if !extreme_results.is_empty() {
        println!("Total extreme temperature conditions in database: {}", extreme_results.len());
    }
    
    // Show total raw sensor data count for reference
    let batch_query = r#"PREFIX ex: <http://example.org#>
SELECT ?reading ?room ?temp
WHERE { 
  ?reading ex:room ?room .
  ?reading ex:temperature ?temp .
}"#;
    
    let batch_results = execute_query(batch_query, database);
    println!("Total raw sensor readings in database: {}", batch_results.len());
    
    alert_count
}

fn run_final_analysis(database: &mut SparqlDatabase) {
    println!("Performing comprehensive analysis of all windowing results...");
    
    // Run multiple queries to analyze different aspects of the collected data
    let queries = vec![
        ("High Temperature Alerts", r#"PREFIX ex: <http://example.org#>
SELECT ?room ?alert
WHERE { ?room ex:hasAlert ?alert . }"#),
        
        ("Flagged High Readings", r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { ?room ex:newHighReading ?temp . }"#),
        
        ("Extreme Temperature Conditions", r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { ?room ex:extremeLevel ?temp . }"#),
        
        ("All Raw Sensor Data", r#"PREFIX ex: <http://example.org#>
SELECT ?reading ?room ?temp
WHERE { 
  ?reading ex:room ?room .
  ?reading ex:temperature ?temp .
}"#),
    ];
    
    for (name, query) in queries {
        let results = execute_query(query, database);
        println!("{}: {} results found", name, results.len());
        
        // Show a few sample results to give insight into the data
        for (i, result) in results.iter().take(3).enumerate() {
            println!("   Sample {}. {:?}", i + 1, result);
        }
        if results.len() > 3 {
            println!("   ... and {} more results", results.len() - 3);
        }
        println!();
    }
    
    // Calculate and display final performance statistics
    let total_sensor_data = execute_query(r#"PREFIX ex: <http://example.org#>
SELECT ?reading WHERE { ?reading ex:room ?room . }"#, database).len();
    
    let total_alerts = execute_query(r#"PREFIX ex: <http://example.org#>
SELECT ?room WHERE { ?room ex:hasAlert ?alert . }"#, database).len();
    
    println!("Final Performance Summary:");
    println!("   - Total sensor readings processed: {}", total_sensor_data);
    println!("   - Total alerts generated across all rules: {}", total_alerts);
    println!("   - Overall alert detection rate: {:.1}%", if total_sensor_data > 0 { 
        (total_alerts as f64 / total_sensor_data as f64) * 100.0 
    } else { 0.0 });
}

fn get_current_timestamp() -> u64 {
    // Get the current Unix timestamp for labeling sensor readings
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
