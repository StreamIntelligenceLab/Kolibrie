use kolibrie::database::KolibrieDB;
use kolibrie::prelude::*;
use kolibrie::rule_execution::process_rule_definition;
use rand::Rng;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    println!("=== Continuous Real-Time Stream Testing with Windowing ===\n");

    let mut database = KolibrieDB::new();
    let mut stream_counter = 0;
    let mut total_alerts = 0;

    let stream_duration = Duration::from_secs(4);
    let batch_interval = Duration::from_millis(500);
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

        println!(
            "=== PROCESSING BATCH {} === [{}s elapsed]",
            stream_counter,
            elapsed.as_secs()
        );

        let sensor_data = generate_dynamic_sensor_batch(stream_counter, current_timestamp);

        let rdf_xml = create_sensor_rdf_batch(sensor_data.clone(), stream_counter);
        database.parse_rdf(&rdf_xml);

        println!(
            "Successfully loaded batch {} - Database now contains {} triples",
            stream_counter,
            database.default_triple_count()
        );

        for (room, temp, timestamp) in &sensor_data {
            let status = if *temp > 100 {
                "CRITICAL"
            } else if *temp > 85 {
                "HIGH"
            } else if *temp < 30 {
                "LOW"
            } else {
                "NORMAL"
            };
            println!(
                "  {} -> {}°C at {} - Status: {}",
                room, temp, timestamp, status
            );
        }

        let batch_alerts = apply_simplified_windowing_rules(&mut database, stream_counter);
        total_alerts += batch_alerts;

        println!("Current Stream Statistics:");
        println!("   - Batches processed so far: {}", stream_counter);
        println!(
            "   - Total data points in database: {}",
            database.default_triple_count()
        );
        println!("   - Total alerts generated: {}", total_alerts);
        println!(
            "   - Time remaining in simulation: {}s\n",
            (stream_duration.as_secs() as i64 - elapsed.as_secs() as i64).max(0)
        );

        thread::sleep(batch_interval);
    }

    println!("=== STREAMING SIMULATION COMPLETE ===");
    println!("Final Statistics Summary:");
    println!("   - Total batches processed: {}", stream_counter);
    println!(
        "   - Total data points stored: {}",
        database.default_triple_count()
    );
    println!("   - Total alerts generated: {}", total_alerts);
    if stream_counter > 0 {
        println!(
            "   - Average alerts per batch: {:.2}",
            total_alerts as f64 / stream_counter as f64
        );
    }

    println!("\nRunning Final Data Analysis:");
    run_final_analysis(&mut database);
}

fn generate_dynamic_sensor_batch(batch_num: u32, base_timestamp: u64) -> Vec<(String, i32, u64)> {
    let mut rng = rand::rng();
    let rooms = vec!["Room101", "Room102", "Room103", "Room104", "Room105"];
    let mut batch_data = Vec::new();

    let reading_count = rng.random_range(2..=3);

    for i in 0..reading_count {
        let room = rooms[rng.random_range(0..rooms.len())].to_string();

        let base_temp = match room.as_str() {
            "Room101" => 85,
            "Room102" => 72,
            "Room103" => 95,
            "Room104" => 68,
            "Room105" => 78,
            _ => 75,
        };

        let variation = rng.random_range(-10..=25);
        let heat_boost = if batch_num > 5 {
            rng.random_range(0..=15)
        } else {
            0
        };
        let temperature = (base_temp + variation + heat_boost).max(10).min(140);

        let timestamp = base_timestamp + (i as u64 * 5);

        batch_data.push((room, temperature, timestamp));
    }

    batch_data
}

fn create_sensor_rdf_batch(sensor_data: Vec<(String, i32, u64)>, batch_num: u32) -> String {
    let mut rdf = String::from(
        r#"<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org#"
         xmlns:stream="http://example.org/stream#">"#,
    );

    for (i, (room, temp, timestamp)) in sensor_data.iter().enumerate() {
        let reading_id = format!("http://example.org/stream#reading{}_{}", batch_num, i + 1);
        rdf.push_str(&format!(
            r#"
  <rdf:Description rdf:about="{}">
    <ex:room>{}</ex:room>
    <ex:temperature>{}</ex:temperature>
    <ex:timestamp>{}</ex:timestamp>
    <ex:batchNumber>{}</ex:batchNumber>
  </rdf:Description>"#,
            reading_id, room, temp, timestamp, batch_num
        ));
    }

    rdf.push_str("\n</rdf:RDF>");
    rdf
}

fn apply_simplified_windowing_rules(database: &mut KolibrieDB, _batch_num: u32) -> u32 {
    let mut alert_count = 0;

    println!("Applying RSTREAM rule for high temperature detection...");
    let high_temp_rule = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :TemperatureAlert :-
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
                println!(
                    "RSTREAM rule detected {} high temperature alerts",
                    inferred_facts.len()
                );
                alert_count += inferred_facts.len() as u32;

                for (i, triple) in inferred_facts.iter().enumerate() {
                    let dict = database.dictionary();
                    let s = dict.decode(triple.subject).unwrap_or("unknown");
                    let p = dict.decode(triple.predicate).unwrap_or("unknown");
                    let o = dict.decode(triple.object).unwrap_or("unknown");
                    println!("    Alert {}: {} -> {} -> {}", i + 1, s, p, o);
                }
            }
        }
        Err(e) => println!("RSTREAM rule processing failed: {}", e),
    }

    println!("Applying ISTREAM rule for moderate temperature detection...");
    let moderate_rule = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :NewHighTemp :-
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
                println!(
                    "ISTREAM rule found {} new high temperature readings",
                    inferred_facts.len()
                );
                alert_count += inferred_facts.len() as u32;

                for (i, triple) in inferred_facts.iter().enumerate() {
                    let dict = database.dictionary();
                    let s = dict.decode(triple.subject).unwrap_or("unknown");
                    let p = dict.decode(triple.predicate).unwrap_or("unknown");
                    let o = dict.decode(triple.object).unwrap_or("unknown");
                    println!("    New reading {}: {} -> {} -> {}", i + 1, s, p, o);
                }
            }
        }
        Err(e) => println!("ISTREAM rule processing failed: {}", e),
    }

    println!("Applying DSTREAM rule for extreme temperature detection...");
    let extreme_rule = r#"PREFIX ex: <http://example.org#>
PREFIX stream: <http://example.org/stream#>

RULE :ExtremeAlert :-
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
                println!(
                    "DSTREAM rule detected {} extreme temperature conditions",
                    inferred_facts.len()
                );
                alert_count += inferred_facts.len() as u32;

                for (i, triple) in inferred_facts.iter().enumerate() {
                    let dict = database.dictionary();
                    let s = dict.decode(triple.subject).unwrap_or("unknown");
                    let p = dict.decode(triple.predicate).unwrap_or("unknown");
                    let o = dict.decode(triple.object).unwrap_or("unknown");
                    println!("    Extreme condition {}: {} -> {} -> {}", i + 1, s, p, o);
                }
            }
        }
        Err(e) => println!("DSTREAM rule processing failed: {}", e),
    }

    println!("Checking cumulative results in database...");

    let all_alerts_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?alert
WHERE { 
  ?room ex:hasAlert ?alert . 
}"#;

    let all_alerts = execute(all_alerts_query, database)
        .expect("query should run")
        .into_rows();
    if !all_alerts.is_empty() {
        println!(
            "Total high temperature alerts in database: {}",
            all_alerts.len()
        );
    }

    let all_readings_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { 
  ?room ex:newHighReading ?temp . 
}"#;

    let all_readings = execute(all_readings_query, database)
        .expect("query should run")
        .into_rows();
    if !all_readings.is_empty() {
        println!(
            "Total flagged high readings in database: {}",
            all_readings.len()
        );
    }

    let extreme_query = r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { 
  ?room ex:extremeLevel ?temp . 
}"#;

    let extreme_results = execute(extreme_query, database)
        .expect("query should run")
        .into_rows();
    if !extreme_results.is_empty() {
        println!(
            "Total extreme temperature conditions in database: {}",
            extreme_results.len()
        );
    }

    let batch_query = r#"PREFIX ex: <http://example.org#>
SELECT ?reading ?room ?temp
WHERE { 
  ?reading ex:room ?room .
  ?reading ex:temperature ?temp .
}"#;

    let batch_results = execute(batch_query, database)
        .expect("query should run")
        .into_rows();
    println!(
        "Total raw sensor readings in database: {}",
        batch_results.len()
    );

    alert_count
}

fn run_final_analysis(database: &mut KolibrieDB) {
    println!("Performing comprehensive analysis of all windowing results...");

    let queries = vec![
        (
            "High Temperature Alerts",
            r#"PREFIX ex: <http://example.org#>
SELECT ?room ?alert
WHERE { ?room ex:hasAlert ?alert . }"#,
        ),
        (
            "Flagged High Readings",
            r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { ?room ex:newHighReading ?temp . }"#,
        ),
        (
            "Extreme Temperature Conditions",
            r#"PREFIX ex: <http://example.org#>
SELECT ?room ?temp
WHERE { ?room ex:extremeLevel ?temp . }"#,
        ),
        (
            "All Raw Sensor Data",
            r#"PREFIX ex: <http://example.org#>
SELECT ?reading ?room ?temp
WHERE { 
  ?reading ex:room ?room .
  ?reading ex:temperature ?temp .
}"#,
        ),
    ];

    for (name, query) in queries {
        let results = execute(query, database)
            .expect("query should run")
            .into_rows();
        println!("{}: {} results found", name, results.len());

        for (i, result) in results.iter().take(3).enumerate() {
            println!("   Sample {}. {:?}", i + 1, result);
        }
        if results.len() > 3 {
            println!("   ... and {} more results", results.len() - 3);
        }
        println!();
    }

    let total_sensor_data = execute(
        r#"PREFIX ex: <http://example.org#>
SELECT ?reading WHERE { ?reading ex:room ?room . }"#,
        database,
    )
    .expect("query should run")
    .into_rows()
    .len();

    let total_alerts = execute(
        r#"PREFIX ex: <http://example.org#>
SELECT ?room WHERE { ?room ex:hasAlert ?alert . }"#,
        database,
    )
    .expect("query should run")
    .into_rows()
    .len();

    println!("Final Performance Summary:");
    println!(
        "   - Total sensor readings processed: {}",
        total_sensor_data
    );
    println!(
        "   - Total alerts generated across all rules: {}",
        total_alerts
    );
    println!(
        "   - Overall alert detection rate: {:.1}%",
        if total_sensor_data > 0 {
            (total_alerts as f64 / total_sensor_data as f64) * 100.0
        } else {
            0.0
        }
    );
}

fn get_current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
