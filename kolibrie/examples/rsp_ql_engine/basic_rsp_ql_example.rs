/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::rsp::{RSPBuilder, ResultConsumer, SimpleR2R};
use kolibrie::sparql_database::SparqlDatabase;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    println!("RSP-QL Engine Example");
    println!("=====================");

    // Create result container to collect outputs
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);

    // Create result consumer that prints and stores results
    let function = Box::new(move |r| {
        println!("📊 Query Result: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };

    // Create R2R operator with Volcano optimizer
    let r2r = Box::new(SimpleR2R::with_execution_mode(
        kolibrie::rsp::QueryExecutionMode::Volcano,
    ));

    // RSP-QL query with multiple windows
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://example.org/output> AS
        SELECT *
        FROM NAMED WINDOW :tempWindow ON <http://example.org/temperatureStream> [RANGE 10 STEP 5]
        FROM NAMED WINDOW :humidWindow ON <http://example.org/humidityStream> [RANGE 8 STEP 3]
        WHERE {
            WINDOW :tempWindow {
                ?sensor a <http://example.org/TemperatureSensor> .
                ?sensor <http://example.org/hasValue> ?tempValue .
            }
            WINDOW :humidWindow {
                ?sensor2 a <http://example.org/HumiditySensor> .
                ?sensor2 <http://example.org/hasValue> ?humidValue .
            }
        }
    "#;

    println!("\n🔍 Parsing RSP-QL Query:");
    println!("{}", rsp_ql_query.trim());

    // Build the RSP engine with Volcano optimizer
    let mut engine = match RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_query_execution_mode(kolibrie::rsp::QueryExecutionMode::Volcano)
        .build()
    {
        Ok(engine) => {
            println!("✅ RSP Engine with Volcano optimizer built successfully!");
            engine
        }
        Err(e) => {
            println!("❌ Failed to build RSP engine: {}", e);
            return;
        }
    };

    // Display window information
    println!("\n🪟 Configured Windows:");
    for (i, window_info) in engine.get_window_info().iter().enumerate() {
        println!(
            "  Window {}: {} (Stream: {})",
            i + 1,
            window_info.window_iri,
            window_info.stream_iri
        );
        println!(
            "    - Width: {}, Slide: {}",
            window_info.width, window_info.slide
        );
        println!("    - Query: {}", window_info.query.replace('\n', " "));
    }

    println!("\n🌊 Simulating data streams...");

    // Simulate temperature sensor data
    for i in 0..15 {
        let data = format!(
            "<http://example.org/temp_sensor{}> a <http://example.org/TemperatureSensor> .\n<http://example.org/temp_sensor{}> <http://example.org/hasValue> \"{}\"^^<http://www.w3.org/2001/XMLSchema#decimal> .",
            i, i, 20.0 + (i as f64) * 0.5
        );

        println!(
            "🌡️  Adding temperature data at time {}: sensor{} = {}°C",
            i,
            i,
            20.0 + (i as f64) * 0.5
        );

        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("<http://example.org/temperatureStream>", triple, i);
        }

        thread::sleep(Duration::from_millis(100));
    }

    // Simulate humidity sensor data
    for i in 0..12 {
        let data = format!(
            "<http://example.org/humid_sensor{}> a <http://example.org/HumiditySensor> .\n<http://example.org/humid_sensor{}> <http://example.org/hasValue> \"{}\"^^<http://www.w3.org/2001/XMLSchema#decimal> .",
            i, i, 60.0 + (i as f64) * 2.0
        );

        println!(
            "💧 Adding humidity data at time {}: sensor{} = {}%",
            i + 15,
            i,
            60.0 + (i as f64) * 2.0
        );

        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("<http://example.org/humidityStream>", triple, i + 15);
        }

        thread::sleep(Duration::from_millis(100));
    }

    println!("\n⏸️  Stopping the RSP engine...");
    engine.stop();

    // Wait for processing to complete
    thread::sleep(Duration::from_secs(3));

    // Display results summary
    let results = result_container.lock().unwrap();
    println!("\n📈 Processing Summary:");
    println!("  Total results received: {}", results.len());

    if results.is_empty() {
        println!("⚠️  No results were generated. This might be because:");
        println!("     - The SimpleR2R implementation has limited functionality");
        println!("     - Window conditions weren't met");
        println!("     - Data parsing issues");
    } else {
        println!("✅ RSP-QL query processing completed successfully!");
        println!("\n📋 Sample results:");
        for (i, result) in results.iter().take(5).enumerate() {
            println!("  Result {}: {:?}", i + 1, result);
        }
        if results.len() > 5 {
            println!("  ... and {} more results", results.len() - 5);
        }
    }

    println!("\n🚀 Benefits of Volcano Optimizer Integration:");
    println!("   📊 Cost-based query optimization for window queries");
    println!("   🔧 Efficient join ordering and operator selection");
    println!("   ⚡ Parallel execution with optimized query plans");
    println!("   📈 Better performance for complex window queries");

    println!("\n🎉 RSP-QL Engine with Volcano Optimizer Example completed!");
}
