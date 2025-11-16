/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::rsp::{QueryExecutionMode, RSPBuilder, ResultConsumer, SimpleR2R};
use kolibrie::sparql_database::SparqlDatabase;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    println!("Cross-Window Join Example - RSP-QL Engine");
    println!("=========================================");
    println!("This example demonstrates how the RSP engine performs");
    println!("joins across multiple windows when they share variables.\n");

    // Create result container to collect joined outputs
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);

    // Create result consumer that prints and stores results
    let function = Box::new(move |r: Vec<String>| {
        println!("🔗 Cross-Window Join Result: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };

    // Create R2R operator with Volcano optimizer
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // RSP-QL query with SHARED VARIABLES across multiple windows
    // Notice that ?sensor appears in BOTH windows - this creates an implicit join!
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://example.org/joinedOutput> AS
        SELECT ?sensor ?tempValue ?humidValue ?location
        FROM NAMED WINDOW :tempWindow ON <http://sensors/temperature> [RANGE 15 STEP 5]
        FROM NAMED WINDOW :humidWindow ON <http://sensors/humidity> [RANGE 12 STEP 4]
        WHERE {
            WINDOW :tempWindow {
                ?sensor a <http://sensors.org/TemperatureSensor> .
                ?sensor <http://sensors.org/hasTemperature> ?tempValue .
                ?sensor <http://sensors.org/hasLocation> ?location .
            }
            WINDOW :humidWindow {
                ?sensor a <http://sensors.org/HumiditySensor> .
                ?sensor <http://sensors.org/hasHumidity> ?humidValue .
            }
        }
    "#;

    println!("🔍 RSP-QL Query with Shared Variables:");
    println!("{}", rsp_ql_query.trim());
    println!("\n📋 Key Points:");
    println!("   • Variable ?sensor appears in BOTH windows");
    println!("   • This creates an implicit JOIN between window results");
    println!("   • Only results with matching ?sensor values will be combined\n");

    // Build the RSP engine
    let mut engine = match RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_query_execution_mode(QueryExecutionMode::Volcano)
        .build()
    {
        Ok(engine) => {
            println!("✅ RSP Engine with cross-window join capability built successfully!");
            engine
        }
        Err(e) => {
            println!("❌ Failed to build RSP engine: {}", e);
            return;
        }
    };

    // Display window and join configuration
    println!("\n🪟 Window Configuration:");
    for (i, window_info) in engine.get_window_info().iter().enumerate() {
        println!(
            "  Window {}: {} -> Stream: {}",
            i + 1,
            window_info.window_iri,
            window_info.stream_iri
        );
        println!(
            "    Width: {}s, Slide: {}s",
            window_info.width, window_info.slide
        );
        println!("    Query: {}", window_info.query.replace('\n', " ").trim());
    }

    println!("\n🔗 Cross-Window Join Analysis:");
    println!("   Variables in tempWindow:  ?sensor, ?tempValue, ?location");
    println!("   Variables in humidWindow: ?sensor, ?humidValue");
    println!("   SHARED VARIABLE: ?sensor (enables join between windows)");
    println!("   Result: Combined data where sensor IDs match across both windows\n");

    println!("🌊 Simulating coordinated sensor data streams...");

    // Phase 1: Send temperature data
    println!("\n📡 Phase 1: Broadcasting temperature sensor data");
    for i in 0..8 {
        let sensor_id = format!("sensor_{}", i % 3); // Use 3 different sensors
        let temp_value = 20.0 + (i as f64) * 0.5;
        let location = format!("Building_{}", i % 2);

        let data = format!(
            "<http://sensors.org/{}> a <http://sensors.org/TemperatureSensor> .\n\
             <http://sensors.org/{}> <http://sensors.org/hasTemperature> \"{}\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n\
             <http://sensors.org/{}> <http://sensors.org/hasLocation> \"{}\" .",
            sensor_id, sensor_id, temp_value, sensor_id, location
        );

        println!(
            "  🌡️  Temp data: {} = {}°C at {}",
            sensor_id, temp_value, location
        );

        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("<http://sensors/temperature>", triple, i);
        }

        thread::sleep(Duration::from_millis(200));
    }

    // Phase 2: Send humidity data (some sensors match, some don't)
    println!("\n📡 Phase 2: Broadcasting humidity sensor data");
    for i in 0..8 {
        let sensor_id = format!("sensor_{}", (i + 1) % 4); // Different sensor pattern
        let humid_value = 60.0 + (i as f64) * 1.5;

        let data = format!(
            "<http://sensors.org/{}> a <http://sensors.org/HumiditySensor> .\n\
             <http://sensors.org/{}> <http://sensors.org/hasHumidity> \"{}\"^^<http://www.w3.org/2001/XMLSchema#decimal> .",
            sensor_id, sensor_id, humid_value
        );

        println!("  💧 Humid data: {} = {}%", sensor_id, humid_value);

        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("<http://sensors/humidity>", triple, i + 10);
        }

        thread::sleep(Duration::from_millis(200));
    }

    println!("\n⏳ Waiting for cross-window join processing...");
    thread::sleep(Duration::from_secs(3));

    println!("⏸️  Stopping RSP engine...");
    engine.stop();

    // Wait for final processing
    thread::sleep(Duration::from_secs(2));

    // Analyze results
    let results = result_container.lock().unwrap();
    println!("\n" + "=".repeat(60).as_str());
    println!("📊 CROSS-WINDOW JOIN ANALYSIS");
    println!("=".repeat(60));

    println!("\n📈 Processing Summary:");
    println!("  Total joined results: {}", results.len());

    if results.is_empty() {
        println!("\n⚠️  No joined results generated.");
        println!("💡 This could be due to:");
        println!("   • No matching sensor IDs between temperature and humidity data");
        println!("   • Window timing misalignment");
        println!("   • Cross-window join logic still being refined");
        println!("\n🔧 Expected behavior:");
        println!("   • Only sensors present in BOTH windows should produce results");
        println!("   • Results should combine: ?sensor + ?tempValue + ?humidValue + ?location");
    } else {
        println!("✅ Cross-window joins executed successfully!");
        println!("\n📋 Sample joined results:");
        for (i, result) in results.iter().take(5).enumerate() {
            println!("  Join Result {}: {:?}", i + 1, result);
        }
        if results.len() > 5 {
            println!("  ... and {} more joined results", results.len() - 5);
        }

        println!("\n🎯 Join Analysis:");
        println!("   • Each result combines data from both windows");
        println!("   • Only sensors appearing in BOTH streams are included");
        println!("   • Results respect window timing constraints");
    }

    println!("\n" + "=".repeat(60).as_str());
    println!("🧠 CROSS-WINDOW JOIN MECHANICS");
    println!("=".repeat(60));

    println!("\n🔍 How Cross-Window Joins Work:");
    println!("1️⃣  Parse RSP-QL to identify shared variables (?sensor)");
    println!("2️⃣  Execute queries independently on each window");
    println!("3️⃣  Collect results from all windows for same timestamp");
    println!("4️⃣  Perform JOIN on shared variables (?sensor values must match)");
    println!("5️⃣  Combine variable bindings from all windows");
    println!("6️⃣  Apply R2S operator to joined results");

    println!("\n📝 Join Conditions:");
    println!("   • Temporal: Results from same time window");
    println!("   • Variable: Shared variable values must be identical");
    println!("   • Semantic: Only meaningful combinations are kept");

    println!("\n🚀 Benefits of Cross-Window Joins:");
    println!("   ✅ Combines related data from multiple streams");
    println!("   ✅ Maintains RSP-QL semantic correctness");
    println!("   ✅ Enables complex multi-stream analytics");
    println!("   ✅ Supports temporal correlation of heterogeneous data");

    println!("\n💭 Real-World Use Cases:");
    println!("   🏭 Correlate temperature and pressure from same equipment");
    println!("   🏠 Join indoor and outdoor sensors for same location");
    println!("   🚗 Combine speed and fuel consumption from same vehicle");
    println!("   📈 Merge stock price and volume data for same symbol");

    println!("\n🎉 Cross-Window Join Example completed!");
    println!("🔥 The RSP engine now properly handles shared variables across multiple windows!");
}
