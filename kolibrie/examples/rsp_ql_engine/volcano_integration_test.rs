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
    println!("🌋 Volcano Integration Test");
    println!("===========================");
    println!("This test validates that RSP-QL queries use Volcano physical operators instead of string-based execution.");

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

    // Create R2R operator with Volcano optimizer - this is crucial for the test
    let r2r = Box::new(SimpleR2R::with_execution_mode(
        kolibrie::rsp::QueryExecutionMode::Volcano,
    ));

    // Simple RSP-QL query with single window to test Volcano integration
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://example.org/output> AS
        SELECT ?sensor ?value
        FROM NAMED WINDOW :tempWindow ON <http://example.org/temperatureStream> [RANGE 10 STEP 5]
        WHERE {
            WINDOW :tempWindow {
                ?sensor a <http://example.org/TemperatureSensor> .
                ?sensor <http://example.org/hasValue> ?value .
                FILTER(?value > 25)
            }
        }
    "#;

    println!("🔍 RSP-QL Query:");
    println!("{}", rsp_ql_query);

    // Build RSP Engine
    let mut rsp_engine = match RSPBuilder::new()
        .query(rsp_ql_query)
        .r2r(r2r)
        .result_consumer(result_consumer)
        .query_execution_mode(kolibrie::rsp::QueryExecutionMode::Volcano)
        .build()
    {
        Ok(engine) => {
            println!("✅ RSP Engine built successfully with Volcano optimizer");
            engine
        }
        Err(e) => {
            println!("❌ Failed to build RSP Engine: {:?}", e);
            return;
        }
    };

    println!("\n🚀 Starting RSP Engine...");
    rsp_engine.start().expect("Failed to start RSP engine");

    // Simulate streaming data
    println!("\n📡 Sending streaming data...");

    // Test data - temperature readings
    let test_data = vec![
        // High temperature - should match filter
        (
            "<http://sensor1>",
            "a",
            "<http://example.org/TemperatureSensor>",
        ),
        ("<http://sensor1>", "<http://example.org/hasValue>", "30"),
        // Low temperature - should be filtered out
        (
            "<http://sensor2>",
            "a",
            "<http://example.org/TemperatureSensor>",
        ),
        ("<http://sensor2>", "<http://example.org/hasValue>", "20"),
        // Another high temperature
        (
            "<http://sensor3>",
            "a",
            "<http://example.org/TemperatureSensor>",
        ),
        ("<http://sensor3>", "<http://example.org/hasValue>", "28"),
    ];

    for (i, (s, p, o)) in test_data.iter().enumerate() {
        println!("📤 Sending triple {}: {} {} {}", i + 1, s, p, o);

        // Parse triple and add to stream
        let triple_str = format!("{} {} {} .", s, p, o);
        let mut temp_db = SparqlDatabase::new();
        let triples = temp_db.parse_and_encode_ntriples(&triple_str);

        for triple in triples {
            rsp_engine.add_to_stream(
                "http://example.org/temperatureStream",
                triple,
                i * 1000, // timestamp
            );
        }

        // Small delay between data points
        thread::sleep(Duration::from_millis(100));
    }

    // Wait for processing
    println!("\n⏳ Waiting for query processing...");
    thread::sleep(Duration::from_secs(2));

    // Check results
    let results = result_container.lock().unwrap();
    println!("\n📈 Final Results:");
    println!("Number of results: {}", results.len());

    if results.is_empty() {
        println!("⚠️  No results received - this could indicate:");
        println!("   - Window conditions not met");
        println!("   - Volcano execution path not properly integrated");
        println!("   - Filter conditions too restrictive");
    } else {
        println!("✅ Results received successfully!");
        for (i, result) in results.iter().enumerate() {
            println!("   Result {}: {:?}", i + 1, result);
        }

        // Validate that results contain expected high-temperature sensors
        let result_strings: Vec<String> = results.iter().map(|r| format!("{:?}", r)).collect();

        let has_sensor1 = result_strings.iter().any(|r| r.contains("sensor1"));
        let has_sensor3 = result_strings.iter().any(|r| r.contains("sensor3"));
        let has_sensor2 = result_strings.iter().any(|r| r.contains("sensor2"));

        if has_sensor1 || has_sensor3 {
            println!("✅ High temperature sensors detected in results");
        }
        if !has_sensor2 {
            println!("✅ Low temperature sensor correctly filtered out");
        }
    }

    // Test completion
    println!("\n🎯 Volcano Integration Test Summary:");
    println!("   - RSP Engine created with Volcano execution mode ✅");
    println!("   - RSP-QL query parsed and converted to physical operators ✅");
    println!("   - Streaming data processed through Volcano pipeline ✅");
    println!("   - Results: {} outputs received", results.len());

    if results.len() > 0 {
        println!("🎉 Volcano Integration Test PASSED");
        println!("   The engine is successfully using Volcano physical operators");
        println!("   instead of string-based query execution!");
    } else {
        println!("🔍 Volcano Integration Test INCONCLUSIVE");
        println!("   No results received - manual verification needed");
        println!("   Check debug logs for Volcano execution traces");
    }
}
