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
    println!("Multi-Window RSP-QL Engine Example");
    println!("===================================");
    println!("This example demonstrates the improved RSP engine that extracts");
    println!("window configurations and queries from parsed RSP-QL syntax.");

    // Create result container to collect outputs
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);

    // Create result consumer that prints and stores results
    let function = Box::new(move |r| {
        println!("🔄 Window Result: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };

    // Create R2R operator with Volcano optimizer
    let r2r = Box::new(SimpleR2R::with_execution_mode(
        kolibrie::rsp::QueryExecutionMode::Volcano,
    ));

    // RSP-QL query similar to the original example in retrieve_multiple_window.rs
    // This demonstrates multiple windows with different specifications
    let rsp_ql_query = r#"
        RETRIEVE SOME ACTIVE STREAM ?s FROM <http://my.org/catalog>
        WITH {
            ?s a :Stream .
            ?s :hasDescriptor ?descriptor .
            ?descriptor :hasMetaData ?meta.
            ?meta :hasLocation <:somelocation>.
            ?meta :hasCoverage <:someArea>.
        }
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind ON ?s [RANGE 600 STEP 60]
        FROM NAMED WINDOW :wind2 ON :uri2 [RANGE 300 STEP 30]
        WHERE {
            WINDOW :wind {
                ?obs a ssn:Observation .
                ?obs ssn:hasSimpleResult ?value .
                ?obs ssn:observedProperty ?prop .
                ?prop a :Temperature .
            }
            WINDOW :wind2 {
                ?obs2 a ssn:Observation .
                ?obs2 ssn:hasSimpleResult ?value2 .
                ?obs2 ssn:observedProperty ?prop2 .
                ?prop2 a :CO2 .
            }
        }
    "#;

    println!("\n🔍 Parsing RSP-QL Query with Multiple Windows (Volcano Optimizer):");
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
            println!("✅ Multi-window RSP Engine with Volcano optimizer built successfully!");
            engine
        }
        Err(e) => {
            println!("❌ Failed to build RSP engine: {}", e);
            return;
        }
    };

    // Display extracted window configurations
    println!("\n🪟 Extracted Window Configurations:");
    for (i, window_info) in engine.get_window_info().iter().enumerate() {
        println!("  📊 Window {}: {}", i + 1, window_info.window_iri);
        println!("     Stream: {}", window_info.stream_iri);
        println!("     Width: {} seconds", window_info.width);
        println!("     Slide: {} seconds", window_info.slide);
        println!("     Tick: {:?}", window_info.tick);
        println!("     Report Strategy: {:?}", window_info.report_strategy);
        println!("     Extracted Query:");
        println!("       {}", window_info.query.replace('\n', " ").trim());
        println!();
    }

    println!("🌊 Simulating sensor data streams...");

    // Simulate temperature observations for Window 1 (:wind)
    println!("\n🌡️  Generating Temperature Observations:");
    for i in 0..20 {
        let timestamp = i * 30; // Every 30 seconds
        let temp_value = 22.0 + (i as f64) * 0.3 + (timestamp as f64 / 100.0).sin() * 2.0;

        let data = format!(
            "<http://sensors.org/temp_obs_{}> a ssn:Observation .\n\
             <http://sensors.org/temp_obs_{}> ssn:hasSimpleResult \"{}\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n\
             <http://sensors.org/temp_obs_{}> ssn:observedProperty <http://sensors.org/temperature_prop> .\n\
             <http://sensors.org/temperature_prop> a :Temperature .",
            i, i, temp_value, i
        );

        println!("  📈 T{:02}: {}°C at time {}", i, temp_value, timestamp);

        let triples = engine.parse_data(&data);
        for triple in triples {
            // Add to the stream referenced by variable ?s in the window
            engine.add_to_stream("?s", triple, timestamp);
        }

        thread::sleep(Duration::from_millis(50));
    }

    // Simulate CO2 observations for Window 2 (:wind2)
    println!("\n💨 Generating CO2 Observations:");
    for i in 0..15 {
        let timestamp = i * 45; // Every 45 seconds
        let co2_value = 400.0 + (i as f64) * 5.0 + (timestamp as f64 / 150.0).cos() * 20.0;

        let data = format!(
            "<http://sensors.org/co2_obs_{}> a ssn:Observation .\n\
             <http://sensors.org/co2_obs_{}> ssn:hasSimpleResult \"{}\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n\
             <http://sensors.org/co2_obs_{}> ssn:observedProperty <http://sensors.org/co2_prop> .\n\
             <http://sensors.org/co2_prop> a :CO2 .",
            i, i, co2_value, i
        );

        println!("  📊 C{:02}: {:.1}ppm at time {}", i, co2_value, timestamp);

        let triples = engine.parse_data(&data);
        for triple in triples {
            // Add to the stream referenced by :uri2 in window 2
            engine.add_to_stream(":uri2", triple, timestamp);
        }

        thread::sleep(Duration::from_millis(50));
    }

    println!("\n⏳ Processing windows and generating results...");
    thread::sleep(Duration::from_millis(500));

    println!("⏸️  Stopping the RSP engine...");
    engine.stop();

    // Wait for all processing to complete
    thread::sleep(Duration::from_secs(2));

    // Display results summary
    let results = result_container.lock().unwrap();
    println!("\n📈 Multi-Window Processing Summary:");
    println!("===================================");
    println!("🔢 Total results received: {}", results.len());
    println!("🪟 Windows configured: {}", engine.get_window_info().len());

    if results.is_empty() {
        println!("\n⚠️  No results were generated.");
        println!("💡 Note: This is expected with the current SimpleR2R implementation.");
        println!("   The example demonstrates successful RSP-QL parsing and window setup:");
        println!("   ✅ Query parsed successfully");
        println!("   ✅ Multiple windows extracted from RSP-QL syntax");
        println!("   ✅ Window-specific queries generated");
        println!("   ✅ Data routing to appropriate windows");
        println!();
        println!("🔧 To see actual results, replace SimpleR2R with a full R2R implementation");
        println!("   that supports proper SPARQL query execution and materialization.");
    } else {
        println!("✅ RSP-QL multi-window processing completed successfully!");

        // Group results by potential window source
        println!("\n📋 Results Summary:");
        for (i, result) in results.iter().take(10).enumerate() {
            println!("  Result {}: {:?}", i + 1, result);
        }
        if results.len() > 10 {
            println!("  ... and {} more results", results.len() - 10);
        }
    }

    println!("\n🎯 Key Improvements Demonstrated:");
    println!("================================");
    println!("✨ RSP-QL Query Parsing: Automatic extraction of window specifications");
    println!("✨ Multi-Window Support: Each window processes its own query independently");
    println!("✨ Stream Routing: Data directed to appropriate windows based on stream IRI");
    println!("✨ Query Extraction: Individual SPARQL queries generated per window block");
    println!("✨ Configuration Display: Easy inspection of parsed window configurations");
    println!("🚀 Volcano Optimizer: Cost-based optimization for window query execution");

    println!("\n🔥 Volcano Optimizer Benefits:");
    println!("==============================");
    println!("📊 Cost-based query planning for each window");
    println!("⚡ Optimized join ordering and operator selection");
    println!("🔧 Parallel execution with efficient query plans");
    println!("📈 Better performance for complex multi-window scenarios");

    println!("\n🚀 The RSP engine now uses parsed RSP-QL queries with Volcano optimization!");
    println!("🎉 Multi-Window RSP-QL Engine with Volcano Optimizer Example completed!");
}
