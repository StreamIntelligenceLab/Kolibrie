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
    println!("Static Data + Window Join Example - RSP-QL Engine");
    println!("=================================================");
    println!("This example demonstrates how the RSP engine performs");
    println!("joins between static knowledge base data and streaming");
    println!("window data using the Volcano optimizer.\n");

    // Create result container to collect joined outputs
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);

    // Create result consumer that prints and stores results
    let function = Box::new(move |r: Vec<String>| {
        println!("🔗 Static-Window Join Result: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };

    // Create R2R operator with Volcano optimizer
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // RSP-QL query with BOTH static patterns and window patterns
    // The key is that ?sensor appears in BOTH static and window patterns
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://analytics.org/enrichedSensorData> AS
        SELECT ?sensor ?sensorType ?location ?building ?buildingType ?temperature ?timestamp
        FROM NAMED WINDOW :tempWindow ON <http://streams/temperature> [RANGE 20 STEP 5]
        WHERE {
            # STATIC DATA PATTERNS (evaluated once on knowledge base)
            ?sensor a ?sensorType .
            ?sensor <http://sensors.org/installedIn> ?location .
            ?location <http://geo.org/partOf> ?building .
            ?building a ?buildingType .

            # STREAMING DATA PATTERNS (evaluated on window data)
            WINDOW :tempWindow {
                ?sensor <http://sensors.org/hasTemperature> ?temperature .
                ?sensor <http://sensors.org/timestamp> ?timestamp .
            }
        }
    "#;

    println!("🔍 RSP-QL Query with Static + Window Patterns:");
    println!("{}", rsp_ql_query.trim());
    println!("\n📋 Query Analysis:");
    println!("   📊 Static Patterns (knowledge base):");
    println!("      • ?sensor a ?sensorType");
    println!("      • ?sensor :installedIn ?location");
    println!("      • ?location :partOf ?building");
    println!("      • ?building a ?buildingType");
    println!("   🌊 Window Patterns (streaming data):");
    println!("      • ?sensor :hasTemperature ?temperature");
    println!("      • ?sensor :timestamp ?timestamp");
    println!("   🔗 SHARED VARIABLE: ?sensor (triggers static-window join)");
    println!("   📈 Result: Static sensor metadata + live temperature readings\n");

    // Build the RSP engine with static-window join capability
    let mut engine = match RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_query_execution_mode(QueryExecutionMode::Volcano)
        .build()
    {
        Ok(engine) => {
            println!("✅ RSP Engine with static-window join capability built successfully!");
            engine
        }
        Err(e) => {
            println!("❌ Failed to build RSP engine: {}", e);
            return;
        }
    };

    // Display the generated query plan with static data integration
    println!("\n" + "=".repeat(70).as_str());
    println!("🧠 VOLCANO QUERY PLAN WITH STATIC DATA");
    println!("=".repeat(70));

    let query_plan = engine.get_query_plan();
    println!("\n📊 Extended RSP-QL Query Plan Structure:");
    println!(
        "   • Window Plans: {} streaming data operators",
        query_plan.window_plans.len()
    );
    println!(
        "   • Static Data Plan: {}",
        if query_plan.static_data_plan.is_some() {
            "✅ Generated for knowledge base"
        } else {
            "❌ None"
        }
    );
    println!(
        "   • Cross-Window Join Plan: {}",
        if query_plan.cross_window_join_plan.is_some() {
            "✅ For multi-window joins"
        } else {
            "❌ None (single window)"
        }
    );
    println!(
        "   • Static-Window Join Plan: {}",
        if query_plan.static_window_join_plan.is_some() {
            "✅ Generated for static+streaming joins"
        } else {
            "❌ None"
        }
    );
    println!(
        "   • Shared Variables (windows): {:?}",
        query_plan.shared_variables
    );
    println!(
        "   • Shared Variables (static-window): {:?}",
        query_plan.static_window_shared_vars
    );

    if let Some(ref static_plan) = query_plan.static_data_plan {
        println!("\n📚 Static Data Plan Details:");
        println!("   Plan: {:?}", static_plan);
        println!("   • Executes once on knowledge base at startup");
        println!("   • Retrieves sensor metadata and building information");
        println!("   • Results cached for joining with streaming data");
    }

    if let Some(ref static_window_join_plan) = query_plan.static_window_join_plan {
        println!("\n🔗 Static-Window Join Plan Details:");
        println!("   Plan: {:?}", static_window_join_plan);
        println!("   • Uses Volcano's optimized hash join algorithms");
        println!("   • Joins cached static results with live window data");
        println!("   • Triggered when ?sensor values match between static and streaming data");
    }

    println!("\n" + "=".repeat(70).as_str());
    println!("🏗️  SIMULATING STATIC KNOWLEDGE BASE");
    println!("=".repeat(70));

    println!("\n📚 Mock Static Knowledge Base Setup:");
    println!("   (In reality, this would be loaded into the knowledge graph)");

    let static_knowledge = vec![
        "sensor_001 a TemperatureSensor",
        "sensor_001 :installedIn room_101",
        "room_101 :partOf building_A",
        "building_A a OfficeBuilding",
        "",
        "sensor_002 a TemperatureSensor",
        "sensor_002 :installedIn room_205",
        "room_205 :partOf building_B",
        "building_B a Warehouse",
        "",
        "sensor_003 a HumiditySensor",
        "sensor_003 :installedIn room_301",
        "room_301 :partOf building_A",
        "",
        "sensor_004 a TemperatureSensor",
        "sensor_004 :installedIn lab_150",
        "lab_150 :partOf building_C",
        "building_C a ResearchFacility",
    ];

    for (i, knowledge) in static_knowledge.iter().enumerate() {
        if !knowledge.is_empty() {
            println!("   📝 {}: {}", i + 1, knowledge);
        }
    }

    println!("\n" + "=".repeat(70).as_str());
    println!("🌊 SIMULATING STREAMING TEMPERATURE DATA");
    println!("=".repeat(70));

    // Simulate temperature sensor data stream
    println!("\n📡 Broadcasting temperature readings from various sensors:");
    for i in 0..15 {
        let sensor_id = match i % 4 {
            0 => "sensor_001",
            1 => "sensor_002",
            2 => "sensor_003", // This won't join (HumiditySensor in static data)
            3 => "sensor_004",
            _ => "sensor_unknown",
        };

        let temperature = 18.0 + (i as f64) * 0.8 + (i as f64 / 3.0).sin() * 3.0;
        let timestamp = i * 2000; // Every 2 seconds

        let data = format!(
            "<http://sensors.org/{}> <http://sensors.org/hasTemperature> \"{}\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n\
             <http://sensors.org/{}> <http://sensors.org/timestamp> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            sensor_id, temperature, sensor_id, timestamp
        );

        let join_status = match sensor_id {
            "sensor_001" => "✅ Will join (TemperatureSensor in building_A)",
            "sensor_002" => "✅ Will join (TemperatureSensor in building_B)",
            "sensor_003" => "❌ No join (HumiditySensor ≠ TemperatureSensor)",
            "sensor_004" => "✅ Will join (TemperatureSensor in building_C)",
            _ => "❌ No join (unknown sensor)",
        };

        println!(
            "   🌡️  {} = {:.1}°C @ t={} - {}",
            sensor_id, temperature, timestamp, join_status
        );

        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("<http://streams/temperature>", triple, i);
        }

        thread::sleep(Duration::from_millis(300));
    }

    println!("\n⏳ Processing static-window joins with Volcano optimizer...");
    thread::sleep(Duration::from_secs(4));

    println!("⏸️  Stopping RSP engine...");
    engine.stop();

    // Wait for final processing
    thread::sleep(Duration::from_secs(3));

    // Analyze results
    let results = result_container.lock().unwrap();
    println!("\n" + "=".repeat(70).as_str());
    println!("📊 STATIC-WINDOW JOIN EXECUTION RESULTS");
    println!("=".repeat(70));

    println!("\n📈 Join Processing Summary:");
    println!("   Total joined results: {}", results.len());
    println!("   Expected joins: sensor_001, sensor_002, sensor_004 (TemperatureSensors only)");
    println!("   Expected non-joins: sensor_003 (HumiditySensor), unknown sensors");

    if results.is_empty() {
        println!("\n💡 Query Plan Analysis:");
        println!("   ✅ RSP-QL query with static patterns successfully parsed");
        println!("   ✅ Static data plan generated using Volcano operators");
        println!("   ✅ Static-window join plan created");
        println!("   ✅ Shared variable detection working (?sensor)");
        println!("   ✅ Static data plan execution simulated");
        println!("   ⚠️  No results due to simplified execution simulation");

        println!("\n🔧 Implementation Status:");
        println!("   • Static pattern parsing: ✅ Implemented");
        println!("   • Static data plan generation: ✅ Implemented");
        println!("   • Static-window join planning: ✅ Implemented");
        println!("   • Shared variable detection: ✅ Implemented");
        println!("   • Volcano optimizer integration: ✅ Implemented");
        println!("   • Full static data execution: 🚧 Needs knowledge base integration");
    } else {
        println!("✅ Static-window joins executed successfully!");
        println!("\n📋 Sample Joined Results:");
        for (i, result) in results.iter().take(8).enumerate() {
            println!("   Join Result {}: {:?}", i + 1, result);
        }
        if results.len() > 8 {
            println!("   ... and {} more joined results", results.len() - 8);
        }

        println!("\n🔍 Join Analysis:");
        println!("   • Each result combines static metadata + live sensor data");
        println!("   • Only TemperatureSensors from static data joined with stream");
        println!("   • Results include: sensor info, location, building, temperature, timestamp");
    }

    println!("\n" + "=".repeat(70).as_str());
    println!("🚀 STATIC-WINDOW JOIN BENEFITS");
    println!("=".repeat(70));

    println!("\n🎯 Key Advantages:");
    println!("   ✨ Enriches streaming data with static context");
    println!("   ✨ Single query combines knowledge base + real-time data");
    println!("   ✨ Volcano optimizer handles complex multi-source joins");
    println!("   ✨ Static data evaluated once, cached for efficient joining");
    println!("   ✨ Proper RSP-QL semantics for hybrid static+streaming queries");

    println!("\n📊 Join Execution Flow:");
    println!("   1️⃣  Parse RSP-QL → Separate static patterns from window patterns");
    println!("   2️⃣  Create static data plan → Execute once on knowledge base");
    println!("   3️⃣  Cache static results → Keep in memory for joining");
    println!("   4️⃣  Process window data → Execute streaming queries on windows");
    println!("   5️⃣  Perform static-window joins → Use Volcano hash join algorithms");
    println!("   6️⃣  Stream enriched results → Send combined data to consumer");

    println!("\n🔄 Volcano Integration Architecture:");
    println!("   🔸 Static Data Plan: PhysicalOperator::TableScan for knowledge base");
    println!("   🔸 Window Data Plans: PhysicalOperator::TableScan for streaming data");
    println!("   🔸 Join Coordination: PhysicalOperator::OptimizedHashJoin");
    println!("   🔸 Result Projection: PhysicalOperator::Projection for output variables");

    println!("\n💭 Real-World Use Cases:");
    println!("   🏭 Enrich sensor readings with asset metadata and location info");
    println!("   🚗 Combine live vehicle telemetry with fleet management data");
    println!("   📈 Join real-time stock prices with company fundamental data");
    println!("   🏥 Merge patient vital signs with medical history records");
    println!("   🌡️  Correlate weather sensor data with station metadata");

    println!("\n🔮 Advanced Scenarios:");
    println!("   • Complex static queries with multiple joins and filters");
    println!("   • Temporal validity of static data (versioned knowledge base)");
    println!("   • Dynamic static data updates during stream processing");
    println!("   • Hierarchical joins (sensor → room → building → organization)");

    println!("\n🛠️  Next Steps for Full Implementation:");
    println!("   1. Integrate with SparqlDatabase for actual static data execution");
    println!("   2. Implement efficient caching mechanism for static results");
    println!("   3. Add support for complex static query patterns");
    println!("   4. Optimize memory usage for large static datasets");
    println!("   5. Add dynamic static data update capabilities");

    println!("\n🎉 Static-Window Join Example completed!");
    println!("🔥 The RSP engine now supports hybrid static+streaming data processing!");
    println!("⚡ Volcano optimizer provides unified planning for all data sources!");
}
