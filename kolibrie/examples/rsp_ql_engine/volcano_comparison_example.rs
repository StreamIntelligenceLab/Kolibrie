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
use std::time::{Duration, Instant};

fn main() {
    println!("RSP Engine: Volcano vs Standard Execution Comparison");
    println!("===================================================");
    println!("This example compares the performance and behavior of");
    println!("standard query execution vs Volcano optimizer execution");
    println!("for RSP-QL window queries.\n");

    // Common RSP-QL query for testing both modes
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://example.org/output> AS
        SELECT ?sensor ?temp ?location
        FROM NAMED WINDOW :tempWindow ON <http://sensors/temperature> [RANGE 20 STEP 5]
        WHERE {
            WINDOW :tempWindow {
                ?sensor a <http://sensors.org/TemperatureSensor> .
                ?sensor <http://sensors.org/hasTemperature> ?temp .
                ?sensor <http://sensors.org/hasLocation> ?location .
                ?sensor <http://sensors.org/isActive> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            }
        }
    "#;

    println!("🔍 Test Query:");
    println!("{}", rsp_ql_query.trim());

    // Test data generation
    let test_data = generate_test_data(100);
    println!("\n📊 Generated {} test triples", test_data.len());

    println!("🚀 PERFORMANCE COMPARISON");

    // Test 1: Standard Execution Mode
    println!("\n1️⃣  Testing Standard Execution Mode");
    println!("   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let (standard_results, standard_duration) =
        run_rsp_test(rsp_ql_query, &test_data, QueryExecutionMode::Standard);

    // Test 2: Volcano Execution Mode
    println!("\n2️⃣  Testing Volcano Optimizer Mode");
    println!("   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let (volcano_results, volcano_duration) =
        run_rsp_test(rsp_ql_query, &test_data, QueryExecutionMode::Volcano);

    // Performance Analysis
    println!("📈 PERFORMANCE ANALYSIS");

    println!("\n⏱️  Execution Time Comparison:");
    println!("   Standard Mode:  {:?}", standard_duration);
    println!("   Volcano Mode:   {:?}", volcano_duration);

    let speedup = if volcano_duration.as_millis() > 0 {
        standard_duration.as_millis() as f64 / volcano_duration.as_millis() as f64
    } else {
        1.0
    };

    if speedup > 1.0 {
        println!("   🚀 Volcano is {:.2}x faster!", speedup);
    } else if speedup < 1.0 {
        println!("   ⚡ Standard is {:.2}x faster", 1.0 / speedup);
    } else {
        println!("   ⚖️  Both modes performed similarly");
    }

    println!("\n📊 Result Comparison:");
    println!("   Standard Results: {} result sets", standard_results);
    println!("   Volcano Results:  {} result sets", volcano_results);

    if standard_results == volcano_results {
        println!("   ✅ Both modes produced the same number of results");
    } else {
        println!("   ⚠️  Different result counts - may indicate optimization differences");
    }

    // Analysis and recommendations
    println!("🎯 ANALYSIS & RECOMMENDATIONS");

    analyze_results(
        standard_duration,
        volcano_duration,
        standard_results,
        volcano_results,
    );

    println!("\n🔧 Technical Details:");
    println!("━━━━━━━━━━━━━━━━━━━━━");
    println!("Standard Mode:");
    println!("  • Uses direct query execution without optimization");
    println!("  • Simple join algorithms and fixed execution order");
    println!("  • Lower overhead for simple queries");

    println!("\nVolcano Mode:");
    println!("  • Cost-based query optimization");
    println!("  • Dynamic join reordering based on statistics");
    println!("  • Parallel execution capabilities");
    println!("  • Better for complex queries with multiple joins");

    println!("\n💡 When to use each mode:");
    println!("Standard Mode: Simple queries, low latency requirements, small datasets");
    println!("Volcano Mode:  Complex queries, large datasets, batch processing scenarios");

    println!("\n🎉 Volcano vs Standard Comparison completed!");
}

fn run_rsp_test(
    rsp_ql_query: &str,
    test_data: &[String],
    execution_mode: QueryExecutionMode,
) -> (usize, Duration) {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);

    let function = Box::new(move |r| {
        result_container_clone.lock().unwrap().push(r);
    });

    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };

    // Create R2R operator with specified execution mode
    let r2r = Box::new(SimpleR2R::with_execution_mode(execution_mode));

    println!(
        "   🏗️  Building RSP engine with {:?} execution mode...",
        execution_mode
    );

    let mut engine = match RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_query_execution_mode(execution_mode)
        .build()
    {
        Ok(engine) => {
            println!("   ✅ Engine built successfully");
            engine
        }
        Err(e) => {
            println!("   ❌ Failed to build engine: {}", e);
            return (0, Duration::from_secs(0));
        }
    };

    println!("   🌊 Processing {} data items...", test_data.len());
    let start_time = Instant::now();

    // Process test data
    for (i, data) in test_data.iter().enumerate() {
        let triples = engine.parse_data(data);
        for triple in triples {
            engine.add_to_stream("<http://sensors/temperature>", triple, i);
        }

        // Small delay to simulate realistic streaming
        thread::sleep(Duration::from_millis(1));
    }

    // Allow processing time
    thread::sleep(Duration::from_millis(100));

    let processing_duration = start_time.elapsed();

    println!("   ⏹️  Stopping engine...");
    engine.stop();

    // Wait for cleanup
    thread::sleep(Duration::from_millis(500));

    let total_duration = start_time.elapsed();
    let result_count = result_container.lock().unwrap().len();

    println!("   📊 Processing completed:");
    println!("      - Processing time: {:?}", processing_duration);
    println!("      - Total time:      {:?}", total_duration);
    println!("      - Results:         {} sets", result_count);

    (result_count, processing_duration)
}

fn generate_test_data(count: usize) -> Vec<String> {
    let mut data = Vec::new();

    for i in 0..count {
        let sensor_data = format!(
            "<http://sensors.org/sensor{}> a <http://sensors.org/TemperatureSensor> .\n\
             <http://sensors.org/sensor{}> <http://sensors.org/hasTemperature> \"{}\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n\
             <http://sensors.org/sensor{}> <http://sensors.org/hasLocation> \"Building_{}\" .\n\
             <http://sensors.org/sensor{}> <http://sensors.org/isActive> \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> .",
            i, i, 20.0 + (i as f64) * 0.1, i, i % 10, i
        );
        data.push(sensor_data);
    }

    data
}

fn analyze_results(
    standard_duration: Duration,
    volcano_duration: Duration,
    standard_results: usize,
    volcano_results: usize,
) {
    let standard_ms = standard_duration.as_millis();
    let volcano_ms = volcano_duration.as_millis();

    if volcano_ms < standard_ms {
        let improvement = ((standard_ms - volcano_ms) as f64 / standard_ms as f64) * 100.0;
        println!("🚀 Volcano Optimizer Performance:");
        println!("   • {:.1}% faster than standard execution", improvement);
        println!("   • Better suited for this query complexity");
        println!("   • Recommended for production use");
    } else if standard_ms < volcano_ms {
        let overhead = ((volcano_ms - standard_ms) as f64 / standard_ms as f64) * 100.0;
        println!("⚡ Standard Execution Performance:");
        println!("   • {:.1}% faster than Volcano optimizer", overhead);
        println!("   • Lower optimization overhead");
        println!("   • Good for simple queries");
    } else {
        println!("⚖️  Performance Parity:");
        println!("   • Both modes performed similarly");
        println!("   • Query complexity may be at the threshold");
    }

    if standard_results != volcano_results {
        println!("\n⚠️  Result Count Differences:");
        println!("   • This may indicate different optimization strategies");
        println!("   • Both should logically produce the same results");
        println!("   • Differences might be due to timing or batching");
    }

    println!("\n📝 Recommendations:");
    if volcano_ms <= standard_ms {
        println!("   ✅ Use Volcano optimizer for this type of query");
        println!("   ✅ Consider Volcano for production workloads");
    } else {
        println!("   ✅ Standard execution is sufficient for this query");
        println!("   ✅ Consider Volcano for more complex queries");
    }
}
