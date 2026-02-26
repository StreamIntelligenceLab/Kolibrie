/*
* Copyright © 2025 Volodymyr Kadzhaia
* Copyright © 2025 Pieter Bonte
* KU Leuven — Stream Intelligence Lab, Belgium
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this file,
* you can obtain one at https://mozilla.org/MPL/2.0/.
*/

use kolibrie::rsp_engine::{
    OperationMode, QueryExecutionMode, RSPBuilder, RSPEngine, ResultConsumer, SimpleR2R,
};
use shared::query::{Fallback, SyncPolicy};
use shared::triple::Triple;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn make_two_window_engine(
    policy: SyncPolicy,
) -> (
    RSPEngine<Triple, Vec<(String, String)>>,
    Arc<Mutex<Vec<Vec<(String, String)>>>>,
) {
    let result_container = Arc::new(Mutex::new(Vec::<Vec<(String, String)>>::new()));
    let rc = Arc::clone(&result_container);
    let consumer = ResultConsumer {
        function: Arc::new(move |r: Vec<(String, String)>| {
            rc.lock().unwrap().push(r);
        }),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :windA ON :streamA [RANGE 10 STEP 2]
        FROM NAMED WINDOW :windB ON :streamB [RANGE 10 STEP 2]
        WHERE {
            WINDOW :windA { ?s1 a <http://test/TypeA> . }
            WINDOW :windB { ?s2 a <http://test/TypeB> . }
        }
    "#;
    let engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .set_sync_policy(policy)
        .build()
        .expect("Failed to build engine");
    (engine, result_container)
}

#[test]
fn rsp_ql_integration() {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        println!("Bindings: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // RSP-QL query with single window
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind ON ?s [RANGE 10 STEP 2]
        WHERE {
            WINDOW :wind {
                ?s a <http://www.w3.org/test/SuperType> .
            }
        }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .build()
        .expect("Failed to build RSP engine");
    //small hack to make sure the encoder is aligned between parsing and query injection
    engine.parse_data("a a <http://www.w3.org/test/SuperType> .");

    // Add data to the stream
    for i in 0..20 {
        let data = format!(
            "<http://test.be/subject{}> a <http://www.w3.org/test/SuperType> .",
            i
        );
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add(triple, i);
        }
    }

    engine.stop();
    thread::sleep(Duration::from_secs(2));

    // Should have results from window processing
    assert!(!result_container.lock().unwrap().is_empty());
}

#[test]
fn rsp_ql_integration_with_join() {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        println!("Bindings: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // RSP-QL query with single window
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind ON ?s [RANGE 10 STEP 2]
        WHERE {
            WINDOW :wind {
                ?s a <http://www.w3.org/test/SuperType> .
                ?s a <http://www.w3.org/test/MegaType> .
            }
        }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .build()
        .expect("Failed to build RSP engine");

    // Add data to the stream
    for i in 0..20 {
        let data = format!(
            "<http://test.be/subject{}> a <http://www.w3.org/test/SuperType> .\n\
            <http://test.be/subject{}> a <http://www.w3.org/test/MegaType> .",
            i, i
        );
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add(triple, i);
        }
    }

    engine.stop();
    thread::sleep(Duration::from_secs(2));

    // Should have results from window processing
    assert!(!result_container.lock().unwrap().is_empty());
}

#[test]
fn rsp_ql_multi_window_integration() {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        println!("Multi-window Bindings: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // RSP-QL query with multiple windows (similar to the example)
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind ON :stream1 [RANGE 10 STEP 2]
        FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 5 STEP 1]
        WHERE {
            WINDOW :wind {
                ?s a <http://www.w3.org/test/Temperature> .
            }
            WINDOW :wind2 {
                ?s2 a <http://www.w3.org/test/CO2> .
            }
        }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .build()
        .expect("Failed to build RSP engine");

    // Add temperature data
    for i in 0..10 {
        let data = format!(
            "<http://test.be/temp{}> a <http://www.w3.org/test/Temperature> .",
            i
        );
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream1", triple, i);
        }
    }

    // Add CO2 data
    for i in 0..10 {
        let data = format!("<http://test.be/co2{}> a <http://www.w3.org/test/CO2> .", i);
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream2", triple, i + 10);
        }
    }

    engine.stop();
    thread::sleep(Duration::from_secs(2));

    // Should have results from both windows
    assert!(!result_container.lock().unwrap().is_empty());
}

#[test]
fn rsp_ql_joining_multi_window_integration() {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        println!("Multi-window Bindings: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // RSP-QL query with multiple windows (similar to the example)
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind ON :stream1 [RANGE 10 STEP 2]
        FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 5 STEP 1]
        WHERE {
            WINDOW :wind {
                ?s a <http://www.w3.org/test/Temperature> .
            }
            WINDOW :wind2 {
                ?s a <http://www.w3.org/test/CO2> .
            }
        }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .build()
        .expect("Failed to build RSP engine");

    // Add temperature data
    for i in 0..10 {
        let data = format!(
            "<http://test.be/temp{}> a <http://www.w3.org/test/Temperature> .",
            i
        );
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream1", triple, i);
        }
    }

    // Add CO2 data
    for i in 0..10 {
        let data = format!("<http://test.be/co2{}> a <http://www.w3.org/test/CO2> .", i);
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream2", triple, i + 10);
        }
    }

    engine.stop();
    thread::sleep(Duration::from_secs(2));

    // Should have no results if windows are correctly joined together
    assert!(result_container.lock().unwrap().is_empty());
}

#[test]
fn rsp_ql_single_thread_multi_window_integration() {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);

    let function = Box::new(move |r: Vec<(String, String)>| {
        println!("SingleThread Multi-Window Bindings: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });

    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };

    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // FIXED:  Consistent spacing and proper SPARQL variable syntax
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind1 ON :stream1 [RANGE 10 STEP 2]
        FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 5 STEP 1]
        WHERE {
            WINDOW :wind1 {
                ?s1 a <http://www.w3.org/test/TypeOne> .
            }
            WINDOW :wind2 {
                ?s2 a <http://www.w3.org/test/TypeTwo> .
            }
        }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("Failed to build RSP engine");

    // Feed data to both streams
    for i in 0..5 {
        // Add to stream1
        let data1 = format!(
            "<http://test.be/one_{}> a <http://www.w3.org/test/TypeOne> .",
            i
        );
        let triples1 = engine.parse_data(&data1);
        for triple in triples1 {
            engine.add_to_stream("stream1", triple, i);
        }

        // Add to stream2
        let data2 = format!(
            "<http://test.be/two_{}> a <http://www.w3.org/test/TypeTwo> .",
            i
        );
        let triples2 = engine.parse_data(&data2);
        for triple in triples2 {
            engine.add_to_stream("stream2", triple, i + 10);
        }
    }

    engine.stop();

    // Verify we got results
    let results = result_container.lock().unwrap();
    println!("Total results captured:  {}", results.len());

    for (i, result) in results.iter().take(3).enumerate() {
        println!("Result {}: {:?}", i, result);
    }

    // Check if there are results with BOTH s1 AND s2 in the same binding
    let has_joined_results = results.iter().any(|binding| {
        let has_s1 = binding.iter().any(|(k, _)| k == "s1");
        let has_s2 = binding.iter().any(|(k, _)| k == "s2");
        has_s1 && has_s2
    });

    assert!(
        has_joined_results,
        "Should have joined results with both s1 and s2 in the same binding"
    );
    assert!(!results.is_empty(), "Should have at least some results");

    let joined_count = results
        .iter()
        .filter(|binding| {
            let has_s1 = binding.iter().any(|(k, _)| k == "s1");
            let has_s2 = binding.iter().any(|(k, _)| k == "s2");
            has_s1 && has_s2
        })
        .count();

    println!("Number of properly joined results: {}", joined_count);
    assert!(
        joined_count > 0,
        "Should have at least one properly joined result"
    );
}

/// Single window + static WHERE patterns: results must contain both
/// the window variable (?sensor) and the static variable (?room).
#[test]
fn rsp_ql_single_window_static_join() {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        println!("Static-join Bindings: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // Query: window pattern + static pattern joined on ?sensor
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind ON :stream1 [RANGE 10 STEP 2]
        WHERE {
            WINDOW :wind {
                ?sensor a <http://www.w3.org/test/Sensor> .
            }
            ?sensor <http://www.w3.org/test/locatedIn> ?room .
        }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("Failed to build RSP engine");

    // Add the location triple to the static background store.
    let location_triple_str =
        "<http://test.be/sensor0> <http://www.w3.org/test/locatedIn> <http://test.be/room1> .";
    engine.add_static_ntriples(location_triple_str);

    // Stream: sensor data
    for i in 0..5 {
        let data = format!(
            "<http://test.be/sensor{}> a <http://www.w3.org/test/Sensor> .",
            i
        );
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream1", triple, i);
        }
    }

    engine.stop();

    let results = result_container.lock().unwrap();
    println!("Static-join total results: {}", results.len());

    // At least one result must have both ?sensor and ?room
    let has_static_join = results.iter().any(|binding| {
        let has_sensor = binding.iter().any(|(k, _)| k == "sensor");
        let has_room = binding.iter().any(|(k, _)| k == "room");
        has_sensor && has_room
    });

    assert!(
        has_static_join,
        "Expected joined results with both ?sensor and ?room, got: {:?}",
        *results
    );
}

// -----------------------------------------------------------------------
// Sync-policy tests
// -----------------------------------------------------------------------

/// Steal policy: window A fires first, B never fires → no emission
/// (last_mat only has A, never reaches num_windows=2).
#[test]
fn test_steal_policy_emits_after_first_window() {
    let (mut engine, results) = make_two_window_engine(SyncPolicy::Steal);
    for i in 0..5usize {
        let data = format!("<http://test/a{}> a <http://test/TypeA> .", i);
        let triples = engine.parse_data(&data);
        for t in triples {
            engine.add_to_stream("streamA", t, i);
        }
    }
    engine.stop();
    assert!(
        results.lock().unwrap().is_empty(),
        "Steal: no emission expected when B has never fired"
    );
}

/// Steal policy: B fires once then A fires repeatedly → emission on each A trigger.
#[test]
fn test_steal_policy_emits_with_stale() {
    let (mut engine, results) = make_two_window_engine(SyncPolicy::Steal);

    // Fire B first
    for i in 0..3usize {
        let data = format!("<http://test/b{}> a <http://test/TypeB> .", i);
        let triples = engine.parse_data(&data);
        for t in triples {
            engine.add_to_stream("streamB", t, i);
        }
    }
    // Fire A repeatedly at much later timestamps so B's windows have already closed
    for i in 0..5usize {
        let data = format!("<http://test/a{}> a <http://test/TypeA> .", i);
        let triples = engine.parse_data(&data);
        for t in triples {
            engine.add_to_stream("streamA", t, i + 20);
        }
    }
    engine.stop();
    assert!(
        !results.lock().unwrap().is_empty(),
        "Steal: should emit once both windows have been materialized"
    );
}

/// Wait policy (default): only A fires → no emission.
#[test]
fn test_wait_policy_waits_for_both() {
    let (mut engine, results) = make_two_window_engine(SyncPolicy::Wait);
    for i in 0..5usize {
        let data = format!("<http://test/a{}> a <http://test/TypeA> .", i);
        let triples = engine.parse_data(&data);
        for t in triples {
            engine.add_to_stream("streamA", t, i);
        }
    }
    engine.stop();
    assert!(
        results.lock().unwrap().is_empty(),
        "Wait: no emission when only A fires"
    );
}

/// Timeout(100ms, Steal) in SingleThread mode is treated as Wait.
/// Only A fires → no emission on first cycle.
#[test]
fn test_timeout_steal_policy() {
    let policy = SyncPolicy::Timeout {
        duration: Duration::from_millis(100),
        fallback: Fallback::Steal,
    };
    let (mut engine, results) = make_two_window_engine(policy);
    for i in 0..5usize {
        let data = format!("<http://test/a{}> a <http://test/TypeA> .", i);
        let triples = engine.parse_data(&data);
        for t in triples {
            engine.add_to_stream("streamA", t, i);
        }
    }
    engine.stop();
    // SingleThread: Timeout treated as Wait; B never fired → no emit
    assert!(
        results.lock().unwrap().is_empty(),
        "Timeout/Steal (SingleThread = Wait): no emit when B never fires"
    );
}

/// Timeout(100ms, Drop) in SingleThread mode is treated as Wait.
/// Only A fires → no emission.
#[test]
fn test_timeout_drop_policy() {
    let policy = SyncPolicy::Timeout {
        duration: Duration::from_millis(100),
        fallback: Fallback::Drop,
    };
    let (mut engine, results) = make_two_window_engine(policy);
    for i in 0..5usize {
        let data = format!("<http://test/a{}> a <http://test/TypeA> .", i);
        let triples = engine.parse_data(&data);
        for t in triples {
            engine.add_to_stream("streamA", t, i);
        }
    }
    engine.stop();
    assert!(
        results.lock().unwrap().is_empty(),
        "Timeout/Drop (SingleThread = Wait): no emit when B never fires"
    );
}

// -----------------------------------------------------------------------

/// Two windows + static WHERE patterns: results must contain variables
/// from both windows (?sensor, ?room) and confirm the static join filtered
/// them correctly.
#[test]
fn rsp_ql_multi_window_static_join() {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        println!("Multi-window static-join Bindings: {:?}", r);
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // Two windows (sensor / room) joined with static location triples.
    let rsp_ql_query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind1 ON :stream1 [RANGE 10 STEP 2]
        FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 10 STEP 2]
        WHERE {
            WINDOW :wind1 {
                ?sensor a <http://www.w3.org/test/Sensor> .
            }
            WINDOW :wind2 {
                ?room a <http://www.w3.org/test/Room> .
            }
            ?sensor <http://www.w3.org/test/locatedIn> ?room .
        }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_ql_query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("Failed to build RSP engine");

    // Add static location triple: sensor0 is located in room0.
    let location_triple_str =
        "<http://test.be/sensor0> <http://www.w3.org/test/locatedIn> <http://test.be/room0> .";
    engine.add_static_ntriples(location_triple_str);

    // Stream1: sensors (sensor0 will match the static location triple)
    for i in 0..3 {
        let data = format!(
            "<http://test.be/sensor{}> a <http://www.w3.org/test/Sensor> .",
            i
        );
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream1", triple, i);
        }
    }

    // Stream2: rooms (room0 will match the static location triple)
    for i in 0..3 {
        let data = format!(
            "<http://test.be/room{}> a <http://www.w3.org/test/Room> .",
            i
        );
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream2", triple, i + 10);
        }
    }

    engine.stop();

    let results = result_container.lock().unwrap();
    println!("Multi-window static-join total results: {}", results.len());

    // Results must have both ?sensor and ?room
    let has_both_windows = results.iter().any(|binding| {
        let has_sensor = binding.iter().any(|(k, _)| k == "sensor");
        let has_room = binding.iter().any(|(k, _)| k == "room");
        has_sensor && has_room
    });

    assert!(
        has_both_windows,
        "Expected joined results with both ?sensor and ?room, got: {:?}",
        *results
    );

    // Verify that the static join filtered: only (sensor0, room0) should appear
    // (sensor1/sensor2 have no location triple, room1/room2 are not sensor0's location)
    let all_valid = results.iter().all(|binding| {
        let sensor = binding
            .iter()
            .find(|(k, _)| k == "sensor")
            .map(|(_, v)| v.as_str());
        let room = binding
            .iter()
            .find(|(k, _)| k == "room")
            .map(|(_, v)| v.as_str());
        match (sensor, room) {
            (Some(s), Some(r)) => s.contains("sensor0") && r.contains("room0"),
            _ => true,
        }
    });

    assert!(
        all_valid,
        "Static join should only produce (sensor0, room0) pairs, got: {:?}",
        *results
    );
}

#[test]
fn test_static_data_not_visible_in_window_query() {
    // Query has ONLY window patterns — no non-window triple patterns.
    // Static data matches the window pattern; it must NOT appear in results.
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let rc_clone = Arc::clone(&result_container);
    let result_consumer = ResultConsumer {
        function: Arc::new(move |r: Vec<(String, String)>| {
            rc_clone.lock().unwrap().push(r);
        }),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT ?s
        FROM NAMED WINDOW :w ON ?stream [RANGE 10 STEP 10]
        WHERE { WINDOW :w { ?s a <http://example.org/Type> . } }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .build()
        .expect("Failed to build RSP engine");

    // Prime dictionary
    engine.parse_data("<http://example.org/static1> a <http://example.org/Type> .");

    // Add static triple that matches the window pattern
    engine.add_static_ntriples("<http://example.org/static1> a <http://example.org/Type> .");

    // Push exactly one stream event
    let triples =
        engine.parse_data("<http://example.org/stream1> a <http://example.org/Type> .");
    for triple in triples {
        engine.add(triple, 1);
    }

    engine.stop();
    thread::sleep(Duration::from_secs(2));

    let all = result_container.lock().unwrap();
    // With fix: 1 result (stream1 only).
    // Without fix: 2 results (stream1 + static1 leaking in).
    assert_eq!(
        all.len(),
        1,
        "Window must return only stream events, not static data (got {})",
        all.len()
    );
}

#[test]
fn test_window_evicts_old_data() {
    // Non-overlapping windows (RANGE 10 STEP 10) with one subject per window.
    // Without eviction the R2R store accumulates all triples, so window 2 returns
    // 2 rows and window 3 returns 3 rows (total 6). With eviction each window
    // returns exactly 1 row (total 3).
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let rc_clone = Arc::clone(&result_container);
    let result_consumer = ResultConsumer {
        function: Arc::new(move |r: Vec<(String, String)>| {
            rc_clone.lock().unwrap().push(r);
        }),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT ?s
        FROM NAMED WINDOW :w ON ?stream [RANGE 10 STEP 10]
        WHERE { WINDOW :w { ?s a <http://example.org/Type> . } }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .build()
        .expect("Failed to build RSP engine");

    // Prime the dictionary so query and data use the same term IDs
    engine.parse_data("a a <http://example.org/Type> .");

    // Push one subject per non-overlapping window
    for (i, ts) in [(1usize, 1usize), (2, 11), (3, 21)] {
        let data = format!(
            "<http://example.org/subject{}> a <http://example.org/Type> .",
            i
        );
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add(triple, ts);
        }
    }

    engine.stop();
    thread::sleep(Duration::from_secs(2));

    let all = result_container.lock().unwrap();
    // Each of the 3 windows must return exactly 1 result row.
    // Stale accumulation without the fix gives 1+2+3=6 total rows.
    assert_eq!(
        all.len(),
        3,
        "Each window should return exactly 1 result row (got {}); \
         stale data from previous firings is leaking into the store",
        all.len()
    );
}
