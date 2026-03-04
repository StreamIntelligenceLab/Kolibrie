/*
* Copyright © 2025 Volodymyr Kadzhaia
* Copyright © 2025 Pieter Bonte
* KU Leuven — Stream Intelligence Lab, Belgium
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this file,
* you can obtain one at https://mozilla.org/MPL/2.0/.
*/
// -----------------------------------------------------------------------
// ISTREAM / DSTREAM semantics tests
// -----------------------------------------------------------------------

/// ISTREAM: sliding window (RANGE=3 STEP=1) across 3 firings.
///
/// 3-firing sequence:
///   - ts=1: add subjectA → no fire (opens first window).
///   - ts=2: add subjectB → fires [-1,1] with {A};   ISTREAM: old=∅   → emit A.
///   - ts=3: add subjectC → fires [0,2]  with {A,B}; ISTREAM: old={A}  → emit B only.
///   - ts=4: add subjectD → fires [1,3]  with {A,B,C}; ISTREAM: old={A,B} → emit C only.
///
/// Total consumer calls: 3 → [A], [B], [C].
/// No stop() — flushing all active windows would corrupt R2S state.
#[test]
fn rsp_ql_istream_semantics() {
    let result_container = Arc::new(Mutex::new(Vec::<Vec<(String, String)>>::new()));
    let rc = Arc::clone(&result_container);
    let result_consumer = ResultConsumer {
        function: Arc::new(move |r: Vec<(String, String)>| {
            rc.lock().unwrap().push(r);
        }),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let query = r#"
        REGISTER ISTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :w ON ?stream [RANGE 3 STEP 1]
        WHERE { WINDOW :w { ?s a <http://test/IType> . } }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("Failed to build ISTREAM engine");

    // Prime dictionary so query and data share term IDs.
    engine.parse_data("<http://test/s0> a <http://test/IType> .");

    // ts=1: A → no fire (opens first window).
    for t in engine.parse_data("<http://test/subjectA> a <http://test/IType> .") {
        engine.add(t, 1);
    }

    // ts=2: B → fires [-1,1] with {A}; ISTREAM: old=∅ → emit A.
    for t in engine.parse_data("<http://test/subjectB> a <http://test/IType> .") {
        engine.add(t, 2);
    }

    // ts=3: C → fires [0,2] with {A,B}; ISTREAM: old={A} → emit B only.
    for t in engine.parse_data("<http://test/subjectC> a <http://test/IType> .") {
        engine.add(t, 3);
    }

    // ts=4: D → fires [1,3] with {A,B,C}; ISTREAM: old={A,B} → emit C only.
    for t in engine.parse_data("<http://test/subjectD> a <http://test/IType> .") {
        engine.add(t, 4);
    }

    let results = result_container.lock().unwrap();
    assert_eq!(
        results.len(),
        3,
        "ISTREAM: 3 firings → 3 consumer calls. Got: {:?}",
        *results
    );
    // Firing 1: [-1,1] → {A}, new since ∅ → emit A.
    assert_eq!(results[0].len(),1);
    assert!(
        results[0].iter().any(|(k, v)| k == "s" && v.contains("subjectA")),
        "ISTREAM firing 1 must emit subjectA, got: {:?}",
        results[0]
    );
    // Firing 2: [0,2] → {A,B}, new since {A} → emit B only.
    assert_eq!(results[1].len(),1);
    assert!(
        results[1].iter().any(|(k, v)| k == "s" && v.contains("subjectB")),
        "ISTREAM firing 2 must emit subjectB, got: {:?}",
        results[1]
    );
    // Firing 3: [1,3] → {A,B,C}, new since {A,B} → emit C only.
    assert_eq!(results[2].len(),1);
    assert!(
        results[2].iter().any(|(k, v)| k == "s" && v.contains("subjectC")),
        "ISTREAM firing 3 must emit subjectC, got: {:?}",
        results[2]
    );

}

/// DSTREAM: sliding window (RANGE=3 STEP=1) — 5 window firings, 1 DSTREAM emission.
///
/// Window firing sequence (OnWindowClose fires when ts > window.close):
///   - ts=1: add A → no fire yet.
///   - ts=2: add B → window (0,1) fires with {A};       DSTREAM: old=∅     → last={A},       no emission.
///   - ts=3: add C → window (0,2) fires with {A,B};     DSTREAM: old={A}   → last={A,B},     no emission.
///   - ts=4: add D → window (0,3) fires with {A,B,C};   DSTREAM: old={A,B} → last={A,B,C},   no emission.
///   - ts=5: add E → window (1,4) fires with {A,B,C,D}; DSTREAM: old={A,B,C} → last={A,B,C,D}, no emission.
///   - ts=6: add F → window (2,5) fires with {B,C,D,E}; DSTREAM: old={A,B,C,D} → deleted={A} → emit A.
///
/// Total consumer calls: 1 → [A].
/// No stop() — flushing all active windows would corrupt R2S state.
#[test]
fn rsp_ql_dstream_semantics() {
    let result_container = Arc::new(Mutex::new(Vec::<Vec<(String, String)>>::new()));
    let rc = Arc::clone(&result_container);
    let result_consumer = ResultConsumer {
        function: Arc::new(move |r: Vec<(String, String)>| {
            rc.lock().unwrap().push(r);
        }),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let query = r#"
        REGISTER DSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :w ON ?stream [RANGE 3 STEP 1]
        WHERE { WINDOW :w { ?s a <http://test/DType> . } }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("Failed to build DSTREAM engine");

    // Prime dictionary.
    engine.parse_data("<http://test/s0> a <http://test/DType> .");

    // ts=1: A into windows; no fire.
    for t in engine.parse_data("<http://test/subjectA> a <http://test/DType> .") {
        engine.add(t, 1);
    }

    // ts=2: B → window (0,1) fires with {A}; DSTREAM: old=∅ → no emission.
    for t in engine.parse_data("<http://test/subjectB> a <http://test/DType> .") {
        engine.add(t, 2);
    }

    // ts=3: C → window (0,2) fires with {A,B}; DSTREAM: old={A} → no emission.
    for t in engine.parse_data("<http://test/subjectC> a <http://test/DType> .") {
        engine.add(t, 3);
    }

    // ts=4: D → window (0,3) fires with {A,B,C}; DSTREAM: old={A,B} → no emission.
    for t in engine.parse_data("<http://test/subjectD> a <http://test/DType> .") {
        engine.add(t, 4);
    }

    // ts=5: E → window (1,4) fires with {A,B,C,D}; DSTREAM: old={A,B,C} → no emission.
    for t in engine.parse_data("<http://test/subjectE> a <http://test/DType> .") {
        engine.add(t, 5);
    }

    // ts=6: F → window (2,5) fires with {B,C,D,E}; DSTREAM: old={A,B,C,D} → deleted={A} → emit A.
    for t in engine.parse_data("<http://test/subjectF> a <http://test/DType> .") {
        engine.add(t, 6);
    }

    let results = result_container.lock().unwrap();
    assert_eq!(
        results.len(),
        1,
        "DSTREAM: 5 window firings → 1 consumer call (window (2,5) deletes subjectA). Got: {:?}",
        *results
    );
    // The one result must bind ?s to subjectA (deleted from window (1,4) → (2,5)).
    assert!(
        results[0].iter().any(|(k, v)| k == "s" && v.contains("subjectA")),
        "DSTREAM result must bind ?s to subjectA (deleted), got: {:?}",
        results[0]
    );
}

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

/// ISTREAM: sliding window (RANGE=3 STEP=1) across 3 firings.
///
/// The first event never triggers a firing (same behaviour as other sliding-window
/// tests).  4 events are needed to produce 3 firings:
///
///   ts=1: subjectA added  → no fire (opens first window)
///   ts=2: subjectB added  → fires [−1,1] → content {A}   → ISTREAM: old=∅   → emit A
///   ts=3: subjectC added  → fires [0,2]  → content {A,B} → ISTREAM: old={A}  → emit B only
///   ts=4: subjectA (again)→ fires [1,3]  → content {A,B,C} → ISTREAM: old={A,B} → emit C only
#[test]
fn rsp_ql_istream_range3_step1() {
    let result_container = Arc::new(Mutex::new(Vec::<Vec<(String, String)>>::new()));
    let rc = Arc::clone(&result_container);
    let result_consumer = ResultConsumer {
        function: Arc::new(move |r: Vec<(String, String)>| {
            rc.lock().unwrap().push(r);
        }),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let query = r#"
        REGISTER ISTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :w ON ?stream [RANGE 3 STEP 1]
        WHERE { WINDOW :w { ?s a <http://test/RType> . } }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("Failed to build ISTREAM RANGE3 engine");

    // Prime dictionary so query and data share term IDs.
    engine.parse_data("<http://test/s0> a <http://test/RType> .");

    // ts=1: A → no fire (first event opens the window but nothing closes yet).
    for t in engine.parse_data("<http://test/subjectA> a <http://test/RType> .") {
        engine.add(t, 1);
    }

    // ts=2: B → fires window [−1,1] → content {A} → ISTREAM: old=∅ → emit A.
    for t in engine.parse_data("<http://test/subjectB> a <http://test/RType> .") {
        engine.add(t, 2);
    }

    // ts=3: C → fires window [0,2] → content {A,B} → ISTREAM: old={A} → emit B only.
    for t in engine.parse_data("<http://test/subjectC> a <http://test/RType> .") {
        engine.add(t, 3);
    }

    // ts=4: A again (trigger) → fires window [1,3] → content {A,B,C}
    //       → ISTREAM: old={A,B} → emit C only.
    for t in engine.parse_data("<http://test/subjectA> a <http://test/RType> .") {
        engine.add(t, 4);
    }

    let results = result_container.lock().unwrap();
    assert_eq!(
        results.len(),
        3,
        "ISTREAM RANGE3/STEP1: expected 3 firings → 3 consumer calls. Got: {:?}",
        *results
    );

    // Firing 1 (triggered at ts=2): window {A}, ISTREAM emits A.
    assert!(
        results[0].iter().any(|(k, v)| k == "s" && v.contains("subjectA")),
        "Firing 1 must emit subjectA, got: {:?}",
        results[0]
    );

    // Firing 2 (triggered at ts=3): window {A,B}, ISTREAM emits B only.
    assert!(
        results[1].iter().any(|(k, v)| k == "s" && v.contains("subjectB")),
        "Firing 2 must emit subjectB, got: {:?}",
        results[1]
    );
    assert!(
        !results[1].iter().any(|(k, v)| k == "s" && v.contains("subjectA")),
        "Firing 2 must NOT re-emit subjectA (already seen), got: {:?}",
        results[1]
    );

    // Firing 3 (triggered at ts=4): window {A,B,C}, ISTREAM emits C only.
    assert!(
        results[2].iter().any(|(k, v)| k == "s" && v.contains("subjectC")),
        "Firing 3 must emit subjectC, got: {:?}",
        results[2]
    );
    assert!(
        !results[2].iter().any(|(k, v)| k == "s" && v.contains("subjectA")),
        "Firing 3 must NOT re-emit subjectA, got: {:?}",
        results[2]
    );
    assert!(
        !results[2].iter().any(|(k, v)| k == "s" && v.contains("subjectB")),
        "Firing 3 must NOT re-emit subjectB, got: {:?}",
        results[2]
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

/// Regression test: ISTREAM with same subject+predicate and changing object values.
///
/// Each firing introduces one new (reading1, temp=N) triple.  Because the subject
/// and predicate are identical across triples, only the object differs.  A HashMap
/// non-determinism bug caused the Vec<(String,String)> serialisation of a row to
/// vary between firings, making ISTREAM either re-emit stale rows or drop new ones.
///
/// Window RANGE=3 STEP=1.  Event sequence:
///   ts=1: triple (reading1, hasTemp, "1") — no fire
///   ts=2: (reading1, hasTemp, "2") — fires window with {"1"}   → ISTREAM: old=∅   → emit "1"
///   ts=3: (reading1, hasTemp, "3") — fires window with {"1","2"} → ISTREAM: old={"1"} → emit "2"
///   ts=4: (reading1, hasTemp, "4") — fires window with {"1","2","3"} → ISTREAM: old={"1","2"} → emit "3"
///
/// Expected: 3 consumer calls, each with exactly one row.
#[test]
fn rsp_ql_istream_same_sp_diff_object() {
    let result_container = Arc::new(Mutex::new(Vec::<Vec<(String, String)>>::new()));
    let rc = Arc::clone(&result_container);
    let result_consumer = ResultConsumer {
        function: Arc::new(move |r: Vec<(String, String)>| {
            rc.lock().unwrap().push(r);
        }),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let query = r#"
        REGISTER ISTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :w ON ?stream [RANGE 3 STEP 1]
        WHERE { WINDOW :w { ?reading <http://test/hasTemp> ?temp . } }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("Failed to build ISTREAM same-sp engine");

    // Prime dictionary so query and data share term IDs.
    engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"0\" .");

    // ts=1: temp=1 — no fire.
    for t in engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"1\" .") {
        engine.add(t, 1);
    }

    // ts=2: temp=2 — fires window with {temp=1}; ISTREAM: old=∅ → emit temp=1.
    for t in engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"2\" .") {
        engine.add(t, 2);
    }

    // ts=3: temp=3 — fires window with {temp=1,temp=2}; ISTREAM: old={temp=1} → emit temp=2.
    for t in engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"3\" .") {
        engine.add(t, 3);
    }

    // ts=4: temp=4 — fires window with {temp=1,temp=2,temp=3}; ISTREAM: old={temp=1,temp=2} → emit temp=3.
    for t in engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"4\" .") {
        engine.add(t, 4);
    }

    let results = result_container.lock().unwrap();
    assert_eq!(
        results.len(),
        3,
        "ISTREAM same-sp: expected 3 consumer calls, got: {:?}",
        *results
    );

    // Each firing must emit exactly one row.
    for (i, row) in results.iter().enumerate() {
        assert_eq!(row.len(), 2, "Firing {}: row should have 2 bindings, got: {:?}", i + 1, row);
    }

    // Firing 1 → temp=1.
    assert!(
        results[0].iter().any(|(k, v)| k == "temp" && v.contains('1')),
        "Firing 1 must emit temp=1, got: {:?}",
        results[0]
    );
    // Firing 2 → temp=2 (not temp=1 again).
    assert!(
        results[1].iter().any(|(k, v)| k == "temp" && v.contains('2')),
        "Firing 2 must emit temp=2, got: {:?}",
        results[1]
    );
    assert!(
        !results[1].iter().any(|(k, v)| k == "temp" && v == "\"1\""),
        "Firing 2 must NOT re-emit temp=1, got: {:?}",
        results[1]
    );
    // Firing 3 → temp=3 (not temp=1 or temp=2 again).
    assert!(
        results[2].iter().any(|(k, v)| k == "temp" && v.contains('3')),
        "Firing 3 must emit temp=3, got: {:?}",
        results[2]
    );
    assert!(
        !results[2].iter().any(|(k, v)| k == "temp" && v == "\"1\""),
        "Firing 3 must NOT re-emit temp=1, got: {:?}",
        results[2]
    );
    assert!(
        !results[2].iter().any(|(k, v)| k == "temp" && v == "\"2\""),
        "Firing 3 must NOT re-emit temp=2, got: {:?}",
        results[2]
    );
}

/// Reasoning test: forward-chaining derives `?s a <http://test/HasValue>` from
/// `?s <http://test/hasValue> ?v`, enabling a window query that matches on the
/// inferred type — even though no explicit type triple exists in the stream.
#[test]
fn rsp_ql_reasoning_derives_types() {
    let result_container = Arc::new(Mutex::new(Vec::<Vec<(String, String)>>::new()));
    let rc = Arc::clone(&result_container);
    let result_consumer = ResultConsumer {
        function: Arc::new(move |r: Vec<(String, String)>| {
            rc.lock().unwrap().push(r);
        }),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    // N3 rule: { ?s test:hasValue ?v } => { ?s rdf:type test:HasValue }
    // Passed as a string so load_rules() parses it using the shared dictionary
    // (same IDs as the query plan).
    let rule_str = concat!(
        "@prefix test: <http://test/>.\n",
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.\n",
        "{ ?s test:hasValue ?v . } => { ?s rdf:type test:HasValue . } .\n",
    );

    let query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :w ON ?stream [RANGE 10 STEP 1]
        WHERE { WINDOW :w { ?s a <http://test/HasValue> . } }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_rules(rule_str)
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("Failed to build reasoning engine");

    // Feed stream triple: sensor1 hasValue "42" (no explicit type triple)
    for t in engine.parse_data("<http://test/sensor1> <http://test/hasValue> \"42\" .") {
        engine.add(t, 1);
    }
    // Trigger a second event to fire the window
    for t in engine.parse_data("<http://test/sensor2> <http://test/hasValue> \"99\" .") {
        engine.add(t, 2);
    }

    engine.stop();

    let results = result_container.lock().unwrap();
    assert!(
        !results.is_empty(),
        "Reasoning: expected at least one consumer call (inferred type should match query), got none"
    );

    // At least one row should bind ?s to sensor1
    let has_sensor1 = results.iter().any(|row| {
        row.iter().any(|(k, v)| k == "s" && v.contains("sensor1"))
    });
    assert!(
        has_sensor1,
        "Reasoning: expected sensor1 to appear via inferred type. Got: {:?}",
        *results
    );
}
