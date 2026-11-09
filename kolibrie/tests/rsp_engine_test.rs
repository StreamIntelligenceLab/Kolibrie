/*
* Copyright © 2025 Volodymyr Kadzhaia
* Copyright © 2025 Pieter Bonte
* KU Leuven — Stream Intelligence Lab, Belgium
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this file,
* you can obtain one at https://mozilla.org/MPL/2.0/.
*/

/// ISTREAM: sliding window (RANGE=3 STEP=1) across 3 firings
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

    engine.parse_data("<http://test/s0> a <http://test/IType> .");

    for t in engine.parse_data("<http://test/subjectA> a <http://test/IType> .") {
        engine.add(t, 1);
    }

    for t in engine.parse_data("<http://test/subjectB> a <http://test/IType> .") {
        engine.add(t, 2);
    }

    for t in engine.parse_data("<http://test/subjectC> a <http://test/IType> .") {
        engine.add(t, 3);
    }

    for t in engine.parse_data("<http://test/subjectD> a <http://test/IType> .") {
        engine.add(t, 4);
    }

    let results = result_container.lock().unwrap();
    assert_eq!(
        results.len(),
        3,
        "ISTREAM: 3 firings -> 3 consumer calls. Got: {:?}",
        *results
    );
    assert_eq!(results[0].len(), 1);
    assert!(
        results[0]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectA")),
        "ISTREAM firing 1 must emit subjectA, got: {:?}",
        results[0]
    );
    assert_eq!(results[1].len(), 1);
    assert!(
        results[1]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectB")),
        "ISTREAM firing 2 must emit subjectB, got: {:?}",
        results[1]
    );
    assert_eq!(results[2].len(), 1);
    assert!(
        results[2]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectC")),
        "ISTREAM firing 3 must emit subjectC, got: {:?}",
        results[2]
    );
}

/// DSTREAM: sliding window (RANGE=3 STEP=1) — 5 window firings, 2 DSTREAM emissions
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

    engine.parse_data("<http://test/s0> a <http://test/DType> .");

    for t in engine.parse_data("<http://test/subjectA> a <http://test/DType> .") {
        engine.add(t, 1);
    }

    for t in engine.parse_data("<http://test/subjectB> a <http://test/DType> .") {
        engine.add(t, 2);
    }

    for t in engine.parse_data("<http://test/subjectC> a <http://test/DType> .") {
        engine.add(t, 3);
    }

    for t in engine.parse_data("<http://test/subjectD> a <http://test/DType> .") {
        engine.add(t, 4);
    }

    for t in engine.parse_data("<http://test/subjectE> a <http://test/DType> .") {
        engine.add(t, 5);
    }

    for t in engine.parse_data("<http://test/subjectF> a <http://test/DType> .") {
        engine.add(t, 6);
    }

    let results = result_container.lock().unwrap();
    assert_eq!(
        results.len(),
        2,
        "DSTREAM: 5 window firings -> 2 consumer calls ([2,4] drops subjectA, [3,5] drops subjectB). Got: {:?}",
        *results
    );
    assert_eq!(results[0].len(), 1);
    assert!(
        results[0]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectA")),
        "DSTREAM emission 1 must bind ?s to subjectA (deleted), got: {:?}",
        results[0]
    );
    assert_eq!(results[1].len(), 1);
    assert!(
        results[1]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectB")),
        "DSTREAM emission 2 must bind ?s to subjectB (deleted), got: {:?}",
        results[1]
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
    engine.parse_data("a a <http://www.w3.org/test/SuperType> .");

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

    for i in 0..10 {
        let data = format!("<http://test.be/co2{}> a <http://www.w3.org/test/CO2> .", i);
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream2", triple, i + 10);
        }
    }

    engine.stop();

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

    for i in 0..10 {
        let data = format!("<http://test.be/co2{}> a <http://www.w3.org/test/CO2> .", i);
        let triples = engine.parse_data(&data);
        for triple in triples {
            engine.add_to_stream("stream2", triple, i + 10);
        }
    }

    engine.stop();

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

    for i in 0..5 {
        let data1 = format!(
            "<http://test.be/one_{}> a <http://www.w3.org/test/TypeOne> .",
            i
        );
        let triples1 = engine.parse_data(&data1);
        for triple in triples1 {
            engine.add_to_stream("stream1", triple, i);
        }

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

    let results = result_container.lock().unwrap();
    println!("Total results captured:  {}", results.len());

    for (i, result) in results.iter().take(3).enumerate() {
        println!("Result {}: {:?}", i, result);
    }

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

/// Single window + static WHERE patterns: results must contain both the window variable (?sensor)
/// and the static variable (?room)
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

    let location_triple_str =
        "<http://test.be/sensor0> <http://www.w3.org/test/locatedIn> <http://test.be/room1> .";
    engine.add_static_ntriples(location_triple_str);

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

/// Steal policy: window A fires first, B never fires -> no emission (last_mat only has A, never
/// reaches num_windows=2)
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

/// Steal policy: B fires once then A fires repeatedly -> emission on each A trigger
#[test]
fn test_steal_policy_emits_with_stale() {
    let (mut engine, results) = make_two_window_engine(SyncPolicy::Steal);

    for i in 0..3usize {
        let data = format!("<http://test/b{}> a <http://test/TypeB> .", i);
        let triples = engine.parse_data(&data);
        for t in triples {
            engine.add_to_stream("streamB", t, i);
        }
    }
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

/// Wait policy (default): only A fires -> no emission
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

/// Timeout(100ms, Steal) in SingleThread mode is treated as Wait
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
    assert!(
        results.lock().unwrap().is_empty(),
        "Timeout/Steal (SingleThread = Wait): no emit when B never fires"
    );
}

/// Timeout(100ms, Drop) in SingleThread mode is treated as Wait
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

/// Two windows plus static WHERE patterns: results carry both windows' variables after the static
/// join
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

    let location_triple_str =
        "<http://test.be/sensor0> <http://www.w3.org/test/locatedIn> <http://test.be/room0> .";
    engine.add_static_ntriples(location_triple_str);

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

fn make_cross_window_query() -> &'static str {
    r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :wind1 ON :stream1 [RANGE 10 STEP 10]
        FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 10 STEP 10]
        WHERE {
            WINDOW :wind1 {
                ?sensor <http://test/hotspot> ?room .
            }
            WINDOW :wind2 {
                ?sensor <http://test/location> ?room .
            }
        }
    "#
}

fn make_cross_window_rules() -> &'static str {
    r#"
{ ?sensor <:wind1http://test/reading> ?value .
  ?sensor <:wind2http://test/location> ?room }
=> { ?sensor <:wind1http://test/hotspot> ?room }
    "#
}

fn cross_window_result_engine(
    with_rules: bool,
) -> (
    RSPEngine<Triple, Vec<(String, String)>>,
    Arc<Mutex<Vec<Vec<(String, String)>>>>,
) {
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r: Vec<(String, String)>| {
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer {
        function: Arc::new(function),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let builder = RSPBuilder::new()
        .add_rsp_ql_query(make_cross_window_query())
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread);

    let builder = if with_rules {
        builder.add_cross_window_rules(make_cross_window_rules())
    } else {
        builder
    };

    let engine = builder
        .build()
        .expect("Failed to build cross-window RSP engine");
    (engine, result_container)
}

#[test]
fn rsp_cross_window_rule_derives_queryable_fact() {
    let (mut engine, results) = cross_window_result_engine(true);

    for triple in engine.parse_data("<http://test/sensorA> <http://test/reading> \"25\" .") {
        engine.add_to_stream("stream1", triple, 1);
    }
    for triple in
        engine.parse_data("<http://test/sensorA> <http://test/location> <http://test/room1> .")
    {
        engine.add_to_stream("stream2", triple, 2);
    }

    engine.stop();

    let results = results.lock().unwrap();
    assert!(
        results.iter().any(|row| {
            row.iter()
                .any(|(k, v)| k == "sensor" && v.contains("sensorA"))
                && row.iter().any(|(k, v)| k == "room" && v.contains("room1"))
        }),
        "cross-window SDS+ rule should derive hotspot visible to the window query; got {:?}",
        *results
    );
}

#[test]
fn rsp_cross_window_without_rules_produces_no_derived_result() {
    let (mut engine, results) = cross_window_result_engine(false);

    for triple in engine.parse_data("<http://test/sensorA> <http://test/reading> \"25\" .") {
        engine.add_to_stream("stream1", triple, 1);
    }
    for triple in
        engine.parse_data("<http://test/sensorA> <http://test/location> <http://test/room1> .")
    {
        engine.add_to_stream("stream2", triple, 2);
    }

    engine.stop();

    assert!(
        results.lock().unwrap().is_empty(),
        "without cross-window rules there should be no hotspot result"
    );
}

#[test]
fn rsp_cross_window_expired_support_no_longer_emits() {
    let (mut engine, results) = cross_window_result_engine(true);

    for triple in engine.parse_data("<http://test/sensorA> <http://test/reading> \"25\" .") {
        engine.add_to_stream("stream1", triple, 1);
    }
    for triple in
        engine.parse_data("<http://test/sensorA> <http://test/location> <http://test/room1> .")
    {
        engine.add_to_stream("stream2", triple, 2);
    }

    for triple in engine.parse_data("<http://test/otherSensor> <http://test/reading> \"10\" .") {
        engine.add_to_stream("stream1", triple, 15);
    }
    for triple in
        engine.parse_data("<http://test/otherSensor> <http://test/location> <http://test/room2> .")
    {
        engine.add_to_stream("stream2", triple, 15);
    }
    engine.process_single_thread_window_results();
    results.lock().unwrap().clear();

    for triple in engine.parse_data("<http://test/noReadingJoin> <http://test/reading> \"10\" .") {
        engine.add_to_stream("stream1", triple, 25);
    }
    for triple in engine
        .parse_data("<http://test/noLocationJoin> <http://test/location> <http://test/room3> .")
    {
        engine.add_to_stream("stream2", triple, 25);
    }

    engine.stop();

    assert!(
        results.lock().unwrap().is_empty(),
        "expired support should prevent hotspot emission"
    );
}

#[test]
fn test_static_data_not_visible_in_window_query() {
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

    engine.parse_data("<http://example.org/static1> a <http://example.org/Type> .");

    engine.add_static_ntriples("<http://example.org/static1> a <http://example.org/Type> .");

    let triples = engine.parse_data("<http://example.org/stream1> a <http://example.org/Type> .");
    for triple in triples {
        engine.add(triple, 1);
    }

    engine.stop();

    let all = result_container.lock().unwrap();
    assert_eq!(
        all.len(),
        1,
        "Window must return only stream events, not static data (got {})",
        all.len()
    );
}

/// ISTREAM: sliding window (RANGE=3 STEP=1) across 3 firings
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

    engine.parse_data("<http://test/s0> a <http://test/RType> .");

    for t in engine.parse_data("<http://test/subjectA> a <http://test/RType> .") {
        engine.add(t, 1);
    }

    for t in engine.parse_data("<http://test/subjectB> a <http://test/RType> .") {
        engine.add(t, 2);
    }

    for t in engine.parse_data("<http://test/subjectC> a <http://test/RType> .") {
        engine.add(t, 3);
    }

    for t in engine.parse_data("<http://test/subjectA> a <http://test/RType> .") {
        engine.add(t, 4);
    }

    let results = result_container.lock().unwrap();
    assert_eq!(
        results.len(),
        3,
        "ISTREAM RANGE3/STEP1: expected 3 firings -> 3 consumer calls. Got: {:?}",
        *results
    );

    assert!(
        results[0]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectA")),
        "Firing 1 must emit subjectA, got: {:?}",
        results[0]
    );

    assert!(
        results[1]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectB")),
        "Firing 2 must emit subjectB, got: {:?}",
        results[1]
    );
    assert!(
        !results[1]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectA")),
        "Firing 2 must NOT re-emit subjectA (already seen), got: {:?}",
        results[1]
    );

    assert!(
        results[2]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectC")),
        "Firing 3 must emit subjectC, got: {:?}",
        results[2]
    );
    assert!(
        !results[2]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectA")),
        "Firing 3 must NOT re-emit subjectA, got: {:?}",
        results[2]
    );
    assert!(
        !results[2]
            .iter()
            .any(|(k, v)| k == "s" && v.contains("subjectB")),
        "Firing 3 must NOT re-emit subjectB, got: {:?}",
        results[2]
    );
}

#[test]
fn test_window_evicts_old_data() {
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

    engine.parse_data("a a <http://example.org/Type> .");

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

    let all = result_container.lock().unwrap();
    assert_eq!(
        all.len(),
        3,
        "Each window should return exactly 1 result row (got {}); \
         stale data from previous firings is leaking into the store",
        all.len()
    );
}

/// Regression test: ISTREAM with same subject+predicate and changing object values
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

    engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"0\" .");

    for t in engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"1\" .") {
        engine.add(t, 1);
    }

    for t in engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"2\" .") {
        engine.add(t, 2);
    }

    for t in engine.parse_data("<http://test/reading1> <http://test/hasTemp> \"3\" .") {
        engine.add(t, 3);
    }

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

    for (i, row) in results.iter().enumerate() {
        assert_eq!(
            row.len(),
            2,
            "Firing {}: row should have 2 bindings, got: {:?}",
            i + 1,
            row
        );
    }

    assert!(
        results[0]
            .iter()
            .any(|(k, v)| k == "temp" && v.contains('1')),
        "Firing 1 must emit temp=1, got: {:?}",
        results[0]
    );
    assert!(
        results[1]
            .iter()
            .any(|(k, v)| k == "temp" && v.contains('2')),
        "Firing 2 must emit temp=2, got: {:?}",
        results[1]
    );
    assert!(
        !results[1].iter().any(|(k, v)| k == "temp" && v == "\"1\""),
        "Firing 2 must NOT re-emit temp=1, got: {:?}",
        results[1]
    );
    assert!(
        results[2]
            .iter()
            .any(|(k, v)| k == "temp" && v.contains('3')),
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

/// Forward chaining derives a type that a window query then matches
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

    for t in engine.parse_data("<http://test/sensor1> <http://test/hasValue> \"42\" .") {
        engine.add(t, 1);
    }
    for t in engine.parse_data("<http://test/sensor2> <http://test/hasValue> \"99\" .") {
        engine.add(t, 2);
    }

    engine.stop();

    let results = result_container.lock().unwrap();
    assert!(
        !results.is_empty(),
        "Reasoning: expected at least one consumer call (inferred type should match query), got none"
    );

    let has_sensor1 = results
        .iter()
        .any(|row| row.iter().any(|(k, v)| k == "s" && v.contains("sensor1")));
    assert!(
        has_sensor1,
        "Reasoning: expected sensor1 to appear via inferred type. Got: {:?}",
        *results
    );
}

#[test]
fn rsp_probabilistic_occurrences_keep_stable_distinct_seed_ids() {
    let result_consumer = ResultConsumer {
        function: Arc::new(|_row: Vec<(String, String)>| {}),
    };
    let query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :w ON ?stream [RANGE 2 STEP 1]
        WHERE { WINDOW :w {
            ?s <http://test/result> <http://test/yes> .
        } }
    "#;
    let hybrid_rule = r#"
        RULE :Hybrid PROB(provenance=hybrid, threshold=0.7) :-
        CONSTRUCT { ?s <http://test/result> <http://test/yes> . }
        WHERE { ?s <http://test/input> <http://test/yes> . } .
    "#;
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));
    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_sparql_rules(vec![hybrid_rule.to_string()])
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("hybrid RSP engine should build");

    let first = engine
        .parse_data("<http://test/a> <http://test/input> <http://test/yes> .")
        .remove(0);
    let second = engine
        .parse_data("<http://test/b> <http://test/input> <http://test/yes> .")
        .remove(0);
    let first_id = engine
        .add_probabilistic_to_stream("stream", first.clone(), 0.8, 1)
        .unwrap();
    engine
        .add_probabilistic_to_stream("stream", second, 0.6, 2)
        .unwrap();

    let window_iri = engine.get_window_info()[0].window_iri.clone();
    let latest = engine
        .latest_hybrid_results(&window_iri)
        .expect("window should publish hybrid results");
    assert!(latest.values().any(|result| matches!(
        result,
        shared::hybrid::HybridProbabilityResult::Exact { probability, .. }
            if (*probability - 0.8).abs() < 1e-9
    )));

    let repeated_id = engine
        .add_probabilistic_to_stream("stream", first, 0.8, 3)
        .unwrap();
    assert_ne!(
        first_id, repeated_id,
        "identical later arrivals need distinct identities"
    );
}

fn hybrid_rsp_query() -> &'static str {
    r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :w ON ?stream [RANGE 2 STEP 1]
        WHERE { WINDOW :w {
            ?s <http://test/result> <http://test/yes> .
        } }
    "#
}

fn hybrid_rsp_rule() -> String {
    r#"
        RULE :Hybrid PROB(provenance=hybrid, threshold=0.7) :-
        CONSTRUCT { ?s <http://test/result> <http://test/yes> . }
        WHERE { ?s <http://test/input> <http://test/yes> . } .
    "#
    .to_string()
}

#[test]
fn rsp_deterministic_fact_dominates_probabilistic_copy() {
    let consumer = ResultConsumer {
        function: Arc::new(|_row: Vec<(String, String)>| {}),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));
    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(hybrid_rsp_query())
        .add_sparql_rules(vec![hybrid_rsp_rule()])
        .add_consumer(consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .unwrap();
    let certain = engine
        .parse_data("<http://test/certain> <http://test/input> <http://test/yes> .")
        .remove(0);
    engine.add_to_stream("stream", certain.clone(), 1);
    engine
        .add_probabilistic_to_stream("stream", certain, 0.2, 1)
        .unwrap();
    let trigger = engine
        .parse_data("<http://test/trigger> <http://test/input> <http://test/yes> .")
        .remove(0);
    engine
        .add_probabilistic_to_stream("stream", trigger, 0.6, 2)
        .unwrap();

    let window = engine.get_window_info()[0].window_iri.clone();
    let results = engine.latest_hybrid_results(&window).unwrap();
    assert!(results.values().any(|result| matches!(
        result,
        shared::hybrid::HybridProbabilityResult::Exact { probability, .. }
            if (*probability - 1.0).abs() < 1e-9
    )));
}

#[test]
fn rsp_multithread_probabilistic_ingestion_publishes_hybrid_result() {
    let consumer = ResultConsumer {
        function: Arc::new(|_row: Vec<(String, String)>| {}),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));
    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(hybrid_rsp_query())
        .add_sparql_rules(vec![hybrid_rsp_rule()])
        .add_consumer(consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::MultiThread)
        .build()
        .unwrap();
    let first = engine
        .parse_data("<http://test/multi> <http://test/input> <http://test/yes> .")
        .remove(0);
    let trigger = engine
        .parse_data("<http://test/trigger> <http://test/input> <http://test/yes> .")
        .remove(0);
    engine
        .add_probabilistic_to_stream("stream", first, 0.8, 1)
        .unwrap();
    engine
        .add_probabilistic_to_stream("stream", trigger, 0.6, 2)
        .unwrap();

    let window = engine.get_window_info()[0].window_iri.clone();
    let mut observed = None;
    for _ in 0..100 {
        if let Some(results) = engine.latest_hybrid_results(&window) {
            if !results.is_empty() {
                observed = Some(results);
                break;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    engine.stop();
    let results = observed.expect("multi-thread window should publish within one second");
    assert!(results.values().any(|result| matches!(
        result,
        shared::hybrid::HybridProbabilityResult::Exact { probability, .. }
            if (*probability - 0.8).abs() < 1e-9
    )));
}

#[test]
fn rsp_overlapping_windows_share_one_occurrence_identity() {
    let query = r#"
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :short ON :stream [RANGE 2 STEP 1]
        FROM NAMED WINDOW :long ON :stream [RANGE 3 STEP 1]
        WHERE {
            WINDOW :short { ?short <http://test/result> <http://test/yes> . }
            WINDOW :long { ?long <http://test/result> <http://test/yes> . }
        }
    "#;
    let consumer = ResultConsumer {
        function: Arc::new(|_row: Vec<(String, String)>| {}),
    };
    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));
    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(query)
        .add_sparql_rules(vec![hybrid_rsp_rule()])
        .add_consumer(consumer)
        .add_r2r(r2r)
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .unwrap();
    let first = engine
        .parse_data("<http://test/shared> <http://test/input> <http://test/yes> .")
        .remove(0);
    let trigger = engine
        .parse_data("<http://test/trigger> <http://test/input> <http://test/yes> .")
        .remove(0);
    let seed = engine
        .add_probabilistic_to_stream("stream", first, 0.8, 1)
        .unwrap();
    engine
        .add_probabilistic_to_stream("stream", trigger, 0.6, 2)
        .unwrap();

    for window in engine.get_window_info() {
        let results = engine
            .latest_hybrid_results(&window.window_iri)
            .expect("each overlapping window should publish hybrid results");
        assert!(results.values().any(|result| matches!(
            result,
            shared::hybrid::HybridProbabilityResult::Exact { probability, .. }
                if (*probability - 0.8).abs() < 1e-9
        )));
    }
    let later = engine
        .parse_data("<http://test/shared> <http://test/input> <http://test/yes> .")
        .remove(0);
    let later_seed = engine
        .add_probabilistic_to_stream("stream", later, 0.8, 3)
        .unwrap();
    assert_ne!(seed, later_seed);
}
