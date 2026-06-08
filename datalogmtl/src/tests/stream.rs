/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::stream::{StreamShape, StalenessPolicy, ShapeIngester, RdfEvent};
use super::{make_dict, enc, triple};

/// Test 8: Stream shape — single sensor channel.
#[test]
fn test_stream_single_sensor() {
    let dict = make_dict();
    let obs1  = enc(&dict, "obs:1");
    let obs2  = enc(&dict, "obs:2");
    let obs3  = enc(&dict, "obs:3");
    let room42 = enc(&dict, ":room42");
    let has_foi = enc(&dict, "sosa:hasFeatureOfInterest");
    let has_result = enc(&dict, "sosa:hasSimpleResult");
    let val22  = enc(&dict, "22");

    let shape = StreamShape {
        stream_iri:    "http://example.org/tempStream".into(),
        event_pattern: vec![
            (shared::terms::Term::Variable("obs".to_string()),
             shared::terms::Term::Constant(has_foi),
             shared::terms::Term::Variable("room".to_string())),
            (shared::terms::Term::Variable("obs".to_string()),
             shared::terms::Term::Constant(has_result),
             shared::terms::Term::Variable("val".to_string())),
        ],
        channel_key:  vec!["room".into()],
        staleness:    StalenessPolicy { max_gap_ms: 2000 },
    };

    let mut ingester = ShapeIngester::new(vec![shape], dict.clone());

    // Events at t=0, 1000, 2000 — same room, different obs IRIs, same val.
    for (t, obs) in [(0u64, obs1), (1000, obs2), (2000, obs3)] {
        let event = RdfEvent {
            stream_iri: "http://example.org/tempStream".into(),
            timestamp:  t,
            triples: vec![
                triple(obs, has_foi, room42),
                triple(obs, has_result, val22),
            ],
        };
        let inserted = ingester.process_event(&event);
        assert_eq!(inserted.len(), 2,
            "Should insert 2 triples per event at t={}", t);
        for (tr, ts) in &inserted {
            assert_eq!(*ts, t, "Triple should be timestamped at {}", t);
            let _ = tr;
        }
    }

    // Channel {room: room42} should be active after t=2000.
    let active = ingester.active_channels();
    assert_eq!(active.len(), 1, "Should have exactly 1 active channel");
    let channel_state = active.values().next().unwrap();
    assert_eq!(channel_state.last_event_time, 2000);

    // At t=5000 (gap > 2000ms after t=2000), channel should be expired.
    let expired = ingester.expired_channels(5000);
    assert_eq!(expired.len(), 1, "Channel should be expired at t=5000");

    ingester.evict_expired(5000);
    assert_eq!(ingester.active_channels().len(), 0, "No active channels after eviction");
}

/// Test 9: Stream shape — composite key, two sensors in same room.
#[test]
fn test_stream_composite_key() {
    let dict = make_dict();
    let obs_a1 = enc(&dict, "obs:a1");
    let obs_b1 = enc(&dict, "obs:b1");
    let obs_a2 = enc(&dict, "obs:a2");
    let room42  = enc(&dict, ":room42");
    let sensor_a = enc(&dict, ":sensorA");
    let sensor_b = enc(&dict, ":sensorB");
    let has_foi    = enc(&dict, "sosa:hasFeatureOfInterest");
    let has_result = enc(&dict, "sosa:hasSimpleResult");
    let has_sensor = enc(&dict, "sosa:madeBySensor");
    let val22 = enc(&dict, "22");
    let val23 = enc(&dict, "23");

    let shape = StreamShape {
        stream_iri:    "http://example.org/tempStream".into(),
        event_pattern: vec![
            (shared::terms::Term::Variable("obs".to_string()),
             shared::terms::Term::Constant(has_foi),
             shared::terms::Term::Variable("room".to_string())),
            (shared::terms::Term::Variable("obs".to_string()),
             shared::terms::Term::Constant(has_sensor),
             shared::terms::Term::Variable("sensor".to_string())),
            (shared::terms::Term::Variable("obs".to_string()),
             shared::terms::Term::Constant(has_result),
             shared::terms::Term::Variable("val".to_string())),
        ],
        channel_key: vec!["room".into(), "sensor".into()],
        staleness:   StalenessPolicy { max_gap_ms: 5000 },
    };

    let mut ingester = ShapeIngester::new(vec![shape], dict.clone());

    // t=0: sensorA in room42, val=22
    ingester.process_event(&RdfEvent {
        stream_iri: "http://example.org/tempStream".into(),
        timestamp: 0,
        triples: vec![
            triple(obs_a1, has_foi, room42),
            triple(obs_a1, has_sensor, sensor_a),
            triple(obs_a1, has_result, val22),
        ],
    });

    // t=0: sensorB in room42, val=23
    ingester.process_event(&RdfEvent {
        stream_iri: "http://example.org/tempStream".into(),
        timestamp: 0,
        triples: vec![
            triple(obs_b1, has_foi, room42),
            triple(obs_b1, has_sensor, sensor_b),
            triple(obs_b1, has_result, val23),
        ],
    });

    // Two distinct channels should exist.
    assert_eq!(ingester.active_channels().len(), 2,
        "Should have 2 distinct channels for sensorA and sensorB");

    // t=1000: sensorA update
    ingester.process_event(&RdfEvent {
        stream_iri: "http://example.org/tempStream".into(),
        timestamp: 1000,
        triples: vec![
            triple(obs_a2, has_foi, room42),
            triple(obs_a2, has_sensor, sensor_a),
            triple(obs_a2, has_result, val22),
        ],
    });

    // Still 2 channels.
    assert_eq!(ingester.active_channels().len(), 2,
        "sensorA update should not affect sensorB channel count");

    // Find sensorB channel — its last_event_time should still be 0.
    let channels = ingester.active_channels();
    let sensor_b_state = channels.iter().find(|(_, state)| {
        state.active_triples.iter().any(|t| t.object == val23)
    });
    assert!(sensor_b_state.is_some(), "sensorB channel should still exist");
    assert_eq!(sensor_b_state.unwrap().1.last_event_time, 0,
        "sensorB last_event_time should still be 0");
}
