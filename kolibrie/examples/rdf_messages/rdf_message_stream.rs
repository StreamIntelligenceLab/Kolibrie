/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Feeds an RDF Message Log into the RSP engine as a stream
//!
//! The query registers the stream source and states that every message carries
//! its own timestamp under sosa:resultTime, so the windows advance on the time
//! recorded in the data rather than on arrival time

use kolibrie::rdf_messages::{parse_message_log, replay_log, MessageBaseFormat};
use kolibrie::rsp::simple_r2r::SimpleR2R;
use kolibrie::rsp_engine::{
    OperationMode, QueryExecutionMode, RSPBuilder, ResultConsumer, TimestampAssignment,
};
use std::sync::{Arc, Mutex};

// Each MESSAGE is one observation, a minute apart from the previous one
const MESSAGE_LOG: &str = r#"VERSION "1.2-messages"
<http://example.org/obs1> <http://www.w3.org/ns/sosa/resultTime> "2026-01-01T00:00:00Z" .
<http://example.org/obs1> <http://example.org/value> "18" .
MESSAGE
<http://example.org/obs2> <http://www.w3.org/ns/sosa/resultTime> "2026-01-01T00:01:00Z" .
<http://example.org/obs2> <http://example.org/value> "21" .
MESSAGE
<http://example.org/obs3> <http://www.w3.org/ns/sosa/resultTime> "2026-01-01T00:02:00Z" .
<http://example.org/obs3> <http://example.org/value> "25" .
"#;

const QUERY: &str = r#"
PREFIX sosa: <http://www.w3.org/ns/sosa/>

REGISTER STREAM :sensorStream FROM <file:///data/observations.nt> WITH TIMESTAMP sosa:resultTime

REGISTER RSTREAM <http://out/readings> AS
SELECT ?value
FROM NAMED WINDOW :w ON :sensorStream [RANGE 60 STEP 60]
WHERE {
    WINDOW :w { ?obs <http://example.org/value> ?value . }
}
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let results: Arc<Mutex<Vec<Vec<(String, String)>>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&results);

    let mut engine = RSPBuilder::new()
        .add_rsp_ql_query(QUERY)
        .set_operation_mode(OperationMode::SingleThread)
        .add_consumer(ResultConsumer {
            function: Arc::new(move |bindings: Vec<(String, String)>| {
                sink.lock().unwrap().push(bindings);
            }),
        })
        .add_r2r(Box::new(SimpleR2R::with_execution_mode(
            QueryExecutionMode::Volcano,
        )))
        .build()?;

    // The registration tells us where the data comes from and how it is timed
    for source in engine.stream_sources() {
        println!("stream    {}", source.stream_iri);
        println!("source    {}", source.source);
        match &source.timestamp {
            TimestampAssignment::SysTime => println!("timestamp arrival time"),
            TimestampAssignment::Property(iri) => println!("timestamp {}", iri),
        }
    }

    let policy = engine
        .timestamp_policy_for(":sensorStream")
        .cloned()
        .unwrap_or(TimestampAssignment::SysTime);

    let log = parse_message_log(MESSAGE_LOG, MessageBaseFormat::NTriples)?;
    println!("\nreplaying {} messages", log.len());

    let stats = replay_log(&mut engine, ":sensorStream", &log, &policy)?;
    engine.stop();

    println!(
        "pushed    {} triples from {} messages ({} fell back to arrival time)",
        stats.triples, stats.messages, stats.systime_fallbacks
    );

    let collected = results.lock().unwrap();
    println!("\nwindow results: {}", collected.len());
    for bindings in collected.iter() {
        println!("  {:?}", bindings);
    }

    Ok(())
}
