/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

extern crate kolibrie;
use kolibrie::rsp_engine::{QueryExecutionMode, RSPBuilder, RSPEngine, ResultConsumer, SimpleR2R};
use shared::triple::Triple;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let result_consumer = ResultConsumer {
        function: Arc::new(Box::new(|bindings: Vec<(String, String)>| {
            println!("Window fired — bindings: {:?}", bindings);
        })),
    };

    // Replaces the old `set_sliding_window(10, 5)` + imperative `auto_policy_evaluation`
    // with an RSP-QL query that declares a 10-tick window sliding every 5 ticks, firing
    // on window close and streaming out via RSTREAM.
    let rsp_query = r#"
        PREFIX ex: <http://example.org/>

        REGISTER RSTREAM <http://example.org/out> AS
        SELECT ?s ?p ?o
        FROM NAMED WINDOW :policyWindow ON :policyStream [RANGE 10 STEP 5]
        WHERE {
            WINDOW :policyWindow {
                ?s ?p ?o .
            }
        }
    "#;

    let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
        .add_rsp_ql_query(rsp_query)
        .add_consumer(result_consumer)
        .add_r2r(Box::new(SimpleR2R::with_execution_mode(
            QueryExecutionMode::Volcano,
        )))
        .build()?;

    for counter in 1..=20 {
        let ntriples = format!(
            "<http://example.org/subject{}> <http://example.org/predicate{}> <http://example.org/object{}> .",
            counter, counter, counter
        );
        for triple in engine.parse_data(&ntriples) {
            // `counter` is the stream's clock. The window is `[RANGE 10 STEP 5]`
            // with no `TICK` clause, so it fires on these timestamps and not on
            // elapsed wall time -- which is why this loop does not sleep. It used
            // to sleep a second per iteration, twenty-one seconds in all, for no
            // effect on the result beyond making the example the slowest thing in
            // `scripts/verify.sh` after one other.
            engine.add_to_stream("policyStream", triple, counter);
        }
    }

    // No sleep after stopping either: `stop` flushes every window and waits for the
    // coordinator, so every result has been delivered by the time it returns.
    engine.stop();
    Ok(())
}
