/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Coverage for the REGISTER STREAM clause and the RDF Message stream bridge

#[cfg(test)]
mod tests {
    use kolibrie::parser::parse_combined_query;
    use kolibrie::rdf_messages::{
        parse_message_log, replay_file, replay_log, MessageBaseFormat, ReplayStats,
    };
    use kolibrie::rsp::simple_r2r::SimpleR2R;
    use kolibrie::rsp_engine::{
        OperationMode, QueryExecutionMode, RSPBuilder, ResultConsumer, TimestampAssignment,
    };
    use shared::query::TimestampPolicy;
    use shared::triple::Triple;
    use std::sync::{Arc, Mutex};

    const SOSA_RESULT_TIME: &str = "http://www.w3.org/ns/sosa/resultTime";

    // ── Parser ───────────────────────────────────────────────────────────────

    #[test]
    fn parses_systime_policy() {
        let query = concat!(
            "REGISTER STREAM :sensorStream FROM <file:///data/log.nt> WITH TIMESTAMP SYSTIME\n",
            "REGISTER RSTREAM <http://out/s> AS\n",
            "SELECT ?v\n",
            "FROM NAMED WINDOW :w ON :sensorStream [RANGE 10 STEP 10]\n",
            "WHERE { WINDOW :w { ?o <http://example.org/v> ?v . } }",
        );
        let (_, parsed) = parse_combined_query(query).unwrap();
        assert_eq!(parsed.stream_registrations.len(), 1);
        let registration = &parsed.stream_registrations[0];
        assert_eq!(registration.stream_iri, ":sensorStream");
        assert_eq!(registration.source, "file:///data/log.nt");
        assert_eq!(registration.timestamp, TimestampPolicy::SysTime);
        assert!(parsed.register_clause.is_some());
    }

    #[test]
    fn parses_prefixed_property_policy() {
        let query = concat!(
            "PREFIX sosa: <http://www.w3.org/ns/sosa/>\n",
            "REGISTER STREAM :sensorStream FROM <file:///data/log.nt> WITH TIMESTAMP sosa:resultTime\n",
            "REGISTER RSTREAM <http://out/s> AS\n",
            "SELECT ?v\n",
            "FROM NAMED WINDOW :w ON :sensorStream [RANGE 10 STEP 10]\n",
            "WHERE { WINDOW :w { ?o <http://example.org/v> ?v . } }",
        );
        let (_, parsed) = parse_combined_query(query).unwrap();
        assert_eq!(
            parsed.stream_registrations[0].timestamp,
            TimestampPolicy::Property("sosa:resultTime")
        );
    }

    #[test]
    fn parses_full_iri_property_policy() {
        let query = concat!(
            "REGISTER STREAM :s FROM <file:///log.nt> WITH TIMESTAMP <http://www.w3.org/ns/sosa/resultTime>\n",
            "REGISTER RSTREAM <http://out/s> AS\n",
            "SELECT ?v\n",
            "FROM NAMED WINDOW :w ON :s [RANGE 10 STEP 10]\n",
            "WHERE { WINDOW :w { ?o <http://example.org/v> ?v . } }",
        );
        let (_, parsed) = parse_combined_query(query).unwrap();
        assert_eq!(
            parsed.stream_registrations[0].timestamp,
            TimestampPolicy::Property(SOSA_RESULT_TIME)
        );
    }

    #[test]
    fn timestamp_clause_is_optional() {
        let query = concat!(
            "REGISTER STREAM :s FROM <file:///log.nt>\n",
            "REGISTER RSTREAM <http://out/s> AS\n",
            "SELECT ?v\n",
            "FROM NAMED WINDOW :w ON :s [RANGE 10 STEP 10]\n",
            "WHERE { WINDOW :w { ?o <http://example.org/v> ?v . } }",
        );
        let (_, parsed) = parse_combined_query(query).unwrap();
        assert_eq!(
            parsed.stream_registrations[0].timestamp,
            TimestampPolicy::SysTime
        );
    }

    #[test]
    fn parses_multiple_registrations() {
        let query = concat!(
            "PREFIX sosa: <http://www.w3.org/ns/sosa/>\n",
            "REGISTER STREAM :a FROM <file:///a.nt> WITH TIMESTAMP sosa:resultTime\n",
            "REGISTER STREAM :b FROM <file:///b.nt> WITH TIMESTAMP SYSTIME\n",
            "REGISTER RSTREAM <http://out/s> AS\n",
            "SELECT ?v\n",
            "FROM NAMED WINDOW :w ON :a [RANGE 10 STEP 10]\n",
            "WHERE { WINDOW :w { ?o <http://example.org/v> ?v . } }",
        );
        let (_, parsed) = parse_combined_query(query).unwrap();
        assert_eq!(parsed.stream_registrations.len(), 2);
        assert_eq!(parsed.stream_registrations[0].stream_iri, ":a");
        assert_eq!(parsed.stream_registrations[1].stream_iri, ":b");
        assert_eq!(
            parsed.stream_registrations[1].timestamp,
            TimestampPolicy::SysTime
        );
    }

    #[test]
    fn registration_parses_without_a_query() {
        let query = "REGISTER STREAM :s FROM <file:///log.nt> WITH TIMESTAMP SYSTIME\n";
        let (_, parsed) = parse_combined_query(query).unwrap();
        assert_eq!(parsed.stream_registrations.len(), 1);
        assert!(parsed.register_clause.is_none());
    }

    #[test]
    fn existing_queries_report_no_registrations() {
        let query = concat!(
            "REGISTER RSTREAM <http://out/s> AS\n",
            "SELECT ?v\n",
            "FROM NAMED WINDOW :w ON :sensorStream [RANGE 10 STEP 10]\n",
            "WHERE { WINDOW :w { ?o <http://example.org/v> ?v . } }",
        );
        let (_, parsed) = parse_combined_query(query).unwrap();
        assert!(parsed.stream_registrations.is_empty());
        assert!(parsed.register_clause.is_some());
    }

    #[test]
    fn malformed_registration_is_rejected() {
        // FROM is required, so the clause cannot be completed
        let query = "REGISTER STREAM :s WITH TIMESTAMP SYSTIME\n";
        assert!(parse_combined_query(query).is_err());
    }

    // ── Builder wiring ───────────────────────────────────────────────────────

    fn build_engine(
        query: &str,
    ) -> kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>> {
        let consumer = ResultConsumer {
            function: Arc::new(|_: Vec<(String, String)>| {}),
        };
        RSPBuilder::new()
            .add_rsp_ql_query(query)
            .set_operation_mode(OperationMode::SingleThread)
            .add_consumer(consumer)
            .add_r2r(Box::new(SimpleR2R::with_execution_mode(
                QueryExecutionMode::Volcano,
            )))
            .build()
            .expect("engine builds")
    }

    fn sensor_query() -> &'static str {
        concat!(
            "PREFIX sosa: <http://www.w3.org/ns/sosa/>\n",
            "REGISTER STREAM :sensorStream FROM <file:///data/log.nt> WITH TIMESTAMP sosa:resultTime\n",
            "REGISTER RSTREAM <http://out/s> AS\n",
            "SELECT ?v\n",
            "FROM NAMED WINDOW :w ON :sensorStream [RANGE 60 STEP 60]\n",
            "WHERE { WINDOW :w { ?o <http://example.org/value> ?v . } }",
        )
    }

    #[test]
    fn engine_exposes_resolved_stream_source() {
        let engine = build_engine(sensor_query());
        let sources = engine.stream_sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source, "file:///data/log.nt");
        // The prefixed property is expanded against the query prologue
        assert_eq!(
            sources[0].timestamp,
            TimestampAssignment::Property(SOSA_RESULT_TIME.to_string())
        );
    }

    #[test]
    fn timestamp_policy_lookup_ignores_iri_notation() {
        let engine = build_engine(sensor_query());
        let expected = TimestampAssignment::Property(SOSA_RESULT_TIME.to_string());
        assert_eq!(engine.timestamp_policy_for(":sensorStream"), Some(&expected));
        assert_eq!(engine.timestamp_policy_for("sensorStream"), Some(&expected));
        assert_eq!(engine.timestamp_policy_for("other"), None);
    }

    // ── Replay ───────────────────────────────────────────────────────────────

    fn message_log() -> String {
        // Three observations one minute apart, so each lands in its own window
        concat!(
            "VERSION \"1.2-messages\"\n",
            "<http://example.org/o1> <http://www.w3.org/ns/sosa/resultTime> \"2026-01-01T00:00:00Z\" .\n",
            "<http://example.org/o1> <http://example.org/value> \"1\" .\n",
            "MESSAGE\n",
            "<http://example.org/o2> <http://www.w3.org/ns/sosa/resultTime> \"2026-01-01T00:01:00Z\" .\n",
            "<http://example.org/o2> <http://example.org/value> \"2\" .\n",
            "MESSAGE\n",
            "<http://example.org/o3> <http://www.w3.org/ns/sosa/resultTime> \"2026-01-01T00:02:00Z\" .\n",
            "<http://example.org/o3> <http://example.org/value> \"3\" .\n",
        )
        .to_string()
    }

    #[test]
    fn replay_pushes_every_message_using_extracted_time() {
        let mut engine = build_engine(sensor_query());
        let log = parse_message_log(&message_log(), MessageBaseFormat::NTriples).unwrap();
        let policy = TimestampAssignment::Property(SOSA_RESULT_TIME.to_string());

        let stats = replay_log(&mut engine, ":sensorStream", &log, &policy).unwrap();

        assert_eq!(
            stats,
            ReplayStats {
                messages: 3,
                triples: 6,
                systime_fallbacks: 0,
            }
        );
    }

    #[test]
    fn replay_reports_systime_fallback_for_untimed_messages() {
        let input = concat!(
            "VERSION \"1.2-messages\"\n",
            "<http://example.org/o1> <http://www.w3.org/ns/sosa/resultTime> \"2026-01-01T00:00:00Z\" .\n",
            "MESSAGE\n",
            "<http://example.org/o2> <http://example.org/value> \"no timestamp here\" .\n",
        );
        let mut engine = build_engine(sensor_query());
        let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();
        let policy = TimestampAssignment::Property(SOSA_RESULT_TIME.to_string());

        let stats = replay_log(&mut engine, ":sensorStream", &log, &policy).unwrap();

        // Both messages are ingested, the second one on arrival time
        assert_eq!(stats.messages, 2);
        assert_eq!(stats.triples, 2);
        assert_eq!(stats.systime_fallbacks, 1);
    }

    #[test]
    fn extracted_time_drives_window_firing() {
        let fired: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&fired);
        let consumer = ResultConsumer {
            function: Arc::new(move |results: Vec<(String, String)>| {
                sink.lock().unwrap().push(results.len());
            }),
        };

        let mut engine = RSPBuilder::new()
            .add_rsp_ql_query(sensor_query())
            .set_operation_mode(OperationMode::SingleThread)
            .add_consumer(consumer)
            .add_r2r(Box::new(SimpleR2R::with_execution_mode(
                QueryExecutionMode::Volcano,
            )))
            .build()
            .expect("engine builds");

        let log = parse_message_log(&message_log(), MessageBaseFormat::NTriples).unwrap();
        let policy = TimestampAssignment::Property(SOSA_RESULT_TIME.to_string());
        replay_log(&mut engine, ":sensorStream", &log, &policy).unwrap();
        engine.stop();

        // Messages a minute apart cross the 60 second window boundaries, so the
        // windows advanced on message time rather than on arrival time
        assert!(
            !fired.lock().unwrap().is_empty(),
            "windows should fire when message timestamps advance"
        );
    }

    #[test]
    fn replay_file_reads_a_log_from_disk() {
        let mut path = std::env::temp_dir();
        path.push(format!("kolibrie_replay_{}.nt", std::process::id()));
        std::fs::write(&path, message_log()).unwrap();

        let mut engine = build_engine(sensor_query());
        let policy = TimestampAssignment::Property(SOSA_RESULT_TIME.to_string());
        let stats = replay_file(&mut engine, ":sensorStream", &path, &policy).unwrap();

        std::fs::remove_file(&path).ok();

        assert_eq!(stats.messages, 3);
        assert_eq!(stats.triples, 6);
    }
}
