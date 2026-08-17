/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::*;
use crate::sparql_database::SparqlDatabase;

fn statement_counts(log: &RdfMessageLog) -> Vec<usize> {
    log.iter().map(|m| m.statements().len()).collect()
}

#[test]
fn ntriples_two_messages() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "<http://example.org/message_1> <http://example.org/hasValue> \"Hello, world!\" .\n",
        "<http://example.org/message_1> <http://example.org/hasNumber> \"1\" .\n",
        "MESSAGE\n",
        "<http://example.org/message_2> <http://example.org/hasValue> \"Goodbye, world!\" .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();
    assert_eq!(log.version, Some(VersionLabel::V1_2Messages));
    assert_eq!(statement_counts(&log), vec![2, 1]);
}

#[test]
fn ntriples_leading_empty_message() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "MESSAGE\n",
        "<http://example.org/message_1> <http://example.org/hasValue> \"Hello, world!\" .\n",
        "<http://example.org/message_1> <http://example.org/hasNumber> \"2\" .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();
    assert_eq!(statement_counts(&log), vec![0, 2]);
    assert!(log.messages[0].is_empty());
}

#[test]
fn ntriples_trailing_delimiter_is_ignored() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "<http://example.org/message_1> <http://example.org/hasValue> \"Hello, world!\" .\n",
        "MESSAGE\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();
    assert_eq!(statement_counts(&log), vec![1]);
}

#[test]
fn ntriples_consecutive_trailing_delimiters() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "<http://example.org/message_1> <http://example.org/hasValue> \"Hello, world!\" .\n",
        "MESSAGE\n",
        "MESSAGE\n",
        "MESSAGE # last delimiter ignored\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();
    assert_eq!(statement_counts(&log), vec![1, 0, 0]);
}

#[test]
fn ntriples_quads_decode() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "<http://example.org/s> <http://example.org/p> \"a\" .\n",
        "MESSAGE\n",
        "<http://example.org/s> <http://example.org/p> \"b\" .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();
    let m0 = log.messages[0].to_quads().unwrap();
    assert_eq!(m0.len(), 1);
    assert_eq!(m0[0].subject, "http://example.org/s");
    assert_eq!(m0[0].predicate, "http://example.org/p");
    assert_eq!(m0[0].object, "a");
    assert!(m0[0].graph.is_none());
}

#[test]
fn nquads_named_graph_quads() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "<http://example.org/s> <http://example.org/p> <http://example.org/o> <http://example.org/g> .\n",
        "MESSAGE\n",
        "<http://example.org/s2> <http://example.org/p2> <http://example.org/o2> .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::NQuads).unwrap();
    assert_eq!(log.len(), 2);
    let m0 = log.messages[0].to_quads().unwrap();
    assert_eq!(m0.len(), 1);
    assert_eq!(m0[0].graph.as_deref(), Some("http://example.org/g"));
    let m1 = log.messages[1].to_quads().unwrap();
    assert_eq!(m1[0].graph, None);
}

#[test]
fn turtle_old_style_two_messages() {
    let input = concat!(
        "@version \"1.2-messages\" .\n",
        "@prefix ex: <http://example.org/> .\n",
        "ex:person ex:says \"Hello, world!\" .\n",
        "@message .\n",
        "ex:person ex:says \"Goodbye, world!\" .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::Turtle).unwrap();
    assert_eq!(log.version, Some(VersionLabel::V1_2Messages));
    assert_eq!(statement_counts(&log), vec![1, 1]);

    let m1 = log.messages[1].to_quads().unwrap();
    assert_eq!(m1.len(), 1);
    assert_eq!(m1[0].subject, "http://example.org/person");
    assert_eq!(m1[0].predicate, "http://example.org/says");
    assert_eq!(m1[0].object, "Goodbye, world!");
}

#[test]
fn turtle_sparql_style_version_and_message() {
    let input = concat!(
        "VERSION \"1.2-basic-messages\"\n",
        "PREFIX ex: <http://example.org/>\n",
        "ex:spiderman ex:enemyOf ex:green-goblin .\n",
        "MESSAGE\n",
        "ex:spiderman ex:enemyOf ex:doc-ock .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::Turtle).unwrap();
    assert_eq!(log.version, Some(VersionLabel::V1_2BasicMessages));
    assert_eq!(statement_counts(&log), vec![1, 1]);
}

#[test]
fn turtle_repeated_prefix_overrides_per_message() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "PREFIX ex: <http://example.org/>\n",
        "ex:person ex:says \"Hello, world!\" .\n",
        "MESSAGE\n",
        "PREFIX ex: <http://different-iri.org/>\n",
        "ex:person ex:says \"Goodbye, world!\" .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::Turtle).unwrap();
    assert_eq!(log.len(), 2);

    let m0 = log.messages[0].to_quads().unwrap();
    assert_eq!(m0[0].subject, "http://example.org/person");

    let m1 = log.messages[1].to_quads().unwrap();
    assert_eq!(m1[0].subject, "http://different-iri.org/person");
}

#[test]
fn trig_two_messages_with_graph_blocks() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "PREFIX ex: <http://example.org/>\n",
        "ex:message_1 {\n",
        "  ex:person ex:says \"Hello, world!\" .\n",
        "}\n",
        "ex:message_1 ex:hasNumber 1 .\n",
        "MESSAGE\n",
        "ex:message_2 {\n",
        "  ex:person ex:says \"Goodbye, world!\" .\n",
        "}\n",
        "ex:message_2 ex:hasNumber 2 .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::TriG).unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(statement_counts(&log), vec![2, 2]);

    let m0 = log.messages[0].to_quads().unwrap();
    let says = m0
        .iter()
        .find(|q| q.predicate == "http://example.org/says")
        .expect("says triple present");
    assert_eq!(says.graph.as_deref(), Some("http://example.org/message_1"));
    let number = m0
        .iter()
        .find(|q| q.predicate == "http://example.org/hasNumber")
        .expect("hasNumber triple present");
    assert_eq!(number.graph, None);
}

#[test]
fn missing_version_still_splits_on_message() {
    let input = concat!(
        "<http://example.org/s> <http://example.org/p> \"a\" .\n",
        "MESSAGE\n",
        "<http://example.org/s> <http://example.org/p> \"b\" .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();
    assert_eq!(log.version, None);
    assert_eq!(log.len(), 2);
}

#[test]
fn non_message_version_with_delimiter_is_rejected() {
    let input = concat!(
        "VERSION \"1.2\"\n",
        "<http://example.org/s> <http://example.org/p> \"a\" .\n",
        "MESSAGE\n",
    );
    let err = parse_message_log(input, MessageBaseFormat::NTriples).unwrap_err();
    assert!(matches!(err, RdfMessageError::VersionMismatch(_)));
}

#[test]
fn unknown_version_label_is_rejected() {
    let input = "VERSION \"9.9-turbo\"\n<http://example.org/s> <http://example.org/p> \"a\" .\n";
    let err = parse_message_log(input, MessageBaseFormat::NTriples).unwrap_err();
    assert!(matches!(err, RdfMessageError::UnknownVersionLabel(_)));
}

#[test]
fn empty_document_yields_no_messages() {
    let log = parse_message_log("   \n # just a comment \n", MessageBaseFormat::NTriples).unwrap();
    assert_eq!(log.len(), 0);
    assert!(log.is_empty());
}

#[test]
fn lone_delimiter_yields_single_empty_message() {
    let log = parse_message_log("MESSAGE\n", MessageBaseFormat::NTriples).unwrap();
    assert_eq!(statement_counts(&log), vec![0]);
}

#[test]
fn load_into_merges_all_messages_into_store() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "<http://example.org/s1> <http://example.org/p> \"first\" .\n",
        "MESSAGE\n",
        "<http://example.org/s2> <http://example.org/p> \"second\" .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();

    let mut db = SparqlDatabase::new();
    log.load_into(&mut db).unwrap();

    let quads = db.dataset_index.all_quads();
    assert_eq!(quads.len(), 2);
}

#[test]
fn document_reconstruction_is_self_contained() {
    let input = concat!(
        "VERSION \"1.2-messages\"\n",
        "PREFIX ex: <http://example.org/>\n",
        "ex:a ex:b ex:c .\n",
        "MESSAGE\n",
        "ex:d ex:e ex:f .\n",
    );
    let log = parse_message_log(input, MessageBaseFormat::Turtle).unwrap();
    let doc = log.messages[1].to_document();
    assert!(doc.contains("@prefix ex: <http://example.org/> ."));
    assert!(doc.contains("ex:d ex:e ex:f ."));
}
