/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 */

use kolibrie::parser::process_rule_definition;
use kolibrie::sparql_database::SparqlDatabase;

#[test]
fn hybrid_rule_emits_typed_status_annotations() {
    let mut database = SparqlDatabase::new();
    database.add_tagged_triple(
        "http://example.org/a",
        "http://example.org/input",
        "http://example.org/yes",
        0.8,
    );
    let rule = r#"
        RULE :Hybrid PROB(provenance=hybrid, threshold=0.7) :-
        CONSTRUCT { ?x <http://example.org/result> <http://example.org/yes> . }
        WHERE { ?x <http://example.org/input> <http://example.org/yes> . } .
    "#;
    let (_rule, inferred) =
        process_rule_definition(rule, &mut database).expect("hybrid rule should execute");
    assert_eq!(inferred.len(), 1);

    let status = database
        .dictionary
        .write()
        .unwrap()
        .encode("http://www.w3.org/ns/prob#status");
    let value = database
        .dictionary
        .write()
        .unwrap()
        .encode("http://www.w3.org/ns/prob#value");
    assert!(!database
        .query_default_triples(None, Some(status), None)
        .is_empty());
    assert!(!database
        .query_default_triples(None, Some(value), None)
        .is_empty());
}

#[test]
fn hybrid_rule_rejects_recursive_dependency() {
    let mut database = SparqlDatabase::new();
    database.add_tagged_triple("a", "ancestor", "b", 0.8);
    let rule = r#"
        RULE :Recursive PROB(provenance=hybrid, threshold=0.7) :-
        CONSTRUCT { ?x <ancestor> ?z . }
        WHERE { ?x <ancestor> ?y . ?y <ancestor> ?z . } .
    "#;
    let error = process_rule_definition(rule, &mut database)
        .expect_err("recursive hybrid rules must be rejected");
    assert!(error.contains("recursion"), "unexpected error: {error}");
}

#[test]
fn cost_ratio_threshold_is_recorded_in_rdf_metadata() {
    let mut database = SparqlDatabase::new();
    database.add_tagged_triple(
        "http://example.org/a",
        "http://example.org/input",
        "http://example.org/yes",
        0.8,
    );
    let rule = r#"
        RULE :Hybrid PROB(
            provenance=hybrid,
            threshold=auto:cost(fp=1,fn=3)
        ) :-
        CONSTRUCT { ?x <http://example.org/result> <http://example.org/yes> . }
        WHERE { ?x <http://example.org/input> <http://example.org/yes> . } .
    "#;
    process_rule_definition(rule, &mut database).expect("cost policy should execute");
    let threshold = database
        .dictionary
        .write()
        .unwrap()
        .encode("http://www.w3.org/ns/prob#effectiveThreshold");
    let policy = database
        .dictionary
        .write()
        .unwrap()
        .encode("http://www.w3.org/ns/prob#thresholdPolicy");
    let threshold_object = database
        .query_default_triples(None, Some(threshold), None)
        .first()
        .and_then(|triple| {
            database
                .dictionary
                .read()
                .unwrap()
                .decode(triple.object)
                .map(str::to_string)
        })
        .unwrap();
    let policy_object = database
        .query_default_triples(None, Some(policy), None)
        .first()
        .and_then(|triple| {
            database
                .dictionary
                .read()
                .unwrap()
                .decode(triple.object)
                .map(str::to_string)
        })
        .unwrap();
    assert!(threshold_object.contains("0.25"));
    assert!(policy_object.contains("auto:cost"));
}
