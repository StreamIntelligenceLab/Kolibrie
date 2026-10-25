/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 */

use kolibrie::rule_execution::process_rule_definition;
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
        .dictionary_mut()
        .encode("http://www.w3.org/ns/prob#status");
    let value = database
        .dictionary_mut()
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
    assert!(
        error.to_string().contains("recursion"),
        "unexpected error: {error}"
    );
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
        .dictionary_mut()
        .encode("http://www.w3.org/ns/prob#effectiveThreshold");
    let policy = database
        .dictionary_mut()
        .encode("http://www.w3.org/ns/prob#thresholdPolicy");
    let threshold_object = database
        .query_default_triples(None, Some(threshold), None)
        .first()
        .and_then(|triple| {
            database
                .dictionary()
                .decode(triple.object)
                .map(str::to_string)
        })
        .unwrap();
    let policy_object = database
        .query_default_triples(None, Some(policy), None)
        .first()
        .and_then(|triple| {
            database
                .dictionary()
                .decode(triple.object)
                .map(str::to_string)
        })
        .unwrap();
    assert!(threshold_object.contains("0.25"));
    assert!(policy_object.contains("auto:cost"));
}

/// A plain rule — no window clause, no `PROB` annotation — derives from the store's
/// facts.
///
/// This exists because nothing covered it. Every other `process_rule_definition`
/// test here uses the annotated path, and the `ml` ones assert on predictions rather
/// than on derivation, so the plain path could have stopped seeing the store's facts
/// entirely and the whole of `verify.sh` would still have passed: 646 tests green and
/// all 51 examples running, because the examples print results rather than assert on
/// them and the smoke run only checks exit status.
///
/// Verified to fail when the base facts are withheld, which is the only reason to
/// trust it.
#[test]
fn plain_rule_derives_from_the_stores_facts() {
    let mut database = SparqlDatabase::new();
    database.add_triple_parts(
        "http://example.org/alice",
        "http://example.org/parent",
        "http://example.org/bob",
    );
    let rule = r#"
        RULE :Ancestor :-
        CONSTRUCT { ?x <http://example.org/ancestor> ?y . }
        WHERE { ?x <http://example.org/parent> ?y . } .
    "#;
    let (_rule, inferred) =
        process_rule_definition(rule, &mut database).expect("plain rule should execute");
    assert_eq!(
        inferred.len(),
        1,
        "one parent fact should derive one ancestor fact"
    );

    let ancestor = database
        .dictionary_mut()
        .encode("http://example.org/ancestor");
    let derived = database.query_default_triples(None, Some(ancestor), None);
    assert_eq!(derived.len(), 1, "the derived fact should reach the store");
}
