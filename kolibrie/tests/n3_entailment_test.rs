/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! SPARQL under N3 entailment: forward rules materialised into the store, then queried

use kolibrie::prelude::*;

const DATA: &str = "@prefix ex: <http://example.org/> .\n\
                    ex:alice a ex:Student ; ex:age 20 ; ex:supervisor ex:bob .\n\
                    ex:bob a ex:Professor ; ex:age 47 .\n";

const PREFIX: &str = "@prefix ex: <http://example.org/> .\n\
                      @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
                      @prefix math: <http://www.w3.org/2000/10/swap/math#> .\n";

fn loaded() -> KolibrieDB {
    let mut database = KolibrieDB::new();
    database
        .try_parse_turtle(DATA)
        .expect("the data should parse");
    database
}

fn select(database: &mut KolibrieDB, query: &str) -> Vec<Vec<String>> {
    let mut rows = execute(
        &format!("PREFIX ex: <http://example.org/>\n{query}"),
        database,
    )
    .expect("the query should run")
    .into_rows();
    rows.sort();
    rows
}

fn rows(values: &[&[&str]]) -> Vec<Vec<String>> {
    values
        .iter()
        .map(|row| row.iter().map(|value| value.to_string()).collect())
        .collect()
}

#[test]
fn a_query_sees_what_rules_and_the_rule_document_add() {
    let mut database = loaded();
    let added = database
        .materialize_n3(&format!(
            "{PREFIX}ex:Student rdfs:subClassOf ex:Person .\n\
             ex:Professor rdfs:subClassOf ex:Person .\n\
             {{ ?x a ?c . ?c rdfs:subClassOf ?d }} => {{ ?x a ?d }} .\n"
        ))
        .expect("the rules should run");
    assert_eq!(added, 4, "two subclass facts and two derived types");
    assert_eq!(
        select(&mut database, "SELECT ?x WHERE { ?x a ex:Person }"),
        rows(&[&["http://example.org/alice"], &["http://example.org/bob"]])
    );
}

#[test]
fn a_computed_value_is_a_typed_literal_filters_can_compare() {
    let mut database = loaded();
    database
        .materialize_n3(&format!(
            "{PREFIX}{{ ?x ex:age ?a . (?a 1) math:sum ?b }} => {{ ?x ex:nextAge ?b }} .\n"
        ))
        .expect("the rule should run");
    assert_eq!(
        select(
            &mut database,
            "SELECT ?x ?n WHERE { ?x ex:nextAge ?n FILTER(?n > 30) }"
        ),
        rows(&[&[
            "http://example.org/bob",
            "\"48\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        ]])
    );
}

#[test]
fn a_rule_can_join_derived_facts_with_stored_ones() {
    let mut database = loaded();
    database
        .materialize_n3(&format!(
            "{PREFIX}{{ ?s ex:supervisor ?p }} => {{ ?p ex:supervises ?s }} .\n\
             {{ ?p ex:supervises ?s . ?p ex:age ?a . ?a math:greaterThan 40 }} => {{ ?s ex:hasSeniorSupervisor ?p }} .\n"
        ))
        .expect("the rules should run");
    assert_eq!(
        select(
            &mut database,
            "SELECT ?s WHERE { ?s ex:hasSeniorSupervisor ex:bob }"
        ),
        rows(&[&["http://example.org/alice"]])
    );
}

#[test]
fn blank_nodes_from_separate_runs_stay_distinct() {
    let mut database = loaded();
    database
        .materialize_n3(&format!(
            "{PREFIX}{{ ?x a ex:Professor }} => {{ ?x ex:office [ ex:in ex:building1 ] }} .\n"
        ))
        .expect("the first rules should run");
    database
        .materialize_n3(&format!(
            "{PREFIX}{{ ?x a ex:Professor }} => {{ ?x ex:desk [ ex:in ex:building2 ] }} .\n"
        ))
        .expect("the second rules should run");
    assert_eq!(
        select(
            &mut database,
            "SELECT ?b WHERE { ex:bob ex:office ?o . ?o ex:in ?b }"
        ),
        rows(&[&["http://example.org/building1"]]),
        "the office and the desk must not be one node"
    );
}

#[test]
fn materializing_the_same_rules_again_adds_nothing() {
    let mut database = loaded();
    let rules =
        format!("{PREFIX}{{ ?x a ex:Professor }} => {{ ?x ex:office [ ex:in ex:building1 ] }} .\n");
    assert_eq!(database.materialize_n3(&rules).expect("first run"), 2);
    assert_eq!(database.materialize_n3(&rules).expect("second run"), 0);
}

#[test]
fn a_list_in_a_conclusion_is_stored_as_a_collection() {
    let mut database = loaded();
    database
        .materialize_n3(&format!(
            "{PREFIX}{{ ?x a ex:Professor }} => {{ ?x ex:tags ( ex:a ex:b ) }} .\n"
        ))
        .expect("the rule should run");
    assert_eq!(
        select(
            &mut database,
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
             SELECT ?first ?second WHERE { ex:bob ex:tags ?l . ?l rdf:first ?first . \
             ?l rdf:rest ?r . ?r rdf:first ?second . ?r rdf:rest rdf:nil }"
        ),
        rows(&[&["http://example.org/a", "http://example.org/b"]])
    );
}

#[test]
fn a_formula_in_a_conclusion_is_not_stored() {
    let mut database = loaded();
    let before = database.default_triple_count();
    let added = database
        .materialize_n3(&format!(
            "{PREFIX}{{ ?x a ex:Professor }} => {{ ?x ex:says {{ ?x a ex:Genius }} }} .\n"
        ))
        .expect("the rule should run");
    assert_eq!(added, 0);
    assert_eq!(database.default_triple_count(), before);
}

#[test]
fn backward_rules_are_refused_and_change_nothing() {
    let mut database = loaded();
    let before = database.default_triple_count();
    let result = database.materialize_n3(&format!(
        "{PREFIX}{{ ?x ex:mentor ?y }} <= {{ ?y ex:supervisor ?x }} .\n\
         {{ ?x a ex:Student }} => {{ ?x a ex:Learner }} .\n"
    ));
    assert!(matches!(result, Err(N3Error::Unsupported(_))), "{result:?}");
    assert_eq!(database.default_triple_count(), before);
}

#[test]
fn a_backward_rule_a_forward_rule_concludes_is_refused() {
    let mut database = loaded();
    let before = database.default_triple_count();
    let result = database.materialize_n3(&format!(
        "{PREFIX}{{ ?x a ex:Professor }} => {{ {{ ?y ex:mentor ?x }} <= {{ ?y ex:supervisor ?x }} }} .\n"
    ));
    assert!(matches!(result, Err(N3Error::Unsupported(_))), "{result:?}");
    assert_eq!(database.default_triple_count(), before);
}

#[test]
fn log_query_is_refused() {
    let mut database = loaded();
    let result = database.materialize_n3(&format!(
        "{PREFIX}@prefix log: <http://www.w3.org/2000/10/swap/log#> .\n\
         {{ ?x a ex:Student }} log:query {{ ?x a ex:Student }} .\n"
    ));
    assert!(matches!(result, Err(N3Error::Unsupported(_))), "{result:?}");
}

#[test]
fn a_fuse_reports_the_contradiction_and_changes_nothing() {
    let mut database = loaded();
    let before = database.default_triple_count();
    let result = database.materialize_n3(&format!(
        "{PREFIX}{{ ?x a ex:Student }} => {{ ?x a ex:Learner }} .\n\
         {{ ?x a ex:Professor . ?x a ex:Student }} => false .\n\
         ex:bob a ex:Student .\n"
    ));
    assert!(matches!(result, Err(N3Error::Fuse(_))), "{result:?}");
    assert_eq!(database.default_triple_count(), before);
}

#[test]
fn the_dictionary_survives_a_failed_run() {
    let mut database = loaded();
    let _ = database.materialize_n3("this is not N3 {");
    let _ = database.materialize_n3(&format!(
        "{PREFIX}{{ ?x ex:mentor ?y }} <= {{ ?y ex:supervisor ?x }} .\n"
    ));
    assert_eq!(
        select(&mut database, "SELECT ?x WHERE { ?x a ex:Student }"),
        rows(&[&["http://example.org/alice"]])
    );
}
