/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Forward N3 reasoning, checked on small programs whose closure is known

use n3::canon::canonical_lines;
use n3::{parse_document, N3Error, N3Reasoner};

const PREFIX: &str = "@prefix : <http://example.org/t#> .\n";

fn derive(program: &str) -> Result<Vec<String>, N3Error> {
    let document = parse_document(&format!("{PREFIX}{program}"), None)?;
    let mut reasoner = N3Reasoner::new();
    reasoner.load(&document)?;
    reasoner.run()?;
    let statements: Vec<_> = reasoner
        .derived()
        .iter()
        .map(|t| reasoner.store.statement(t))
        .collect();
    Ok(canonical_lines(&statements))
}

fn expect(text: &str) -> Vec<String> {
    let document = parse_document(&format!("{PREFIX}{text}"), None).expect("expected text parses");
    canonical_lines(&document.statements)
}

#[test]
fn a_recursive_rule_reaches_its_fixpoint() {
    let derived = derive(
        ":a :parent :b . :b :parent :c .
         { ?x :parent ?y } => { ?x :ancestor ?y } .
         { ?x :parent ?y . ?y :ancestor ?z } => { ?x :ancestor ?z } .",
    )
    .expect("reasons");
    assert_eq!(
        derived,
        expect(":a :ancestor :b . :b :ancestor :c . :a :ancestor :c .")
    );
}

#[test]
fn list_patterns_bind_their_members() {
    let derived = derive(
        ":x :pair (:a (:b :c)) .
         { ?s :pair (?first (?second ?third)) } => { ?s :last ?third . ?s :swapped (?third ?first) } .",
    )
    .expect("reasons");
    assert_eq!(derived, expect(":x :last :c . :x :swapped (:c :a) ."));
}

#[test]
fn a_list_pattern_does_not_match_a_list_of_another_length() {
    let derived = derive(":x :pair (:a :b :c) . { ?s :pair (?p ?q) } => { ?s :two true } .")
        .expect("reasons");
    assert!(derived.is_empty(), "{derived:?}");
}

#[test]
fn formula_patterns_match_quoted_graphs() {
    let derived = derive(
        ":alice :says { :sky :is :blue } .
         { ?who :says { ?thing :is ?colour } } => { ?who :claims ?colour } .",
    )
    .expect("reasons");
    assert_eq!(derived, expect(":alice :claims :blue ."));
}

#[test]
fn a_conclusion_can_build_a_quoted_graph() {
    let derived =
        derive(":alice :likes :tea . { ?x :likes ?y } => { :log :entry { ?x :liked ?y } } .")
            .expect("reasons");
    assert_eq!(derived, expect(":log :entry { :alice :liked :tea } ."));
}

#[test]
fn a_premise_blank_node_matches_like_a_variable() {
    let derived = derive(
        ":alice :knows [ :name \"Bob\" ] .
         { ?x :knows _:someone . _:someone :name ?n } => { ?x :knowsNamed ?n } .",
    )
    .expect("reasons");
    assert_eq!(derived, expect(":alice :knowsNamed \"Bob\" ."));
}

#[test]
fn an_existential_is_introduced_only_where_none_is_known() {
    let derived = derive(
        ":alice a :Person . :bob a :Person . :bob :parent :carol .
         { ?p a :Person } => { ?p :parent _:someone } .",
    )
    .expect("reasons");
    assert_eq!(derived, expect(":alice :parent _:p ."));
}

#[test]
fn a_rule_whose_conclusion_satisfies_another_existential_runs_first() {
    let derived = derive(
        ":Cobbler :kind :Profession . :alice :job :Cobbler .
         { ?p :kind :Profession } => { [ a :Type ; :goodOf ?p ] } .
         { ?x :job :Cobbler } => { ?x :hasType [ a :Type ; :goodOf :Cobbler ] } .",
    )
    .expect("reasons");
    assert_eq!(
        derived,
        expect(":alice :hasType _:t . _:t a :Type ; :goodOf :Cobbler ."),
        "the larger piece goes first and then satisfies the smaller one"
    );
}

#[test]
fn a_derived_rule_fires_over_existing_facts() {
    let derived = derive(
        ":knows a :Symmetric . :alice :knows :bob .
         { ?p a :Symmetric } => { { ?x ?p ?y } => { ?y ?p ?x } } .",
    )
    .expect("reasons");
    assert_eq!(
        derived,
        expect("{ ?x :knows ?y } => { ?y :knows ?x } . :bob :knows :alice .")
    );
}

#[test]
fn rules_match_only_patterns_that_name_log_implies() {
    let derived = derive(
        "{ ?s ?p ?o } => { :any :predicate ?p } .
         { ?a => ?b } => { :some :rule true } .",
    )
    .expect("reasons");
    assert_eq!(
        derived,
        expect(":some :rule true . :any :predicate :rule . :any :predicate :predicate .")
    );
}

#[test]
fn a_rule_with_an_empty_premise_fires_once() {
    let derived = derive("{} => { :a :b :c } . true => { :d :e :f } .").expect("reasons");
    assert_eq!(derived, expect(":a :b :c . :d :e :f ."));
}

#[test]
fn input_facts_are_not_reported_as_derived() {
    let derived = derive(":a :p :b . { ?x :p ?y } => { ?x :p ?y } .").expect("reasons");
    assert!(derived.is_empty(), "{derived:?}");
}

#[test]
fn unsupported_features_are_named_not_ignored() {
    for (program, feature) in [
        ("{ ?x :p ?y } <= { ?y :q ?x } .", "backward rules"),
        (
            "{ ?x :p ?y . ?y string:matches \"a\" } => { ?x :big true } .",
            "string#matches",
        ),
        ("{ ?x :p ?y } log:query { ?x :q ?y } .", "log:query"),
        ("{ ?x :p ?y } => false .", "fuses"),
        ("{ ?x :p ?y } => { ?x :q ?z } .", "?z"),
    ] {
        match derive(program) {
            Err(N3Error::Unsupported(message)) => {
                assert!(message.contains(feature), "{program}: {message}")
            }
            other => panic!("{program}: expected unsupported {feature}, got {other:?}"),
        }
    }
}

#[test]
fn the_fact_budget_stops_a_run_that_never_saturates() {
    let document = parse_document(
        &format!("{PREFIX}:a :n (). {{ ?x :n ?l }} => {{ ?x :n (?l) }} ."),
        None,
    )
    .expect("parses");
    let mut reasoner = N3Reasoner::new();
    reasoner.limits.max_facts = 1_000;
    reasoner.load(&document).expect("loads");
    assert!(matches!(reasoner.run(), Err(N3Error::Limit(_))));
}
