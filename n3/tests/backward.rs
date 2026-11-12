/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Backward rules proved on demand, and `log:query` selecting what a run outputs

use n3::canon::canonical_lines;
use n3::{parse_document, N3Error, N3Reasoner};

const PREFIX: &str = "@prefix : <http://example.org/t#> .\n";

fn run(program: &str) -> Result<Vec<String>, N3Error> {
    let document = parse_document(&format!("{PREFIX}{program}"), None).expect("parses");
    let mut reasoner = N3Reasoner::new();
    reasoner.load(&document)?;
    reasoner.run()?;
    let statements: Vec<_> = reasoner
        .output()
        .iter()
        .map(|t| reasoner.store.statement(t))
        .collect();
    Ok(canonical_lines(&statements))
}

fn output(program: &str) -> Vec<String> {
    run(program).expect("reasons")
}

fn expect(text: &str) -> Vec<String> {
    let document = parse_document(&format!("{PREFIX}{text}"), None).expect("expected text parses");
    canonical_lines(&document.statements)
}

#[test]
fn a_backward_rule_answers_a_forward_premise_without_being_output() {
    assert_eq!(
        output(
            ":a :parent :b . :b :parent :c .
             { ?x :ancestor ?y } <= { ?x :parent ?y } .
             { ?x :ancestor ?z } <= { ?x :parent ?y . ?y :ancestor ?z } .
             { :a :ancestor ?who } => { :a :reaches ?who } ."
        ),
        expect(":a :reaches :b , :c ."),
        "only the forward conclusions are output, not the :ancestor answers"
    );
}

#[test]
fn a_backward_rule_with_an_empty_premise_states_its_head() {
    assert_eq!(
        output("{ :a :b :c } <= true . { :a :b ?x } => { :r :is ?x } ."),
        expect(":r :is :c .")
    );
}

#[test]
fn backward_rules_compute_with_builtins() {
    assert_eq!(
        output(
            "{ (?n) :fib ?n } <= { ?n math:lessThan 2 } .
             { (?n) :fib ?f } <= { ?n math:greaterThan 1 . (?n 1) math:difference ?a . (?n 2) math:difference ?b . (?a) :fib ?x . (?b) :fib ?y . (?x ?y) math:sum ?f } .
             { (30) :fib ?f } => { :fib30 :is ?f } ."
        ),
        expect(":fib30 :is 832040 ."),
        "repeated goals are proved once, or this would take millions of steps"
    );
}

#[test]
fn the_depth_limit_can_be_raised() {
    let document = parse_document(
        &format!(
            "{PREFIX}{{ (?n) :down true }} <= {{ ?n math:lessThan 1 }} .
             {{ (?n) :down true }} <= {{ ?n math:notLessThan 1 . (?n 1) math:difference ?m . (?m) :down true }} .
             {{ (40) :down true }} => {{ :r :is true }} ."
        ),
        None,
    )
    .expect("parses");
    let mut reasoner = N3Reasoner::new();
    reasoner.limits.max_backward_depth = 64;
    reasoner.load(&document).expect("loads");
    assert!(reasoner.run().is_ok(), "40 levels fit under a limit of 64");
}

#[test]
fn recursion_past_the_depth_limit_is_reported() {
    assert!(matches!(
        run(
            "{ (?n) :down true } <= { ?n math:lessThan 1 } .
             { (?n) :down true } <= { ?n math:notLessThan 1 . (?n 1) math:difference ?m . (?m) :down true } .
             { (40) :down true } => { :r :is true } ."
        ),
        Err(N3Error::Limit(_))
    ));
}

#[test]
fn every_proof_is_its_own_solution() {
    assert_eq!(
        output(
            ":x :p :y .
             { ?a :q ?b } <= { ?a :p ?b } .
             { ?a :q ?b } <= { ?a :p ?b } .
             { (1 { :x :q :y } ?ones) log:collectAllIn ?scope . ?ones list:length ?n } => { :proofs :count ?n } ."
        ),
        expect(":proofs :count 2 ."),
        "two rules prove the same goal, so a collection over its proofs sees two"
    );
}

#[test]
fn a_premise_backward_rules_answer_waits_for_its_inputs() {
    assert_eq!(
        output(
            ":g :chain (:p :p) . :a :p :b . :b :p :c .
             { (() ?n) :follow ?n } <= true .
             { (?ps ?s) :follow ?e } <= { ?ps list:firstRest (?q ?t) . ?s ?q ?m . (?t ?m) :follow ?e } .
             { ?g :chain ?l . ?l list:firstRest (?first ?rest) . ?s ?first ?m . (?rest ?m) :follow ?o } => { ?s ?g ?o } ."
        ),
        expect(":a :g :c ."),
        "the :follow goal needs ?rest and ?m, which the premises before it bind"
    );
}

#[test]
fn a_rule_derived_as_a_backward_rule_is_used() {
    assert_eq!(
        output(
            "true => { { :s a :H } <= true } .
             { :s a :H } => { :s :seen true } ."
        ),
        expect("{ :s a :H } <= true . :s :seen true .")
    );
}

#[test]
fn queries_replace_the_output_and_feed_nothing_back() {
    assert_eq!(
        output(
            ":a :p :b . :c :p :d .
             { ?x :p ?y } => { ?x :q ?y } .
             { ?x :q ?y } log:query { :answer :is ?x } .
             { :answer :is ?x } => { :never :derived true } ."
        ),
        expect(":answer :is :a , :c .")
    );
}

#[test]
fn a_value_a_proof_leaves_open_is_bound_by_a_later_premise() {
    assert_eq!(
        output(
            "{ (?x) :wrap (:box ?x) } <= true .
             { (5) :wrap ?w } => { :r :is ?w } ."
        ),
        expect(":r :is (:box 5) ."),
        "the head's ?x is open while the rule is proved, then the goal's 5 binds it"
    );
}

#[test]
fn an_open_variable_travels_through_a_list_and_is_bound_later() {
    assert_eq!(
        output(
            "{ (?env ?name) :lookup ?t } <= { ?env list:firstRest ((?name ?t) ?rest) } .
             { (?env (:lam ?n ?b)) :typeOf (:fun ?arg ?res) } <= {
                 ?inner list:firstRest ((?n ?arg) ?env) . (?inner ?b) :typeOf ?res } .
             { (?env (:var ?n)) :typeOf ?t } <= { (?env ?n) :lookup ?t } .
             { (?env (:int ?v)) :typeOf :int } <= true .
             { (?env (:app ?f ?a)) :typeOf ?res } <= {
                 (?env ?f) :typeOf (:fun ?in ?res) . (?env ?a) :typeOf ?in } .
             { (() (:app (:lam :x (:var :x)) (:int 7))) :typeOf ?t } => { :query :type ?t } ."
        ),
        expect(":query :type :int ."),
        "the argument type is unknown inside the lambda until the application supplies it"
    );
}

#[test]
fn a_conclusion_whose_value_stays_open_is_left_out() {
    assert!(output("{ :a :p ?x } <= true . { :a :p ?y } => { :r :is ?y } .").is_empty());
}
