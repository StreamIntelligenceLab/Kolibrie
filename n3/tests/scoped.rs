/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Built-ins that read a graph, the deferral they need, and inference fuses

use n3::canon::canonical_lines;
use n3::{parse_document, N3Error, N3Reasoner};

const PREFIX: &str = "@prefix : <http://example.org/t#> .\n";

fn run(program: &str) -> Result<Vec<String>, N3Error> {
    let document = parse_document(&format!("{PREFIX}{program}"), None).expect("parses");
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

fn derive(program: &str) -> Vec<String> {
    run(program).expect("reasons")
}

fn expect(text: &str) -> Vec<String> {
    let document = parse_document(&format!("{PREFIX}{text}"), None).expect("expected text parses");
    canonical_lines(&document.statements)
}

#[test]
fn not_includes_waits_until_the_graph_is_saturated() {
    assert_eq!(
        derive(
            ":a :p :b .
             { ?x :p ?y } => { ?y :q ?x } .
             { ?x :p ?y . ?scope log:notIncludes { ?y :q ?x } } => { ?x :unanswered true } ."
        ),
        expect(":b :q :a ."),
        "the negation sees the derived :q, so it does not fire"
    );
}

#[test]
fn includes_over_a_quoted_formula_binds_from_its_triples() {
    assert_eq!(
        derive(
            ":a :says { :b :age 3 . :c :age 4 } .
             { ?s :says ?f . ?f log:includes { ?who :age 4 } } => { ?s :mentions ?who } ."
        ),
        expect(":a :mentions :c .")
    );
}

#[test]
fn includes_instantiates_a_formula_held_by_a_variable() {
    assert_eq!(
        derive(
            ":t :rule { ?x :p ?y } . :t :data { :a :p :b } .
             { :t :rule ?r . :t :data ?d . ?d log:includes ?r } => { :t :matched ?r } ."
        ),
        expect(":t :matched { :a :p :b } ."),
        "matching binds the quoted variables, and ?r stands for the instance after that"
    );
}

#[test]
fn collect_all_in_keeps_the_order_facts_were_added_in() {
    assert_eq!(
        derive(
            ":s :item :c . :s :item :a . :s :item :b .
             { (?i { :s :item ?i } ?all) log:collectAllIn ?scope } => { :s :items ?all } ."
        ),
        expect(":s :items (:c :a :b) .")
    );
}

#[test]
fn collect_all_in_over_nothing_is_the_empty_list() {
    assert_eq!(
        derive("{ (?i { :s :item ?i } ?all) log:collectAllIn ?scope } => { :s :items ?all } ."),
        expect(":s :items () .")
    );
}

#[test]
fn for_all_in_needs_every_generator_solution_to_meet_the_condition() {
    assert_eq!(
        derive(
            ":a :knows :b , :c . :d :knows :b . :b a :V . :c a :W .
             { ?p :knows ?any . ({ ?p :knows ?q } { ?q a :V }) log:forAllIn ?scope } => { ?p :onlyV true } ."
        ),
        expect(":d :onlyV true .")
    );
}

#[test]
fn conjunction_merges_formulas() {
    assert_eq!(
        derive("{ ({ :a :p :b } { :c :p :d }) log:conjunction ?f } => { :r :is ?f } ."),
        expect(":r :is { :a :p :b . :c :p :d } .")
    );
}

#[test]
fn conclusion_runs_a_formulas_own_rules() {
    assert_eq!(
        derive("{ { :a :p :b . { ?s :p ?o } => { ?o :q ?s } } log:conclusion ?c . ?c log:includes { :b :q :a } } => { :r :concluded true } ."),
        expect(":r :concluded true .")
    );
}

#[test]
fn a_fuse_stops_the_run_when_its_premise_holds() {
    assert!(matches!(
        run(":a :p :b . { ?x :p ?y } => false ."),
        Err(N3Error::Fuse(_))
    ));
    assert!(run(":a :p :b . { ?x :q ?y } => false .").is_ok());
}

#[test]
fn a_negation_later_facts_contradict_trips_a_fuse() {
    assert!(
        matches!(
            run(":a :p :b .
                 { ?x :p ?y . ?scope log:notIncludes { ?x :blocked true } } => { ?x :ok true } .
                 { ?x :ok true } => { ?x :blocked true } ."),
            Err(N3Error::Fuse(_))
        ),
        "the rule fired because :a was not blocked, and that firing blocked it"
    );
}

#[test]
fn an_existential_rule_runs_before_a_collection_only_in_source_order() {
    assert_eq!(
        derive(
            ":s :x 1 . :s :x 2 .
             { :s :x ?v } => { :s :result [ :v ?v ] } .
             { (?v { :s :result ?r . ?r :v ?v } ?all) log:collectAllIn ?scope } => { :s :all ?all } ."
        ),
        expect(":s :result [ :v 1 ] , [ :v 2 ] . :s :all (1 2) ."),
        "the earlier existential rule fills the graph before the later collection reads it"
    );
}

#[test]
fn a_rule_completing_another_piece_with_known_facts_runs_first() {
    assert_eq!(
        derive(
            ":c :examiner :r . :c :participant :w .
             { ?c :participant ?p } => { ?c :examiner _:e . _:e a :Person } .
             { ?c :examiner ?e } => { ?e a :Person } ."
        ),
        expect(":r a :Person ."),
        ":r already examines, so making :r a Person satisfies the first rule without a new examiner"
    );
}
