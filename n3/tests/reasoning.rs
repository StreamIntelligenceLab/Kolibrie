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
fn facts_since_lists_only_what_came_later_in_order() {
    let mut reasoner = N3Reasoner::new();
    let input = parse_document(&format!("{PREFIX}:a :p :b .\n"), None).expect("input parses");
    reasoner.load(&input).expect("loads");
    let before = reasoner.fact_count();
    let program = parse_document(
        &format!("{PREFIX}:c :p :d .\n{{ ?x :p ?y }} => {{ ?y :q ?x }} .\n"),
        None,
    )
    .expect("program parses");
    reasoner.load(&program).expect("loads");
    reasoner.run().expect("reasons");
    let later: Vec<_> = reasoner
        .facts_since(before)
        .iter()
        .map(|t| reasoner.store.statement(t))
        .collect();
    assert_eq!(later.len(), 3, "{later:?}");
    assert_eq!(
        canonical_lines(&later[..1]),
        expect(":c :p :d ."),
        "the program's own fact comes first"
    );
    assert_eq!(
        canonical_lines(&later),
        expect(":c :p :d . :b :q :a . :d :q :c .")
    );
}

#[test]
fn unsupported_features_are_named_not_ignored() {
    for (program, feature) in [(
        "{ ?x :p ?y . ?y log:content \"a\" } => { ?x :big true } .",
        "log#content",
    )] {
        match derive(program) {
            Err(N3Error::Unsupported(message)) => {
                assert!(message.contains(feature), "{program}: {message}")
            }
            other => panic!("{program}: expected unsupported {feature}, got {other:?}"),
        }
    }
}

#[test]
fn a_conclusion_triple_with_an_unbound_variable_is_left_out() {
    let derived = derive(":a :p :b . { ?x :p ?y } => { ?x :q ?y . ?x :r ?z } .").expect("reasons");
    assert_eq!(derived, expect(":a :q :b ."));
}

#[test]
fn a_rule_can_conclude_the_formula_a_variable_holds() {
    let derived = derive(
        ":c a :Cat . :Cat :rule { ?x a :Animal } .
         { ?x a ?k . ?k :rule ?f } => ?f .",
    )
    .expect("reasons");
    assert_eq!(
        derived,
        expect(":c a :Animal ."),
        "?x in the formula is the rule's ?x"
    );
}

#[test]
fn a_universal_fact_answers_every_instance() {
    let derived = derive(
        "@forAll :x . @forSome :m . :x :hasMother :m .
         { :bob :hasMother ?b } => { :bob :mother ?b } .
         { :liz :hasMother ?l } => { :liz :mother ?l } .",
    )
    .expect("reasons");
    assert_eq!(
        derived,
        expect(":bob :mother _:b . :liz :mother _:l ."),
        "each instance gets its own mother"
    );
}

#[test]
fn formulas_that_differ_only_in_quantified_names_are_the_same() {
    let derived = derive(
        ":a :g { @forAll :x . @forSome :y . :x :p :y } .
         { :a :g { @forAll :u . @forSome :v . :u :p :v } } => { :r :sameUpToRenaming true } .",
    )
    .expect("reasons");
    assert_eq!(derived, expect(":r :sameUpToRenaming true ."));
}

#[test]
fn inclusion_generalises_to_existentials_but_never_instantiates_universals() {
    let derived = derive(
        "{ { @forAll :x . :x :p :o } log:includes { :s :p :o } } => { :r :instantiated true } .
         { { :s :p :o } log:includes { @forAll :x . :x :p :o } } => { :r :generalised true } .
         { { @forAll :x . :x :p :o } log:includes { @forAll :y . @forSome :z . :y :p :z } } => { :r :existential true } ."
    )
    .expect("reasons");
    assert_eq!(derived, expect(":r :existential true ."));
}

#[test]
fn a_universal_conclusion_answers_every_instance_with_one_witness() {
    let derived = derive(
        "@forSome :m . {} => { @forAll :x . :x :hasMother :m } .
         { :bob :hasMother ?b . :liz :hasMother ?l } => { :r :same (?b ?l) } .",
    )
    .expect("reasons");
    assert_eq!(
        derived,
        expect(":r :same (_:m _:m) ."),
        "the existential sits outside the universal, so everyone has the same mother"
    );
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

#[test]
fn a_plain_rule_matches_a_repeated_variable_and_skips_an_unbound_conclusion() {
    let derived = derive(
        ":a :p :a . :a :p :b .
         { ?x :p ?x } => { ?x :loop true . ?x :seen ?nobody } .",
    )
    .expect("reasons");
    assert_eq!(derived, expect(":a :loop true ."));
}

#[test]
fn many_new_facts_reach_premises_of_every_constant_shape() {
    // Over 64 facts derived in one round, read by premises fixing different positions.
    let facts: String = (0..100)
        .map(|i| format!(":s{i} :p :o{} . ", i % 3))
        .collect();
    let derived = derive(&format!(
        "{facts}
         {{ ?s :p ?o }} => {{ ?s :q ?o }} .
         {{ ?s :q :o1 }} => {{ ?s :one true }} .
         {{ :s7 ?p :o1 }} => {{ :s7 :has ?p }} .
         {{ ?s ?p :o2 . ?s :q ?x }} => {{ ?s :two ?p }} ."
    ))
    .expect("reasons");
    let count = |local: &str| {
        let predicate = format!("<http://example.org/t#{local}>");
        derived
            .iter()
            .filter(|line| line.split(' ').nth(1) == Some(predicate.as_str()))
            .count()
    };
    assert_eq!(count("q"), 100);
    assert_eq!(count("one"), 33);
    assert_eq!(count("two"), 2 * 33);
    assert!(derived
        .iter()
        .any(|l| l.contains("#s7> <http://example.org/t#has> <http://example.org/t#q>")));
}
