/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Proofs in eyeron's `pe:` vocabulary, checked for the shape eyeron's checker reads

use n3::{parse_document, parse_input, N3Reasoner};

const PREFIX: &str = "@prefix : <http://example.org/t#> .\n\
                      @prefix math: <http://www.w3.org/2000/10/swap/math#> .\n";

fn proof(program: &str) -> String {
    let document = parse_document(&format!("{PREFIX}{program}"), None).expect("program parses");
    let mut reasoner = N3Reasoner::new();
    reasoner.enable_proofs();
    reasoner.set_source_label("case.n3");
    reasoner.load(&document).expect("program loads");
    reasoner.run().expect("program runs");
    reasoner.proof_n3()
}

/// The step that justifies `statement`, up to the blank line that ends it
fn step<'a>(proof: &'a str, statement: &str) -> &'a str {
    let head = format!("{{ {statement} . }}\n  pe:");
    let start = proof
        .find(&head)
        .unwrap_or_else(|| panic!("no step for {statement} in\n{proof}"));
    let rest = &proof[start..];
    rest[..rest.find("\n\n").unwrap_or(rest.len())].trim_end()
}

const N: &str = "<http://example.org/t#n>";
const NEXT: &str = "<http://example.org/t#next>";
const ABOUT: &str = "<http://example.org/t#about>";
const SUM: &str = "<http://www.w3.org/2000/10/swap/math#sum>";

fn int(value: u32) -> String {
    format!("\"{value}\"^^<http://www.w3.org/2001/XMLSchema#integer>")
}

#[test]
fn a_proof_lists_the_claims_then_a_step_for_each_triple_they_rest_on() {
    let proof = proof(":a :n 2 . { ?x :n ?n . (?n 1) math:sum ?m } => { ?x :next ?m } .");
    let claim = format!("<http://example.org/t#a> {NEXT} {} .", int(3));
    let body = proof
        .strip_prefix("@prefix pe: <https://eyereasoner.github.io/pe#> .\n\n")
        .expect("the proof declares its vocabulary first");
    assert!(body.starts_with(&format!("{claim}\n")), "{proof}");

    let derived = step(
        &proof,
        &format!("<http://example.org/t#a> {NEXT} {}", int(3)),
    );
    assert!(derived.contains("pe:rule 1;"), "{derived}");
    assert!(
        derived.contains(&format!("[ pe:var \"n\"; pe:value {} ]", int(2))),
        "{derived}"
    );
    assert!(derived.contains(&format!(
        "pe:uses\n    {{ <http://example.org/t#a> {N} {} . }},\n    {{ ({} {}) {SUM} {} . }}.",
        int(2),
        int(2),
        int(1),
        int(3)
    )));

    let fact = step(&proof, &format!("<http://example.org/t#a> {N} {}", int(2)));
    assert!(fact.ends_with("pe:fact \"case.n3\"."), "{fact}");
    let builtin = step(&proof, &format!("({} {}) {SUM} {}", int(2), int(1), int(3)));
    assert!(
        builtin.ends_with(&format!("pe:builtin {SUM}.")),
        "{builtin}"
    );
}

#[test]
fn a_witness_is_named_apart_from_the_blank_nodes_the_source_spells() {
    let proof = proof("_:sk_1 :n 2 . { ?x :n ?n } => { [] :about ?x } .");
    assert!(
        proof.contains(&format!("\n_:sk0_1 {ABOUT} _:sk_1 .\n")),
        "{proof}"
    );
}

#[test]
fn a_formula_declares_its_own_quantifiers() {
    let proof = proof(
        "{ { @forAll :u . :u :p :o } log:includes { @forAll :v . :v :p :o } } => { :q :holds true } .",
    );
    let declared = "{ @forAll <urn:kolibrie:quantified:1> . \
                    <urn:kolibrie:quantified:1> <http://example.org/t#p> <http://example.org/t#o> . }";
    assert!(proof.contains(declared), "{proof}");
    assert!(!proof.contains("?__l"), "{proof}");
}

#[test]
fn a_rule_blank_node_in_a_quoted_premise_is_written_as_the_rule_spells_it() {
    let proof = proof(
        ":g :is { :a :p :b } .
         { :g :is ?G . ?G log:notIncludes { :a :q [] } } => { :g :lacks :q } .",
    );
    let premise = proof
        .lines()
        .find(|line| line.contains("log#notIncludes"))
        .expect("the scoped test is a premise");
    assert!(
        premise.contains("<http://example.org/t#q> _:b"),
        "{premise}"
    );
}

#[test]
fn message_blank_nodes_are_labelled_by_their_message() {
    let log = "VERSION \"1.2-messages\"\n\
               PREFIX : <http://example.org/t#>\n\
               _:b0 :v 1 .\n\
               MESSAGE\n\
               _:b0 :v 2 .\n";
    let document = format!(
        "{:?}",
        parse_input(log, None, true).expect("the log parses")
    );
    assert!(
        document.contains("m1_b0") && document.contains("m2_b0"),
        "{document}"
    );
}
