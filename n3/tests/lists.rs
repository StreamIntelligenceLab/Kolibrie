/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! `list:` built-ins and `rdf:first`/`rdf:rest`, over native lists and RDF chains

use n3::canon::canonical_lines;
use n3::{parse_document, N3Reasoner};

const PREFIX: &str = "@prefix : <http://example.org/t#> .\n";

fn derive(program: &str) -> Vec<String> {
    let document = parse_document(&format!("{PREFIX}{program}"), None).expect("parses");
    let mut reasoner = N3Reasoner::new();
    reasoner.load(&document).expect("loads");
    reasoner.run().expect("reasons");
    let statements: Vec<_> = reasoner
        .derived()
        .iter()
        .map(|t| reasoner.store.statement(t))
        .collect();
    canonical_lines(&statements)
}

fn expect(text: &str) -> Vec<String> {
    let document = parse_document(&format!("{PREFIX}{text}"), None).expect("expected text parses");
    canonical_lines(&document.statements)
}

const CHAIN: &str =
    ":x :items _:a . _:a rdf:first 1 ; rdf:rest _:b . _:b rdf:first 2 ; rdf:rest rdf:nil .\n";

#[test]
fn an_rdf_chain_is_a_list_to_the_list_builtins() {
    assert_eq!(
        derive(&format!(
            "{CHAIN}{{ :x :items ?l . ?l list:length ?n . ?l list:last ?z . ?l math:sum ?t }} => {{ :x :n ?n ; :last ?z ; :sum ?t }} ."
        )),
        expect(":x :n 2 ; :last 2 ; :sum 3 .")
    );
}

#[test]
fn a_chain_and_a_native_list_with_the_same_members_are_equal() {
    assert_eq!(
        derive(&format!(
            "{CHAIN}{{ :x :items ?l . ?l list:reverse (2 1) }} => {{ :x :reversible true }} ."
        )),
        expect(":x :reversible true .")
    );
}

#[test]
fn rdf_first_and_rest_also_hold_of_native_lists() {
    assert_eq!(
        derive(
            ":x :items (1 2 3) .
             { ?l rdf:first 1 . ?l rdf:rest ?r . ?r rdf:first ?second } => { :r :second ?second ; :rest ?r } ."
        ),
        expect(":r :second 2 ; :rest (2 3) .")
    );
}

#[test]
fn member_and_in_enumerate() {
    assert_eq!(
        derive("{ (:a :b) list:member ?m . ?n list:in (:c) } => { ?m :with ?n } ."),
        expect(":a :with :c . :b :with :c .")
    );
}

#[test]
fn append_concatenates_and_splits() {
    assert_eq!(
        derive(
            "{ ((1) () (2 3)) list:append ?all } => { :r :all ?all } .
             { (?front (?last)) list:append (1 2 3) } => { :r :front ?front ; :last ?last } ."
        ),
        expect(":r :all (1 2 3) ; :front (1 2) ; :last 3 .")
    );
}

#[test]
fn first_rest_builds_and_takes_apart() {
    assert_eq!(
        derive(
            "{ (1 2) list:firstRest (?h ?t) . ?whole list:firstRest (0 (5 6)) } => { :r :h ?h ; :t ?t ; :whole ?whole } ."
        ),
        expect(":r :h 1 ; :t (2) ; :whole (0 5 6) .")
    );
}

#[test]
fn sort_orders_numbers_by_value_and_text_by_characters() {
    assert_eq!(
        derive(
            "{ (10 9.5 2) list:sort ?n . (\"b\" \"a\") list:sort ?s } => { :r :n ?n ; :s ?s } ."
        ),
        expect(":r :n (2 9.5 10) ; :s (\"a\" \"b\") .")
    );
}

#[test]
fn indexed_access_counts_from_zero() {
    assert_eq!(
        derive(
            "{ ((:a :b :c) 2) list:memberAt ?m . (:a :b) list:iterate (?i :b) . ((:a :b :a) :a) list:remove ?rest } => { :r :m ?m ; :i ?i ; :rest ?rest } ."
        ),
        expect(":r :m :c ; :i 1 ; :rest (:b) .")
    );
}

#[test]
fn not_member_compares_numbers_by_value() {
    assert_eq!(
        derive(
            "{ (1 2) list:notMember 3 } => { :r :lacksThree true } .
             { (1 2) list:notMember 2 } => { :r :lacksTwo true } ."
        ),
        expect(":r :lacksThree true .")
    );
}

#[test]
fn map_applies_a_builtin_or_a_property_to_each_member() {
    assert_eq!(
        derive(
            ":a :p :x . :b :p :y .
             { ((-1 2) math:absoluteValue) list:map ?abs . ((:a :b) :p) list:map ?ps } => { :r :abs ?abs ; :ps ?ps } ."
        ),
        expect(":r :abs (1 2) ; :ps (:x :y) .")
    );
}
