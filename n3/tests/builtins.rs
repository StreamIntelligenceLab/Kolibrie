/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! `math:`, `string:` and `log:` equality built-ins, evaluated inside rule premises

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

#[test]
fn integer_arithmetic_is_exact_past_sixty_four_bits() {
    assert_eq!(
        derive("{ (99999999999999999999 1) math:sum ?s . (2 70) math:exponentiation ?p } => { :r :sum ?s ; :pow ?p } ."),
        expect(":r :sum 100000000000000000000 ; :pow 1180591620717411303424 .")
    );
}

#[test]
fn results_take_the_datatype_eyeron_gives_them() {
    assert_eq!(
        derive(
            "{ (7 2) math:quotient ?q . (6 2) math:quotient ?w . (1.5 2.5) math:sum ?d . (1e2 1) math:sum ?e } => { :r :q ?q ; :w ?w ; :d ?d ; :e ?e } ."
        ),
        expect(":r :q 3.5 ; :w 3 ; :d \"4\"^^xsd:decimal ; :e \"101\"^^xsd:decimal ."),
        "integers alone give an integer when whole; any non-integer operand gives a decimal"
    );
}

#[test]
fn integer_division_truncates_towards_zero() {
    assert_eq!(
        derive("{ (-7 2) math:integerQuotient ?q . (-7 2) math:remainder ?r } => { :r :q ?q ; :rem ?r } ."),
        expect(":r :q -3 ; :rem -1 .")
    );
}

#[test]
fn division_by_zero_has_no_result() {
    assert!(derive("{ (1 0) math:quotient ?q } => { :r :q ?q } .").is_empty());
}

#[test]
fn inverse_modes_solve_for_the_subject() {
    assert_eq!(
        derive(
            "{ ?x math:negation 5 . (2 ?e) math:exponentiation 32 } => { :r :neg ?x ; :exp ?e } ."
        ),
        expect(":r :neg -5 ; :exp 5 .")
    );
}

#[test]
fn a_bound_result_is_checked_by_value() {
    assert_eq!(
        derive(":a :n 4 . :b :n 5 . { ?s :n ?x . (?x 1) math:sum 5.0 } => { ?s :plusOneIsFive true } ."),
        expect(":a :plusOneIsFive true ."),
        "5.0 and 5 are the same number"
    );
}

#[test]
fn a_builtin_waits_until_its_inputs_are_bound() {
    assert_eq!(
        derive(":a :n 3 . { (?x 1) math:sum ?y . ?s :n ?x } => { ?s :next ?y } ."),
        expect(":a :next 4 ."),
        "the sum is written first but needs ?x from the later atom"
    );
}

#[test]
fn a_builtin_that_never_becomes_ready_matches_nothing() {
    assert!(derive("{ (?x 1) math:sum ?y } => { :r :y ?y } .").is_empty());
}

#[test]
fn comparisons_are_exact_for_integers_and_numeric_across_datatypes() {
    assert_eq!(
        derive(
            ":a :n 10000000000000000001 . :b :n 2.5 . :c :n 3 .
             { ?s :n ?x . ?x math:greaterThan 10000000000000000000 } => { ?s :huge true } .
             { ?s :n ?x . ?x math:lessThan 3 } => { ?s :small true } ."
        ),
        expect(":a :huge true . :b :small true .")
    );
}

#[test]
fn comparisons_ignore_non_numbers() {
    assert!(
        derive(":a :n \"5\" . { ?s :n ?x . ?x math:greaterThan 1 } => { ?s :big true } .")
            .is_empty()
    );
}

#[test]
fn string_builtins_read_lexical_forms() {
    assert_eq!(
        derive(
            ":a :name \"Ada\" .
             { ?s :name ?n . (?n \"-\" 42) string:concatenation ?c . ?n string:containsIgnoringCase \"DA\" } => { ?s :tag ?c } ."
        ),
        expect(":a :tag \"Ada-42\" .")
    );
}

#[test]
fn string_format_pads_and_rounds() {
    assert_eq!(
        derive(
            "{ (\"[%5s|%-3s|%04d|%.1f]\" \"ab\" \"c\" 7 2.25) string:format ?s } => { :r :s ?s } ."
        ),
        expect(":r :s \"[   ab|c  |0007|2.2]\" .")
    );
}

#[test]
fn log_equality_binds_one_side_and_compares_numbers_by_value() {
    assert_eq!(
        derive(
            ":a :n 1 .
             { ?s :n ?x . ?y log:equalTo ?x } => { ?s :copy ?y } .
             { 1 log:equalTo 1.0 } => { :r :decimalEqualsInteger true } .
             { 2.0 log:equalTo 2.00 } => { :r :decimalsEqual true } .
             { :a log:notEqualTo :b } => { :r :distinct true } ."
        ),
        expect(":a :copy 1 . :r :decimalsEqual true ; :distinct true ."),
        "log:equalTo compares numbers only within one datatype"
    );
}
