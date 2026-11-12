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

#[test]
fn regular_expressions_match_replace_and_scrape() {
    assert_eq!(
        derive(
            "{ \"hello world\" string:matches \"^h.*d$\" . \"abc\" string:notMatches \"x\" .
               (\"a-b-c\" \"-\" \"+\") string:replace ?r . (\"id=42;\" \"id=([0-9]+)\") string:scrape ?n }
             => { :r :replaced ?r ; :scraped ?n } ."
        ),
        expect(":r :replaced \"a+b+c\" ; :scraped \"42\" .")
    );
}

#[test]
fn a_pattern_that_does_not_compile_matches_nothing() {
    assert!(derive("{ \"abc\" string:matches \"(\" } => { :r :matched true } .").is_empty());
}

#[test]
fn digests_and_term_inspection() {
    assert_eq!(
        derive(
            "{ \"abc\" crypto:sha ?h . :x log:uri ?u . ?i log:uri \"http://example.org/t#y\" .
               ?p log:dtlit \"7\"^^xsd:integer . (\"hi\" \"en\") log:langlit ?l . { :a :b :c } log:rawType ?f }
             => { :r :h ?h ; :u ?u ; :i ?i ; :p ?p ; :l ?l ; :f ?f } ."
        ),
        expect(
            ":r :h \"a9993e364706816aba3e25717850c26c9cd0d89d\" ; :u \"http://example.org/t#x\" ;
               :i :y ; :p (\"7\" xsd:integer) ; :l \"hi\"@en ; :f log:Formula ."
        )
    );
}

#[test]
fn dates_subtract_to_durations_and_compare_with_dates_only() {
    assert_eq!(
        derive(
            "{ (\"2024-03-01\"^^xsd:date \"2024-02-28\"^^xsd:date) math:difference ?d .
               ?d math:greaterThan \"P1D\"^^xsd:duration .
               \"2024-01-01\"^^xsd:date math:lessThan \"2024-01-02T00:00:01Z\"^^xsd:dateTime } => { :r :d ?d } .
             { \"2024-01-01\"^^xsd:date math:lessThan 5 } => { :r :mixed true } ."
        ),
        expect(":r :d \"PT172800S\"^^xsd:duration .")
    );
}

#[test]
fn time_builtins_read_date_time_fields() {
    assert_eq!(
        derive("{ \"2021-07-07T10:20:30Z\" time:year ?y ; time:minute ?m ; time:timeZone ?z } => { :r :y ?y ; :m ?m ; :z ?z } ."),
        expect(":r :y 2021 ; :m 20 ; :z \"Z\" .")
    );
}

#[test]
fn local_time_reports_the_pinned_instant() {
    let document = parse_document(
        &format!("{PREFIX}{{ \"\" time:localTime ?t }} => {{ :r :now ?t }} ."),
        None,
    )
    .expect("parses");
    let mut reasoner = N3Reasoner::new();
    reasoner.pin_clock(86_400_000);
    reasoner.load(&document).expect("loads");
    reasoner.run().expect("reasons");
    let statements: Vec<_> = reasoner
        .derived()
        .iter()
        .map(|t| reasoner.store.statement(t))
        .collect();
    assert_eq!(
        canonical_lines(&statements),
        expect(":r :now \"1970-01-02T00:00:00.000+00:00\"^^xsd:dateTime .")
    );
}

#[test]
fn a_name_in_a_builtin_namespace_that_is_no_builtin_is_data() {
    assert_eq!(
        derive("\"abc\" string:length 3 . { \"abc\" string:length ?n } => { :r :length ?n } ."),
        expect(":r :length 3 .")
    );
}

#[test]
fn an_exponent_too_large_for_a_float_logarithm_is_found_exactly() {
    // With the power bound first, the last premise solves for its exponent, 2^65536 has no
    // float logarithm, and the backward goal that binds the exponent only answers after it.
    assert_eq!(
        derive(
            "{ (?b 4) :tower 65536 } <= { } .
             { (2 65536) math:exponentiation ?v . (2 4) :tower ?p . (2 ?p) math:exponentiation ?v }
                 => { :power :found true } ."
        ),
        expect(":power :found true .")
    );
}
