/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, RwLock};
use shared::dictionary::Dictionary;
use shared::terms::Term;
use crate::parser::{parse_data, parse_program as parse_program_raw, RDF_TYPE};
use crate::syntax::{DatalogMTLRule, Interval, Mode, TemporalAtom};

fn dict() -> Arc<RwLock<Dictionary>> {
    Arc::new(RwLock::new(Dictionary::new()))
}

/// Parse in streaming mode (past-only) — the default for these tests.
fn parse_program(text: &str, d: &Arc<RwLock<Dictionary>>) -> Result<Vec<DatalogMTLRule>, String> {
    parse_program_raw(text, d, Mode::Streaming)
}

fn id(d: &Arc<RwLock<Dictionary>>, s: &str) -> u32 {
    d.write().unwrap().encode(s)
}

/// Unary body atom maps to (?x rdf:type A) and Diamondminus -> Diamond.
#[test]
fn test_unary_diamond_mapping() {
    let d = dict();
    let rules = parse_program("B(X):-Diamondminus[1,5]A(X)", &d).unwrap();
    assert_eq!(rules.len(), 1);
    let rule = &rules[0];

    // Head: (X, rdf:type, B)
    assert_eq!(rule.head.0, Term::Variable("X".into()));
    assert_eq!(rule.head.1, Term::Constant(id(&d, RDF_TYPE)));
    assert_eq!(rule.head.2, Term::Constant(id(&d, "B")));

    match &rule.body[0] {
        TemporalAtom::Diamond { interval, inner } => {
            assert_eq!(*interval, Interval { start: 1, end: 5 });
            match inner.as_ref() {
                TemporalAtom::Base((s, p, o)) => {
                    assert_eq!(*s, Term::Variable("X".into()));
                    assert_eq!(*p, Term::Constant(id(&d, RDF_TYPE)));
                    assert_eq!(*o, Term::Constant(id(&d, "A")));
                }
                other => panic!("expected Base, got {:?}", other),
            }
        }
        other => panic!("expected Diamond, got {:?}", other),
    }
}

/// Binary atom maps to (?x C ?y).
#[test]
fn test_binary_mapping() {
    let d = dict();
    let rules = parse_program("Reach(X,Z):-Edge(X,Y),Reach(Y,Z)", &d).unwrap();
    let body = &rules[0].body;
    assert_eq!(body.len(), 2);
    match &body[0] {
        TemporalAtom::Base((s, p, o)) => {
            assert_eq!(*s, Term::Variable("X".into()));
            assert_eq!(*p, Term::Constant(id(&d, "Edge")));
            assert_eq!(*o, Term::Variable("Y".into()));
        }
        other => panic!("expected Base, got {:?}", other),
    }
}

/// `L Since[a,b] R` -> Since { phi = L, psi = R }.
#[test]
fn test_since_mapping() {
    let d = dict();
    let rules = parse_program("C(X):-A(X)Since[1,5]B(X)", &d).unwrap();
    match &rules[0].body[0] {
        TemporalAtom::Since { interval, phi, psi } => {
            assert_eq!(*interval, Interval { start: 1, end: 5 });
            assert!(matches!(phi.as_ref(), TemporalAtom::Base(_)));
            assert!(matches!(psi.as_ref(), TemporalAtom::Base(_)));
        }
        other => panic!("expected Since, got {:?}", other),
    }
}

/// Stacked operators nest outermost-first.
#[test]
fn test_stacked_operators() {
    let d = dict();
    let rules = parse_program("B(X):-Boxminus[1,2]Diamondminus[0,1]A(X)", &d).unwrap();
    match &rules[0].body[0] {
        TemporalAtom::Box_ { interval, inner } => {
            assert_eq!(*interval, Interval { start: 1, end: 2 });
            assert!(matches!(inner.as_ref(), TemporalAtom::Diamond { .. }));
        }
        other => panic!("expected Box_, got {:?}", other),
    }
}

/// Facts parse into grounded triples with closed integer intervals.
#[test]
fn test_fact_parsing() {
    let d = dict();
    let facts = parse_data("A(a)@[0,10]\nC(a,b)@3", &d).unwrap();
    assert_eq!(facts.len(), 2);
    // A(a)@[0,10] -> (a, rdf:type, A) over [0,10]
    assert_eq!(facts[0].triple.subject, id(&d, "a"));
    assert_eq!(facts[0].triple.predicate, id(&d, RDF_TYPE));
    assert_eq!(facts[0].triple.object, id(&d, "A"));
    assert_eq!((facts[0].start, facts[0].end), (0, 10));
    // C(a,b)@3 -> (a, C, b) at [3,3]
    assert_eq!(facts[1].triple.predicate, id(&d, "C"));
    assert_eq!((facts[1].start, facts[1].end), (3, 3));
}

/// Out-of-fragment inputs are rejected with a descriptive error.
#[test]
fn test_fragment_rejections() {
    let d = dict();
    assert!(parse_program("P:-Q", &d).is_err(), "arity-0 should be rejected");
    assert!(parse_program("R(X,Y,Z):-S(X,Y,Z)", &d).is_err(), "arity-3 should be rejected");
    assert!(parse_program("B(X):-Boxplus[1,2]A(X)", &d).is_err(), "future op rejected in streaming");
    assert!(parse_program("B(X):-A(X)Until[1,2]C(X)", &d).is_err(), "Until rejected in streaming");
    assert!(parse_program("B(X):-Boxminus(1,2]A(X)", &d).is_err(), "open interval should be rejected");
    assert!(parse_program("Boxminus[1,2]B(X):-A(X)", &d).is_err(), "head operator should be rejected");
    assert!(parse_data("A(a)@1.5", &d).is_err(), "non-integer time should be rejected");
}

/// Future operators parse only in Static mode, into the right variants.
#[test]
fn test_future_operators_static() {
    let d = dict();
    // Streaming rejects; static accepts.
    assert!(parse_program_raw("B(X):-Boxplus[1,2]A(X)", &d, Mode::Streaming).is_err());
    assert!(parse_program_raw("B(X):-Boxplus[1,2]A(X)", &d, Mode::Static).is_ok());

    let rules = parse_program_raw("B(X):-Diamondplus[0,3]A(X)", &d, Mode::Static).unwrap();
    match &rules[0].body[0] {
        TemporalAtom::DiamondPlus { interval, inner } => {
            assert_eq!(*interval, Interval { start: 0, end: 3 });
            assert!(matches!(inner.as_ref(), TemporalAtom::Base(_)));
        }
        other => panic!("expected DiamondPlus, got {:?}", other),
    }

    let rules = parse_program_raw("C(X):-A(X)Until[1,5]B(X)", &d, Mode::Static).unwrap();
    match &rules[0].body[0] {
        TemporalAtom::Until { interval, phi, psi } => {
            assert_eq!(*interval, Interval { start: 1, end: 5 });
            assert!(matches!(phi.as_ref(), TemporalAtom::Base(_)));
            assert!(matches!(psi.as_ref(), TemporalAtom::Base(_)));
        }
        other => panic!("expected Until, got {:?}", other),
    }
}
