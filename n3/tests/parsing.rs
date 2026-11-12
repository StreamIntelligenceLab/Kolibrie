/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! N3 syntax read into statements

use n3::parser::resolve_iri;
use n3::syntax::{Node, Statement, RDF_TYPE, XSD_DECIMAL, XSD_DOUBLE, XSD_INTEGER};
use n3::{parse_document, N3Error};

fn parse(text: &str) -> Vec<Statement> {
    parse_document(text, None).expect("parses").statements
}

fn iri(local: &str) -> Node {
    Node::iri(format!("http://example.org/t#{local}"))
}

const PREFIX: &str = "@prefix : <http://example.org/t#> .\n";

#[test]
fn predicate_and_object_lists_expand_to_triples() {
    let statements = parse(&format!("{PREFIX}:a :p :b , :c ; a :T ;; ."));
    assert_eq!(
        statements,
        vec![
            Statement::new(iri("a"), iri("p"), iri("b")),
            Statement::new(iri("a"), iri("p"), iri("c")),
            Statement::new(iri("a"), Node::iri(RDF_TYPE), iri("T")),
        ]
    );
}

#[test]
fn local_names_keep_inner_dots_and_escapes() {
    let statements = parse(&format!("{PREFIX}:St.%20Helena :p :a.b.c ."));
    assert_eq!(statements[0].subject, iri("St.%20Helena"));
    assert_eq!(statements[0].object, iri("a.b.c"));
}

#[test]
fn numbers_take_the_datatype_their_form_implies() {
    let statements = parse(&format!("{PREFIX}:x :p 7, -1.5, 2e3 ."));
    let datatypes: Vec<_> = statements
        .iter()
        .map(|s| match &s.object {
            Node::Literal { datatype, .. } => datatype.clone(),
            other => panic!("not a literal: {other:?}"),
        })
        .collect();
    assert_eq!(
        datatypes,
        vec![
            Some(XSD_INTEGER.to_string()),
            Some(XSD_DECIMAL.to_string()),
            Some(XSD_DOUBLE.to_string())
        ]
    );
}

#[test]
fn a_statement_can_end_right_after_a_number() {
    let statements = parse(&format!("{PREFIX}:x :p 7.\n:y :p 8 ."));
    assert_eq!(statements.len(), 2);
}

#[test]
fn strings_unescape_and_long_strings_span_lines() {
    let statements = parse(&format!(
        "{PREFIX}:x :p \"a\\tb\", \"\"\"two\nlines\"\"\", 'single'@EN ."
    ));
    let lexicals: Vec<_> = statements
        .iter()
        .map(|s| match &s.object {
            Node::Literal {
                lexical, language, ..
            } => (lexical.clone(), language.clone()),
            other => panic!("not a literal: {other:?}"),
        })
        .collect();
    assert_eq!(
        lexicals,
        vec![
            ("a\tb".to_string(), None),
            ("two\nlines".to_string(), None),
            ("single".to_string(), Some("en".to_string())),
        ]
    );
}

#[test]
fn is_of_reverses_and_paths_introduce_blank_nodes() {
    let statements = parse(&format!("{PREFIX}:b is :child of :a .\n:a!:child :p :o ."));
    assert_eq!(
        statements[0],
        Statement::new(iri("a"), iri("child"), iri("b"))
    );
    let Node::Blank(step) = &statements[1].object else {
        panic!("a path step is a blank node: {:?}", statements[1]);
    };
    assert_eq!(statements[2].subject, Node::Blank(step.clone()));
}

#[test]
fn blank_node_labels_are_scoped_to_their_formula() {
    let statements = parse(&format!("{PREFIX}_:x :p {{ _:x :q :r }} ."));
    let Node::Formula(inner) = &statements[0].object else {
        panic!("object is a formula");
    };
    assert_ne!(statements[0].subject, inner[0].subject);
}

#[test]
fn rules_parse_as_log_implies_and_log_implied_by() {
    let statements = parse(&format!(
        "{PREFIX}{{ ?x :p ?y }} => {{ ?y :q ?x }} .\n{{ :a :b :c }} <= {{}} ."
    ));
    assert_eq!(
        statements[0].predicate,
        Node::iri("http://www.w3.org/2000/10/swap/log#implies")
    );
    assert_eq!(
        statements[1].predicate,
        Node::iri("http://www.w3.org/2000/10/swap/log#impliedBy")
    );
}

#[test]
fn sparql_style_directives_and_versions_are_read() {
    let statements = parse("VERSION \"1.2\"\n@prefix : <http://example.org/t#> .\nPREFIX ex: <http://example.org/e#>\n:a ex:p :b .");
    assert_eq!(statements[0].predicate, Node::iri("http://example.org/e#p"));
}

#[test]
fn well_known_prefixes_need_no_declaration() {
    let statements = parse(&format!("{PREFIX}:a rdfs:label \"x\" ."));
    assert_eq!(
        statements[0].predicate,
        Node::iri("http://www.w3.org/2000/01/rdf-schema#label")
    );
}

#[test]
fn an_unknown_prefix_is_a_syntax_error() {
    assert!(matches!(
        parse_document(":a :b :c .", None),
        Err(N3Error::Syntax { .. })
    ));
}

#[test]
fn relative_references_resolve_against_the_base() {
    let base = "http://example.org/a/b/c.n3";
    assert_eq!(resolve_iri(base, "d"), "http://example.org/a/b/d");
    assert_eq!(resolve_iri(base, "../d"), "http://example.org/a/d");
    assert_eq!(resolve_iri(base, "/d"), "http://example.org/d");
    assert_eq!(
        resolve_iri(base, "#frag"),
        "http://example.org/a/b/c.n3#frag"
    );
    assert_eq!(resolve_iri(base, ""), "http://example.org/a/b/c.n3");
    assert_eq!(resolve_iri(base, "urn:x"), "urn:x");
}

#[test]
fn trig_names_graphs_with_log_name_of() {
    let document = n3::parse_trig(
        &format!("{PREFIX}:a :p :b .\n:g {{ :c :p :d }}\nGRAPH :h {{ :e :p :f }}\n{{ :x :p :y }}"),
        None,
    )
    .expect("parses");
    let name_of = Node::iri("http://www.w3.org/2000/10/swap/log#nameOf");
    let named: Vec<&Node> = document
        .statements
        .iter()
        .filter(|s| s.predicate == name_of)
        .map(|s| &s.subject)
        .collect();
    assert_eq!(named, vec![&iri("g"), &iri("h")]);
    assert!(document
        .statements
        .contains(&Statement::new(iri("x"), iri("p"), iri("y"))));
}

#[test]
fn triple_terms_are_quoted_formulas_and_reifiers_reify_them() {
    let document = n3::parse_trig(
        &format!("{PREFIX}:a :says <<( :b :p :c )>> .\n:d :says << :e :p :f >> ."),
        None,
    )
    .expect("parses");
    let quoted = Node::Formula(vec![Statement::new(iri("b"), iri("p"), iri("c"))]);
    assert_eq!(document.statements[0].object, quoted);
    let reifies = Node::iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies");
    assert!(document.statements.iter().any(|s| s.predicate == reifies));
}

#[test]
fn a_message_log_becomes_ordered_envelopes_with_payload_graphs() {
    let log = "VERSION \"1.2-messages\"\nPREFIX : <http://example.org/t#>\n_:b :p 1 .\nMESSAGE\nMESSAGE\n_:b :p 2 .\n";
    assert!(n3::parser::is_message_log(log));
    let document = n3::parse_message_log(log, None).expect("parses");
    let payloads: Vec<&Node> = document
        .statements
        .iter()
        .filter(|s| s.predicate == Node::iri("http://www.w3.org/2000/10/swap/log#nameOf"))
        .map(|s| &s.object)
        .collect();
    assert_eq!(
        payloads.len(),
        2,
        "the empty middle message has no payload graph"
    );
    let blank = |payload: &Node| match payload {
        Node::Formula(statements) => statements[0].subject.clone(),
        other => panic!("payload is a formula: {other:?}"),
    };
    assert_ne!(
        blank(payloads[0]),
        blank(payloads[1]),
        "each message scopes its blank nodes"
    );
}

#[test]
fn quantifiers_bind_iris_to_variables_and_blank_nodes() {
    let statements = parse(&format!(
        "{PREFIX}@forAll :x .\n@forSome :y .\n:x :loves :y ."
    ));
    assert!(matches!(statements[0].subject, Node::Var(_)));
    assert!(matches!(statements[0].object, Node::Blank(_)));
    assert_eq!(statements[0].predicate, iri("loves"));
}

#[test]
fn a_statement_comes_before_those_its_bracketed_object_implies() {
    let statements = parse(&format!("{PREFIX}[ :p :a ] :q [ :r :b ] ."));
    let [first, second, third] = statements.as_slice() else {
        panic!("three statements expected, got {statements:?}");
    };
    assert_eq!(
        (&first.predicate, &second.predicate, &third.predicate),
        (&iri("p"), &iri("q"), &iri("r"))
    );
    assert_eq!(second.subject, first.subject);
    assert_eq!(second.object, third.subject);
}
