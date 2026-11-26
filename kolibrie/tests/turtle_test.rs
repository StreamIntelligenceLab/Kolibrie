/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reading Turtle: every abbreviation stores the triples its N-Triples spelling stores

use kolibrie::database::KolibrieDB;

/// The store's default-graph triples as decoded spellings, sorted
fn spelled(database: &KolibrieDB) -> Vec<[String; 3]> {
    let dictionary = database.dictionary();
    let mut triples: Vec<[String; 3]> = database
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| {
            [triple.subject, triple.predicate, triple.object]
                .map(|id| dictionary.decode(id).unwrap_or("<undecodable>").to_string())
        })
        .collect();
    triples.sort();
    triples
}

fn from_turtle(turtle: &str) -> Vec<[String; 3]> {
    let mut database = KolibrieDB::new();
    database
        .try_parse_turtle(turtle)
        .expect("the Turtle should parse");
    spelled(&database)
}

fn from_ntriples(ntriples: &str) -> Vec<[String; 3]> {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add(ntriples);
    spelled(&database)
}

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

#[test]
fn a_statement_continues_across_lines() {
    let turtle = "@prefix ex: <http://example.org/> .\n\
                  ex:alice ex:knows ex:bob ,\n\
                      ex:carol ;\n\
                  \x20   ex:name \"Alice\"@en .\n";
    let ntriples = "<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .\n\
                    <http://example.org/alice> <http://example.org/knows> <http://example.org/carol> .\n\
                    <http://example.org/alice> <http://example.org/name> \"Alice\"@en .\n";
    assert_eq!(from_turtle(turtle), from_ntriples(ntriples));
}

#[test]
fn the_keyword_a_is_rdf_type() {
    let turtle = "<urn:alice> a <urn:Student> .\n";
    let ntriples = format!("<urn:alice> <{RDF_TYPE}> <urn:Student> .\n");
    assert_eq!(from_turtle(turtle), from_ntriples(&ntriples));
}

#[test]
fn numbers_and_booleans_keep_their_datatypes() {
    let turtle = "<urn:s> <urn:i> 47 ; <urn:d> 1.5 ; <urn:e> 1e3 ; <urn:b> true .\n";
    let ntriples = format!(
        "<urn:s> <urn:i> \"47\"^^<{XSD}integer> .\n\
         <urn:s> <urn:d> \"1.5\"^^<{XSD}decimal> .\n\
         <urn:s> <urn:e> \"1e3\"^^<{XSD}double> .\n\
         <urn:s> <urn:b> \"true\"^^<{XSD}boolean> .\n"
    );
    assert_eq!(from_turtle(turtle), from_ntriples(&ntriples));
}

#[test]
fn a_directive_and_a_statement_can_share_a_line() {
    let turtle = "@prefix ex: <http://example.org/> . ex:a ex:p ex:b .";
    let ntriples = "<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n";
    assert_eq!(from_turtle(turtle), from_ntriples(ntriples));
}

#[test]
fn long_strings_and_comments_are_read_whole() {
    let turtle = "<urn:s> <urn:long> \"\"\"two\nlines\"\"\" ; # a comment\n\
                  <urn:hash> \"a # inside a string\" .\n";
    let ntriples = "<urn:s> <urn:long> \"two\\nlines\" .\n\
                    <urn:s> <urn:hash> \"a # inside a string\" .\n";
    assert_eq!(from_turtle(turtle), from_ntriples(ntriples));
}

#[test]
fn prefixes_from_an_earlier_document_still_resolve() {
    let mut database = KolibrieDB::new();
    database
        .try_parse_turtle("@prefix ex: <http://example.org/> .\n")
        .expect("the prefix should parse");
    database
        .try_parse_turtle("ex:a ex:p ex:b .\n")
        .expect("the statement should parse");
    let ntriples = "<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n";
    assert_eq!(spelled(&database), from_ntriples(ntriples));
}

#[test]
fn a_blank_node_property_list_shares_one_node() {
    let triples = from_turtle("[ <urn:p> <urn:o> ] <urn:q> <urn:r> .\n");
    assert_eq!(triples.len(), 2, "{triples:?}");
    assert_eq!(
        triples[0][0], triples[1][0],
        "both triples are about one node"
    );
    assert!(triples[0][0].starts_with("_:"), "{triples:?}");
}

#[test]
fn a_collection_becomes_a_first_rest_chain() {
    let triples = from_turtle("<urn:s> <urn:list> ( <urn:a> <urn:b> ) .\n");
    let first = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
    let rest = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
    let nil = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
    let object_of = |subject: &str, predicate: &str| {
        triples
            .iter()
            .find(|[s, p, _]| s == subject && p == predicate)
            .map(|[_, _, o]| o.clone())
            .expect("the chain triple")
    };
    assert_eq!(triples.len(), 5, "{triples:?}");
    let head = object_of("urn:s", "urn:list");
    assert_eq!(object_of(&head, first), "urn:a");
    let second = object_of(&head, rest);
    assert_eq!(object_of(&second, first), "urn:b");
    assert_eq!(object_of(&second, rest), nil);
}

#[test]
fn an_annotation_is_about_the_quoted_triple() {
    let mut database = KolibrieDB::new();
    let added = database
        .try_parse_turtle("<urn:a> <urn:p> <urn:b> {| <urn:source> <urn:c> |} .\n")
        .expect("the annotation should parse");
    assert_eq!(added, 2);
    let quoted = database.quoted_triples();
    assert_eq!(quoted.len(), 1, "the annotated triple is quoted once");
}

#[test]
fn a_malformed_document_adds_nothing() {
    let mut database = KolibrieDB::new();
    let result = database.try_parse_turtle("<urn:a> <urn:p> <urn:b> .\n<urn:c> <urn:p> .\n");
    assert!(result.is_err());
    assert!(spelled(&database).is_empty());
}

#[test]
fn a_variable_is_not_turtle() {
    let mut database = KolibrieDB::new();
    assert!(database.try_parse_turtle("?x <urn:p> <urn:b> .\n").is_err());
    assert!(spelled(&database).is_empty());
}
