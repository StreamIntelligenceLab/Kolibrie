/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::execute_query::execute_query_rayon_parallel2_volcano;
use shared::quoted_triple_store::{QuotedTripleStore, is_quoted_triple_id};

// QuotedTripleStore unit tests
#[test]
fn test_quoted_triple_store_encode_decode() {
    let mut store = QuotedTripleStore::new();
    let id = store.encode(1, 2, 3);
    assert!(is_quoted_triple_id(id));
    assert_eq!(store.decode(id), Some((1, 2, 3)));
}

#[test]
fn test_quoted_triple_store_deduplication() {
    let mut store = QuotedTripleStore::new();
    let id1 = store.encode(10, 20, 30);
    let id2 = store.encode(10, 20, 30);
    assert_eq!(id1, id2);
    assert_eq!(store.len(), 1);
}

#[test]
fn test_quoted_triple_store_nested() {
    let mut store = QuotedTripleStore::new();
    let inner = store.encode(1, 2, 3);
    let outer = store.encode(inner, 4, 5);
    assert_ne!(inner, outer);
    let (s, p, o) = store.decode(outer).unwrap();
    assert_eq!(s, inner);
    assert_eq!(p, 4);
    assert_eq!(o, 5);
}

// N-Triples-star parsing tests
#[test]
fn test_ntriples_star_basic_quoted_triple() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/emp38> <http://example.org/jobTitle> <http://example.org/AssistantDesigner> >> <http://example.org/statedBy> <http://example.org/emp22> .
"#;
    db.parse_ntriples_and_add(ntriples);

    // The outer triple should exist: qt_id :statedBy :emp22
    assert!(!db.triples.is_empty(), "Should have parsed the outer triple");

    // The quoted triple store should have one entry
    let qt = db.quoted_triple_store.read().unwrap();
    assert_eq!(qt.len(), 1, "Should have one quoted triple");
}

#[test]
fn test_ntriples_star_multiple_quoted_triples() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/a> <http://example.org/b> <http://example.org/c> >> <http://example.org/source> <http://example.org/x> .
<< <http://example.org/d> <http://example.org/e> <http://example.org/f> >> <http://example.org/source> <http://example.org/y> .
"#;
    db.parse_ntriples_and_add(ntriples);
    assert_eq!(db.triples.len(), 2, "Should have two outer triples");
    let qt = db.quoted_triple_store.read().unwrap();
    assert_eq!(qt.len(), 2, "Should have two quoted triples");
}

// Turtle-star parsing tests
#[test]
fn test_turtle_star_basic() {
    let mut db = SparqlDatabase::new();
    let turtle = r#"<< <http://example.org/emp38> <http://example.org/jobTitle> <http://example.org/AssistantDesigner> >> <http://example.org/statedBy> <http://example.org/emp22> ."#;
    db.parse_turtle(turtle);

    assert!(!db.triples.is_empty(), "Should have parsed at least one triple");
    let qt = db.quoted_triple_store.read().unwrap();
    assert_eq!(qt.len(), 1, "Should have one quoted triple");
}

// Dictionary decode_term tests
#[test]
fn test_dictionary_decode_term_with_quoted_triple() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/a> <http://example.org/b> <http://example.org/c> >> <http://example.org/p> <http://example.org/x> .
"#;
    db.parse_ntriples_and_add(ntriples);

    // Get the quoted triple ID
    let qt = db.quoted_triple_store.read().unwrap();
    let qt_id = *qt.components_to_id.values().next().unwrap();
    let dict = db.dictionary.read().unwrap();
    let decoded = dict.decode_term(qt_id, &qt).unwrap();
    assert!(decoded.starts_with("<<"), "Decoded should start with <<: {}", decoded);
    assert!(decoded.ends_with(">>"), "Decoded should end with >>: {}", decoded);
    assert!(decoded.contains("http://example.org/a"), "Should contain subject: {}", decoded);
    assert!(decoded.contains("http://example.org/b"), "Should contain predicate: {}", decoded);
    assert!(decoded.contains("http://example.org/c"), "Should contain object: {}", decoded);
}

// SPARQL-star query tests (via Streamertail optimizer)
#[test]
fn test_sparql_star_query_constant_quoted_triple() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/emp38> <http://example.org/jobTitle> <http://example.org/AssistantDesigner> >> <http://example.org/statedBy> <http://example.org/emp22> .
"#;
    db.parse_ntriples_and_add(ntriples);
    db.get_or_build_stats();

    let query = r#"
        SELECT ?who WHERE {
            << <http://example.org/emp38> <http://example.org/jobTitle> <http://example.org/AssistantDesigner> >> <http://example.org/statedBy> ?who .
        }
    "#;

    let results = execute_query_rayon_parallel2_volcano(query, &mut db);
    assert!(!results.is_empty(), "Should return at least one result, got: {:?}", results);
    assert_eq!(results[0][0], "http://example.org/emp22");
}

#[test]
fn test_sparql_star_query_variable_in_quoted_triple() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/emp38> <http://example.org/jobTitle> <http://example.org/AssistantDesigner> >> <http://example.org/statedBy> <http://example.org/emp22> .
<< <http://example.org/emp39> <http://example.org/jobTitle> <http://example.org/Designer> >> <http://example.org/statedBy> <http://example.org/emp23> .
"#;
    db.parse_ntriples_and_add(ntriples);
    db.get_or_build_stats();

    // Query with variable inside quoted triple
    let query = r#"
        SELECT ?emp ?title ?who WHERE {
            << ?emp <http://example.org/jobTitle> ?title >> <http://example.org/statedBy> ?who .
        }
    "#;

    let results = execute_query_rayon_parallel2_volcano(query, &mut db);
    assert_eq!(results.len(), 2, "Should return two results, got: {:?}", results);
}

// Debug: verify parser output for quoted triples in WHERE
#[test]
fn test_sparql_parser_quoted_triple_pattern() {
    use kolibrie::parser::parse_where;
    let input = r#"WHERE { << <http://example.org/emp38> <http://example.org/jobTitle> <http://example.org/AssistantDesigner> >> <http://example.org/statedBy> ?who . }"#;
    let result = parse_where(input);
    assert!(result.is_ok(), "Should parse WHERE with quoted triple: {:?}", result);
    let (_, (patterns, _, _, _, _, _)) = result.unwrap();
    assert_eq!(patterns.len(), 1, "Should have one pattern");
    let (s, p, o) = &patterns[0];
    println!("Subject: '{}'", s);
    println!("Predicate: '{}'", p);
    println!("Object: '{}'", o);
    assert!(s.starts_with("<<"), "Subject should start with <<: '{}'", s);
    assert!(s.ends_with(">>"), "Subject should end with >>: '{}'", s);
}

// Parser combinator tests for parse_quoted_triple
#[test]
fn test_parse_quoted_triple_simple() {
    use kolibrie::parser::parse_quoted_triple;
    let input = "<< <http://example.org/a> <http://example.org/b> <http://example.org/c> >> rest";
    let result = parse_quoted_triple(input);
    assert!(result.is_ok(), "Should parse quoted triple: {:?}", result);
    let (remaining, matched) = result.unwrap();
    assert_eq!(remaining, " rest");
    assert!(matched.starts_with("<<"));
    assert!(matched.ends_with(">>"));
}

#[test]
fn test_parse_quoted_triple_with_variable() {
    use kolibrie::parser::parse_quoted_triple;
    let input = "<< ?s <http://example.org/p> ?o >>";
    let result = parse_quoted_triple(input);
    assert!(result.is_ok(), "Should parse quoted triple with variables: {:?}", result);
}

#[test]
fn test_parse_quoted_triple_nested() {
    use kolibrie::parser::parse_quoted_triple;
    let input = "<< << <http://example.org/a> <http://example.org/b> <http://example.org/c> >> <http://example.org/d> <http://example.org/e> >>";
    let result = parse_quoted_triple(input);
    assert!(result.is_ok(), "Should parse nested quoted triple: {:?}", result);
}

#[test]
fn test_parse_triple_block_with_quoted_subject() {
    use kolibrie::parser::parse_triple_block;
    let input = "<< <http://example.org/a> <http://example.org/b> <http://example.org/c> >> <http://example.org/d> <http://example.org/e>";
    let result = parse_triple_block(input);
    assert!(result.is_ok(), "Should parse triple block with quoted subject: {:?}", result);
    let (_, triples) = result.unwrap();
    assert_eq!(triples.len(), 1);
    assert!(triples[0].0.starts_with("<<"), "Subject should be a quoted triple");
}

// SparqlDatabase helper tests
#[test]
fn test_split_quoted_triple_content() {
    let content = "<http://example.org/a> <http://example.org/b> <http://example.org/c>";
    let (s, p, o) = SparqlDatabase::split_quoted_triple_content(content);
    assert_eq!(s, "<http://example.org/a>");
    assert_eq!(p, "<http://example.org/b>");
    assert_eq!(o, "<http://example.org/c>");
}

#[test]
fn test_split_quoted_triple_content_nested() {
    let content = "<< <http://a> <http://b> <http://c> >> <http://d> <http://e>";
    let (s, p, o) = SparqlDatabase::split_quoted_triple_content(content);
    assert_eq!(s, "<< <http://a> <http://b> <http://c> >>");
    assert_eq!(p, "<http://d>");
    assert_eq!(o, "<http://e>");
}

#[test]
fn test_encode_term_star_basic() {
    let db = SparqlDatabase::new();
    let id = db.encode_term_star("http://example.org/foo");
    assert!(!is_quoted_triple_id(id));
}

#[test]
fn test_encode_term_star_quoted_triple() {
    let db = SparqlDatabase::new();
    let id = db.encode_term_star("<< http://a http://b http://c >>");
    assert!(is_quoted_triple_id(id));

    let qt = db.quoted_triple_store.read().unwrap();
    let (s, p, o) = qt.decode(id).unwrap();
    let dict = db.dictionary.read().unwrap();
    assert_eq!(dict.decode(s).unwrap(), "http://a");
    assert_eq!(dict.decode(p).unwrap(), "http://b");
    assert_eq!(dict.decode(o).unwrap(), "http://c");
}

#[test]
fn test_decode_any_regular() {
    let db = SparqlDatabase::new();
    let id = db.encode_term_star("http://example.org/test");
    let decoded = db.decode_any(id).unwrap();
    assert_eq!(decoded, "http://example.org/test");
}

#[test]
fn test_decode_any_quoted_triple() {
    let db = SparqlDatabase::new();
    let id = db.encode_term_star("<< http://a http://b http://c >>");
    let decoded = db.decode_any(id).unwrap();
    assert!(decoded.contains("http://a"));
    assert!(decoded.contains("http://b"));
    assert!(decoded.contains("http://c"));
    assert!(decoded.starts_with("<<"));
    assert!(decoded.ends_with(">>"));
}

// Built-in SPARQL-star function tests
#[test]
fn test_parse_function_call_istriple() {
    use kolibrie::parser::parse_filter;
    let input = "FILTER(isTRIPLE(?x))";
    let result = parse_filter(input);
    assert!(result.is_ok(), "Should parse FILTER(isTRIPLE(?x))");
}

#[test]
fn test_parse_function_call_subject() {
    use kolibrie::parser::parse_filter;
    let input = "FILTER(SUBJECT(?t))";
    let result = parse_filter(input);
    // SUBJECT returns a value, not a boolean, but it should still parse
    assert!(result.is_ok(), "Should parse FILTER(SUBJECT(?t))");
}

#[test]
fn test_bind_subject_predicate_object() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> >> <http://example.org/source> <http://example.org/doc1> .
"#;
    db.parse_ntriples_and_add(ntriples);
    let triples_vec: Vec<_> = db.triples.iter().cloned().collect();
    db.index_manager.build_from_triples(&triples_vec);
    db.get_or_build_stats();

    // Query using BIND(SUBJECT(?t) AS ?s)
    let query = r#"
        SELECT ?t ?s WHERE {
            ?t <http://example.org/source> <http://example.org/doc1> .
            BIND(SUBJECT(?t) AS ?s)
        }
    "#;
    let results = execute_query_rayon_parallel2_volcano(query, &mut db);
    assert_eq!(results.len(), 1, "Should return one result");
    // The subject of the quoted triple should be alice
    let row = &results[0];
    let has_alice = row.iter().any(|v| v.contains("alice"));
    assert!(has_alice, "SUBJECT should extract alice, got: {:?}", row);
}

#[test]
fn test_bind_triple_constructor() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
"#;
    db.parse_ntriples_and_add(ntriples);
    let triples_vec: Vec<_> = db.triples.iter().cloned().collect();
    db.index_manager.build_from_triples(&triples_vec);
    db.get_or_build_stats();

    // Use BIND(TRIPLE(...) AS ?t) to construct a quoted triple
    let query = r#"
        SELECT ?s ?p ?o ?t WHERE {
            ?s ?p ?o .
            BIND(TRIPLE(?s, ?p, ?o) AS ?t)
        }
    "#;
    let results = execute_query_rayon_parallel2_volcano(query, &mut db);
    assert_eq!(results.len(), 1, "Should return one result");
    // The constructed triple should contain <<
    let row = &results[0];
    let has_qt = row.iter().any(|v| v.contains("<<"));
    assert!(has_qt, "TRIPLE should construct a quoted triple, got: {:?}", row);
}

// INSERT/DELETE tests
#[test]
fn test_insert_quoted_triple() {
    let mut db = SparqlDatabase::new();

    // Use handle_update for INSERT
    let result = db.handle_update(r#"INSERT { << <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> >> <http://example.org/source> <http://example.org/doc1> . }"#);
    assert_eq!(result, "Update Successful");

    // Verify the triple was inserted
    assert!(!db.triples.is_empty(), "Triple should have been inserted");

    // Verify the quoted triple store has an entry
    let qt_store = db.quoted_triple_store.read().unwrap();
    assert!(!qt_store.is_empty(), "QuotedTripleStore should have an entry");
}

#[test]
fn test_insert_normal_triple() {
    let mut db = SparqlDatabase::new();

    let result = db.handle_update(r#"INSERT { <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> . }"#);
    assert_eq!(result, "Update Successful");
    assert_eq!(db.triples.len(), 1, "Should have inserted one triple");
}

#[test]
fn test_delete_basic() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/name> "Alice" .
"#;
    db.parse_ntriples_and_add(ntriples);
    assert_eq!(db.triples.len(), 2, "Should start with 2 triples");

    // Use handle_update for simple DELETE
    let result = db.handle_update(r#"DELETE { <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> . }"#);
    assert_eq!(result, "Update Successful");
    assert_eq!(db.triples.len(), 1, "Should have 1 triple after delete");
}

#[test]
fn test_delete_quoted_triple() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> >> <http://example.org/source> <http://example.org/doc1> .
"#;
    db.parse_ntriples_and_add(ntriples);
    assert_eq!(db.triples.len(), 1, "Should start with 1 triple");

    let result = db.handle_update(r#"DELETE { << <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> >> <http://example.org/source> <http://example.org/doc1> . }"#);
    assert_eq!(result, "Update Successful");
    assert_eq!(db.triples.len(), 0, "Should have 0 triples after delete");
}

#[test]
fn test_delete_where() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/knows> <http://example.org/carol> .
<http://example.org/alice> <http://example.org/name> "Alice" .
"#;
    db.parse_ntriples_and_add(ntriples);
    let triples_vec: Vec<_> = db.triples.iter().cloned().collect();
    db.index_manager.build_from_triples(&triples_vec);
    db.get_or_build_stats();
    assert_eq!(db.triples.len(), 3, "Should start with 3 triples");

    // DELETE WHERE — delete all "knows" triples
    let delete_query = r#"
        DELETE { ?s <http://example.org/knows> ?o . }
        WHERE { ?s <http://example.org/knows> ?o . }
    "#;
    execute_query_rayon_parallel2_volcano(delete_query, &mut db);
    assert_eq!(db.triples.len(), 1, "Should have 1 triple after DELETE WHERE, got {}", db.triples.len());
}

// Serialization tests
#[test]
fn test_generate_ntriples_basic() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
"#;
    db.parse_ntriples_and_add(ntriples);

    let output = db.generate_ntriples();
    assert!(output.contains("<http://example.org/alice>"), "Should contain subject URI");
    assert!(output.contains("<http://example.org/knows>"), "Should contain predicate URI");
    assert!(output.contains("http://example.org/bob"), "Should contain object URI");
    assert!(output.ends_with(".\n"), "Should end with dot-newline");
}

#[test]
fn test_generate_ntriples_star() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> >> <http://example.org/source> <http://example.org/doc1> .
"#;
    db.parse_ntriples_and_add(ntriples);

    let output = db.generate_ntriples();
    assert!(output.contains("<<"), "Should contain quoted triple opening");
    assert!(output.contains(">>"), "Should contain quoted triple closing");
    assert!(output.contains("http://example.org/alice"), "Should contain inner subject");
    assert!(output.contains("<http://example.org/source>"), "Should contain outer predicate");
}

#[test]
fn test_generate_turtle_basic() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/alice> <http://example.org/name> "Alice" .
"#;
    db.parse_ntriples_and_add(ntriples);

    let output = db.generate_turtle();
    // Should group by subject — alice appears once as subject, with ; separating predicates
    let alice_count = output.matches("<http://example.org/alice>").count();
    assert_eq!(alice_count, 1, "Subject should appear once (grouped), got output:\n{}", output);
    assert!(output.contains(";"), "Should use ; to separate predicates");
    assert!(output.contains("."), "Should end statements with .");
}

#[test]
fn test_generate_turtle_star() {
    let mut db = SparqlDatabase::new();
    let ntriples = r#"<< <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> >> <http://example.org/source> <http://example.org/doc1> .
"#;
    db.parse_ntriples_and_add(ntriples);

    let output = db.generate_turtle();
    assert!(output.contains("<<"), "Should render quoted triple subject");
    assert!(output.contains(">>"), "Should render quoted triple closing");
}

// handle_update with proper parsers
#[test]
fn test_handle_update_insert() {
    let mut db = SparqlDatabase::new();
    let result = db.handle_update(r#"INSERT { <http://example.org/a> <http://example.org/b> <http://example.org/c> . }"#);
    assert_eq!(result, "Update Successful");
    assert_eq!(db.triples.len(), 1);
}

#[test]
fn test_handle_update_delete() {
    let mut db = SparqlDatabase::new();
    db.handle_update(r#"INSERT { <http://example.org/a> <http://example.org/b> <http://example.org/c> . }"#);
    assert_eq!(db.triples.len(), 1);

    let result = db.handle_update(r#"DELETE { <http://example.org/a> <http://example.org/b> <http://example.org/c> . }"#);
    assert_eq!(result, "Update Successful");
    assert_eq!(db.triples.len(), 0);
}

#[test]
fn test_handle_update_insert_quoted_triple() {
    let mut db = SparqlDatabase::new();
    let result = db.handle_update(r#"INSERT { << <http://example.org/a> <http://example.org/b> <http://example.org/c> >> <http://example.org/src> <http://example.org/d> . }"#);
    assert_eq!(result, "Update Successful");
    assert_eq!(db.triples.len(), 1);
    let qt_store = db.quoted_triple_store.read().unwrap();
    assert!(!qt_store.is_empty(), "QuotedTripleStore should have entry");
}
