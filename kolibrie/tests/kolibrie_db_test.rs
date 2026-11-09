/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! One test per file of `kolibrie/src/database/`, through the public API only, so a split or
//! move that drops a method or breaks a cross-file call shows up here

use kolibrie::database::{KolibrieDB, PlanningStatsPolicy};
use kolibrie::execute_query::execute;
use shared::dataset_index::GraphId;
use shared::quad_source::QuadSource;
use std::collections::HashMap;

const NTRIPLES: &str =
    "<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .\n\
<http://example.org/bob> <http://example.org/name> \"Bob\" .\n";

const NQUADS: &str = "<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n\
<http://example.org/c> <http://example.org/p> <http://example.org/d> <http://example.org/g> .\n";

const RDF_XML: &str = r#"<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/alice">
    <ex:name>Alice</ex:name>
    <ex:knows rdf:resource="http://example.org/bob"/>
  </rdf:Description>
</rdf:RDF>
"#;

#[test]
fn prelude_exports_the_renamed_store() {
    use kolibrie::prelude::*;
    let db = KolibrieDB::new();
    assert_eq!(db.default_triple_count(), 0);
}

/// `terms.rs`
#[test]
fn terms_encode_decode_and_share() {
    let db = KolibrieDB::new();

    let plain = db.encode_term_star("<http://example.org/a>");
    assert_eq!(
        db.decode_any(plain).as_deref(),
        Some("http://example.org/a")
    );
    assert_eq!(db.dictionary().decode(plain), Some("http://example.org/a"));

    let quoted = db.encode_term_star(
        "<< <http://example.org/a> <http://example.org/p> <http://example.org/b> >>",
    );
    assert_ne!(quoted, plain);
    assert_eq!(db.quoted_triples().len(), 1);
    assert!(db
        .decode_any(quoted)
        .unwrap()
        .contains("http://example.org/p"));

    let (s, p, o) = KolibrieDB::split_quoted_triple_content("<urn:s> <urn:p> \"x y\"");
    assert_eq!(
        (s.as_str(), p.as_str(), o.as_str()),
        ("<urn:s>", "<urn:p>", "\"x y\"")
    );

    let mut other = KolibrieDB::new();
    assert!(!db.shares_dictionary_with(&other));
    other.adopt_terms(db.terms());
    assert!(db.shares_dictionary_with(&other));
    assert!(db.shares_quoted_triples_with(&db.clone()));
}

/// `prefixes.rs`
#[test]
fn prefixes_bind_and_resolve() {
    let mut db = KolibrieDB::new();
    db.add_prefix("ex", "http://example.org/");
    assert_eq!(
        db.resolve_query_term("ex:alice", &HashMap::new()),
        "http://example.org/alice"
    );

    db.register_prefixes_from_query(
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT ?s WHERE { }",
    );
    assert_eq!(
        db.prefixes().get("foaf").map(String::as_str),
        Some("http://xmlns.com/foaf/0.1/")
    );

    let mut copied = HashMap::new();
    db.share_prefixes_with(&mut copied);
    assert_eq!(copied.len(), 2);

    db.set_prefixes(HashMap::new());
    assert!(db.prefixes().is_empty());
}

/// `dataset.rs`
#[test]
fn dataset_add_delete_count_and_union() {
    let mut db = KolibrieDB::new();
    db.add_triple_parts("urn:s", "urn:p", "urn:o");
    db.add_triple_parts("urn:s", "urn:p", "urn:o2");
    assert_eq!(db.default_triple_count(), 2);
    assert!(db.delete_triple_parts("urn:s", "urn:p", "urn:o2"));
    assert_eq!(db.default_triple_count(), 1);

    assert!(db.add_quad_parts("urn:s", "urn:p", "urn:o", "urn:g"));
    assert_eq!(db.quad_count(), 2);
    assert_eq!(db.named_graphs().len(), 1);
    assert_eq!(QuadSource::named_graphs(&db).len(), 1);

    let mut other = KolibrieDB::new();
    other.add_triple_parts("urn:x", "urn:p", "urn:y");
    let merged = db.union(&other);
    assert_eq!(merged.default_triple_count(), 2);
    assert_eq!(merged.quad_count(), 3);

    db.build_all_indexes();
    assert_eq!(db.quad_count(), 2);
    db.clear_dataset();
    assert_eq!(db.quad_count(), 0);
}

/// `planning_stats.rs`
#[test]
fn planning_stats_follow_the_policy() {
    let mut db = KolibrieDB::new();
    db.add_triple_parts("urn:s", "urn:p", "urn:o");
    assert_eq!(db.get_or_build_stats().total_triples, 1);

    db.set_planning_stats_policy(PlanningStatsPolicy::AlwaysFresh);
    assert_eq!(db.planning_stats_policy(), PlanningStatsPolicy::AlwaysFresh);
    db.add_triple_parts("urn:s", "urn:p", "urn:o2");
    assert_eq!(db.get_or_build_planning_stats().total_triples, 2);

    let before = db.stats_rebuild_count();
    db.invalidate_stats_cache();
    db.get_or_build_planning_stats();
    assert!(db.stats_rebuild_count() > before);
    assert_eq!(db.stats_rebuild_metrics().0, db.stats_rebuild_count());
}

/// `parse_ntriples.rs` and `serialize.rs`
#[test]
fn ntriples_and_nquads_round_trip() {
    let mut db = KolibrieDB::new();
    db.parse_ntriples_and_add(NTRIPLES);
    assert_eq!(db.default_triple_count(), 2);

    let mut back = KolibrieDB::new();
    back.parse_ntriples_and_add(&db.generate_ntriples());
    assert_eq!(back.default_triple_count(), 2);

    let encoded = KolibrieDB::new().parse_and_encode_ntriples(NTRIPLES);
    assert_eq!(encoded.len(), 2);

    let mut quads = KolibrieDB::new();
    quads.parse_nquads_and_add(NQUADS);
    assert_eq!(quads.quad_count(), 2);
    assert_eq!(quads.default_triple_count(), 1);
    assert!(quads.graphs().contains(&GraphId::Default));

    let mut quads_back = KolibrieDB::new();
    quads_back.parse_nquads_and_add(&quads.generate_nquads());
    assert_eq!(quads_back.quad_count(), 2);
    assert_eq!(quads_back.named_graphs().len(), 1);
}

/// `parse_turtle.rs`, `serialize.rs`, and a query over the result
#[test]
fn turtle_parses_and_is_queryable() {
    let mut db = KolibrieDB::new();
    db.parse_turtle(
        "@prefix ex: <http://example.org/> .\n\
         ex:alice ex:knows ex:bob , ex:carol ; ex:name \"Alice\" .\n",
    );
    assert_eq!(db.default_triple_count(), 3);

    let turtle = db.generate_turtle();
    assert!(turtle.contains("@prefix ex: <http://example.org/> ."));
    assert!(turtle.contains("<http://example.org/alice>"));
    assert!(db.generate_rdf_xml().contains("rdf:Description"));

    let mut rows = execute(
        "SELECT ?o WHERE { <http://example.org/alice> <http://example.org/knows> ?o . }",
        &mut db,
    )
    .expect("query should run")
    .into_rows();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec!["http://example.org/bob".to_string()],
            vec!["http://example.org/carol".to_string()],
        ]
    );
}

/// `parse_n3.rs`
#[test]
fn n3_parses_with_prefixes() {
    let mut db = KolibrieDB::new();
    db.parse_n3("@prefix ex: <http://example.org/> .\nex:a ex:p ex:b .\nex:b ex:p ex:c .\n");
    assert_eq!(db.default_triple_count(), 2);
    assert_eq!(
        db.prefixes().get("ex").map(String::as_str),
        Some("http://example.org/")
    );
}

/// `parse_rdf_xml.rs`
#[test]
fn rdf_xml_parses_from_string_and_file() {
    let mut db = KolibrieDB::new();
    db.parse_rdf(RDF_XML);
    assert_eq!(db.default_triple_count(), 2);

    let path = std::env::temp_dir().join(format!("kolibrie_db_test_{}.rdf", std::process::id()));
    std::fs::write(&path, RDF_XML).unwrap();
    let mut from_file = KolibrieDB::new();
    from_file.parse_rdf_from_file(path.to_str().unwrap());
    let _ = std::fs::remove_file(&path);
    assert_eq!(from_file.default_triple_count(), 2);
}

/// `extensions.rs`
#[test]
fn extensions_hold_seeds_and_probabilities() {
    let mut db = KolibrieDB::new();
    assert_eq!(db.training_seed(), None);
    db.set_training_seed(Some(7));
    assert_eq!(db.training_seed(), Some(7));

    db.add_tagged_triple("urn:s", "urn:p", "urn:o", 0.25);
    assert_eq!(
        db.probability_seeds().values().copied().collect::<Vec<_>>(),
        vec![0.25]
    );

    db.register_udf("echo", |args: Vec<&str>| args.join(""));
    db.add_rule_mapping("r", "http://example.org/rule");
    assert!(db.model_decls().is_empty());
    assert!(db.neural_relation_decls().is_empty());
    assert!(db.take_neural_materialized("urn:p").is_none());
}
