/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! When two RDF terms are the same term, and when they only look alike

use kolibrie::database::KolibrieDB;

const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// Parses N-Triples and returns the object term of `<urn:s{n}>`, as the store kept it
fn object_terms(ntriples: &str) -> Vec<String> {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add(ntriples);
    let dictionary = database.dictionary();
    let mut objects: Vec<(u32, String)> = database
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| {
            (
                triple.subject,
                dictionary
                    .decode(triple.object)
                    .unwrap_or("<undecodable>")
                    .to_string(),
            )
        })
        .collect();
    objects.sort_by_key(|(subject, _)| *subject);
    objects.into_iter().map(|(_, object)| object).collect()
}

#[test]
fn a_typed_literal_is_not_the_same_term_as_its_lexical_form() {
    let objects = object_terms(&format!(
        "<urn:s1> <urn:p> \"25\"^^<{XSD_INTEGER}> .\n<urn:s2> <urn:p> \"25\" .\n"
    ));
    assert_eq!(objects.len(), 2, "both triples should be stored");
    assert_ne!(
        objects[0], objects[1],
        "a literal typed xsd:integer and a plain literal with the same lexical form \
         are different RDF terms, so they must not intern to one term"
    );
}

#[test]
fn two_literals_differing_only_in_datatype_are_distinct() {
    let objects = object_terms(&format!(
        "<urn:s1> <urn:p> \"25\"^^<{XSD_INTEGER}> .\n<urn:s2> <urn:p> \"25\"^^<{XSD_STRING}> .\n"
    ));
    assert_eq!(objects.len(), 2, "both triples should be stored");
    assert_ne!(
        objects[0], objects[1],
        "xsd:integer and xsd:string literals sharing a lexical form are different terms"
    );
}

#[test]
fn a_datatype_survives_a_round_trip() {
    let objects = object_terms(&format!("<urn:s1> <urn:p> \"25\"^^<{XSD_INTEGER}> .\n"));
    assert_eq!(objects.len(), 1);
    assert!(
        objects[0].contains(XSD_INTEGER),
        "the datatype IRI must still be reachable from the stored term, got {:?}",
        objects[0]
    );
}

#[test]
fn a_language_tag_is_not_part_of_the_lexical_form() {
    let objects =
        object_terms("<urn:s1> <urn:p> \"hello\"@en .\n<urn:s2> <urn:p> \"hello@en\" .\n");
    assert_eq!(objects.len(), 2, "both triples should be stored");
    assert_ne!(
        objects[0], objects[1],
        "a language-tagged literal and a plain literal spelled the same way are \
         different RDF terms"
    );
    assert!(
        objects[0].ends_with("@en"),
        "the language tag must survive, got {:?}",
        objects[0]
    );
}

fn query_one(ntriples: &str, sparql: &str) -> Vec<Vec<String>> {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add(ntriples);
    kolibrie::execute_query::execute(sparql, &mut database)
        .expect("query should run")
        .into_rows()
}

/// Three ages, written as `xsd:integer` literals the way a real graph would
fn typed_ages() -> String {
    format!(
        "<urn:a> <urn:age> \"30\"^^<{XSD_INTEGER}> .\n\
         <urn:b> <urn:age> \"7\"^^<{XSD_INTEGER}> .\n\
         <urn:c> <urn:age> \"21\"^^<{XSD_INTEGER}> .\n"
    )
}

#[test]
fn a_typed_integer_sums_as_a_number() {
    let rows = query_one(
        &typed_ages(),
        "SELECT SUM(?age) WHERE { ?s <urn:age> ?age }",
    );
    assert_eq!(
        rows,
        vec![vec!["58".to_string()]],
        "30 + 7 + 21; a typed integer must be read through its lexical form"
    );
}

#[test]
fn a_typed_integer_orders_as_a_number() {
    let rows = query_one(
        &typed_ages(),
        "SELECT ?age WHERE { ?s <urn:age> ?age } ORDER BY ?age",
    );
    let ordered: Vec<&str> = rows.iter().map(|row| row[0].as_str()).collect();
    assert_eq!(
        ordered.len(),
        3,
        "all three ages should come back, got {ordered:?}"
    );
    let numbers: Vec<f64> = ordered
        .iter()
        .map(|term| {
            shared::rdf_term::numeric_value(term)
                .unwrap_or_else(|| panic!("{term:?} should denote a number"))
        })
        .collect();
    assert_eq!(numbers, vec![7.0, 21.0, 30.0], "from {ordered:?}");
}

#[test]
fn a_typed_integer_filters_as_a_number() {
    let rows = query_one(
        &typed_ages(),
        "SELECT ?age WHERE { ?s <urn:age> ?age . FILTER(?age > 10) }",
    );
    let mut numbers: Vec<f64> = rows
        .iter()
        .map(|row| {
            shared::rdf_term::numeric_value(&row[0])
                .unwrap_or_else(|| panic!("{:?} should denote a number", row[0]))
        })
        .collect();
    numbers.sort_by(f64::total_cmp);
    assert_eq!(numbers, vec![21.0, 30.0], "7 is below the threshold");
}

#[test]
fn arithmetic_over_a_typed_integer_agrees_with_comparison() {
    let rows = query_one(
        &typed_ages(),
        "SELECT ?age WHERE { ?s <urn:age> ?age . FILTER(?age + 1 > 22) }",
    );
    let mut numbers: Vec<f64> = rows
        .iter()
        .filter_map(|row| shared::rdf_term::numeric_value(&row[0]))
        .collect();
    numbers.sort_by(f64::total_cmp);
    assert_eq!(numbers, vec![30.0], "only 30 + 1 exceeds 22");
}

#[test]
fn every_reader_spells_one_term_one_way() {
    let from_ntriples = object_terms(&format!("<urn:a> <urn:age> \"30\"^^<{XSD_INTEGER}> .\n"));

    let mut prefixed = KolibrieDB::new();
    prefixed.parse_turtle(
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
         <urn:a> <urn:age> \"30\"^^xsd:integer .\n",
    );
    let from_turtle_prefixed = stored_objects(&prefixed);

    let mut full = KolibrieDB::new();
    full.parse_turtle(&format!("<urn:a> <urn:age> \"30\"^^<{XSD_INTEGER}> .\n"));
    let from_turtle_full = stored_objects(&full);

    assert_eq!(
        from_ntriples, from_turtle_prefixed,
        "Turtle with a prefixed datatype must agree with N-Triples"
    );
    assert_eq!(
        from_ntriples, from_turtle_full,
        "Turtle with a full datatype IRI must agree with N-Triples"
    );
}

/// The object terms of a database, as stored
fn stored_objects(database: &KolibrieDB) -> Vec<String> {
    let dictionary = database.dictionary();
    database
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| {
            dictionary
                .decode(triple.object)
                .unwrap_or("<undecodable>")
                .to_string()
        })
        .collect()
}

#[test]
fn rdf_xml_keeps_a_datatype() {
    let mut database = KolibrieDB::new();
    database.parse_rdf(&format!(
        r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   xmlns:ex="http://example.org/">
             <rdf:Description rdf:about="urn:a">
               <ex:age rdf:datatype="{XSD_INTEGER}">30</ex:age>
             </rdf:Description>
           </rdf:RDF>"#
    ));
    let stored = stored_objects(&database);
    assert_eq!(stored.len(), 1, "one triple, got {stored:?}");
    assert_eq!(
        stored,
        object_terms(&format!("<urn:a> <urn:age> \"30\"^^<{XSD_INTEGER}> .\n")),
        "RDF/XML must spell a typed literal the way N-Triples does"
    );
}

#[test]
fn rdf_xml_keeps_a_language_tag() {
    let mut database = KolibrieDB::new();
    database.parse_rdf(
        r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
             <rdf:Description rdf:about="urn:a">
               <rdfs:label xml:lang="en">hello</rdfs:label>
             </rdf:Description>
           </rdf:RDF>"#,
    );
    let stored = stored_objects(&database);
    assert_eq!(stored.len(), 1, "one triple, got {stored:?}");
    assert_eq!(
        stored,
        object_terms("<urn:a> <urn:label> \"hello\"@en .\n"),
        "RDF/XML must spell a tagged literal the way N-Triples does"
    );
}

#[test]
fn rdf_xml_leaves_a_plain_literal_alone() {
    let mut database = KolibrieDB::new();
    database.parse_rdf(
        r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   xmlns:ex="http://example.org/">
             <rdf:Description rdf:about="urn:a">
               <ex:age>30</ex:age>
             </rdf:Description>
           </rdf:RDF>"#,
    );
    assert_eq!(stored_objects(&database), vec!["30".to_string()]);
}

#[test]
fn rdf_xml_does_not_carry_a_datatype_to_the_next_literal() {
    let mut database = KolibrieDB::new();
    database.parse_rdf(&format!(
        r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   xmlns:ex="http://example.org/">
             <rdf:Description rdf:about="urn:a">
               <ex:age rdf:datatype="{XSD_INTEGER}">30</ex:age>
               <ex:nickname>bob</ex:nickname>
             </rdf:Description>
           </rdf:RDF>"#
    ));
    let mut stored = stored_objects(&database);
    stored.sort();
    assert_eq!(
        stored,
        vec![
            "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_string(),
            "bob".to_string(),
        ],
        "the plain literal must stay plain"
    );
}

#[test]
fn rdf_xml_inherits_a_language_from_an_enclosing_element() {
    let mut database = KolibrieDB::new();
    database.parse_rdf(
        r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   xmlns:ex="http://example.org/" xml:lang="fr">
             <rdf:Description rdf:about="urn:a">
               <ex:label>bonjour</ex:label>
             </rdf:Description>
           </rdf:RDF>"#,
    );
    assert_eq!(
        stored_objects(&database),
        object_terms("<urn:a> <urn:label> \"bonjour\"@fr .\n")
    );
}

#[test]
fn rdf_xml_lets_an_inner_language_override_an_outer_one() {
    let mut database = KolibrieDB::new();
    database.parse_rdf(
        r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   xmlns:ex="http://example.org/" xml:lang="fr">
             <rdf:Description rdf:about="urn:a">
               <ex:label xml:lang="en">hello</ex:label>
               <ex:other>bonjour</ex:other>
             </rdf:Description>
           </rdf:RDF>"#,
    );
    let mut stored = stored_objects(&database);
    stored.sort();
    assert_eq!(
        stored,
        vec!["\"bonjour\"@fr".to_string(), "\"hello\"@en".to_string()],
        "the inner tag wins where it is given, and the outer one resumes after it"
    );
}

#[test]
fn rdf_xml_does_not_let_a_language_outlive_its_element() {
    let mut database = KolibrieDB::new();
    database.parse_rdf(
        r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   xmlns:ex="http://example.org/">
             <rdf:Description rdf:about="urn:a">
               <ex:label xml:lang="en">hello</ex:label>
               <ex:plain>30</ex:plain>
             </rdf:Description>
           </rdf:RDF>"#,
    );
    let mut stored = stored_objects(&database);
    stored.sort();
    assert_eq!(
        stored,
        vec!["\"hello\"@en".to_string(), "30".to_string()],
        "the plain literal must stay plain"
    );
}

/// The subject terms of a database, as stored
fn stored_subjects(database: &KolibrieDB) -> Vec<String> {
    let dictionary = database.dictionary();
    let mut subjects: Vec<String> = database
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| {
            dictionary
                .decode(triple.subject)
                .unwrap_or("<undecodable>")
                .to_string()
        })
        .collect();
    subjects.sort();
    subjects
}

#[test]
fn two_documents_do_not_share_a_blank_node() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("_:b <urn:p> <urn:first> .\n");
    database.parse_ntriples_and_add("_:b <urn:p> <urn:second> .\n");
    let subjects = stored_subjects(&database);
    assert_eq!(subjects.len(), 2, "two triples, got {subjects:?}");
    assert_ne!(
        subjects[0], subjects[1],
        "`_:b` in two documents is two nodes, so merging them must not fuse the two \
         statements onto one subject"
    );
}

#[test]
fn one_document_keeps_its_blank_node_shared() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("_:b <urn:p> <urn:first> .\n_:b <urn:q> <urn:second> .\n");
    let subjects = stored_subjects(&database);
    assert_eq!(subjects.len(), 2, "two triples, got {subjects:?}");
    assert_eq!(
        subjects[0], subjects[1],
        "one document's `_:b` is one node in every statement that names it"
    );
}

#[test]
fn a_blank_node_label_is_distinguishable_from_the_label_it_was_written_as() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("_:original <urn:p> <urn:o> .\n");
    let subjects = stored_subjects(&database);
    assert!(
        subjects[0].starts_with("_:"),
        "still a blank node: {:?}",
        subjects[0]
    );
    assert_ne!(
        subjects[0], "_:original",
        "the bare label cannot be the term"
    );
    assert!(
        subjects[0].ends_with("original"),
        "the written label should remain legible, got {:?}",
        subjects[0]
    );
}

#[test]
fn turtle_scopes_blank_nodes_as_well() {
    let mut database = KolibrieDB::new();
    database.parse_turtle("_:b <urn:p> <urn:first> .\n");
    database.parse_turtle("_:b <urn:p> <urn:second> .\n");
    let subjects = stored_subjects(&database);
    assert_eq!(subjects.len(), 2, "two triples, got {subjects:?}");
    assert_ne!(
        subjects[0], subjects[1],
        "the Turtle reader must scope labels the way the N-Triples reader does"
    );
}

#[test]
fn a_blank_node_in_the_object_is_scoped_with_its_subject() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("<urn:s> <urn:p> _:b .\n_:b <urn:q> <urn:o> .\n");
    let dictionary = database.dictionary();
    let triples = database.query_default_triples(None, None, None);
    let object_of_first = triples
        .iter()
        .find(|t| dictionary.decode(t.subject) == Some("urn:s"))
        .map(|t| t.object)
        .expect("the statement about urn:s");
    let subject_of_second = triples
        .iter()
        .find(|t| dictionary.decode(t.subject) != Some("urn:s"))
        .map(|t| t.subject)
        .expect("the statement about the blank node");
    assert_eq!(
        object_of_first, subject_of_second,
        "the object of one statement and the subject of the other are the same node"
    );
}

#[test]
fn nquads_scopes_blank_nodes_including_the_graph_name() {
    let mut database = KolibrieDB::new();
    database.parse_nquads_and_add("_:b <urn:p> <urn:first> .\n");
    database.parse_nquads_and_add("_:b <urn:p> <urn:second> .\n");
    let subjects = stored_subjects(&database);
    assert_eq!(subjects.len(), 2, "two quads, got {subjects:?}");
    assert_ne!(subjects[0], subjects[1]);
}

#[test]
fn n3_scopes_blank_nodes_as_well() {
    let mut database = KolibrieDB::new();
    database.parse_n3("_:b <urn:p> <urn:first> .\n");
    database.parse_n3("_:b <urn:p> <urn:second> .\n");
    let subjects = stored_subjects(&database);
    assert_eq!(subjects.len(), 2, "two triples, got {subjects:?}");
    assert_ne!(subjects[0], subjects[1]);
}

#[test]
fn a_blank_node_inside_a_quoted_triple_is_scoped_too() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("<< _:b <urn:p> <urn:o> >> <urn:says> <urn:first> .\n");
    database.parse_ntriples_and_add("<< _:b <urn:p> <urn:o> >> <urn:says> <urn:second> .\n");
    let mut subjects: Vec<String> = database
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| {
            database
                .decode_any(triple.subject)
                .unwrap_or_else(|| "<undecodable>".to_string())
        })
        .collect();
    subjects.sort();
    assert_eq!(subjects.len(), 2, "two triples, got {subjects:?}");
    assert_ne!(
        subjects[0], subjects[1],
        "the quoted triples name different blank nodes, so they are different terms"
    );
    assert!(
        !subjects.iter().any(|term| term.contains("<< _:b ")),
        "no bare label should survive inside a quoted triple: {subjects:?}"
    );
}

#[test]
fn a_second_n3_read_keeps_its_own_document() {
    let mut database = KolibrieDB::new();
    database.parse_n3("<urn:a> <urn:p> <urn:first> .\n");
    database.parse_n3("<urn:b> <urn:q> <urn:second> .\n");
    let dictionary = database.dictionary();
    let mut rows: Vec<String> = database
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| {
            format!(
                "{} {} {}",
                dictionary.decode(triple.subject).unwrap_or("?"),
                dictionary.decode(triple.predicate).unwrap_or("?"),
                dictionary.decode(triple.object).unwrap_or("?")
            )
        })
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            "urn:a urn:p urn:first".to_string(),
            "urn:b urn:q urn:second".to_string(),
        ],
        "both documents must survive, with the terms they were written with"
    );
}

#[test]
fn a_stream_does_not_grow_the_dictionary_per_element() {
    use kolibrie::rsp::r2r::R2ROperator;
    let mut operator = kolibrie::rsp::simple_r2r::SimpleR2R::new();
    for index in 0..200 {
        let _ = operator.parse_data(&format!("_:b <urn:reading> <urn:v{}> .\n", index % 3));
    }
    let with_blank_nodes = operator.item.dictionary().string_to_id.len();

    let mut control = kolibrie::rsp::simple_r2r::SimpleR2R::new();
    for index in 0..200 {
        let _ = control.parse_data(&format!("<urn:s> <urn:reading> <urn:v{}> .\n", index % 3));
    }
    let without_blank_nodes = control.item.dictionary().string_to_id.len();

    assert_eq!(
        with_blank_nodes, without_blank_nodes,
        "200 elements naming one blank node must intern no more terms than the same \
         stream without one; a fresh scope per element interned 204 against 5"
    );
}

#[test]
fn two_streams_do_not_share_a_blank_node() {
    use kolibrie::rsp::r2r::R2ROperator;
    let mut first = kolibrie::rsp::simple_r2r::SimpleR2R::new();
    let mut second = kolibrie::rsp::simple_r2r::SimpleR2R::new();
    let from_first = first.parse_data("_:b <urn:p> <urn:o> .\n");
    let from_second = second.parse_data("_:b <urn:p> <urn:o> .\n");
    let first_label = first
        .item
        .dictionary()
        .decode(from_first[0].subject)
        .expect("a term")
        .to_string();
    let second_label = second
        .item
        .dictionary()
        .decode(from_second[0].subject)
        .expect("a term")
        .to_string();
    assert_ne!(
        first_label, second_label,
        "two streams are two documents, so their labels must differ"
    );
}

fn wire_of(ntriples: &str) -> String {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add(ntriples);
    database.generate_ntriples()
}

#[test]
fn a_typed_literal_goes_out_as_a_typed_literal() {
    let wire = wire_of(&format!("<urn:a> <urn:age> \"30\"^^<{XSD_INTEGER}> .\n"));
    assert!(
        wire.contains(&format!("\"30\"^^<{XSD_INTEGER}>")),
        "the datatype must be on the wire, got {wire:?}"
    );
    assert!(
        !wire.contains("\"\\\""),
        "the literal must not be quoted twice, got {wire:?}"
    );
}

#[test]
fn a_language_tagged_literal_goes_out_with_its_tag() {
    let wire = wire_of("<urn:a> <urn:name> \"Alice\"@en .\n");
    assert!(wire.contains("\"Alice\"@en"), "got {wire:?}");
}

#[test]
fn an_iri_goes_out_in_brackets_whatever_its_scheme() {
    let wire = wire_of("<urn:a> <urn:at> <urn:place> .\n");
    assert!(wire.contains("<urn:place>"), "got {wire:?}");
    assert!(
        !wire.contains("\"urn:place\""),
        "an IRI is not a literal: {wire:?}"
    );
}

#[test]
fn a_blank_node_goes_out_as_a_blank_node() {
    let wire = wire_of("_:b <urn:p> <urn:o> .\n");
    assert!(
        !wire.contains("<_:"),
        "a blank node is not an IRI: {wire:?}"
    );
    assert!(wire.contains("_:kolibrie-doc-"), "got {wire:?}");
}

#[test]
fn a_literal_containing_a_quote_goes_out_escaped() {
    let wire = wire_of("<urn:a> <urn:says> \"she said \\\"hi\\\"\" .\n");
    assert!(
        wire.contains("\\\""),
        "an embedded quote must be escaped, got {wire:?}"
    );
    let mut back = KolibrieDB::new();
    back.parse_ntriples_and_add(&wire);
    let dictionary = back.dictionary();
    let objects: Vec<String> = back
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| dictionary.decode(triple.object).unwrap_or("?").to_string())
        .collect();
    assert_eq!(objects, vec!["she said \"hi\"".to_string()]);
}

#[test]
fn every_term_survives_a_round_trip_through_the_wire() {
    let source = format!(
        "<urn:a> <urn:age> \"30\"^^<{XSD_INTEGER}> .\n\
         <urn:a> <urn:name> \"Alice\"@en .\n\
         <urn:a> <urn:nick> \"Ali\" .\n\
         <urn:a> <urn:at> <urn:place> .\n"
    );
    let wire = wire_of(&source);
    let mut back = KolibrieDB::new();
    back.parse_ntriples_and_add(&wire);
    let dictionary = back.dictionary();
    let mut objects: Vec<String> = back
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| dictionary.decode(triple.object).unwrap_or("?").to_string())
        .collect();
    objects.sort();
    assert_eq!(
        objects,
        vec![
            format!("\"30\"^^<{XSD_INTEGER}>"),
            "\"Alice\"@en".to_string(),
            "Ali".to_string(),
            "urn:place".to_string(),
        ]
    );
}

#[test]
fn two_instances_would_collide_without_skolemisation() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("_:b <urn:p> <urn:o> .\n");
    let wire = database.generate_ntriples();
    assert!(
        wire.contains("_:kolibrie-doc-"),
        "the plain form emits a document-local label: {wire:?}"
    );
    assert!(
        !wire.contains("well-known"),
        "and says nothing about who minted it: {wire:?}"
    );
}

#[test]
fn skolemising_replaces_a_blank_node_with_an_iri_under_the_given_authority() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("_:b <urn:p> <urn:o> .\n");
    let wire = database.generate_ntriples_skolemised("http://alice.example");
    assert!(
        wire.contains("<http://alice.example/.well-known/genid/"),
        "got {wire:?}"
    );
    assert!(
        !wire.contains("_:"),
        "no blank node should remain: {wire:?}"
    );
}

#[test]
fn two_authorities_do_not_collide() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("_:b <urn:p> <urn:o> .\n");
    let from_alice = database.generate_ntriples_skolemised("http://alice.example");
    let from_bob = database.generate_ntriples_skolemised("http://bob.example");
    assert_ne!(from_alice, from_bob);

    let mut merged = KolibrieDB::new();
    merged.parse_ntriples_and_add(&from_alice);
    merged.parse_ntriples_and_add(&from_bob);
    let subjects = stored_subjects(&merged);
    assert_eq!(subjects.len(), 2, "two nodes, got {subjects:?}");
    assert_ne!(subjects[0], subjects[1]);
}

#[test]
fn a_skolemised_node_arrives_as_an_iri_and_can_be_turned_back() {
    use shared::rdf_term::{deskolemise, kind_of, skolemise, TermKind};

    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("_:b <urn:p> <urn:o> .\n");
    let wire = database.generate_ntriples_skolemised("http://alice.example");

    let mut receiver = KolibrieDB::new();
    receiver.parse_ntriples_and_add(&wire);
    let arrived = &stored_subjects(&receiver)[0];

    assert_eq!(kind_of(arrived), TermKind::Iri, "got {arrived:?}");

    let recovered = deskolemise(arrived).expect("a Skolem IRI");
    assert!(recovered.starts_with("_:"), "got {recovered:?}");
    assert_eq!(
        skolemise(&recovered, "http://alice.example"),
        arrived.as_str(),
        "deskolemising and skolemising again should be a round trip"
    );
}

#[test]
fn skolemisation_does_not_touch_a_literal_that_mentions_a_label() {
    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add("<urn:a> <urn:says> \"see _:b here\" .\n");
    let wire = database.generate_ntriples_skolemised("http://alice.example");
    assert!(
        wire.contains("\"see _:b here\""),
        "the literal must be untouched, got {wire:?}"
    );
    assert!(
        !wire.contains("well-known"),
        "nothing to skolemise: {wire:?}"
    );
}
