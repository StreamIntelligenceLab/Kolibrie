/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! When two RDF terms are the same term, and when they only look alike.
//!
//! The store interns every term as one flat string, so a term's identity is whatever
//! string survived parsing. That makes these assertions worth pinning rather than
//! assuming: the lexical form of a literal, its datatype and its language tag all
//! have to reach the dictionary, or terms that RDF says are different collapse into
//! one and the difference cannot be recovered afterwards.
//!
//! Each test below names the RDF rule it rests on. They are about *term* identity
//! (RDF 1.1 §3.3, literal term equality), not value equality: `"1"^^xsd:integer` and
//! `"01"^^xsd:integer` denote the same value and are still two distinct terms, which
//! is why the datatype alone is not enough — the lexical form must survive too.
//!
//! Blank nodes are deliberately absent. Their real defect is that labels are not
//! scoped to the document they came from, so `_:b` read from two files is one node;
//! that is a larger change than literal identity and is not what these pin.

use kolibrie::sparql_database::SparqlDatabase;

const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// Parses N-Triples and returns the object term of `<urn:s{n}>`, as the store kept it.
fn object_terms(ntriples: &str) -> Vec<String> {
    let mut database = SparqlDatabase::new();
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
    // Subject ids are minted in document order, so this restores the input order.
    objects.sort_by_key(|(subject, _)| *subject);
    objects.into_iter().map(|(_, object)| object).collect()
}

#[test]
fn a_typed_literal_is_not_the_same_term_as_its_lexical_form() {
    // RDF 1.1 §3.3: a literal is a triple of lexical form, datatype IRI and optional
    // language tag. `"25"` is `"25"^^xsd:string`; `"25"^^xsd:integer` is a different
    // term, because the datatype differs.
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
    // The sharpest case: identical lexical form, different datatype IRI. Nothing but
    // the datatype distinguishes them, so if the datatype is dropped they are equal.
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
    // Stronger than distinctness, and the property that actually matters for a store:
    // the datatype has to be *recoverable*, not merely accounted for. A term that
    // comes back without it cannot be serialised back to the graph that was read.
    let objects = object_terms(&format!(
        "<urn:s1> <urn:p> \"25\"^^<{XSD_INTEGER}> .\n"
    ));
    assert_eq!(objects.len(), 1);
    assert!(
        objects[0].contains(XSD_INTEGER),
        "the datatype IRI must still be reachable from the stored term, got {:?}",
        objects[0]
    );
}

#[test]
fn a_language_tag_is_not_part_of_the_lexical_form() {
    // `"hello"@en` has lexical form `hello` and language `en`. A plain literal whose
    // lexical form is the six characters `hello@en` is a different term. Gluing the
    // tag onto the text makes them one.
    let objects = object_terms(
        "<urn:s1> <urn:p> \"hello\"@en .\n<urn:s2> <urn:p> \"hello@en\" .\n",
    );
    assert_eq!(objects.len(), 2, "both triples should be stored");
    assert_ne!(
        objects[0], objects[1],
        "a language-tagged literal and a plain literal spelled the same way are \
         different RDF terms"
    );
    // Distinctness alone is too weak: dropping the tag entirely would also satisfy
    // it, and did. The tag has to still be there.
    assert!(
        objects[0].ends_with("@en"),
        "the language tag must survive, got {:?}",
        objects[0]
    );
}

// ── Reading a value out of a term ───────────────────────────────────────────────
//
// Keeping the datatype is only half of it. A literal's *number* lives in its lexical
// form, so anything that reads a term as a number has to look past the spelling. The
// engine parsed the spelling directly, which worked only while the spelling happened
// to be the lexical form — exactly the accident the tests above removed.

fn query_one(ntriples: &str, sparql: &str) -> Vec<Vec<String>> {
    let mut database = SparqlDatabase::new();
    database.parse_ntriples_and_add(ntriples);
    kolibrie::execute_query::execute(sparql, &mut database)
        .expect("query should run")
        .into_rows()
}

/// Three ages, written as `xsd:integer` literals the way a real graph would.
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
    // Numeric order is 7, 21, 30. String order would be "21", "30", "7", and the
    // spelling `"7"^^<…>` would sort differently again -- so this fails in a
    // different way for each thing that could be compared instead of the value.
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
    // FILTER has two paths that must agree: a lexical comparison that already read
    // through the literal's furniture, and an arithmetic one that did not. A typed
    // literal was a number in the first and not in the second.
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
    // Readers that disagree about spelling mint different ids for the same term, so
    // data loaded as Turtle could never join against data loaded as N-Triples. All
    // three of these used to differ: N-Triples kept only the lexical form, and Turtle
    // produced `30"^^xsd:integer` -- a stray quote, and a prefix never expanded.
    let from_ntriples = object_terms(&format!(
        "<urn:a> <urn:age> \"30\"^^<{XSD_INTEGER}> .\n"
    ));

    let mut prefixed = SparqlDatabase::new();
    prefixed.parse_turtle(
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
         <urn:a> <urn:age> \"30\"^^xsd:integer .\n",
    );
    let from_turtle_prefixed = stored_objects(&prefixed);

    let mut full = SparqlDatabase::new();
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

/// The object terms of a database, as stored.
fn stored_objects(database: &SparqlDatabase) -> Vec<String> {
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

// ── RDF/XML ─────────────────────────────────────────────────────────────────────
//
// RDF/XML carries a literal's datatype on the enclosing property element, as
// `rdf:datatype`, and its language as `xml:lang`. The reader looked at neither, so
// every literal it produced was a plain one whatever the document said — the same
// loss as the N-Triples reader's, by a different route.

#[test]
fn rdf_xml_keeps_a_datatype() {
    let mut database = SparqlDatabase::new();
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
    let mut database = SparqlDatabase::new();
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
    // The guard on the change: a property element with neither attribute still
    // produces the plain literal it always did.
    let mut database = SparqlDatabase::new();
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
    // State kept across elements is the obvious way to get this wrong: the second
    // property element has no datatype and must not inherit the first one's.
    let mut database = SparqlDatabase::new();
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
    // `xml:lang` is scoped to an element and its descendants (XML 1.0 §2.12, and
    // RDF/XML §6.1.2 builds on it), and documents routinely declare it once on the
    // root. A reader that only looks at the property element itself sees none.
    let mut database = SparqlDatabase::new();
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
    let mut database = SparqlDatabase::new();
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
    // The failure a scope stack exists to prevent: the tag leaking to a sibling that
    // closed over nothing.
    let mut database = SparqlDatabase::new();
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

// ── Blank nodes ─────────────────────────────────────────────────────────────────
//
// A blank node label identifies a node *within the document it was read from* and
// means nothing outside it (RDF 1.1 §3.4). Two files each saying `_:b` describe two
// different nodes, and merging them must keep them apart. The readers interned the
// label itself, so the two became one node and the merge silently fused unrelated
// statements — the one case in this file where the defect invents a fact rather than
// losing one.
//
// SPARQL updates already scoped their labels, minting `_:kolibrie-update-N-label`;
// the readers did not. Relabelling is forced rather than chosen: the dictionary keys
// on a term's text, so two distinct nodes must have two distinct spellings.

/// The subject terms of a database, as stored.
fn stored_subjects(database: &SparqlDatabase) -> Vec<String> {
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
    let mut database = SparqlDatabase::new();
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
    // The other half, and the one scoping can break: within a document the label
    // does identify one node, so two statements about `_:b` are about one subject.
    // Minting a fresh node per occurrence would satisfy the test above and destroy
    // every blank-node-linked structure in the graph.
    let mut database = SparqlDatabase::new();
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
    // Scoping has to be visible in the term, since that is the only place identity
    // can live, but the label it came from should still be legible in it -- the
    // convention SPARQL updates already set.
    let mut database = SparqlDatabase::new();
    database.parse_ntriples_and_add("_:original <urn:p> <urn:o> .\n");
    let subjects = stored_subjects(&database);
    assert!(
        subjects[0].starts_with("_:"),
        "still a blank node: {:?}",
        subjects[0]
    );
    assert_ne!(subjects[0], "_:original", "the bare label cannot be the term");
    assert!(
        subjects[0].ends_with("original"),
        "the written label should remain legible, got {:?}",
        subjects[0]
    );
}

#[test]
fn turtle_scopes_blank_nodes_as_well() {
    let mut database = SparqlDatabase::new();
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
    // Within one document a label names one node wherever it appears, including
    // across positions: this is the shape that makes a blank node useful at all.
    let mut database = SparqlDatabase::new();
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
    let mut database = SparqlDatabase::new();
    database.parse_nquads_and_add("_:b <urn:p> <urn:first> .\n");
    database.parse_nquads_and_add("_:b <urn:p> <urn:second> .\n");
    let subjects = stored_subjects(&database);
    assert_eq!(subjects.len(), 2, "two quads, got {subjects:?}");
    assert_ne!(subjects[0], subjects[1]);
}

#[test]
fn n3_scopes_blank_nodes_as_well() {
    let mut database = SparqlDatabase::new();
    database.parse_n3("_:b <urn:p> <urn:first> .\n");
    database.parse_n3("_:b <urn:p> <urn:second> .\n");
    let subjects = stored_subjects(&database);
    assert_eq!(subjects.len(), 2, "two triples, got {subjects:?}");
    assert_ne!(subjects[0], subjects[1]);
}

#[test]
fn a_blank_node_inside_a_quoted_triple_is_scoped_too() {
    // Scoping only the outermost term would leave a nested label bare, so two
    // documents would still share the node a quoted triple asserts about.
    let mut database = SparqlDatabase::new();
    database.parse_ntriples_and_add("<< _:b <urn:p> <urn:o> >> <urn:says> <urn:first> .\n");
    database.parse_ntriples_and_add("<< _:b <urn:p> <urn:o> >> <urn:says> <urn:second> .\n");
    // A quoted triple is a tagged id that the dictionary alone cannot resolve, so
    // these are decoded through the store rather than with `stored_subjects`.
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
    // Not about terms' spelling but about their ids, and found by the test above.
    // Each parallel chunk parsed into its own database and its triples were then
    // inserted carrying that database's ids, while only the dictionaries were
    // merged. With an empty target both numbered terms in the same order, so the
    // first read looked right; a second read reinterpreted its ids as the first
    // document's terms, storing a copy of the first document in place of the second.
    let mut database = SparqlDatabase::new();
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
