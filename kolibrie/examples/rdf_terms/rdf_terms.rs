/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Building and reading RDF terms: IRIs, blank nodes and literals

use kolibrie::database::KolibrieDB;
use shared::rdf_term::{
    self, kind_of, lexical_value, numeric_value, read_literal, spell_blank_node, spell_literal,
    TermKind, XSD_STRING,
};

const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";

fn main() {
    iris();
    literals();
    which_terms_are_the_same();
    reading_a_value_back();
    blank_nodes();
    terms_in_a_store();
}

fn heading(text: &str) {
    let rule = 72usize.saturating_sub(text.len() + 4);
    println!("\n── {text} {}", "─".repeat(rule));
}

/// An IRI is spelled as itself
fn iris() {
    heading("IRIs");

    let iri = "http://example.org/alice";
    println!("  {iri:?} is {:?}", kind_of(iri));

    for candidate in ["urn:a", "http://example.org/x", "25", "hello world"] {
        println!("  {candidate:?} is {:?}", kind_of(candidate));
    }
}

/// A literal is a lexical form, plus at most one of a datatype or a language tag
fn literals() {
    heading("Literals");

    let plain = spell_literal("25", None, None);
    let typed = spell_literal("25", Some(XSD_INTEGER), None);
    let tagged = spell_literal("hello", None, Some("en"));

    println!("  plain          {plain}");
    println!("  xsd:integer    {typed}");
    println!("  @en            {tagged}");

    println!(
        "  all three are literals: {}",
        [&plain, &typed, &tagged]
            .iter()
            .all(|term| kind_of(term) == TermKind::Literal)
    );

    let awkward = spell_literal("a\"b\\c", Some(XSD_INTEGER), None);
    println!("  needs escaping {awkward}");
    println!(
        "  reads back as  {:?}",
        read_literal(&awkward).expect("a literal").lexical
    );
}

/// Which spellings denote one term, and which denote two
fn which_terms_are_the_same() {
    heading("Term identity");

    let plain = spell_literal("25", None, None);
    let as_string = spell_literal("25", Some(XSD_STRING), None);
    println!(
        "  \"25\" and \"25\"^^xsd:string      same term? {}",
        plain == as_string
    );

    let as_integer = spell_literal("25", Some(XSD_INTEGER), None);
    println!(
        "  \"25\" and \"25\"^^xsd:integer     same term? {}",
        plain == as_integer
    );

    let tagged = spell_literal("hello", None, Some("en"));
    let looks_tagged = spell_literal("hello@en", None, None);
    println!(
        "  \"hello\"@en and \"hello@en\"      same term? {}",
        tagged == looks_tagged
    );

    let one = spell_literal("1", Some(XSD_INTEGER), None);
    let also_one = spell_literal("01", Some(XSD_INTEGER), None);
    println!(
        "  \"1\" and \"01\" (both integers)   same term? {}  same value? {}",
        one == also_one,
        numeric_value(&one) == numeric_value(&also_one)
    );
}

/// Reading the value a term denotes, rather than the term
fn reading_a_value_back() {
    heading("Reading a value");

    let typed = spell_literal("30", Some(XSD_INTEGER), None);
    println!("  {typed}");
    println!("    numeric_value  {:?}", numeric_value(&typed));
    println!("    lexical_value  {:?}", lexical_value(&typed));

    let double = spell_literal("0.8", Some(XSD_DOUBLE), None);
    let literal = read_literal(&double).expect("a literal");
    println!("  {double}");
    println!(
        "    read_literal   lexical {:?}, datatype {:?}, language {:?}",
        literal.lexical, literal.datatype, literal.language
    );

    let iri = "http://example.org/25";
    println!("  {iri}");
    println!("    numeric_value  {:?}", numeric_value(iri));
    println!("    read_literal   {:?}", read_literal(iri));

    let from_text =
        rdf_term::canonicalise_literal("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>");
    println!(
        "  canonicalise_literal of N-Triples text -> {}",
        from_text.as_deref().unwrap_or("<not a literal>")
    );
}

/// A blank node label belongs to one document and means nothing outside it
fn blank_nodes() {
    heading("Blank nodes");

    let first_document = rdf_term::next_document_scope();
    let second_document = rdf_term::next_document_scope();

    let in_first = spell_blank_node("b", first_document);
    let in_second = spell_blank_node("b", second_document);

    println!("  `_:b` in one document   {in_first}");
    println!("  `_:b` in another        {in_second}");
    println!("  same node? {}", in_first == in_second);
    println!(
        "  still blank nodes? {:?} / {:?}",
        kind_of(&in_first),
        kind_of(&in_second)
    );

    println!(
        "  `_:b` twice in one document is one node? {}",
        spell_blank_node("b", first_document) == spell_blank_node("b", first_document)
    );

    let round_tripped = rdf_term::scope_blank_node(&in_first, second_document);
    println!("  re-read in another document {round_tripped}");
}

/// The same terms, going into a store and coming back out
fn terms_in_a_store() {
    heading("Terms in a store");

    let mut database = KolibrieDB::new();
    database.parse_ntriples_and_add(&format!(
        "<http://example.org/alice> <http://example.org/age> \"30\"^^<{XSD_INTEGER}> .\n\
         <http://example.org/bob> <http://example.org/age> \"12\"^^<{XSD_INTEGER}> .\n\
         <http://example.org/alice> <http://example.org/name> \"Alice\"@en .\n\
         <http://example.org/alice> <http://example.org/nickname> \"Ali\" .\n"
    ));

    let dictionary = database.dictionary();
    let mut rows: Vec<(String, String)> = database
        .query_default_triples(None, None, None)
        .into_iter()
        .map(|triple| {
            let predicate = dictionary.decode(triple.predicate).unwrap_or("?");
            let object = dictionary.decode(triple.object).unwrap_or("?");
            (
                predicate
                    .rsplit('/')
                    .next()
                    .unwrap_or(predicate)
                    .to_string(),
                format!("{object}  [{:?}]", kind_of(object)),
            )
        })
        .collect();
    rows.sort();
    drop(dictionary);
    for (predicate, object) in rows {
        println!("  {predicate:<10} {object}");
    }

    let summed = kolibrie::execute_query::execute(
        "SELECT SUM(?age) WHERE { ?s <http://example.org/age> ?age }",
        &mut database,
    )
    .expect("query should run")
    .into_rows();
    println!("\n  SUM over the two xsd:integer ages (30 + 12): {summed:?}");
}
