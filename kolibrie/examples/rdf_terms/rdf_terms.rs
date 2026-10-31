/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Building and reading RDF terms: IRIs, blank nodes and literals.
//!
//! Run with `cargo run --example rdf_terms`.
//!
//! The store keys every term on its text, so a term's spelling *is* its identity:
//! two terms spelled alike share one id and can never be told apart again. That makes
//! `shared::rdf_term` the place where spelling is decided, and this example is the
//! guided tour — what each kind of term looks like, which terms RDF says are the same,
//! and how to read a value back out of one.

use kolibrie::sparql_database::SparqlDatabase;
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

/// An IRI is spelled as itself.
fn iris() {
    heading("IRIs");

    // There is no constructor, and that is the point: an IRI is its own text, so one
    // would be the identity function and would only suggest otherwise. Angle brackets
    // are syntax belonging to a document, not part of the term.
    let iri = "http://example.org/alice";
    println!("  {iri:?} is {:?}", kind_of(iri));

    // Being an IRI is decided by having a scheme, which is also what keeps IRIs from
    // colliding with plain literals: `25` has no scheme, so it is a literal.
    for candidate in ["urn:a", "http://example.org/x", "25", "hello world"] {
        println!("  {candidate:?} is {:?}", kind_of(candidate));
    }
}

/// A literal is a lexical form, plus at most one of a datatype or a language tag.
fn literals() {
    heading("Literals");

    // Plain: spelled bare, exactly the text it holds.
    let plain = spell_literal("25", None, None);
    // Typed: quoted, carrying the datatype IRI.
    let typed = spell_literal("25", Some(XSD_INTEGER), None);
    // Language-tagged: quoted, carrying the tag.
    let tagged = spell_literal("hello", None, Some("en"));

    println!("  plain          {plain}");
    println!("  xsd:integer    {typed}");
    println!("  @en            {tagged}");

    // Bare where it can be, quoted where it must be. A well-formed IRI cannot begin
    // with a quote, so nothing quoted can be mistaken for one -- which is what lets
    // the common cases stay unquoted and unchanged.
    println!("  all three are literals: {}", [&plain, &typed, &tagged]
        .iter()
        .all(|term| kind_of(term) == TermKind::Literal));

    // A lexical form containing a quote or a backslash is escaped, or the spelling
    // would read back as a different term.
    let awkward = spell_literal("a\"b\\c", Some(XSD_INTEGER), None);
    println!("  needs escaping {awkward}");
    println!(
        "  reads back as  {:?}",
        read_literal(&awkward).expect("a literal").lexical
    );
}

/// Which spellings denote one term, and which denote two.
fn which_terms_are_the_same() {
    heading("Term identity");

    // RDF 1.1 §3.3: a plain literal *is* an `xsd:string` literal, so both get one
    // spelling and therefore one id.
    let plain = spell_literal("25", None, None);
    let as_string = spell_literal("25", Some(XSD_STRING), None);
    println!("  \"25\" and \"25\"^^xsd:string      same term? {}", plain == as_string);

    // Differing only in datatype is still two terms. This is the distinction the
    // readers used to lose: both became `25`, and the datatype was unrecoverable.
    let as_integer = spell_literal("25", Some(XSD_INTEGER), None);
    println!(
        "  \"25\" and \"25\"^^xsd:integer     same term? {}",
        plain == as_integer
    );

    // A language-tagged literal and a plain literal spelled the same way are two
    // terms. Gluing the tag onto the text made them one.
    let tagged = spell_literal("hello", None, Some("en"));
    let looks_tagged = spell_literal("hello@en", None, None);
    println!(
        "  \"hello\"@en and \"hello@en\"      same term? {}",
        tagged == looks_tagged
    );

    // Term identity is not value equality: these denote one number and remain two
    // terms, because their lexical forms differ.
    let one = spell_literal("1", Some(XSD_INTEGER), None);
    let also_one = spell_literal("01", Some(XSD_INTEGER), None);
    println!(
        "  \"1\" and \"01\" (both integers)   same term? {}  same value? {}",
        one == also_one,
        numeric_value(&one) == numeric_value(&also_one)
    );
}

/// Reading the value a term denotes, rather than the term.
fn reading_a_value_back() {
    heading("Reading a value");

    // A literal's number lives in its lexical form, not in its spelling, so anything
    // comparing or summing terms has to look past the spelling. Parsing the spelling
    // directly found nothing once the datatype was kept, which turned ORDER BY into
    // string ordering and SUM into no answer at all.
    let typed = spell_literal("30", Some(XSD_INTEGER), None);
    println!("  {typed}");
    println!("    numeric_value  {:?}", numeric_value(&typed));
    println!("    lexical_value  {:?}", lexical_value(&typed));

    // Taken apart, a literal is its three pieces.
    let double = spell_literal("0.8", Some(XSD_DOUBLE), None);
    let literal = read_literal(&double).expect("a literal");
    println!("  {double}");
    println!(
        "    read_literal   lexical {:?}, datatype {:?}, language {:?}",
        literal.lexical, literal.datatype, literal.language
    );

    // An IRI denotes no number and is not a literal, so both answers say so rather
    // than guessing.
    let iri = "http://example.org/25";
    println!("  {iri}");
    println!("    numeric_value  {:?}", numeric_value(iri));
    println!("    read_literal   {:?}", read_literal(iri));

    // With N-Triples text already in hand -- from a file, a request body, a header --
    // this is what turns it into the term the readers would have produced.
    let from_text = rdf_term::canonicalise_literal("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>");
    println!(
        "  canonicalise_literal of N-Triples text -> {}",
        from_text.as_deref().unwrap_or("<not a literal>")
    );
}

/// A blank node label belongs to one document and means nothing outside it.
fn blank_nodes() {
    heading("Blank nodes");

    // A label identifies a node only within the document it was read from (RDF 1.1
    // §3.4), so a scope has to come from somewhere. One per document.
    let first_document = rdf_term::next_document_scope();
    let second_document = rdf_term::next_document_scope();

    // Built from the bare label: the `_:` prefix and the scoping are both handled.
    let in_first = spell_blank_node("b", first_document);
    let in_second = spell_blank_node("b", second_document);

    println!("  `_:b` in one document   {in_first}");
    println!("  `_:b` in another        {in_second}");
    println!("  same node? {}", in_first == in_second);
    println!("  still blank nodes? {:?} / {:?}", kind_of(&in_first), kind_of(&in_second));

    // Within one document the label does identify one node, which is what makes a
    // blank node useful: it links statements together.
    println!(
        "  `_:b` twice in one document is one node? {}",
        spell_blank_node("b", first_document) == spell_blank_node("b", first_document)
    );

    // Scoping replaces a previous scope rather than nesting in it, so a graph read
    // back from its own serialisation does not collect a prefix per round trip.
    let round_tripped = rdf_term::scope_blank_node(&in_first, second_document);
    println!("  re-read in another document {round_tripped}");
}

/// The same terms, going into a store and coming back out.
fn terms_in_a_store() {
    heading("Terms in a store");

    // Every reader produces these spellings, so a term built by hand and a term read
    // from a document are the same term and join against each other.
    let mut database = SparqlDatabase::new();
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
                predicate.rsplit('/').next().unwrap_or(predicate).to_string(),
                format!("{object}  [{:?}]", kind_of(object)),
            )
        })
        .collect();
    rows.sort();
    drop(dictionary);
    for (predicate, object) in rows {
        println!("  {predicate:<10} {object}");
    }

    // The typed literal is a number to the engine, so it aggregates and orders as
    // one, and the datatype is still there to be written back out.
    let summed = kolibrie::execute_query::execute(
        "SELECT SUM(?age) WHERE { ?s <http://example.org/age> ?age }",
        &mut database,
    )
    .expect("query should run")
    .into_rows();
    // 30 + 12. Read as numbers through their lexical forms: summing the spellings
    // would have found nothing, which is what used to happen once the datatype was
    // kept and the spelling stopped being the lexical form.
    println!("\n  SUM over the two xsd:integer ages (30 + 12): {summed:?}");
}
