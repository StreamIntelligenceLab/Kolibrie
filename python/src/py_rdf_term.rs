/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The term model, for Python.
//!
//! A query returns terms as strings, and a term's string is its canonical spelling:
//! an IRI bare, a plain literal bare, a typed literal as `"25"^^<…#integer>`, a
//! language-tagged one as `"hello"@en`, a blank node as `_:label`. That spelling is
//! the term's identity, which is why it carries the datatype rather than discarding
//! it — but it means a caller wanting the *value* has to ask for it rather than
//! using the string directly.
//!
//! Without these, Python could read a typed literal only by parsing the spelling by
//! hand, which is both tedious and the thing the Rust side stopped doing.

use pyo3::prelude::*;
use pyo3::types::PyModule;
use shared::rdf_term;

/// Which kind of term this is: `"iri"`, `"blank_node"` or `"literal"`.
#[pyfunction]
fn term_kind(term: &str) -> &'static str {
    match rdf_term::kind_of(term) {
        rdf_term::TermKind::Iri => "iri",
        rdf_term::TermKind::BlankNode => "blank_node",
        rdf_term::TermKind::Literal => "literal",
    }
}

/// The text a term denotes: a literal's lexical form, or the term itself.
///
/// `'"25"^^<…#integer>'` gives `'25'`; an IRI gives itself.
#[pyfunction]
fn lexical_value(term: &str) -> String {
    rdf_term::lexical_value(term).to_string()
}

/// The number a term denotes, or `None` if it denotes none.
///
/// Read through the lexical form, so a typed literal is a number: a Python caller
/// doing arithmetic on the spelling would otherwise have to strip the datatype off
/// first, and would get it wrong for a language-tagged literal.
#[pyfunction]
fn numeric_value(term: &str) -> Option<f64> {
    rdf_term::numeric_value(term)
}

/// A literal's datatype IRI, or `None` for a plain or language-tagged literal, or a
/// term that is not a literal.
#[pyfunction]
fn literal_datatype(term: &str) -> Option<String> {
    rdf_term::read_literal(term)?.datatype
}

/// A literal's language tag, or `None` when it has none or the term is not a literal.
#[pyfunction]
fn literal_language(term: &str) -> Option<String> {
    rdf_term::read_literal(term)?.language
}

/// Builds a literal's term string.
///
/// `datatype` and `language` are mutually exclusive in RDF; if both are given the
/// language wins, as it does on the Rust side. A plain literal and an `xsd:string`
/// literal are one term and get one spelling.
#[pyfunction]
#[pyo3(signature = (lexical, datatype=None, language=None))]
fn make_literal(lexical: &str, datatype: Option<&str>, language: Option<&str>) -> String {
    rdf_term::spell_literal(lexical, datatype, language)
}

/// A fresh blank-node scope, for one document's worth of labels.
///
/// A blank node label identifies a node only within the document it came from, so
/// building one needs a scope. Take one per document and pass it to every
/// [`make_blank_node`] call for that document: the same label and scope give the
/// same node, and a different scope gives a different one.
#[pyfunction]
fn new_document_scope() -> u64 {
    rdf_term::next_document_scope()
}

/// Builds a blank node's term string, from the bare label and a document scope.
///
/// `label` is `"b"`, not `"_:b"`; the prefix and the scoping are both handled.
#[pyfunction]
fn make_blank_node(label: &str, document_scope: u64) -> String {
    rdf_term::spell_blank_node(label, document_scope)
}

/// Writes a term as N-Triples syntax: an IRI in brackets, a literal quoted.
///
/// What to use when writing a term to a file or a request rather than comparing it.
/// The canonical spelling is deliberately not N-Triples, so handing a term straight
/// to a serialiser writes an IRI without its brackets.
#[pyfunction]
fn to_ntriples_term(term: &str) -> String {
    rdf_term::to_ntriples_term(term)
}

/// Replaces a blank node with a Skolem IRI under `base`, leaving other terms alone.
///
/// For publishing or exchanging data. A blank node label means nothing outside its
/// document, so two systems merging each other's blank nodes fuse unrelated nodes;
/// `base` must be an authority the caller controls, which is what makes the IRI
/// globally unique.
#[pyfunction]
fn skolemise(term: &str, base: &str) -> String {
    rdf_term::skolemise(term, base).into_owned()
}

/// Recovers the blank node a Skolem IRI stands for, or `None` for any other term.
///
/// Optional: leaving a Skolem IRI as an IRI is what keeps a merge sound, so this is
/// for a consumer that would rather have the node back.
#[pyfunction]
fn deskolemise(term: &str) -> Option<String> {
    rdf_term::deskolemise(term)
}

/// Registers the term helpers on the extension module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(term_kind, module)?)?;
    module.add_function(wrap_pyfunction!(lexical_value, module)?)?;
    module.add_function(wrap_pyfunction!(numeric_value, module)?)?;
    module.add_function(wrap_pyfunction!(literal_datatype, module)?)?;
    module.add_function(wrap_pyfunction!(literal_language, module)?)?;
    module.add_function(wrap_pyfunction!(make_literal, module)?)?;
    module.add_function(wrap_pyfunction!(new_document_scope, module)?)?;
    module.add_function(wrap_pyfunction!(make_blank_node, module)?)?;
    module.add_function(wrap_pyfunction!(to_ntriples_term, module)?)?;
    module.add_function(wrap_pyfunction!(skolemise, module)?)?;
    module.add_function(wrap_pyfunction!(deskolemise, module)?)?;
    module.add("XSD_STRING", rdf_term::XSD_STRING)?;
    module.add("SKOLEM_PATH", rdf_term::SKOLEM_PATH)?;
    Ok(())
}
