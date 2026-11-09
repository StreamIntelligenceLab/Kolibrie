/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The term model, for Python

use pyo3::prelude::*;
use pyo3::types::PyModule;
use shared::rdf_term;

/// Which kind of term this is: `"iri"`, `"blank_node"` or `"literal"`
#[pyfunction]
fn term_kind(term: &str) -> &'static str {
    match rdf_term::kind_of(term) {
        rdf_term::TermKind::Iri => "iri",
        rdf_term::TermKind::BlankNode => "blank_node",
        rdf_term::TermKind::Literal => "literal",
    }
}

/// The text a term denotes: a literal's lexical form, or the term itself
#[pyfunction]
fn lexical_value(term: &str) -> String {
    rdf_term::lexical_value(term).to_string()
}

/// The number a term denotes, or `None` if it denotes none
#[pyfunction]
fn numeric_value(term: &str) -> Option<f64> {
    rdf_term::numeric_value(term)
}

/// A literal's datatype IRI, or `None` for a plain or language-tagged literal, or a term that is
/// not a literal
#[pyfunction]
fn literal_datatype(term: &str) -> Option<String> {
    rdf_term::read_literal(term)?.datatype
}

/// A literal's language tag, or `None` when it has none or the term is not a literal
#[pyfunction]
fn literal_language(term: &str) -> Option<String> {
    rdf_term::read_literal(term)?.language
}

/// Builds a literal's term string
#[pyfunction]
#[pyo3(signature = (lexical, datatype=None, language=None))]
fn make_literal(lexical: &str, datatype: Option<&str>, language: Option<&str>) -> String {
    rdf_term::spell_literal(lexical, datatype, language)
}

/// A fresh blank-node scope, for one document's worth of labels
#[pyfunction]
fn new_document_scope() -> u64 {
    rdf_term::next_document_scope()
}

/// Builds a blank node's term string, from the bare label and a document scope
#[pyfunction]
fn make_blank_node(label: &str, document_scope: u64) -> String {
    rdf_term::spell_blank_node(label, document_scope)
}

/// Writes a term as N-Triples syntax: an IRI in brackets, a literal quoted
#[pyfunction]
fn to_ntriples_term(term: &str) -> String {
    rdf_term::to_ntriples_term(term)
}

/// Replaces a blank node with a Skolem IRI under `base`, leaving other terms alone
#[pyfunction]
fn skolemise(term: &str, base: &str) -> String {
    rdf_term::skolemise(term, base).into_owned()
}

/// Recovers the blank node a Skolem IRI stands for, or `None` for any other term
#[pyfunction]
fn deskolemise(term: &str) -> Option<String> {
    rdf_term::deskolemise(term)
}

/// Registers the term helpers on the extension module
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
