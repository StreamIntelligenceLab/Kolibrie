/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Notation3 (N3) logic over Kolibrie's term dictionary and dataset index

pub mod builtins;
pub mod canon;
pub mod error;
mod lexer;
pub mod lists;
pub mod literals;
mod matcher;
mod numeric;
pub mod parser;
mod proof;
pub mod reasoner;
mod rows;
pub mod rules;
pub mod scoped;
pub mod strings;
pub mod syntax;
pub mod terms;
pub mod time;

pub use error::N3Error;
pub use parser::{parse_document, parse_message_log, parse_rdf_star, parse_trig};
pub use reasoner::N3Reasoner;

/// Parses one input the way eyeron chooses its reader: an RDF Message Log when the text is one,
/// TriG when `trig` is set, N3 otherwise
///
/// # Errors
/// As [`parse_document`]
pub fn parse_input(
    text: &str,
    base: Option<&str>,
    trig: bool,
) -> Result<syntax::Document, N3Error> {
    if parser::is_message_log(text) {
        parse_message_log(text, base)
    } else if trig {
        parse_trig(text, base)
    } else {
        parse_document(text, base)
    }
}

/// Parses N3 text, reasons over it, and returns what the run outputs as N3: the query answers
/// when it has queries, else the derived facts
///
/// # Errors
/// Returns whatever [`parse_document`], [`N3Reasoner::load`] or [`N3Reasoner::run`] returns
pub fn reason(text: &str, base: Option<&str>) -> Result<String, N3Error> {
    let document = parse_document(text, base)?;
    let mut reasoner = N3Reasoner::new();
    reasoner.load(&document)?;
    reasoner.run()?;
    Ok(reasoner.output_n3())
}
