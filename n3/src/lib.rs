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
mod matcher;
mod numeric;
pub mod parser;
pub mod reasoner;
pub mod rules;
pub mod syntax;
pub mod terms;

pub use error::N3Error;
pub use parser::parse_document;
pub use reasoner::N3Reasoner;

/// Parses N3 text, saturates it under its forward rules, and returns the derived facts as N3
///
/// # Errors
/// Returns whatever [`parse_document`], [`N3Reasoner::load`] or [`N3Reasoner::run`] returns
pub fn reason(text: &str, base: Option<&str>) -> Result<String, N3Error> {
    let document = parse_document(text, base)?;
    let mut reasoner = N3Reasoner::new();
    reasoner.load(&document)?;
    reasoner.run()?;
    Ok(reasoner.derived_n3())
}
