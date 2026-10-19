/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`QueryError`]: why a request could not be answered.
//!
//! Its own module rather than part of [`execute_query`](crate::execute_query),
//! because [`ml_runtime`](crate::ml_runtime) names it in a trait that
//! `execute_query` then calls through. An error type that several layers return has
//! to sit below all of them.
//!
//! `kolibrie::execute_query::QueryError` still resolves, as a re-export.

use std::fmt;

/// Why a request could not be answered.
///
/// # Rendering
///
/// `Display` prefixes a stable `QUERY_*` code, as
/// [`ProgramError`](crate::program::ProgramError) does. A [`Parse`](Self::Parse)
/// message is **multi-line**: it carries the annotated snippet the parser produced,
/// pointing at the offending token. It contains no terminal escapes, so it is safe to
/// log or serialise.
///
pub enum QueryError {
    /// The request did not parse as SPARQL or as one of the Kolibrie extensions.
    ///
    /// Carries the parser's message, which names a position in the input.
    Parse(String),
    /// It parsed, but is not a form this entry point accepts — an Update where a
    /// query was required, or more than one rule in a single request.
    Unsupported(String),
    /// The ML policy refused it. The request may be well-formed; the caller simply
    /// lacks the authority. See [`ml_policy`](crate::ml_policy).
    Ml(crate::ml_policy::MlError),
    /// Parsing and policy both passed, and evaluation failed. Covers an unresolvable
    /// dataset clause, an unsupported update template, and failure inside the plan.
    Execution(String),
}

// Debug delegates to Display rather than being derived, because `unwrap` and
// `expect` print the Debug form: a derived one escapes the newlines in a parse
// snippet, turning the annotated, multi-line message that points at the offending
// token into an unreadable single line of `\n`. The `QUERY_*` prefix carries the
// variant name, so nothing is lost by this.
impl fmt::Debug for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(message) => write!(f, "QUERY_PARSE_ERROR: {message}"),
            Self::Unsupported(message) => write!(f, "QUERY_UNSUPPORTED: {message}"),
            Self::Ml(error) => write!(f, "QUERY_POLICY: {error}"),
            Self::Execution(message) => write!(f, "QUERY_EXECUTION_FAILED: {message}"),
        }
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Ml(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::ml_policy::MlError> for QueryError {
    fn from(error: crate::ml_policy::MlError) -> Self {
        Self::Ml(error)
    }
}
