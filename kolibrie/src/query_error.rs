/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`QueryError`]: why a request could not be answered

use std::fmt;

/// Why a request could not be answered
pub enum QueryError {
    /// The request did not parse as SPARQL or as one of the Kolibrie extensions
    Parse(String),
    /// It parsed, but is not a form this entry point accepts — an Update where a query was
    /// required, or more than one rule in a single request
    Unsupported(String),
    /// The ML policy refused it
    Ml(crate::ml_policy::MlError),
    /// Parsing and policy both passed, and evaluation failed
    Execution(String),
}

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
