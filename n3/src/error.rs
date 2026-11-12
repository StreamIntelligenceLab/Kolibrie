/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Why parsing or reasoning stopped

use std::fmt;

/// An error from reading or reasoning over N3
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum N3Error {
    /// The text is not N3 this parser accepts
    Syntax {
        /// What was wrong
        message: String,
        /// Byte offset into the input, where known
        offset: Option<usize>,
    },
    /// The program uses an N3 feature this engine does not implement yet
    Unsupported(String),
    /// Reasoning exceeded a resource budget before reaching a fixpoint
    Limit(String),
}

impl N3Error {
    /// A syntax error at a byte offset
    pub fn at(message: impl Into<String>, offset: usize) -> Self {
        N3Error::Syntax {
            message: message.into(),
            offset: Some(offset),
        }
    }

    /// A syntax error with no position
    pub fn syntax(message: impl Into<String>) -> Self {
        N3Error::Syntax {
            message: message.into(),
            offset: None,
        }
    }

    /// An unimplemented feature
    pub fn unsupported(feature: impl Into<String>) -> Self {
        N3Error::Unsupported(feature.into())
    }
}

impl fmt::Display for N3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            N3Error::Syntax {
                message,
                offset: Some(offset),
            } => write!(f, "syntax error at byte {offset}: {message}"),
            N3Error::Syntax {
                message,
                offset: None,
            } => write!(f, "syntax error: {message}"),
            N3Error::Unsupported(feature) => write!(f, "unsupported: {feature}"),
            N3Error::Limit(budget) => write!(f, "limit reached: {budget}"),
        }
    }
}

impl std::error::Error for N3Error {}
