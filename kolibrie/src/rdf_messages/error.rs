/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdfMessageError {
    UnknownVersionLabel(String),
    UnknownMediaType(String),
    MalformedDirective(String),
    UnterminatedLiteral,
    UnbalancedDelimiters,
    VersionMismatch(String),
    Binding(String),
}

impl fmt::Display for RdfMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdfMessageError::UnknownVersionLabel(label) => {
                write!(f, "unknown RDF version label: {:?}", label)
            }
            RdfMessageError::UnknownMediaType(mt) => {
                write!(f, "unsupported RDF Message media type: {:?}", mt)
            }
            RdfMessageError::MalformedDirective(d) => {
                write!(f, "malformed directive: {}", d)
            }
            RdfMessageError::UnterminatedLiteral => {
                write!(f, "unterminated string literal in RDF Message Log")
            }
            RdfMessageError::UnbalancedDelimiters => {
                write!(f, "unbalanced '{{' / '}}' (or bracket/paren) in RDF Message Log")
            }
            RdfMessageError::VersionMismatch(msg) => {
                write!(f, "version / message-delimiter mismatch: {}", msg)
            }
            RdfMessageError::Binding(msg) => {
                write!(f, "failed to materialize RDF Message: {}", msg)
            }
        }
    }
}

impl std::error::Error for RdfMessageError {}
