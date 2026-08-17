/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::error::RdfMessageError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageBaseFormat {
    NTriples,
    NQuads,
    Turtle,
    TriG,
}

impl MessageBaseFormat {
    pub fn media_type(&self) -> &'static str {
        match self {
            MessageBaseFormat::NTriples => "application/n-triples",
            MessageBaseFormat::NQuads => "application/n-quads",
            MessageBaseFormat::Turtle => "text/turtle",
            MessageBaseFormat::TriG => "application/trig",
        }
    }

    pub fn from_media_type(media_type: &str) -> Result<Self, RdfMessageError> {
        let essence = media_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match essence.as_str() {
            "application/n-triples" => Ok(MessageBaseFormat::NTriples),
            "application/n-quads" => Ok(MessageBaseFormat::NQuads),
            "text/turtle" => Ok(MessageBaseFormat::Turtle),
            "application/trig" => Ok(MessageBaseFormat::TriG),
            other => Err(RdfMessageError::UnknownMediaType(other.to_string())),
        }
    }

    pub fn from_extension(ext: &str) -> Result<Self, RdfMessageError> {
        match ext.trim_start_matches('.').to_ascii_lowercase().as_str() {
            "nt" | "ntriples" => Ok(MessageBaseFormat::NTriples),
            "nq" | "nquads" => Ok(MessageBaseFormat::NQuads),
            "ttl" | "turtle" => Ok(MessageBaseFormat::Turtle),
            "trig" => Ok(MessageBaseFormat::TriG),
            other => Err(RdfMessageError::UnknownMediaType(other.to_string())),
        }
    }

    pub fn is_line_based(&self) -> bool {
        matches!(self, MessageBaseFormat::NTriples | MessageBaseFormat::NQuads)
    }

    pub fn supports_prefixes(&self) -> bool {
        matches!(self, MessageBaseFormat::Turtle | MessageBaseFormat::TriG)
    }

    pub fn supports_graphs(&self) -> bool {
        matches!(self, MessageBaseFormat::NQuads | MessageBaseFormat::TriG)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_type_roundtrip() {
        for f in [
            MessageBaseFormat::NTriples,
            MessageBaseFormat::NQuads,
            MessageBaseFormat::Turtle,
            MessageBaseFormat::TriG,
        ] {
            assert_eq!(MessageBaseFormat::from_media_type(f.media_type()), Ok(f));
        }
    }

    #[test]
    fn media_type_ignores_parameters_and_case() {
        assert_eq!(
            MessageBaseFormat::from_media_type("Application/N-Triples; version=1.2-messages"),
            Ok(MessageBaseFormat::NTriples)
        );
    }

    #[test]
    fn extension_mapping() {
        assert_eq!(MessageBaseFormat::from_extension(".trig"), Ok(MessageBaseFormat::TriG));
        assert_eq!(MessageBaseFormat::from_extension("nq"), Ok(MessageBaseFormat::NQuads));
    }
}
