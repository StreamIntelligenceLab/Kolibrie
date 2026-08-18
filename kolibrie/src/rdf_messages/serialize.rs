/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Renders a message back to N-Triples so it can be handed to the engine, whose
//! only ingestion path is `RSPEngine::parse_data`

use super::error::RdfMessageError;
use super::format::MessageBaseFormat;
use super::model::{MessageQuad, RdfMessage};

impl RdfMessage {
    /// Render this message as N-Triples
    ///
    /// A message that is already N-Triples is passed through untouched,
    /// anything else is rebuilt from its decoded quads
    pub fn to_ntriples(&self) -> Result<String, RdfMessageError> {
        if self.format() == MessageBaseFormat::NTriples {
            let mut out = String::new();
            for statement in self.statements() {
                out.push_str(statement);
                out.push('\n');
            }
            return Ok(out);
        }
        Ok(quads_to_ntriples(&self.to_quads()?))
    }
}

/// Render decoded quads as N-Triples lines
///
/// Graph names have no place in N-Triples and are dropped, which matches the
/// engine's triple-only stream model
pub fn quads_to_ntriples(quads: &[MessageQuad]) -> String {
    let mut out = String::new();
    for quad in quads {
        out.push_str(&format!(
            "{} {} {} .\n",
            render_subject(&quad.subject),
            render_iri(&quad.predicate),
            render_object(&quad.object)
        ));
    }
    out
}

/// Subjects are always IRIs in the engine's string model
fn render_subject(term: &str) -> String {
    render_iri(term)
}

fn render_iri(term: &str) -> String {
    let t = term.trim();
    if t.starts_with('<') && t.ends_with('>') {
        t.to_string()
    } else {
        format!("<{}>", t)
    }
}

/// Objects carrying a scheme are treated as IRIs, everything else as a literal
///
/// The distinction only has to survive N-Triples tokenization, the engine strips
/// both brackets and quotes again before encoding the term
fn render_object(term: &str) -> String {
    let t = term.trim();
    if t.starts_with('<') && t.ends_with('>') {
        return t.to_string();
    }
    if t.contains("://") {
        return format!("<{}>", t);
    }
    format!("\"{}\"", escape_literal(t))
}

fn escape_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdf_messages::parse_message_log;

    #[test]
    fn ntriples_message_passes_through() {
        let input = "<http://example.org/s> <http://example.org/p> \"a\" .\n";
        let log = parse_message_log(input, MessageBaseFormat::NTriples).unwrap();
        let rendered = log.messages[0].to_ntriples().unwrap();
        assert!(rendered.contains("<http://example.org/s>"));
        assert!(rendered.contains("\"a\""));
    }

    #[test]
    fn turtle_message_renders_as_ntriples() {
        let input = concat!(
            "@prefix ex: <http://example.org/> .\n",
            "ex:s ex:p \"a\" .\n",
        );
        let log = parse_message_log(input, MessageBaseFormat::Turtle).unwrap();
        let rendered = log.messages[0].to_ntriples().unwrap();
        assert_eq!(
            rendered.trim(),
            "<http://example.org/s> <http://example.org/p> \"a\" ."
        );
    }

    #[test]
    fn iri_objects_keep_bracket_form() {
        let quads = vec![MessageQuad {
            subject: "http://example.org/s".to_string(),
            predicate: "http://example.org/p".to_string(),
            object: "http://example.org/o".to_string(),
            graph: None,
        }];
        assert_eq!(
            quads_to_ntriples(&quads).trim(),
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> ."
        );
    }

    #[test]
    fn literal_quotes_are_escaped() {
        let quads = vec![MessageQuad {
            subject: "http://example.org/s".to_string(),
            predicate: "http://example.org/p".to_string(),
            object: "he said \"hi\"".to_string(),
            graph: None,
        }];
        assert!(quads_to_ntriples(&quads).contains("\"he said \\\"hi\\\"\""));
    }

    #[test]
    fn rendered_output_reparses_to_the_same_quads() {
        let input = concat!(
            "@prefix ex: <http://example.org/> .\n",
            "ex:s ex:p \"a\" .\n",
            "ex:s ex:q ex:o .\n",
        );
        let log = parse_message_log(input, MessageBaseFormat::Turtle).unwrap();
        let original = log.messages[0].to_quads().unwrap();

        let rendered = log.messages[0].to_ntriples().unwrap();
        let reparsed_log = parse_message_log(&rendered, MessageBaseFormat::NTriples).unwrap();
        let reparsed = reparsed_log.messages[0].to_quads().unwrap();

        assert_eq!(original.len(), reparsed.len());
        for quad in &original {
            assert!(reparsed.contains(quad), "missing {:?} after round trip", quad);
        }
    }
}
