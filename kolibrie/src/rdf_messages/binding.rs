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
use super::format::MessageBaseFormat;
use super::model::{MessageQuad, RdfMessage};
use crate::sparql_database::SparqlDatabase;
use shared::dataset_index::GraphId;

pub(crate) fn message_to_quads(message: &RdfMessage) -> Result<Vec<MessageQuad>, RdfMessageError> {
    match message.format() {
        MessageBaseFormat::NTriples => {
            let mut db = SparqlDatabase::new();
            db.parse_ntriples_and_add(&message.to_binding_document());
            Ok(extract_quads(&db))
        }
        MessageBaseFormat::NQuads => {
            let mut db = SparqlDatabase::new();
            db.parse_nquads_and_add(&message.to_binding_document());
            Ok(extract_quads(&db))
        }
        MessageBaseFormat::Turtle => {
            let mut db = SparqlDatabase::new();
            db.parse_turtle(&message.to_binding_document());
            Ok(extract_quads(&db))
        }
        MessageBaseFormat::TriG => trig_message_to_quads(message),
    }
}

fn extract_quads(db: &SparqlDatabase) -> Vec<MessageQuad> {
    let mut out = Vec::new();
    for quad in db.dataset_index.all_quads() {
        let subject = db.decode_any(quad.subject).unwrap_or_default();
        let predicate = db.decode_any(quad.predicate).unwrap_or_default();
        let object = db.decode_any(quad.object).unwrap_or_default();
        let graph = match quad.graph {
            GraphId::Default => None,
            GraphId::Named(id) => db.decode_any(id),
        };
        out.push(MessageQuad {
            subject,
            predicate,
            object,
            graph,
        });
    }
    out
}

fn trig_message_to_quads(message: &RdfMessage) -> Result<Vec<MessageQuad>, RdfMessageError> {
    let prefixes = message.effective_prefixes();
    let prefix_header = prefixes
        .iter()
        .map(|(name, iri)| format!("@prefix {}: <{}> .\n", name, iri))
        .collect::<String>();

    let mut out = Vec::new();
    for statement in message.statements() {
        match split_graph_block(statement) {
            Some((label, inner)) => {
                let graph = label.map(|raw| resolve_term(&raw, &prefixes));
                let mut db = SparqlDatabase::new();
                db.parse_turtle(&format!("{}{}", prefix_header, inner));
                for quad in db.dataset_index.all_quads() {
                    out.push(MessageQuad {
                        subject: db.decode_any(quad.subject).unwrap_or_default(),
                        predicate: db.decode_any(quad.predicate).unwrap_or_default(),
                        object: db.decode_any(quad.object).unwrap_or_default(),
                        graph: graph.clone(),
                    });
                }
            }
            None => {
                // default-graph triple statement
                let mut db = SparqlDatabase::new();
                db.parse_turtle(&format!("{}{}\n", prefix_header, statement));
                out.extend(extract_quads(&db));
            }
        }
    }
    Ok(out)
}

/// Split a TriG graph block into its optional label and inner Turtle
fn split_graph_block(statement: &str) -> Option<(Option<String>, String)> {
    let chars: Vec<char> = statement.chars().collect();
    let open = find_top_level_brace(&chars)?;
    let close = find_matching_close(&chars, open)?;

    let label_part = chars[..open].iter().collect::<String>();
    let label_part = label_part.trim();
    let label_part = label_part
        .strip_prefix("GRAPH")
        .or_else(|| label_part.strip_prefix("graph"))
        .unwrap_or(label_part)
        .trim();

    let inner = chars[open + 1..close].iter().collect::<String>();
    let label = if label_part.is_empty() {
        None
    } else {
        Some(label_part.to_string())
    };
    Some((label, inner))
}

/// Index of the first `{` outside a string literal or IRI
fn find_top_level_brace(chars: &[char]) -> Option<usize> {
    let mut in_literal = false;
    let mut in_iri = false;
    let mut escaped = false;
    for (i, &c) in chars.iter().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_literal => escaped = true,
            '"' if !in_iri => in_literal = !in_literal,
            '<' if !in_literal => in_iri = true,
            '>' if in_iri => in_iri = false,
            '{' if !in_literal && !in_iri => return Some(i),
            _ => {}
        }
    }
    None
}

/// Index of the `}` matching the `{` at `open`
fn find_matching_close(chars: &[char], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_literal = false;
    let mut in_iri = false;
    let mut escaped = false;
    for (i, &c) in chars.iter().enumerate().skip(open) {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_literal => escaped = true,
            '"' if !in_iri => in_literal = !in_literal,
            '<' if !in_literal => in_iri = true,
            '>' if in_iri => in_iri = false,
            '{' if !in_literal && !in_iri => depth += 1,
            '}' if !in_literal && !in_iri => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Resolve a `<iri>` or `prefix:local` term to its full IRI
fn resolve_term(raw: &str, prefixes: &[(String, String)]) -> String {
    let t = raw.trim();
    if t.starts_with('<') && t.ends_with('>') && t.len() >= 2 {
        return t[1..t.len() - 1].to_string();
    }
    if let Some((prefix, local)) = t.split_once(':') {
        if !t.starts_with("http://") && !t.starts_with("https://") {
            if let Some((_, iri)) = prefixes.iter().find(|(n, _)| n == prefix) {
                return format!("{}{}", iri, local);
            }
        }
    }
    t.to_string()
}
