/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::binding;
use super::error::RdfMessageError;
use super::format::MessageBaseFormat;
use super::version::VersionLabel;
use crate::sparql_database::SparqlDatabase;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageQuad {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub graph: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MessageItem {
    Prefix { name: String, iri: String },
    Base(String),
    Statement(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdfMessage {
    pub(crate) format: MessageBaseFormat,
    pub(crate) inherited_base: Option<String>,
    pub(crate) inherited_prefixes: Vec<(String, String)>,
    pub(crate) items: Vec<MessageItem>,
}

impl RdfMessage {
    pub fn format(&self) -> MessageBaseFormat {
        self.format
    }

    pub fn is_empty(&self) -> bool {
        !self
            .items
            .iter()
            .any(|i| matches!(i, MessageItem::Statement(_)))
    }

    pub fn statements(&self) -> Vec<&str> {
        self.items
            .iter()
            .filter_map(|i| match i {
                MessageItem::Statement(s) => Some(s.as_str()),
                _ => None,
            })
            .collect()
    }

    pub fn effective_prefixes(&self) -> Vec<(String, String)> {
        let mut prefixes = self.inherited_prefixes.clone();
        for item in &self.items {
            if let MessageItem::Prefix { name, iri } = item {
                upsert_prefix(&mut prefixes, name, iri);
            }
        }
        prefixes
    }

    pub fn effective_base(&self) -> Option<String> {
        let mut base = self.inherited_base.clone();
        for item in &self.items {
            if let MessageItem::Base(b) = item {
                base = Some(b.clone());
            }
        }
        base
    }

    pub fn to_document(&self) -> String {
        let mut out = String::new();
        if self.format.supports_prefixes() {
            if let Some(base) = &self.inherited_base {
                out.push_str(&format!("@base <{}> .\n", base));
            }
            for (name, iri) in &self.inherited_prefixes {
                out.push_str(&format!("@prefix {}: <{}> .\n", name, iri));
            }
        }
        for item in &self.items {
            match item {
                MessageItem::Prefix { name, iri } => {
                    out.push_str(&format!("@prefix {}: <{}> .\n", name, iri));
                }
                MessageItem::Base(b) => {
                    out.push_str(&format!("@base <{}> .\n", b));
                }
                MessageItem::Statement(s) => {
                    out.push_str(s);
                    out.push('\n');
                }
            }
        }
        out
    }

    pub(crate) fn to_binding_document(&self) -> String {
        let mut out = String::new();
        if self.format.supports_prefixes() {
            for (name, iri) in self.effective_prefixes() {
                out.push_str(&format!("@prefix {}: <{}> .\n", name, iri));
            }
        }
        for item in &self.items {
            if let MessageItem::Statement(s) = item {
                out.push_str(s);
                out.push('\n');
            }
        }
        out
    }

    pub fn to_quads(&self) -> Result<Vec<MessageQuad>, RdfMessageError> {
        binding::message_to_quads(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdfMessageLog {
    pub version: Option<VersionLabel>,
    pub format: MessageBaseFormat,
    pub messages: Vec<RdfMessage>,
}

impl RdfMessageLog {
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, RdfMessage> {
        self.messages.iter()
    }

    pub fn load_into(&self, db: &mut SparqlDatabase) -> Result<(), RdfMessageError> {
        for message in &self.messages {
            for quad in message.to_quads()? {
                match &quad.graph {
                    Some(graph) => {
                        db.add_quad_parts(&quad.subject, &quad.predicate, &quad.object, graph);
                    }
                    None => {
                        db.add_triple_parts(&quad.subject, &quad.predicate, &quad.object);
                    }
                }
            }
        }
        Ok(())
    }
}

impl<'a> IntoIterator for &'a RdfMessageLog {
    type Item = &'a RdfMessage;
    type IntoIter = std::slice::Iter<'a, RdfMessage>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.iter()
    }
}

pub(crate) fn upsert_prefix(prefixes: &mut Vec<(String, String)>, name: &str, iri: &str) {
    if let Some(entry) = prefixes.iter_mut().find(|(n, _)| n == name) {
        entry.1 = iri.to_string();
    } else {
        prefixes.push((name.to_string(), iri.to_string()));
    }
}
