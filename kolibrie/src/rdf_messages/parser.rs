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
use super::model::{upsert_prefix, MessageItem, RdfMessage, RdfMessageLog};
use super::scanner::{self, Chunk};
use super::version::VersionLabel;

#[derive(Debug, Clone)]
pub struct RdfMessageLogParser {
    format: MessageBaseFormat,
    default_version: Option<VersionLabel>,
}

impl RdfMessageLogParser {
    /// Create a parser for the given serialization
    pub fn new(format: MessageBaseFormat) -> Self {
        RdfMessageLogParser {
            format,
            default_version: None,
        }
    }

    pub fn with_default_version(mut self, version: VersionLabel) -> Self {
        self.default_version = Some(version);
        self
    }

    /// The serialization this parser expects
    pub fn format(&self) -> MessageBaseFormat {
        self.format
    }

    /// Parse a complete RDF Message Log
    pub fn parse(&self, input: &str) -> Result<RdfMessageLog, RdfMessageError> {
        let chunks = scanner::scan(input, self.format)?;

        let mut version = self.default_version;
        let mut env_base: Option<String> = None;
        let mut env_prefixes: Vec<(String, String)> = Vec::new();

        let mut messages: Vec<RdfMessage> = Vec::new();
        let mut current: Option<Builder> = Some(Builder::new(&env_base, &env_prefixes));
        let mut saw_delimiter = false;

        for chunk in chunks {
            match chunk {
                Chunk::Version(label) => {
                    version = Some(VersionLabel::parse(&label)?);
                }
                Chunk::Prefix { name, iri } => {
                    ensure_open(&mut current, &env_base, &env_prefixes);
                    current
                        .as_mut()
                        .unwrap()
                        .items
                        .push(MessageItem::Prefix {
                            name: name.clone(),
                            iri: iri.clone(),
                        });
                    upsert_prefix(&mut env_prefixes, &name, &iri);
                }
                Chunk::Base(iri) => {
                    ensure_open(&mut current, &env_base, &env_prefixes);
                    current
                        .as_mut()
                        .unwrap()
                        .items
                        .push(MessageItem::Base(iri.clone()));
                    env_base = Some(iri);
                }
                Chunk::Statement(text) => {
                    ensure_open(&mut current, &env_base, &env_prefixes);
                    current
                        .as_mut()
                        .unwrap()
                        .items
                        .push(MessageItem::Statement(text));
                }
                Chunk::Message => {
                    saw_delimiter = true;
                    ensure_open(&mut current, &env_base, &env_prefixes);
                    let builder = current.take().unwrap();
                    messages.push(builder.finish(self.format));
                }
            }
        }

        if let Some(builder) = current.take() {
            if !builder.items.is_empty() || !messages.is_empty() {
                messages.push(builder.finish(self.format));
            }
        }

        if let Some(v) = version {
            if !v.supports_messages() && saw_delimiter {
                return Err(RdfMessageError::VersionMismatch(format!(
                    "document declares version {:?} but uses MESSAGE delimiters",
                    v.as_str()
                )));
            }
        }

        Ok(RdfMessageLog {
            version,
            format: self.format,
            messages,
        })
    }
}

/// Accumulator for a single in-progress message
struct Builder {
    inherited_base: Option<String>,
    inherited_prefixes: Vec<(String, String)>,
    items: Vec<MessageItem>,
}

impl Builder {
    fn new(env_base: &Option<String>, env_prefixes: &[(String, String)]) -> Self {
        Builder {
            inherited_base: env_base.clone(),
            inherited_prefixes: env_prefixes.to_vec(),
            items: Vec::new(),
        }
    }

    fn finish(self, format: MessageBaseFormat) -> RdfMessage {
        RdfMessage {
            format,
            inherited_base: self.inherited_base,
            inherited_prefixes: self.inherited_prefixes,
            items: self.items,
        }
    }
}

fn ensure_open(
    current: &mut Option<Builder>,
    env_base: &Option<String>,
    env_prefixes: &[(String, String)],
) {
    if current.is_none() {
        *current = Some(Builder::new(env_base, &env_prefixes.to_vec()));
    }
}
