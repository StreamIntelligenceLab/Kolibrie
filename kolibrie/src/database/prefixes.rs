/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Prefix bindings, and expanding abbreviated terms to full IRIs

use super::KolibrieDB;
use shared::rdf_term;
use std::collections::HashMap;

impl KolibrieDB {
    /// Replaces every prefix binding
    pub fn set_prefixes(&mut self, prefixes: HashMap<String, String>) {
        self.prefixes = prefixes;
    }

    /// Prefix bindings, for resolving abbreviated terms
    pub fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    /// Binds `prefix` to `uri`, replacing any existing binding
    pub fn add_prefix(&mut self, prefix: impl Into<String>, uri: impl Into<String>) {
        self.prefixes.insert(prefix.into(), uri.into());
    }

    /// Merges a set of prefix bindings in, replacing any that collide
    pub fn extend_prefixes(&mut self, bindings: impl IntoIterator<Item = (String, String)>) {
        self.prefixes.extend(bindings);
    }

    /// Adopts the `PREFIX` declarations from a query, so later queries and rules can use them
    /// without repeating them
    pub fn register_prefixes_from_query(&mut self, query: &str) {
        let prefix_pattern = regex::Regex::new(r"PREFIX\s+([a-zA-Z0-9_]+):\s*<([^>]+)>").unwrap();

        for captures in prefix_pattern.captures_iter(query) {
            if captures.len() >= 3 {
                let prefix = captures[1].to_string();
                let uri = captures[2].to_string();
                self.prefixes.insert(prefix, uri);
            }
        }
    }

    /// Copies this store's prefixes into `prefixes`, leaving existing entries alone
    pub fn share_prefixes_with(&self, prefixes: &mut HashMap<String, String>) {
        for (prefix, uri) in &self.prefixes {
            prefixes.insert(prefix.clone(), uri.clone());
        }
    }

    /// Expands a lexical term to its full IRI using `prefixes`, then this store's own
    pub fn resolve_query_term(&self, term: &str, prefixes: &HashMap<String, String>) -> String {
        if term.starts_with("<<") && term.ends_with(">>") {
            return term.to_string();
        }
        if term.starts_with('<') && term.ends_with('>') {
            term.trim_start_matches('<')
                .trim_end_matches('>')
                .to_string()
        } else if term.starts_with('"') {
            if let Some(position) = term.rfind('"') {
                let literal = &term[..=position];
                let rest = term[position + 1..].trim();
                let resolved = if let Some(datatype) = rest.strip_prefix("^^") {
                    format!(
                        "{}^^<{}>",
                        literal,
                        self.resolve_query_term(datatype.trim(), prefixes)
                    )
                } else {
                    format!("{}{}", literal, rest)
                };
                rdf_term::canonicalise_literal(&resolved).unwrap_or(resolved)
            } else {
                term.to_string()
            }
        } else if term.contains(':')
            && !term.starts_with("http://")
            && !term.starts_with("https://")
        {
            let mut parts = term.splitn(2, ':');
            let prefix = parts.next().unwrap();
            let local_name = parts.next().unwrap_or("");

            if let Some(uri) = prefixes.get(prefix) {
                format!("{}{}", uri, local_name)
            } else if let Some(uri) = self.prefixes.get(prefix) {
                format!("{}{}", uri, local_name)
            } else {
                eprintln!("KOLIBRIE_OPERATION_FAILED");
                term.to_string()
            }
        } else {
            term.to_string()
        }
    }

    pub(super) fn resolve_term(&self, term: &str) -> String {
        if term.starts_with('<') && term.ends_with('>') {
            term.trim_start_matches('<')
                .trim_end_matches('>')
                .to_string()
        } else if term.starts_with('"') {
            if let Some(pos) = term.rfind('"') {
                let literal = &term[..=pos];
                let rest = &term[pos + 1..];
                let resolved = if let Some(datatype) = rest.strip_prefix("^^") {
                    format!("{}^^<{}>", literal, self.resolve_term(datatype.trim()))
                } else {
                    format!("{}{}", literal, rest)
                };
                rdf_term::canonicalise_literal(&resolved).unwrap_or(resolved)
            } else {
                term.to_string()
            }
        } else if term.contains(':')
            && !term.starts_with("http://")
            && !term.starts_with("https://")
        {
            let mut parts = term.splitn(2, ':');
            let prefix = parts.next().unwrap();
            let local_name = parts.next().unwrap_or("");
            if let Some(uri) = self.prefixes.get(prefix) {
                format!("{}{}", uri, local_name)
            } else {
                eprintln!("KOLIBRIE_OPERATION_FAILED");
                term.to_string()
            }
        } else {
            term.to_string()
        }
    }
}
