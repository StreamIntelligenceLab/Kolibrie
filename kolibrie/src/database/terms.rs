/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Term encoding: the dictionary, the quoted-triple store, and moving IDs between stores

use super::KolibrieDB;
use shared::dictionary::Dictionary;
use shared::quoted_triple_store::{is_quoted_triple_id, QuotedTripleStore};
use shared::rdf_term;
use shared::term_encoder::SharedTerms;
use std::collections::HashMap;
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};

pub(crate) fn reencode_term_id(
    id: u32,
    source_dictionary: &Dictionary,
    source_quoted_triples: &QuotedTripleStore,
    target_dictionary: &mut Dictionary,
    target_quoted_triples: &mut QuotedTripleStore,
    translated_ids: &mut HashMap<u32, u32>,
) -> u32 {
    if let Some(translated) = translated_ids.get(&id) {
        return *translated;
    }

    let translated = if is_quoted_triple_id(id) {
        let (subject, predicate, object) = source_quoted_triples
            .decode(id)
            .unwrap_or_else(|| panic!("quoted triple ID {id} is missing from its source store"));
        let subject = reencode_term_id(
            subject,
            source_dictionary,
            source_quoted_triples,
            target_dictionary,
            target_quoted_triples,
            translated_ids,
        );
        let predicate = reencode_term_id(
            predicate,
            source_dictionary,
            source_quoted_triples,
            target_dictionary,
            target_quoted_triples,
            translated_ids,
        );
        let object = reencode_term_id(
            object,
            source_dictionary,
            source_quoted_triples,
            target_dictionary,
            target_quoted_triples,
            translated_ids,
        );
        target_quoted_triples.encode(subject, predicate, object)
    } else {
        let lexical = source_dictionary
            .decode(id)
            .unwrap_or_else(|| panic!("term ID {id} is missing from its source dictionary"));
        target_dictionary.encode(lexical)
    };

    translated_ids.insert(id, translated);
    translated
}

/// Read borrow of the term dictionary, from [`KolibrieDB::dictionary`]
pub type DictionaryRef<'a> = shared::term_encoder::TermsRef<'a>;

/// Write borrow of the term dictionary, from [`KolibrieDB::dictionary_mut`]
pub type DictionaryMut<'a> = shared::term_encoder::TermsMut<'a>;

/// Read guard over the quoted-triple store, from [`KolibrieDB::quoted_triples`]
#[derive(Debug)]
pub struct QuotedTriplesRef<'a>(RwLockReadGuard<'a, QuotedTripleStore>);

impl std::ops::Deref for QuotedTriplesRef<'_> {
    type Target = QuotedTripleStore;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Write guard over the quoted-triple store, from [`KolibrieDB::quoted_triples_mut`]
#[derive(Debug)]
pub struct QuotedTriplesMut<'a>(RwLockWriteGuard<'a, QuotedTripleStore>);

impl std::ops::Deref for QuotedTriplesMut<'_> {
    type Target = QuotedTripleStore;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for QuotedTriplesMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl KolibrieDB {
    /// Encode a term that may be a quoted triple `<< s p o >>` (recursive)
    pub fn encode_term_star(&self, term: &str) -> u32 {
        let trimmed = term.trim();
        if trimmed.starts_with("<<") && trimmed.ends_with(">>") {
            let inner = &trimmed[2..trimmed.len() - 2].trim();
            let (s_str, p_str, o_str) = Self::split_quoted_triple_content(inner);
            let s_id = self.encode_term_star(&s_str);
            let p_id = self.encode_term_star(&p_str);
            let o_id = self.encode_term_star(&o_str);
            let mut qt = self.quoted_triple_store.write().unwrap();
            qt.encode(s_id, p_id, o_id)
        } else {
            let cleaned = if trimmed.starts_with('<') && trimmed.ends_with('>') {
                trimmed[1..trimmed.len() - 1].to_string()
            } else if trimmed.starts_with('"') {
                rdf_term::canonicalise_literal(trimmed)
                    .unwrap_or_else(|| trimmed.trim_matches('"').to_string())
            } else {
                trimmed.to_string()
            };
            let mut dict = self.dictionary.write();
            dict.encode(&cleaned)
        }
    }

    /// Rewrites every blank node label in `term` into `document`'s scope
    pub(super) fn scope_blank_nodes(&self, term: String, document: u64) -> String {
        if !term.contains("_:") {
            return term;
        }
        let trimmed = term.trim();
        if trimmed.starts_with("<<") && trimmed.ends_with(">>") {
            let inner = trimmed[2..trimmed.len() - 2].trim();
            let (subject, predicate, object) = Self::split_quoted_triple_content(inner);
            return format!(
                "<< {} {} {} >>",
                self.scope_blank_nodes(subject, document),
                self.scope_blank_nodes(predicate, document),
                self.scope_blank_nodes(object, document)
            );
        }
        if trimmed.starts_with("_:") {
            return rdf_term::scope_blank_node(trimmed, document).into_owned();
        }
        term
    }

    /// Decode a u32 ID that may be a regular dictionary ID or a quoted triple ID
    pub fn decode_any(&self, id: u32) -> Option<String> {
        if is_quoted_triple_id(id) {
            let qt = self.quoted_triple_store.read().unwrap();
            let dict = self.dictionary.read();
            dict.decode_term(id, &qt)
        } else {
            let dict = self.dictionary.read();
            dict.decode(id).map(|s| s.to_string())
        }
    }

    /// Split quoted triple content `s p o` into three parts, respecting nested `<< >>`
    pub fn split_quoted_triple_content(content: &str) -> (String, String, String) {
        let mut parts: Vec<String> = Vec::new();
        let mut current = String::new();
        let mut depth = 0;
        let mut in_uri = false;
        let mut in_literal = false;
        let mut escaped = false;

        for ch in content.chars() {
            if escaped {
                current.push(ch);
                escaped = false;
                continue;
            }
            match ch {
                '\\' if in_literal => {
                    current.push(ch);
                    escaped = true;
                }
                '"' if !in_uri => {
                    in_literal = !in_literal;
                    current.push(ch);
                }
                '<' if !in_literal => {
                    current.push(ch);
                    if current.ends_with("<<") {
                        depth += 1;
                    } else if depth == 0 {
                        in_uri = true;
                    }
                }
                '>' if !in_literal => {
                    current.push(ch);
                    if in_uri {
                        in_uri = false;
                    } else if current.ends_with(">>") && depth > 0 {
                        depth -= 1;
                    }
                }
                ' ' | '\t' | '\n' | '\r' if depth == 0 && !in_uri && !in_literal => {
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        parts.push(trimmed);
                        current.clear();
                    }
                }
                _ => {
                    current.push(ch);
                }
            }
        }
        let trimmed = current.trim().to_string();
        if !trimmed.is_empty() {
            parts.push(trimmed);
        }

        if parts.len() >= 3 {
            (parts[0].clone(), parts[1].clone(), parts[2..].join(" "))
        } else {
            let s = parts.first().cloned().unwrap_or_default();
            let p = parts.get(1).cloned().unwrap_or_default();
            let o = parts.get(2).cloned().unwrap_or_default();
            (s, p, o)
        }
    }

    /// Read access to the term dictionary
    pub fn dictionary(&self) -> DictionaryRef<'_> {
        self.dictionary.read()
    }

    /// Write access to the term dictionary, for interning new terms
    pub fn dictionary_mut(&self) -> DictionaryMut<'_> {
        self.dictionary.write()
    }

    /// Whether `self` and `other` encode terms through the same dictionary, as opposed to holding
    /// separate ones
    pub fn shares_dictionary_with(&self, other: &KolibrieDB) -> bool {
        self.dictionary.shares_space_with(&other.dictionary)
    }

    /// This store's term space, as an encoder another component can adopt
    pub fn terms(&self) -> SharedTerms {
        self.dictionary.clone()
    }

    /// Adopts another component's term space, replacing this store's own
    pub fn adopt_terms(&mut self, terms: SharedTerms) {
        self.dictionary = terms;
    }

    /// Read access to the quoted-triple store
    pub fn quoted_triples(&self) -> QuotedTriplesRef<'_> {
        QuotedTriplesRef(self.quoted_triple_store.read().unwrap())
    }

    /// Write access to the quoted-triple store
    pub fn quoted_triples_mut(&self) -> QuotedTriplesMut<'_> {
        QuotedTriplesMut(self.quoted_triple_store.write().unwrap())
    }

    /// Whether `self` and `other` share one quoted-triple store, as opposed to holding separate
    /// ones
    pub fn shares_quoted_triples_with(&self, other: &KolibrieDB) -> bool {
        Arc::ptr_eq(&self.quoted_triple_store, &other.quoted_triple_store)
    }
}
