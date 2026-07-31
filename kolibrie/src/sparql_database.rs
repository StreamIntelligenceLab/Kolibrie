/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::query_builder::QueryBuilder;
use crate::streamertail_optimizer::DatabaseStats;
use crate::utils;
use crate::utils::ClonableFn;
use crossbeam::channel::unbounded;
use crossbeam::scope;
use percent_encoding::percent_decode;
use quick_xml::events::Event;
use quick_xml::name::QName;
use quick_xml::Reader;
use rayon::prelude::*;
use shared::dataset_index::{DatasetIndex, GraphId, Quad};
use shared::dictionary::Dictionary;
use shared::query::{ModelDecl, NeuralRelationDecl, TrainNeuralRelationDecl};
use shared::quoted_triple_store::{is_quoted_triple_id, QuotedTripleStore};
use shared::triple::Triple;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use url::Url;

fn looks_like_absolute_iri(value: &str) -> bool {
    let Some((scheme, _)) = value.split_once(':') else {
        return false;
    };
    let mut characters = scheme.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
}

fn escape_ntriples_literal(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '"' => "\\\"".chars().collect(),
            '\n' => "\\n".chars().collect(),
            '\r' => "\\r".chars().collect(),
            '\t' => "\\t".chars().collect(),
            other => vec![other],
        })
        .collect()
}

/// Decodes the lexical value of an N-Triples/N-Quads double-quoted literal
/// and returns the suffix following its escape-aware closing quote.
fn decode_ntriples_literal(term: &str) -> Option<(String, &str)> {
    let body = term.strip_prefix('"')?;
    let mut characters = body.char_indices();
    let mut value = String::new();

    while let Some((offset, character)) = characters.next() {
        match character {
            '"' => return Some((value, &body[offset + character.len_utf8()..])),
            '\\' => {
                let (_, escaped) = characters.next()?;
                match escaped {
                    't' => value.push('\t'),
                    'b' => value.push('\u{0008}'),
                    'n' => value.push('\n'),
                    'r' => value.push('\r'),
                    'f' => value.push('\u{000c}'),
                    '"' => value.push('"'),
                    '\'' => value.push('\''),
                    '\\' => value.push('\\'),
                    'u' | 'U' => {
                        let digits = if escaped == 'u' { 4 } else { 8 };
                        let mut scalar = String::with_capacity(digits);
                        for _ in 0..digits {
                            let (_, digit) = characters.next()?;
                            if !digit.is_ascii_hexdigit() {
                                return None;
                            }
                            scalar.push(digit);
                        }
                        let scalar = u32::from_str_radix(&scalar, 16).ok()?;
                        value.push(char::from_u32(scalar)?);
                    }
                    _ => return None,
                }
            }
            character => value.push(character),
        }
    }

    None
}

fn decode_form_component(component: &str) -> String {
    let normalized = component
        .bytes()
        .map(|byte| if byte == b'+' { b' ' } else { byte })
        .collect::<Vec<_>>();
    percent_decode(&normalized).decode_utf8_lossy().into_owned()
}

fn parse_form_urlencoded(body: &str) -> HashMap<String, String> {
    body.split('&')
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode_form_component(name), decode_form_component(value))
        })
        .collect()
}

fn reencode_term_id(
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

#[derive(Debug, Clone)]
pub struct SparqlDatabase {
    pub dataset_index: DatasetIndex,
    pub dictionary: Arc<RwLock<Dictionary>>,
    pub prefixes: HashMap<String, String>,
    pub udfs: HashMap<String, ClonableFn>,
    pub rule_map: HashMap<String, String>,
    pub model_decls: HashMap<String, ModelDecl>,
    pub neural_relation_decls: HashMap<String, NeuralRelationDecl>,
    pub train_neural_relation_decls: HashMap<String, TrainNeuralRelationDecl>,
    pub neural_model_artifacts: HashMap<String, String>,
    pub neural_materialized_triples: HashMap<String, Vec<Triple>>,
    pub ml_predict_materialized_triples: HashMap<String, Vec<Triple>>,
    pub probability_seeds: HashMap<Triple, f64>,
    pub cached_stats: Option<Arc<DatabaseStats>>,
    pub quoted_triple_store: Arc<RwLock<QuotedTripleStore>>,
}

#[allow(dead_code)]
impl SparqlDatabase {
    pub fn new() -> Self {
        Self {
            dataset_index: DatasetIndex::new(),
            dictionary: Arc::new(RwLock::new(Dictionary::new())),
            prefixes: HashMap::new(),
            udfs: HashMap::new(),
            rule_map: HashMap::new(),
            model_decls: HashMap::new(),
            neural_relation_decls: HashMap::new(),
            train_neural_relation_decls: HashMap::new(),
            neural_model_artifacts: HashMap::new(),
            neural_materialized_triples: HashMap::new(),
            ml_predict_materialized_triples: HashMap::new(),
            probability_seeds: HashMap::new(),
            cached_stats: None,
            quoted_triple_store: Arc::new(RwLock::new(QuotedTripleStore::new())),
        }
    }

    /// Encode a term that may be a quoted triple `<< s p o >>` (recursive).
    /// Returns the u32 ID for the term.
    /// Handles stripping `<>` from URIs and `""` from literals.
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
                decode_ntriples_literal(trimmed)
                    .map(|(value, _)| value)
                    .unwrap_or_else(|| trimmed.trim_matches('"').to_string())
            } else {
                trimmed.to_string()
            };
            let mut dict = self.dictionary.write().unwrap();
            dict.encode(&cleaned)
        }
    }

    /// Decode a u32 ID that may be a regular dictionary ID or a quoted triple ID.
    pub fn decode_any(&self, id: u32) -> Option<String> {
        if is_quoted_triple_id(id) {
            let qt = self.quoted_triple_store.read().unwrap();
            let dict = self.dictionary.read().unwrap();
            dict.decode_term(id, &qt)
        } else {
            let dict = self.dictionary.read().unwrap();
            dict.decode(id).map(|s| s.to_string())
        }
    }

    /// Split quoted triple content `s p o` into three parts, respecting nested `<< >>`.
    /// This is used both internally and by the query optimizer for pattern parsing.
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
                    // Check if this starts a quoted triple (<<)
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
            // Fallback: pad with empty strings
            let s = parts.first().cloned().unwrap_or_default();
            let p = parts.get(1).cloned().unwrap_or_default();
            let o = parts.get(2).cloned().unwrap_or_default();
            (s, p, o)
        }
    }

    pub fn set_prefixes(&mut self, prefixes: HashMap<String, String>) {
        self.prefixes = prefixes;
    }

    pub fn get_or_build_stats(&mut self) -> Arc<DatabaseStats> {
        if let Some(stats) = &self.cached_stats {
            return stats.clone(); // ← Clone the Arc (cheap), not the DatabaseStats
        }

        let stats = Arc::new(DatabaseStats::gather_stats_fast(self));
        self.cached_stats = Some(stats.clone());
        stats
    }

    pub fn invalidate_stats_cache(&mut self) {
        self.cached_stats = None;
    }

    pub fn query(&self) -> QueryBuilder<'_> {
        QueryBuilder::new(self)
    }

    pub fn add_triple(&mut self, triple: Triple) {
        self.dataset_index.insert_triple(&triple);
    }

    pub fn delete_triple(&mut self, triple: &Triple) -> bool {
        self.dataset_index.delete_triple(triple)
    }

    pub fn add_quad(&mut self, quad: Quad) -> bool {
        let inserted = self.dataset_index.insert_quad(&quad);
        inserted
    }

    pub fn delete_quad(&mut self, quad: &Quad) -> bool {
        let deleted = self.dataset_index.delete_quad(quad);
        deleted
    }

    pub fn add_quad_parts(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
        graph: &str,
    ) -> bool {
        let subject_id = self.encode_term_star(subject);
        let predicate_id = self.encode_term_star(predicate);
        let object_id = self.encode_term_star(object);
        let graph_id = {
            let mut dict = self.dictionary.write().unwrap();
            dict.encode(graph)
        };

        self.add_quad(Quad {
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
            graph: GraphId::Named(graph_id),
        })
    }

    pub fn query_default_triples(
        &self,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Vec<Triple> {
        self.dataset_index.query_default(s, p, o)
    }

    pub fn query_graph_quads(
        &self,
        graph: GraphId,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Vec<Quad> {
        self.dataset_index.query_graph(graph, s, p, o)
    }

    /// Helper function that accepts parts of a triple, constructs a Triple, and adds it
    pub fn add_triple_parts(&mut self, subject: &str, predicate: &str, object: &str) {
        let mut dict = self.dictionary.write().unwrap();
        let subject_id = dict.encode(subject);
        let predicate_id = dict.encode(predicate);
        let object_id = dict.encode(object);
        drop(dict);

        let triple = Triple {
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
        };
        self.add_triple(triple);
    }

    pub fn add_tagged_triple(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
        probability: f64,
    ) {
        let mut dict = self.dictionary.write().unwrap();
        let s = dict.encode(subject);
        let p = dict.encode(predicate);
        let o = dict.encode(object);
        drop(dict);

        let triple = Triple {
            subject: s,
            predicate: p,
            object: o,
        };
        self.add_triple(triple.clone());
        self.probability_seeds.insert(triple, probability);
    }

    /// Helper function that accepts parts of a triple, constructs a Triple, and deletes it
    pub fn delete_triple_parts(&mut self, subject: &str, predicate: &str, object: &str) -> bool {
        let mut dict = self.dictionary.write().unwrap();
        let subject_id = dict.encode(subject);
        let predicate_id = dict.encode(predicate);
        let object_id = dict.encode(object);
        drop(dict);

        let triple = Triple {
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
        };
        self.delete_triple(&triple)
    }

    pub fn generate_rdf_xml(&mut self) -> String {
        let mut xml = String::new();
        xml.push_str("<?xml version=\"1.0\"?>\n");
        xml.push_str("<rdf:RDF");

        // Write namespace declarations (from the stored prefixes)
        for (prefix, uri) in &self.prefixes {
            if prefix.is_empty() {
                xml.push_str(&format!(" xmlns=\"{}\"", uri));
            } else {
                xml.push_str(&format!(" xmlns:{}=\"{}\"", prefix, uri));
            }
        }
        // Always include the standard RDF namespace
        xml.push_str(" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"");
        xml.push_str(">\n");

        // Group triples by subject
        let dict = self.dictionary.read().unwrap();
        let mut subjects: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        let default_triples = self.query_default_triples(None, None, None);
        for triple in &default_triples {
            let subject = dict.decode(triple.subject);
            let predicate = dict.decode(triple.predicate);
            let object = dict.decode(triple.object);
            subjects
                .entry(subject.unwrap().to_string())
                .or_default()
                .push((predicate.unwrap().to_string(), object.unwrap().to_string()));
        }
        drop(dict);

        // For each subject, create an <rdf:Description> element.
        for (subject, po_pairs) in subjects {
            xml.push_str(&format!("  <rdf:Description rdf:about=\"{}\">\n", subject));
            for (predicate, object) in po_pairs {
                xml.push_str(&format!("    <{}>{}</{}>\n", predicate, object, predicate));
            }
            xml.push_str("  </rdf:Description>\n");
        }

        xml.push_str("</rdf:RDF>\n");
        xml
    }

    /// Serializes all triples as N-Triples-star format
    pub fn generate_ntriples(&self) -> String {
        let mut output = String::new();
        for triple in self.query_default_triples(None, None, None) {
            let s = self.decode_any(triple.subject).unwrap_or_default();
            let p = self.decode_any(triple.predicate).unwrap_or_default();
            let o = self.decode_any(triple.object).unwrap_or_default();

            let s_str = if s.starts_with("<<") {
                s
            } else {
                format!("<{}>", s)
            };
            let p_str = format!("<{}>", p);
            let o_str = if o.starts_with("<<") {
                o
            } else if o.starts_with("http://") || o.starts_with("https://") {
                format!("<{}>", o)
            } else {
                format!("\"{}\"", o)
            };

            output.push_str(&format!("{} {} {} .\n", s_str, p_str, o_str));
        }
        output
    }

    pub fn generate_nquads(&self) -> String {
        let mut output = String::new();
        for quad in self.dataset_index.all_quads() {
            let s = self.decode_any(quad.subject).unwrap_or_default();
            let p = self.decode_any(quad.predicate).unwrap_or_default();
            let o = self.decode_any(quad.object).unwrap_or_default();

            let s_str = if s.starts_with("<<") || s.starts_with("_:") {
                s
            } else {
                format!("<{}>", s)
            };
            let p_str = format!("<{}>", p);
            let o_str = if o.starts_with("<<") || o.starts_with("_:") {
                o
            } else if looks_like_absolute_iri(&o) {
                format!("<{}>", o)
            } else {
                format!("\"{}\"", escape_ntriples_literal(&o))
            };
            match quad.graph {
                GraphId::Default => {
                    output.push_str(&format!("{} {} {} .\n", s_str, p_str, o_str));
                }
                GraphId::Named(graph_id) => {
                    let graph = self.decode_any(graph_id).unwrap_or_default();
                    let graph = if graph.starts_with("_:") {
                        graph
                    } else {
                        format!("<{}>", graph)
                    };
                    output.push_str(&format!("{} {} {} {} .\n", s_str, p_str, o_str, graph));
                }
            }
        }
        output
    }

    /// Serializes all triples as Turtle-star format with prefix declarations
    pub fn generate_turtle(&self) -> String {
        let mut output = String::new();

        // Output prefix declarations
        for (prefix, uri) in &self.prefixes {
            output.push_str(&format!("@prefix {}: <{}> .\n", prefix, uri));
        }
        if !self.prefixes.is_empty() {
            output.push('\n');
        }

        // Group triples by subject, then by predicate
        let mut subjects: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, Vec<String>>,
        > = std::collections::BTreeMap::new();
        for triple in self.query_default_triples(None, None, None) {
            let s = self.decode_any(triple.subject).unwrap_or_default();
            let p = self.decode_any(triple.predicate).unwrap_or_default();
            let o = self.decode_any(triple.object).unwrap_or_default();
            subjects.entry(s).or_default().entry(p).or_default().push(o);
        }

        for (subject, predicates) in &subjects {
            let s_str = if subject.starts_with("<<") {
                subject.clone()
            } else {
                format!("<{}>", subject)
            };
            output.push_str(&s_str);

            let pred_count = predicates.len();
            for (i, (predicate, objects)) in predicates.iter().enumerate() {
                if i == 0 {
                    output.push(' ');
                } else {
                    output.push_str(" ;\n    ");
                }
                output.push_str(&format!("<{}>", predicate));

                for (j, obj) in objects.iter().enumerate() {
                    if j > 0 {
                        output.push_str(" ,");
                    }
                    output.push(' ');
                    if obj.starts_with("<<") {
                        output.push_str(obj);
                    } else if obj.starts_with("http://") || obj.starts_with("https://") {
                        output.push_str(&format!("<{}>", obj));
                    } else {
                        output.push_str(&format!("\"{}\"", obj));
                    }
                }

                if i == pred_count - 1 {
                    output.push_str(" .\n");
                }
            }
        }
        output
    }

    pub fn parse_rdf(&mut self, rdf_xml: &str) {
        let mut reader = Reader::from_str(rdf_xml);

        let mut current_subject = Vec::with_capacity(128);
        let mut current_predicate = Vec::with_capacity(128);

        let (sender, receiver) = unbounded::<Vec<Triple>>();
        let dictionary = Arc::clone(&self.dictionary);
        let triples_set = Arc::new(Mutex::new(Vec::new()));
        let num_threads = utils::get_num_cpus();

        // Crossbeam scope to manage threads
        scope(|s| {
            // Spawn worker threads
            for _ in 0..num_threads {
                let receiver = receiver.clone();
                let triples_set = Arc::clone(&triples_set);
                s.spawn(move |_| {
                    while let Ok(chunk) = receiver.recv() {
                        if chunk.is_empty() {
                            // Termination signal
                            break;
                        }

                        // Process chunk using Rayon
                        let local_triples: BTreeSet<Triple> =
                            chunk.into_par_iter().map(|triple| triple).collect();

                        // Insert into shared triples set
                        let mut triples = triples_set.lock().unwrap();
                        triples.push(local_triples);
                    }
                });
            }

            // Parsing and sending chunks
            let mut triples = Vec::with_capacity(8192);
            loop {
                match reader.read_event() {
                    Ok(Event::Start(ref e)) => match e.name() {
                        QName(b"rdf:RDF") => {
                            for attr in e.attributes().filter_map(Result::ok) {
                                let key = attr.key;
                                let value = attr.value;
                                if key.as_ref().starts_with(b"xmlns:") {
                                    let prefix = std::str::from_utf8(&key.as_ref()[6..])
                                        .unwrap_or("")
                                        .to_string();
                                    let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                    self.prefixes.insert(prefix, uri);
                                } else if key.as_ref() == b"xmlns" {
                                    // Default namespace
                                    let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                    self.prefixes.insert("".to_string(), uri);
                                }
                            }
                        }
                        QName(b"rdf:Description") => {
                            for attr in e.attributes().filter_map(Result::ok) {
                                if attr.key == QName(b"rdf:about") {
                                    current_subject.truncate(0);
                                    current_subject.extend_from_slice(&attr.value);
                                }
                            }
                        }
                        QName(b"rdfs:Class") | QName(b"rdf:type") => {
                            current_predicate.truncate(0);
                            current_predicate.extend_from_slice(b"rdf:type");
                        }
                        QName(b"rdfs:subClassOf") => {
                            current_predicate.truncate(0);
                            current_predicate.extend_from_slice(b"rdfs:subClassOf");
                        }
                        QName(b"rdfs:label") => {
                            current_predicate.truncate(0);
                            current_predicate.extend_from_slice(b"rdfs:label");
                        }
                        name => {
                            let name_str =
                                std::str::from_utf8(name.as_ref()).unwrap_or("").to_string();
                            let resolved_predicate = self.resolve_term(&name_str);
                            current_predicate = resolved_predicate.clone().into_bytes();
                        }
                    },
                    Ok(Event::Empty(ref e)) => {
                        if let Ok(predicate) = std::str::from_utf8(e.name().as_ref()) {
                            let resolved_predicate = self.resolve_term(predicate);
                            let mut object = Vec::with_capacity(128);
                            for attr in e.attributes().filter_map(Result::ok) {
                                if attr.key == QName(b"rdf:resource") {
                                    object.extend_from_slice(&attr.value);
                                }
                            }
                            if !object.is_empty() {
                                if let (Ok(subject_str), Ok(object_str)) = (
                                    std::str::from_utf8(&current_subject),
                                    std::str::from_utf8(&object),
                                ) {
                                    // Lock the dictionary for encoding
                                    let mut dict = dictionary.write().unwrap();
                                    let triple = Triple {
                                        subject: dict.encode(subject_str),
                                        predicate: dict.encode(&resolved_predicate),
                                        object: dict.encode(object_str),
                                    };
                                    drop(dict); // Release the lock
                                    triples.push(triple);
                                }
                            }
                        }
                    }
                    Ok(Event::Text(e)) => {
                        // Use Reader's decode method and trim whitespace
                        if let Ok(object_str) = reader.decoder().decode(e.as_ref()) {
                            let trimmed_object = object_str.trim();
                            // Skip empty or whitespace-only text
                            if !trimmed_object.is_empty() {
                                if let Ok(subject_str) = std::str::from_utf8(&current_subject) {
                                    if let Ok(predicate_str) =
                                        std::str::from_utf8(&current_predicate)
                                    {
                                        let resolved_predicate = self.resolve_term(predicate_str);
                                        // Lock the dictionary for encoding
                                        let mut dict = dictionary.write().unwrap();
                                        let triple = Triple {
                                            subject: dict.encode(subject_str),
                                            predicate: dict.encode(&resolved_predicate),
                                            object: dict.encode(trimmed_object),
                                        };
                                        drop(dict); // Release the lock
                                        triples.push(triple);
                                    }
                                }
                            }
                        }
                    }
                    Ok(Event::End(ref e)) => {
                        if e.name() == QName(b"rdf:Description") {
                            current_subject.truncate(0);
                            current_predicate.truncate(0);
                        }
                    }
                    Ok(Event::Eof) => break,
                    Err(e) => {
                        eprintln!("Error reading XML: {:?}", e);
                        break;
                    }
                    _ => {}
                }

                if triples.len() >= 8192 {
                    sender.send(triples).unwrap();
                    triples = Vec::with_capacity(8192);
                }
            }

            if !triples.is_empty() {
                sender.send(triples).unwrap();
            }

            // Send termination signals
            for _ in 0..num_threads {
                sender.send(Vec::new()).unwrap();
            }
        })
        .unwrap();

        // Merge all BTreeSets into the main triples set
        let triples_sets = Arc::try_unwrap(triples_set).unwrap().into_inner().unwrap();
        for local_triples in triples_sets {
            for triple in local_triples {
                self.add_triple(triple);
            }
        }
    }

    pub fn parse_rdf_from_file(&mut self, filename: &str) {
        let file = std::fs::File::open(filename).expect("Cannot open file");
        let reader = std::io::BufReader::new(file);
        let mut xml_reader = Reader::from_reader(reader);

        let mut current_subject = Vec::with_capacity(128);
        let mut current_predicate = Vec::with_capacity(128);

        // First, read prefixes before spawning worker threads
        let mut buf = Vec::new();
        loop {
            match xml_reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if e.name() == QName(b"rdf:RDF") {
                        // Read prefixes
                        for attr in e.attributes().filter_map(Result::ok) {
                            let key = attr.key;
                            let value = attr.value;
                            if key.as_ref().starts_with(b"xmlns:") {
                                let prefix = std::str::from_utf8(&key.as_ref()[6..])
                                    .unwrap_or("")
                                    .to_string();
                                let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                self.prefixes.insert(prefix, uri);
                            } else if key.as_ref() == b"xmlns" {
                                // Default namespace
                                let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                self.prefixes.insert("".to_string(), uri);
                            }
                        }
                        break; // We have read the prefixes, proceed to the rest
                    }
                }
                Ok(Event::Eof) => {
                    eprintln!("Reached EOF before reading prefixes.");
                    break;
                }
                Err(e) => {
                    eprintln!("Error reading XML: {:?}", e);
                    break;
                }
                _ => {}
            }
            buf.clear();
        }

        // Continue reading and parsing the rest of the file
        let mut triples = Vec::with_capacity(8192);
        loop {
            match xml_reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => match e.name() {
                    QName(b"rdf:Description") => {
                        for attr in e.attributes().filter_map(Result::ok) {
                            if attr.key == QName(b"rdf:about") {
                                current_subject.clear();
                                current_subject.extend_from_slice(&attr.value);
                            }
                        }
                    }
                    QName(b"rdfs:Class") | QName(b"rdf:type") => {
                        current_predicate.clear();
                        current_predicate.extend_from_slice(b"rdf:type");
                    }
                    QName(b"rdfs:subClassOf") => {
                        current_predicate.clear();
                        current_predicate.extend_from_slice(b"rdfs:subClassOf");
                    }
                    QName(b"rdfs:label") => {
                        current_predicate.clear();
                        current_predicate.extend_from_slice(b"rdfs:label");
                    }
                    name => {
                        let name_str = std::str::from_utf8(name.as_ref()).unwrap_or("").to_string();
                        let resolved_predicate = self.resolve_term(&name_str);
                        current_predicate = resolved_predicate.clone().into_bytes();
                    }
                },
                Ok(Event::Empty(ref e)) => {
                    if let Ok(predicate) = std::str::from_utf8(e.name().as_ref()) {
                        let resolved_predicate = self.resolve_term(predicate);
                        let mut object = Vec::with_capacity(128);
                        for attr in e.attributes().filter_map(Result::ok) {
                            if attr.key == QName(b"rdf:resource") {
                                object.extend_from_slice(&attr.value);
                            }
                        }
                        if !object.is_empty() {
                            if let (Ok(subject_str), Ok(object_str)) = (
                                std::str::from_utf8(&current_subject),
                                std::str::from_utf8(&object),
                            ) {
                                let mut dict = self.dictionary.write().unwrap();
                                let triple = Triple {
                                    subject: dict.encode(subject_str),
                                    predicate: dict.encode(&resolved_predicate),
                                    object: dict.encode(object_str),
                                };
                                drop(dict);
                                triples.push(triple);
                            }
                        }
                    }
                }
                Ok(Event::Text(e)) => {
                    // Use Reader's decode method and trim whitespace
                    if let Ok(object_str) = xml_reader.decoder().decode(e.as_ref()) {
                        let trimmed_object = object_str.trim();
                        // Skip empty or whitespace-only text
                        if !trimmed_object.is_empty() {
                            if let Ok(subject_str) = std::str::from_utf8(&current_subject) {
                                if let Ok(predicate_str) = std::str::from_utf8(&current_predicate) {
                                    let resolved_predicate = self.resolve_term(predicate_str);
                                    let mut dict = self.dictionary.write().unwrap();
                                    let triple = Triple {
                                        subject: dict.encode(subject_str),
                                        predicate: dict.encode(&resolved_predicate),
                                        object: dict.encode(trimmed_object),
                                    };
                                    drop(dict);
                                    triples.push(triple);
                                }
                            }
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    if e.name() == QName(b"rdf:Description") {
                        current_subject.clear();
                        current_predicate.clear();
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    eprintln!("Error reading XML: {:?}", e);
                    break;
                }
                _ => {}
            }

            buf.clear();

            if triples.len() >= 8192 {
                // Process triples in parallel using Rayon
                let local_triples: BTreeSet<Triple> = triples.into_par_iter().collect();
                for triple in local_triples {
                    self.add_triple(triple);
                }
                triples = Vec::with_capacity(8192);
            }
        }

        if !triples.is_empty() {
            let local_triples: BTreeSet<Triple> = triples.into_par_iter().collect();
            for triple in local_triples {
                self.add_triple(triple);
            }
        }
    }

    pub fn parse_turtle(&mut self, turtle_data: &str) {
        for raw_line in turtle_data.lines() {
            let line = raw_line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with("#") {
                continue;
            }

            // Prefix declarations
            if line.starts_with("@prefix") || line.starts_with("PREFIX") {
                let prefix_line = line
                    .trim_start_matches("@prefix")
                    .trim_start_matches("PREFIX")
                    .trim_end_matches('.')
                    .trim();

                let parts: Vec<&str> = prefix_line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let prefix = parts[0].trim_end_matches(':').to_string();
                    let uri = parts[1]
                        .trim_start_matches('<')
                        .trim_end_matches('>')
                        .to_string();
                    self.prefixes.insert(prefix, uri);
                } else {
                    eprintln!("Invalid prefix declaration: {}", line);
                }
                continue;
            }

            // Tokenize, but keep ; , . as delimiters only when outside URIs, literals, and quoted triples.
            let tokens = Self::tokenize_turtle_star_line(line);

            let mut subject_raw: Option<String> = None;
            let mut predicate_raw: Option<String> = None;
            let mut object_tokens: Vec<String> = Vec::new();

            let mut expect_subject = true;
            let mut expect_predicate = false;
            let mut expect_object = false;

            let flush_object = |this: &mut Self,
                                subject_raw: &Option<String>,
                                predicate_raw: &Option<String>,
                                object_tokens: &mut Vec<String>| {
                if let (Some(s_raw), Some(p_raw)) = (subject_raw.as_ref(), predicate_raw.as_ref()) {
                    if object_tokens.is_empty() {
                        return;
                    }

                    let object_raw = object_tokens.join(" ");

                    // Handle annotation syntax {| ... |}
                    let (object_part, annotations) = if let Some(ann_start) = object_raw.find("{|")
                    {
                        let obj = object_raw[..ann_start].trim().to_string();

                        if let Some(ann_end) = object_raw.find("|}") {
                            let ann_content = object_raw[ann_start + 2..ann_end].trim();
                            let ann_parts: Vec<&str> =
                                ann_content.splitn(2, char::is_whitespace).collect();

                            if ann_parts.len() == 2 {
                                (
                                    obj,
                                    vec![(ann_parts[0].to_string(), ann_parts[1].to_string())],
                                )
                            } else {
                                (obj, vec![])
                            }
                        } else {
                            (object_raw, vec![])
                        }
                    } else {
                        (object_raw, vec![])
                    };

                    let subject =
                        this.resolve_query_term(&Self::clean_turtle_term(s_raw), &this.prefixes);
                    let predicate =
                        this.resolve_query_term(&Self::clean_turtle_term(p_raw), &this.prefixes);
                    let object = this
                        .resolve_query_term(&Self::clean_turtle_term(&object_part), &this.prefixes);

                    // Emit the main triple
                    if subject.starts_with("<<") || object.starts_with("<<") {
                        let s_id = this.encode_term_star(&subject);
                        let p_id = this.encode_term_star(&predicate);
                        let o_id = this.encode_term_star(&object);
                        let triple = Triple {
                            subject: s_id,
                            predicate: p_id,
                            object: o_id,
                        };
                        this.add_triple(triple);
                    } else {
                        let mut dict = this.dictionary.write().unwrap();
                        let triple = Triple {
                            subject: dict.encode(&subject),
                            predicate: dict.encode(&predicate),
                            object: dict.encode(&object),
                        };
                        drop(dict);
                        this.add_triple(triple);
                    }

                    // Emit annotation triples, if any
                    for (ann_pred, ann_obj) in &annotations {
                        let qt_str = format!("<< {} {} {} >>", subject, predicate, object);
                        let qt_id = this.encode_term_star(&qt_str);

                        let ann_p_id = this.encode_term_star(&this.resolve_query_term(
                            &Self::clean_turtle_term(ann_pred),
                            &this.prefixes,
                        ));
                        let ann_o_id =
                            this.encode_term_star(&this.resolve_query_term(
                                &Self::clean_turtle_term(ann_obj),
                                &this.prefixes,
                            ));

                        let ann_triple = Triple {
                            subject: qt_id,
                            predicate: ann_p_id,
                            object: ann_o_id,
                        };
                        this.add_triple(ann_triple);
                    }

                    object_tokens.clear();
                }
            };

            for token in tokens {
                match token.as_str() {
                    "." => {
                        flush_object(self, &subject_raw, &predicate_raw, &mut object_tokens);
                        subject_raw = None;
                        predicate_raw = None;
                        expect_subject = true;
                        expect_predicate = false;
                        expect_object = false;
                    }
                    ";" => {
                        flush_object(self, &subject_raw, &predicate_raw, &mut object_tokens);
                        predicate_raw = None;
                        expect_predicate = true;
                        expect_object = false;
                    }
                    "," => {
                        flush_object(self, &subject_raw, &predicate_raw, &mut object_tokens);
                        expect_object = true;
                    }
                    _ => {
                        if expect_subject {
                            subject_raw = Some(token);
                            expect_subject = false;
                            expect_predicate = true;
                        } else if expect_predicate {
                            predicate_raw = Some(token);
                            expect_predicate = false;
                            expect_object = true;
                        } else if expect_object {
                            object_tokens.push(token);
                        } else {
                            // Fallback for slightly malformed input
                            object_tokens.push(token);
                        }
                    }
                }
            }

            // Flush any trailing object if the line does not end with '.'
            flush_object(self, &subject_raw, &predicate_raw, &mut object_tokens);
        }
    }

    /// Tokenize a Turtle-star line, keeping `<< ... >>` and punctuation structure intact.
    fn tokenize_turtle_star_line(line: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut current = String::new();

        let mut depth = 0i32; // quoted-triple nesting depth
        let mut in_uri = false; // inside <...>
        let mut in_literal = false; // inside "..."
        let mut escaped = false;

        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
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

                '"' if !in_uri && depth == 0 => {
                    in_literal = !in_literal;
                    current.push(ch);
                }

                '"' if depth > 0 => {
                    in_literal = !in_literal;
                    current.push(ch);
                }

                '<' if !in_literal => {
                    if chars.peek() == Some(&'<') && !in_uri {
                        current.push(ch);
                        current.push(chars.next().unwrap());
                        depth += 1;
                    } else if depth > 0 {
                        current.push(ch);
                        if chars.peek() == Some(&'<') {
                            current.push(chars.next().unwrap());
                            depth += 1;
                        }
                    } else {
                        in_uri = true;
                        current.push(ch);
                    }
                }

                '>' if !in_literal => {
                    if depth > 0 && !in_uri {
                        current.push(ch);
                        if chars.peek() == Some(&'>') {
                            current.push(chars.next().unwrap());
                            depth -= 1;
                            if depth == 0 {
                                tokens.push(current.trim().to_string());
                                current.clear();
                            }
                        }
                    } else if in_uri {
                        in_uri = false;
                        current.push(ch);
                        if depth == 0 {
                            tokens.push(current.trim().to_string());
                            current.clear();
                        }
                    } else {
                        current.push(ch);
                    }
                }

                ';' | ',' | '.' if depth == 0 && !in_uri && !in_literal => {
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        tokens.push(trimmed);
                        current.clear();
                    }
                    tokens.push(ch.to_string());
                }

                ' ' | '\t' | '\n' | '\r' if depth == 0 && !in_uri && !in_literal => {
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        tokens.push(trimmed);
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
            tokens.push(trimmed);
        }

        tokens
    }

    fn clean_turtle_term(term: &str) -> String {
        let term = term.trim();
        if term.starts_with("<<") {
            // Keep quoted triples as-is
            term.to_string()
        } else if term.starts_with('<') && term.ends_with('>') {
            term[1..term.len() - 1].to_string()
        } else if term.starts_with('"') && term.ends_with('"') {
            term[1..term.len() - 1].to_string()
        } else {
            term.trim_matches('"').to_string()
        }
    }

    // New parse_n3 function
    pub fn parse_n3(&mut self, n3_data: &str) {
        let lines: Vec<String> = n3_data.lines().map(|l| l.trim().to_string()).collect();
        let chunk_size = 1000;
        let chunks: Vec<Vec<String>> = lines.chunks(chunk_size).map(|c| c.to_vec()).collect();

        let partial_results: Vec<(
            Vec<Triple>,
            Arc<RwLock<Dictionary>>,
            HashMap<String, String>,
        )> = chunks
            .par_iter()
            .map(|chunk| {
                let mut local_db = SparqlDatabase::new();
                let mut statement = String::new();

                for raw_line in chunk {
                    let mut line = raw_line.as_str();
                    if let Some(comment_start) = line.find('#') {
                        line = &line[..comment_start];
                        line = line.trim();
                    }
                    if line.is_empty() {
                        continue;
                    }
                    if line.starts_with("@prefix") {
                        let line = line.trim_start_matches("@prefix").trim_end_matches('.');
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            let prefix = parts[0].trim_end_matches(':').to_string();
                            let uri = parts[1]
                                .trim_start_matches('<')
                                .trim_end_matches('>')
                                .to_string();
                            local_db.prefixes.insert(prefix, uri);
                        } else {
                            eprintln!("Invalid prefix declaration: {}", line);
                        }
                    } else {
                        statement.push_str(line);
                        statement.push(' ');
                        if line.ends_with('.') {
                            local_db.parse_statement(statement.trim());
                            statement.clear();
                        }
                    }
                }

                (
                    local_db.query_default_triples(None, None, None),
                    local_db.dictionary,
                    local_db.prefixes,
                )
            })
            .collect();

        for (triples, dict_arc, pref) in partial_results {
            for t in triples {
                self.add_triple(t);
            }
            let mut self_dict = self.dictionary.write().unwrap();
            let other_dict = dict_arc.read().unwrap();
            self_dict.merge(&other_dict);
            drop(other_dict);
            drop(self_dict);
            for (k, v) in pref {
                self.prefixes.insert(k, v);
            }
        }
    }

    // Parse_ntriples and add to DB function
    pub fn parse_ntriples_and_add(&mut self, ntriples_data: &str) {
        let partial_results = self.parse_ntriples(ntriples_data);

        let encoded_triples = self.encode_triples(partial_results);
        for encoded_triple in encoded_triples {
            self.add_triple(encoded_triple);
        }
    }

    // Parses ntriples
    pub fn parse_ntriples(&mut self, ntriples_data: &str) -> Vec<Vec<(String, String, String)>> {
        let lines: Vec<&str> = ntriples_data.lines().collect();
        let chunk_size = 1000;
        let chunks: Vec<&[&str]> = lines.chunks(chunk_size).collect();

        let partial_results: Vec<Vec<(String, String, String)>> = chunks
            .par_iter()
            .map(|chunk| {
                let mut local_triples = Vec::new();

                for line in chunk.iter() {
                    let line = line.trim();

                    // Skip empty lines and comments
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }

                    // N-Triples must end with a dot
                    if !line.ends_with('.') {
                        eprintln!("Invalid N-Triples line (missing dot): {}", line);
                        continue;
                    }

                    // Remove the trailing dot
                    let line_without_dot = &line[..line.len() - 1].trim();

                    // Parse the triple
                    if let Some((subject, predicate, object)) =
                        self.parse_ntriples_line(line_without_dot)
                    {
                        local_triples.push((subject, predicate, object));
                    }
                }

                local_triples
            })
            .collect();
        partial_results
    }

    // Encode triples
    pub fn encode_triples(
        &mut self,
        non_encoded_triples: Vec<Vec<(String, String, String)>>,
    ) -> Vec<Triple> {
        let mut encoded_triples = Vec::new();
        for triple_strings in non_encoded_triples {
            for (subject, predicate, object) in triple_strings {
                let main_triple = Triple {
                    subject: self.encode_term_star(&subject),
                    predicate: self.encode_term_star(&predicate),
                    object: self.encode_term_star(&object),
                };
                encoded_triples.push(main_triple);
            }
        }
        encoded_triples
    }

    pub fn parse_and_encode_ntriples(&mut self, ntriples_data: &str) -> Vec<Triple> {
        let partial_results = self.parse_ntriples(ntriples_data);

        self.encode_triples(partial_results)
    }

    pub fn parse_nquads_and_add(&mut self, nquads_data: &str) {
        for raw_line in nquads_data.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let line_without_dot = if line.ends_with('.') {
                line[..line.len() - 1].trim()
            } else {
                eprintln!("Invalid N-Quads line (missing dot): {}", line);
                continue;
            };

            if let Some((subject, predicate, object, graph)) =
                self.parse_nquads_line(line_without_dot)
            {
                match graph {
                    Some(graph) => {
                        self.add_quad_parts(&subject, &predicate, &object, &graph);
                    }
                    None => {
                        let quad = Quad {
                            subject: self.encode_term_star(&subject),
                            predicate: self.encode_term_star(&predicate),
                            object: self.encode_term_star(&object),
                            graph: GraphId::Default,
                        };
                        self.add_quad(quad);
                    }
                }
            }
        }
    }

    fn parse_nquads_line(&self, line: &str) -> Option<(String, String, String, Option<String>)> {
        let mut parts = self.parse_ntriples_parts(line);
        if !matches!(parts.len(), 3 | 4) {
            eprintln!(
                "Invalid N-Quads line (expected 3 or 4 parts, got {}): {}",
                parts.len(),
                line
            );
            return None;
        }
        let subject = self.clean_ntriples_term(&parts.remove(0));
        let predicate = self.clean_ntriples_term(&parts.remove(0));
        let object = self.clean_ntriples_term(&parts.remove(0));
        let graph = (!parts.is_empty()).then(|| self.clean_ntriples_term(&parts.remove(0)));
        Some((subject, predicate, object, graph))
    }

    // Helper method to parse a single N-Triples line
    fn parse_ntriples_line(&self, line: &str) -> Option<(String, String, String)> {
        let parts = self.parse_ntriples_parts(line);
        if parts.len() == 3 {
            let subject = self.clean_ntriples_term(&parts[0]);
            // Expand the Turtle `a` shorthand for rdf:type in predicate position.
            let predicate = if parts[1] == "a" {
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string()
            } else {
                self.clean_ntriples_term(&parts[1])
            };
            let object = self.clean_ntriples_term(&parts[2]);
            Some((subject, predicate, object))
        } else {
            eprintln!(
                "Invalid N-Triples line (expected 3 parts, got {}): {}",
                parts.len(),
                line
            );
            None
        }
    }

    fn parse_ntriples_parts(&self, line: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut current_part = String::new();
        let mut in_uri = false;
        let mut in_literal = false;
        let mut escaped = false;
        let mut qt_depth: i32 = 0; // Track quoted triple nesting depth
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '<' if !in_literal && !escaped => {
                    if chars.peek() == Some(&'<') && !in_uri {
                        // Start of quoted triple <<
                        current_part.push(ch);
                        current_part.push(chars.next().unwrap());
                        qt_depth += 1;
                    } else if qt_depth > 0 {
                        // Inside a quoted triple, could be a nested << or a URI <
                        current_part.push(ch);
                        if chars.peek() == Some(&'<') {
                            // Nested <<
                            current_part.push(chars.next().unwrap());
                            qt_depth += 1;
                        } else {
                            // URI inside quoted triple — just accumulate
                        }
                    } else {
                        in_uri = true;
                        current_part.push(ch);
                    }
                }
                '>' if !in_literal && !escaped => {
                    if qt_depth > 0 && !in_uri {
                        current_part.push(ch);
                        if chars.peek() == Some(&'>') {
                            current_part.push(chars.next().unwrap());
                            qt_depth -= 1;
                            if qt_depth == 0 {
                                // Finished top-level quoted triple
                                parts.push(current_part.trim().to_string());
                                current_part.clear();
                            }
                        }
                        // else: stray > inside quoted triple, just accumulate
                    } else if in_uri {
                        in_uri = false;
                        current_part.push(ch);
                        if qt_depth == 0 {
                            parts.push(current_part.trim().to_string());
                            current_part.clear();
                        }
                    } else {
                        current_part.push(ch);
                    }
                }
                '"' if !in_uri && !escaped => {
                    in_literal = !in_literal;
                    current_part.push(ch);
                    if !in_literal {
                        // Check for datatype or language tag after closing quote
                        while let Some(&next_ch) = chars.peek() {
                            if next_ch == '^' || next_ch == '@' {
                                current_part.push(chars.next().unwrap());
                                // Handle ^^ for datatypes
                                if next_ch == '^' {
                                    if let Some(&second_caret) = chars.peek() {
                                        if second_caret == '^' {
                                            current_part.push(chars.next().unwrap());
                                            // Now consume the datatype URI
                                            while let Some(&datatype_ch) = chars.peek() {
                                                if datatype_ch == '<' {
                                                    // Start of datatype URI
                                                    current_part.push(chars.next().unwrap());
                                                    let mut in_datatype_uri = true;
                                                    while let Some(&uri_ch) = chars.peek() {
                                                        current_part.push(chars.next().unwrap());
                                                        if uri_ch == '>' {
                                                            in_datatype_uri = false;
                                                            break;
                                                        }
                                                    }
                                                    if !in_datatype_uri {
                                                        break;
                                                    }
                                                } else if datatype_ch.is_whitespace() {
                                                    break;
                                                } else {
                                                    current_part.push(chars.next().unwrap());
                                                }
                                            }
                                        }
                                    }
                                } else if next_ch == '@' {
                                    // Language tag
                                    while let Some(&lang_ch) = chars.peek() {
                                        if lang_ch.is_alphanumeric() || lang_ch == '-' {
                                            current_part.push(chars.next().unwrap());
                                        } else {
                                            break;
                                        }
                                    }
                                }
                                break;
                            } else if next_ch.is_whitespace() {
                                break;
                            } else {
                                // Unexpected character after literal
                                break;
                            }
                        }
                        if qt_depth == 0 {
                            parts.push(current_part.trim().to_string());
                            current_part.clear();
                        }
                    }
                }
                '\\' if (in_uri || in_literal) && !escaped => {
                    escaped = true;
                    current_part.push(ch);
                }
                ' ' | '\t' if !in_uri && !in_literal && !escaped && qt_depth == 0 => {
                    if !current_part.is_empty() {
                        parts.push(current_part.trim().to_string());
                        current_part.clear();
                    }
                }
                _ => {
                    escaped = false;
                    current_part.push(ch);
                }
            }
        }

        if !current_part.is_empty() {
            parts.push(current_part.trim().to_string());
        }

        parts
    }

    // Helper method to clean N-Triples terms
    fn clean_ntriples_term(&self, term: &str) -> String {
        let term = term.trim();

        // Keep quoted triples as-is
        if term.starts_with("<<") && term.ends_with(">>") {
            return term.to_string();
        }

        // Handle URIs
        if term.starts_with('<') && term.ends_with('>') {
            return term[1..term.len() - 1].to_string();
        }

        // Handle literals (keep quotes and datatype/language info)
        if term.starts_with('"') {
            if let Some((literal_value, rest)) = decode_ntriples_literal(term) {
                if rest.is_empty() {
                    return literal_value;
                } else if rest.starts_with("^^") {
                    return literal_value;
                } else if rest.starts_with('@') {
                    return format!("{literal_value}{rest}");
                }
            }
        }

        // Return as-is for other cases
        term.to_string()
    }

    fn parse_statement(&mut self, statement: &str) {
        let mut tokens = statement.split_whitespace().peekable();
        let mut subject = String::new();
        let mut predicate = String::new();
        let mut current_state = "subject";

        while let Some(token) = tokens.next() {
            match token {
                ";" => {
                    predicate.clear();
                    current_state = "predicate";
                }
                "." => {
                    // End of statement
                    break;
                }
                _ => match current_state {
                    "subject" => {
                        subject = token.to_string();
                        current_state = "predicate";
                    }
                    "predicate" => {
                        predicate = token.to_string();
                        current_state = "object";
                    }
                    "object" => {
                        let mut object = token.to_string();

                        // Collect tokens until we reach ';', '.', or ','
                        while let Some(next_token) = tokens.peek() {
                            if *next_token == ";" || *next_token == "." || *next_token == "," {
                                break;
                            }
                            // Consume the token
                            let next_token = tokens.next().unwrap();
                            object.push(' ');
                            object.push_str(next_token);
                        }

                        // Resolve terms and store the triple
                        let resolved_subject = self.resolve_term(&subject);
                        let resolved_predicate = self.resolve_term(&predicate);
                        let resolved_object = self.resolve_term(&object);

                        let mut dict = self.dictionary.write().unwrap();
                        let triple = Triple {
                            subject: dict.encode(&resolved_subject),
                            predicate: dict.encode(&resolved_predicate),
                            object: dict.encode(&resolved_object),
                        };
                        drop(dict);
                        self.add_triple(triple);

                        current_state = "predicate";
                    }
                    _ => {}
                },
            }
        }
    }

    fn resolve_term(&self, term: &str) -> String {
        if term.starts_with('<') && term.ends_with('>') {
            term.trim_start_matches('<')
                .trim_end_matches('>')
                .to_string()
        } else if term.starts_with('"') {
            // It's a literal, possibly with a datatype or language tag
            if let Some(pos) = term.rfind('"') {
                let literal = &term[..=pos]; // Include the closing quote
                let rest = &term[pos + 1..]; // After the closing quote
                let mut result = literal.to_string();
                if rest.starts_with("^^") {
                    // It's a typed literal
                    let datatype = rest[2..].trim();
                    let resolved_datatype = self.resolve_term(datatype);
                    result.push_str("^^");
                    result.push_str(&resolved_datatype);
                } else if rest.starts_with('@') {
                    // It's a language-tagged literal
                    result.push_str(rest);
                }
                result
            } else {
                // Malformed literal
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
                eprintln!("Unknown prefix: {}", prefix);
                term.to_string()
            }
        } else {
            term.to_string()
        }
    }

    // Method to automatically extract and register prefixes from a query string
    pub fn register_prefixes_from_query(&mut self, query: &str) {
        // Simple regex to extract PREFIX declarations
        let prefix_pattern = regex::Regex::new(r"PREFIX\s+([a-zA-Z0-9_]+):\s*<([^>]+)>").unwrap();

        for captures in prefix_pattern.captures_iter(query) {
            if captures.len() >= 3 {
                let prefix = captures[1].to_string();
                let uri = captures[2].to_string();
                self.prefixes.insert(prefix, uri);
            }
        }
    }

    // Method to ensure prefixes are properly shared between components
    pub fn share_prefixes_with(&self, prefixes: &mut HashMap<String, String>) {
        for (prefix, uri) in &self.prefixes {
            prefixes.insert(prefix.clone(), uri.clone());
        }
    }

    pub fn resolve_query_term(&self, term: &str, prefixes: &HashMap<String, String>) -> String {
        if term.starts_with("<<") && term.ends_with(">>") {
            // Keep quoted triple patterns as-is (they'll be handled downstream)
            return term.to_string();
        }
        if term.starts_with('<') && term.ends_with('>') {
            term.trim_start_matches('<')
                .trim_end_matches('>')
                .to_string()
        } else if term.starts_with('"') && term.ends_with('"') {
            term.trim_matches('"').to_string()
        } else if term.contains(':')
            && !term.starts_with("http://")
            && !term.starts_with("https://")
        {
            let mut parts = term.splitn(2, ':');
            let prefix = parts.next().unwrap();
            let local_name = parts.next().unwrap_or("");

            // First check the passed prefixes map
            if let Some(uri) = prefixes.get(prefix) {
                format!("{}{}", uri, local_name)
            }
            // Then check the database's own prefixes map as a fallback
            else if let Some(uri) = self.prefixes.get(prefix) {
                format!("{}{}", uri, local_name)
            } else {
                eprintln!("Unknown prefix in query: {}", prefix);
                term.to_string()
            }
        } else {
            term.to_string()
        }
    }

    pub fn union(&mut self, other: &SparqlDatabase) -> Self {
        let self_dict = self.dictionary.read().unwrap();
        let other_dict = other.dictionary.read().unwrap();
        let mut merged_dictionary = self_dict.clone();
        let self_quoted_triples = self.quoted_triple_store.read().unwrap();
        let other_quoted_triples = other.quoted_triple_store.read().unwrap();
        let mut merged_quoted_triples = self_quoted_triples.clone();
        let mut translated_ids = HashMap::new();

        // Preserve the complete lexical dictionary, not only terms currently
        // referenced by default-graph triples.
        let mut other_term_ids: Vec<_> = other_dict.id_to_string.keys().copied().collect();
        other_term_ids.sort_unstable();
        for id in other_term_ids {
            reencode_term_id(
                id,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
        }

        // Preserve even currently-unreferenced quoted terms. Quads and metadata
        // below use the same translation cache, so every occurrence receives
        // the same target ID.
        let mut other_quoted_ids: Vec<_> = other_quoted_triples
            .id_to_components
            .keys()
            .copied()
            .collect();
        other_quoted_ids.sort_unstable();
        for id in other_quoted_ids {
            reencode_term_id(
                id,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
        }

        let mut dataset_index = DatasetIndex::new();
        for graph in self.dataset_index.named_graphs() {
            dataset_index.create_graph(graph);
        }
        for quad in self.dataset_index.all_quads() {
            dataset_index.insert_quad(&quad);
        }

        // Graph names and every term in the other database must be translated:
        // numeric dictionary IDs are local to their originating database.
        for graph in other.dataset_index.named_graphs() {
            let GraphId::Named(graph_id) = graph else {
                continue;
            };
            let graph_id = reencode_term_id(
                graph_id,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            dataset_index.create_graph(GraphId::Named(graph_id));
        }
        for quad in other.dataset_index.all_quads() {
            let subject = reencode_term_id(
                quad.subject,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let predicate = reencode_term_id(
                quad.predicate,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let object = reencode_term_id(
                quad.object,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let graph = match quad.graph {
                GraphId::Default => GraphId::Default,
                GraphId::Named(graph_id) => GraphId::Named(reencode_term_id(
                    graph_id,
                    &other_dict,
                    &other_quoted_triples,
                    &mut merged_dictionary,
                    &mut merged_quoted_triples,
                    &mut translated_ids,
                )),
            };
            dataset_index.insert_quad(&Quad {
                subject,
                predicate,
                object,
                graph,
            });
        }

        let mut merged_seeds = self.probability_seeds.clone();
        for (triple, prob) in &other.probability_seeds {
            let subject = reencode_term_id(
                triple.subject,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let predicate = reencode_term_id(
                triple.predicate,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let object = reencode_term_id(
                triple.object,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            merged_seeds.insert(
                Triple {
                    subject,
                    predicate,
                    object,
                },
                *prob,
            );
        }

        Self {
            dataset_index,
            dictionary: Arc::new(RwLock::new(merged_dictionary)),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            rule_map: HashMap::new(),
            model_decls: self.model_decls.clone(),
            neural_relation_decls: self.neural_relation_decls.clone(),
            train_neural_relation_decls: self.train_neural_relation_decls.clone(),
            neural_model_artifacts: self.neural_model_artifacts.clone(),
            neural_materialized_triples: self.neural_materialized_triples.clone(),
            ml_predict_materialized_triples: self.ml_predict_materialized_triples.clone(),
            probability_seeds: merged_seeds,
            cached_stats: None,
            quoted_triple_store: Arc::new(RwLock::new(merged_quoted_triples)),
        }
    }

    pub fn handle_query(&mut self, query: &str) -> String {
        // Assume the query string is in a basic format like "subject predicate object"
        let parts: Vec<&str> = query.split_whitespace().collect();

        if parts.len() != 3 {
            return "Invalid query format. Expected 'subject predicate object'.".to_string();
        }

        let subject = parts[0];
        let predicate = parts[1];
        let object = parts[2];

        let mut dict = self.dictionary.write().unwrap();
        let subject_id = dict.encode(subject);
        let predicate_id = dict.encode(predicate);
        let object_id = dict.encode(object);

        let mut result = String::new();
        let matching_triples =
            self.query_default_triples(Some(subject_id), Some(predicate_id), Some(object_id));
        for triple in matching_triples {
            if triple.subject == subject_id
                && triple.predicate == predicate_id
                && triple.object == object_id
            {
                result.push_str(&format!(
                    "Subject: {}, Predicate: {}, Object: {}\n",
                    dict.decode(triple.subject).unwrap(),
                    dict.decode(triple.predicate).unwrap(),
                    dict.decode(triple.object).unwrap()
                ));
            }
        }
        drop(dict);

        if result.is_empty() {
            result = "No matching triples found.".to_string();
        }

        result
    }

    fn handle_http_sparql_query(&mut self, query: &str) -> String {
        match crate::execute_query::execute_sparql_query(query, self) {
            Ok(rows) => rows
                .into_iter()
                .map(|row| row.join("\t"))
                .collect::<Vec<_>>()
                .join("\n"),
            Err(error) => format!("Query Failed: {error}"),
        }
    }

    /// Execute one of Kolibrie's supported standard SPARQL Update forms while
    /// preserving parse/evaluation errors for Rust callers.
    pub fn execute_update(
        &mut self,
        update: &str,
    ) -> Result<crate::execute_query::UpdateSummary, String> {
        crate::execute_query::execute_sparql_update(update, self)
    }

    pub fn handle_update(&mut self, update: &str) -> String {
        if let Ok(summary) = self.execute_update(update) {
            return format!(
                "Update Successful (inserted {}, deleted {})",
                summary.inserted_quads, summary.deleted_quads
            );
        }

        // Historical standalone INSERT/DELETE aliases are parsed into the
        // same UpdateOperation and use the same optimized executor. Keeping
        // the old short success text preserves callers that compare it
        // exactly.
        if crate::execute_query::execute_sparql_update_compat(update, self).is_ok() {
            return "Update Successful".to_string();
        }
        "Update Failed".to_string()
    }

    pub fn handle_http_request(&mut self, request: &str) -> String {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        req.parse(request.as_bytes()).unwrap();

        match req.method.unwrap() {
            "GET" => {
                let url = Url::parse(&("http://localhost".to_owned() + req.path.unwrap())).unwrap();
                let query_pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
                if let Some(query) = query_pairs.get("query") {
                    return self.handle_http_sparql_query(query);
                }
            }
            "POST" => {
                let content_type = req
                    .headers
                    .iter()
                    .find(|header| header.name.eq_ignore_ascii_case("Content-Type"))
                    .map(|header| header.value);

                if let Some(content_type) = content_type {
                    if content_type == b"application/sparql-query" {
                        // Direct POST query
                        if let Some(body) = request.split("\r\n\r\n").nth(1) {
                            return self.handle_http_sparql_query(body);
                        }
                    } else if content_type == b"application/x-www-form-urlencoded" {
                        // URL-encoded POST query or update
                        if let Some(body) = request.split("\r\n\r\n").nth(1) {
                            let params = parse_form_urlencoded(body);

                            if let Some(query) = params.get("query") {
                                return self.handle_http_sparql_query(query);
                            } else if let Some(update) = params.get("update") {
                                return self.handle_update(update);
                            }
                        }
                    } else if content_type == b"application/sparql-update" {
                        // Direct POST update
                        if let Some(body) = request.split("\r\n\r\n").nth(1) {
                            return self.handle_update(body);
                        }
                    }
                }
            }
            _ => {}
        }

        "Bad Request".to_string()
    }

    pub fn debug_print_triples(&self) {
        let dict = self.dictionary.read().unwrap();
        let default_triples = self.query_default_triples(None, None, None);
        for triple in &default_triples {
            println!(
                "Stored Triple -> Subject: {}, Predicate: {}, Object: {}",
                dict.decode(triple.subject).unwrap(),
                dict.decode(triple.predicate).unwrap(),
                dict.decode(triple.object).unwrap()
            );
        }
    }

    // Create user defined function
    pub fn register_udf<F>(&mut self, name: &str, f: F)
    where
        F: Fn(Vec<&str>) -> String + Send + Sync + 'static,
    {
        self.udfs.insert(name.to_string(), ClonableFn::new(f));
    }

    /// Rebuild every graph-scoped index without collapsing named graphs into
    /// the default graph or losing empty named-graph identities.
    pub fn build_all_indexes(&mut self) {
        let quads = self.dataset_index.all_quads();
        let named_graphs = self.dataset_index.named_graphs();
        let mut rebuilt = DatasetIndex::new();
        for graph in named_graphs {
            rebuilt.create_graph(graph);
        }
        for quad in quads {
            rebuilt.insert_quad(&quad);
        }
        self.dataset_index = rebuilt;
    }

    /// Triple to string
    pub fn triple_to_string(&self, triple: &Triple, dict: &Dictionary) -> String {
        let subject = dict.decode(triple.subject);
        let predicate = dict.decode(triple.predicate);
        let object = dict.decode(triple.object);
        format!(
            "{} {} {}",
            subject.unwrap(),
            predicate.unwrap(),
            object.unwrap()
        )
    }
}
