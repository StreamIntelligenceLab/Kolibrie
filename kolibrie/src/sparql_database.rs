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
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
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

/// Staleness policy for planning statistics
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PlanningStatsPolicy {
    /// Rebuild on every mutation. Exact, and the planner never sees stale
    /// cardinalities, but it pays a full statistics pass per write.
    AlwaysFresh,
    /// Reuse a snapshot until the store has changed enough to matter.
    Bounded {
        /// Mutations to tolerate before rebuilding, however small the store.
        minimum_batch: u64,
        /// Fraction of the store that must change to force a rebuild, so the
        /// tolerance grows with size instead of being a fixed count.
        refresh_fraction: f64,
    },
}

impl PlanningStatsPolicy {
    /// Refresh after 100 mutations or half the store
    pub const DEFAULT_BOUNDED: Self = PlanningStatsPolicy::Bounded {
        minimum_batch: 100,
        refresh_fraction: 0.5,
    };
}

impl Default for PlanningStatsPolicy {
    fn default() -> Self {
        Self::DEFAULT_BOUNDED
    }
}

/// A planning statistics snapshot
#[derive(Debug, Clone)]
pub struct PlanningStatsSnapshot {
    /// The cardinalities themselves, shared so reusing a snapshot is free.
    pub stats: Arc<DatabaseStats>,
    /// `DatasetIndex::generation` at build time
    pub generation: u64,
    /// `total_triples` at build time
    pub quads_at_build: u64,
    /// Statistics epoch at build time
    pub epoch: u64,
    /// `DatasetIndex::resets` at build time
    pub resets: u64,
}

/// Read guard over the term dictionary, from [`SparqlDatabase::dictionary`].
///
/// Derefs to [`Dictionary`], so `db.dictionary().decode(id)` replaces
/// `db.dictionary.read().unwrap().decode(id)`. Holding the binding holds the lock, so
/// callers performing several lookups under one lock keep working unchanged.
#[derive(Debug)]
pub struct DictionaryRef<'a>(RwLockReadGuard<'a, Dictionary>);

impl std::ops::Deref for DictionaryRef<'_> {
    type Target = Dictionary;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Write guard over the term dictionary, from [`SparqlDatabase::dictionary_mut`].
/// See [`DictionaryRef`].
#[derive(Debug)]
pub struct DictionaryMut<'a>(RwLockWriteGuard<'a, Dictionary>);

impl std::ops::Deref for DictionaryMut<'_> {
    type Target = Dictionary;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DictionaryMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Read guard over the quoted-triple store, from [`SparqlDatabase::quoted_triples`].
///
/// Exists so the public API never names `Arc<RwLock<_>>` (P5). It derefs to
/// [`QuotedTripleStore`], so call sites read `db.quoted_triples().len()` where they
/// previously read `db.quoted_triple_store.read().unwrap().len()`. Holding the
/// binding holds the lock, exactly as before — callers that need several operations
/// under one lock keep working unchanged.
#[derive(Debug)]
pub struct QuotedTriplesRef<'a>(RwLockReadGuard<'a, QuotedTripleStore>);

impl std::ops::Deref for QuotedTriplesRef<'_> {
    type Target = QuotedTripleStore;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Write guard over the quoted-triple store, from
/// [`SparqlDatabase::quoted_triples_mut`]. See [`QuotedTriplesRef`].
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

/// The RDF store: encoded triples, the dictionary that encodes them, indexes over
/// several orderings, and the statistics the planner costs with.
///
/// # No public fields
///
/// Every field is private, reached through accessors. Terms are interned, indexes
/// are maintained in step with writes, and statistics are cached against the
/// index's generation — invariants a caller writing to a field directly would
/// break. Lock-held state is reached through guards rather than handles, so
/// `Arc<RwLock<_>>` does not appear in the public API; the one exception is
/// [`Self::dictionary_handle`], documented there.
///
/// # Encoding
///
/// Triples are stored as `u32` ids, not strings, and those ids mean nothing without
/// this store's dictionary. Two databases cannot exchange encoded data unless they
/// share one — see [`Self::shares_dictionary_with`] and [`Self::union`].
#[derive(Debug, Clone)]
pub struct SparqlDatabase {
    /// Host-selected ML authority
    pub(crate) ml_context: crate::ml_policy::MlExecutionContext,
    pub(crate) dataset_index: DatasetIndex,
    pub(crate) dictionary: Arc<RwLock<Dictionary>>,
    pub(crate) prefixes: HashMap<String, String>,
    udfs: HashMap<String, ClonableFn>,
    pub(crate) rule_map: HashMap<String, String>,
    pub(crate) model_decls: HashMap<String, ModelDecl>,
    pub(crate) neural_relation_decls: HashMap<String, NeuralRelationDecl>,
    pub(crate) train_neural_relation_decls: HashMap<String, TrainNeuralRelationDecl>,
    pub(crate) neural_model_artifacts: HashMap<String, String>,
    pub(crate) neural_materialized_triples: HashMap<String, Vec<Triple>>,
    ml_predict_materialized_triples: HashMap<String, Vec<Triple>>,
    pub(crate) probability_seeds: HashMap<Triple, f64>,
    cached_stats: Option<Arc<DatabaseStats>>,
    exact_stats_generation: u64,
    planning_stats: Option<PlanningStatsSnapshot>,
    pub(crate) planning_stats_policy: PlanningStatsPolicy,
    stats_epoch: u64,
    pub(crate) stats_rebuild_count: u64,
    stats_rebuild_nanos: u64,
    pub(crate) quoted_triple_store: Arc<RwLock<QuotedTripleStore>>,
    implicit_neural_materialization: bool,
}

impl SparqlDatabase {
    /// Initialize with host-selected ML authority
    pub fn with_ml_context(context: crate::ml_policy::MlExecutionContext) -> Self {
        let mut database = Self::new();
        database.ml_context = context;
        database
    }
    /// An empty store with ML disabled and the default statistics policy.
    pub fn new() -> Self {
        Self {
            ml_context: crate::ml_policy::MlExecutionContext::disabled(),
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
            exact_stats_generation: 0,
            planning_stats: None,
            planning_stats_policy: PlanningStatsPolicy::default(),
            stats_epoch: 0,
            stats_rebuild_count: 0,
            stats_rebuild_nanos: 0,
            quoted_triple_store: Arc::new(RwLock::new(QuotedTripleStore::new())),
            implicit_neural_materialization: true,
        }
    }

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

    /// Decode a u32 ID that may be a regular dictionary ID or a quoted triple ID
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

    /// Replaces every prefix binding. See [`Self::add_prefix`] to add one.
    pub fn set_prefixes(&mut self, prefixes: HashMap<String, String>) {
        self.prefixes = prefixes;
    }

    /// Returns exact statistics, rebuilding them if the dataset generation changed
    pub fn get_or_build_stats(&mut self) -> Arc<DatabaseStats> {
        let generation = self.dataset_index.generation();
        if let Some(stats) = &self.cached_stats {
            if self.exact_stats_generation == generation {
                return stats.clone();
            }
        }

        let stats = self.rebuild_stats();
        self.cached_stats = Some(stats.clone());
        self.exact_stats_generation = generation;
        stats
    }

    /// Returns statistics for plan selection; under `Bounded` they may be stale within the threshold
    pub fn get_or_build_planning_stats(&mut self) -> Arc<DatabaseStats> {
        if self.planning_stats_policy == PlanningStatsPolicy::AlwaysFresh {
            return self.get_or_build_stats();
        }

        let generation = self.dataset_index.generation();
        if let Some(snapshot) = &self.planning_stats {
            if self.planning_snapshot_is_usable(snapshot, generation) {
                return snapshot.stats.clone();
            }
        }

        let stats = self.rebuild_stats();
        self.planning_stats = Some(PlanningStatsSnapshot {
            stats: stats.clone(),
            generation,
            quads_at_build: stats.total_triples,
            epoch: self.stats_epoch,
            resets: self.dataset_index.resets(),
        });
        stats
    }

    /// Returns true if the cached planning snapshot is still within the policy threshold
    fn planning_snapshot_is_usable(
        &self,
        snapshot: &PlanningStatsSnapshot,
        generation: u64,
    ) -> bool {
        // Invalidate on explicit reset or full clear
        if snapshot.epoch != self.stats_epoch || snapshot.resets != self.dataset_index.resets() {
            return false;
        }

        // wrapping_sub handles counter wraparound; a replaced index yields a large delta
        let mutations = generation.wrapping_sub(snapshot.generation);
        if mutations == 0 {
            return true;
        }

        // Always rebuild a snapshot taken on an empty dataset
        if snapshot.quads_at_build == 0 {
            return false;
        }

        let PlanningStatsPolicy::Bounded {
            minimum_batch,
            refresh_fraction,
        } = self.planning_stats_policy
        else {
            return false;
        };

        let proportional = (refresh_fraction * snapshot.quads_at_build.max(1) as f64).ceil() as u64;
        mutations < minimum_batch.max(proportional)
    }

    /// Builds statistics and records rebuild count and duration
    fn rebuild_stats(&mut self) -> Arc<DatabaseStats> {
        let started = std::time::Instant::now();
        let stats = Arc::new(DatabaseStats::gather_stats_fast(self));
        self.stats_rebuild_count += 1;
        self.stats_rebuild_nanos = self
            .stats_rebuild_nanos
            .saturating_add(started.elapsed().as_nanos() as u64);
        stats
    }

    /// Invalidates all cached statistics
    pub fn invalidate_stats_cache(&mut self) {
        self.cached_stats = None;
        self.planning_stats = None;
        self.stats_epoch = self.stats_epoch.wrapping_add(1);
    }

    /// Replaces the dataset index and invalidates cached statistics
    pub fn replace_dataset_index(&mut self, dataset_index: DatasetIndex) {
        self.dataset_index = dataset_index;
        self.invalidate_stats_cache();
    }

    /// Number of statistics rebuilds and their total duration
    pub fn stats_rebuild_metrics(&self) -> (u64, std::time::Duration) {
        (
            self.stats_rebuild_count,
            std::time::Duration::from_nanos(self.stats_rebuild_nanos),
        )
    }

    /// Starts a programmatic query. See [`crate::query_builder`].
    pub fn query(&self) -> QueryBuilder<'_> {
        QueryBuilder::new(self)
    }

    /// Adds an already-encoded triple to the default graph.
    ///
    /// Ids must come from this store's dictionary. Use
    /// [`Self::add_triple_parts`] to add one from text.
    pub fn add_triple(&mut self, triple: Triple) {
        self.dataset_index.insert_triple(&triple);
    }

    /// Removes a triple from the default graph. Returns whether it was present.
    pub fn delete_triple(&mut self, triple: &Triple) -> bool {
        self.dataset_index.delete_triple(triple)
    }

    /// Adds an encoded quad, creating its graph if needed. Returns whether it was
    /// new.
    pub fn add_quad(&mut self, quad: Quad) -> bool {
        self.dataset_index.insert_quad(&quad)
    }

    /// Removes a quad. Returns whether it was present.
    pub fn delete_quad(&mut self, quad: &Quad) -> bool {
        self.dataset_index.delete_quad(quad)
    }

    /// Interns the given terms and adds the resulting quad.
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

    /// Default-graph triples matching the bound positions; `None` is a wildcard.
    pub fn query_default_triples(
        &self,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Vec<Triple> {
        self.dataset_index.query_default(s, p, o)
    }

    /// Quads in one graph matching the bound positions; `None` is a wildcard.
    pub fn query_graph_quads(
        &self,
        graph: GraphId,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Vec<Quad> {
        self.dataset_index.query_graph(graph, s, p, o)
    }

    /// Encodes three lexical terms and adds the resulting triple
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

    /// Adds a triple carrying a provenance annotation, for probabilistic
    /// reasoning.
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

    /// Encodes three lexical terms and deletes the resulting triple
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

    /// Serialises the default graph as RDF/XML.
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

        // For each subject, create an <rdf:Description> element
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

    /// Serialises every graph as N-Quads, which unlike N-Triples preserves named
    /// graphs.
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
        let mut subjects: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
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

    /// Parses RDF/XML and adds the triples.
    ///
    /// Malformed input is skipped rather than reported, so a partial parse looks
    /// like a successful one with fewer triples.
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
                    Err(_e) => {
                        eprintln!("KOLIBRIE_OPERATION_FAILED");
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

    /// Reads RDF/XML from a file and adds the triples.
    ///
    /// # Panics
    ///
    /// If the file cannot be read.
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
                    eprintln!("KOLIBRIE_OPERATION_FAILED");
                    break;
                }
                Err(_e) => {
                    eprintln!("KOLIBRIE_OPERATION_FAILED");
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
                Err(_e) => {
                    eprintln!("KOLIBRIE_OPERATION_FAILED");
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

    /// Parses Turtle and adds the triples, honouring `@prefix` declarations and
    /// the `;` and `,` abbreviations.
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
                    eprintln!("KOLIBRIE_OPERATION_FAILED");
                }
                continue;
            }

            // Tokenize, but keep ; , . as delimiters only when outside URIs, literals, and quoted triples
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

    /// Tokenize a Turtle-star line, keeping `<< ... >>` and punctuation structure intact
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
    /// Parses the N3 subset this engine supports and adds the triples.
    ///
    /// Only the Datalog-expressible fragment; a full N3 reasoner is future work.
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
                            eprintln!("KOLIBRIE_OPERATION_FAILED");
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
    /// Parses N-Triples and adds them. This is the usual entry point.
    ///
    /// [`Self::parse_ntriples`] parses without adding, which is only useful when
    /// the caller wants to inspect or filter first.
    pub fn parse_ntriples_and_add(&mut self, ntriples_data: &str) {
        let partial_results = self.parse_ntriples(ntriples_data);

        let encoded_triples = self.encode_triples(partial_results);
        for encoded_triple in encoded_triples {
            self.add_triple(encoded_triple);
        }
    }

    /// Parses N-Triples into lexical triples **without** adding them, in parallel
    /// chunks of 1000 lines.
    ///
    /// Returns one group per chunk. The `a` shorthand is expanded to the full RDF
    /// type IRI in predicate position.
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
                        eprintln!("KOLIBRIE_OPERATION_FAILED");
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
    /// Interns lexical triples into encoded ones.
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

    /// Parses and encodes N-Triples without adding them to the store.
    pub fn parse_and_encode_ntriples(&mut self, ntriples_data: &str) -> Vec<Triple> {
        let partial_results = self.parse_ntriples(ntriples_data);

        self.encode_triples(partial_results)
    }

    /// Parses N-Quads and adds them, preserving named graphs.
    pub fn parse_nquads_and_add(&mut self, nquads_data: &str) {
        for raw_line in nquads_data.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let line_without_dot = if line.ends_with('.') {
                line[..line.len() - 1].trim()
            } else {
                eprintln!("KOLIBRIE_OPERATION_FAILED");
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
            eprintln!("KOLIBRIE_OPERATION_FAILED");
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
            // Expand the Turtle `a` shorthand for rdf:type in predicate position
            let predicate = if parts[1] == "a" {
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string()
            } else {
                self.clean_ntriples_term(&parts[1])
            };
            let object = self.clean_ntriples_term(&parts[2]);
            Some((subject, predicate, object))
        } else {
            eprintln!("KOLIBRIE_OPERATION_FAILED");
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
                eprintln!("KOLIBRIE_OPERATION_FAILED");
                term.to_string()
            }
        } else {
            term.to_string()
        }
    }

    // Method to automatically extract and register prefixes from a query string
    /// Adopts the `PREFIX` declarations from a query, so later queries and rules
    /// can use them without repeating them.
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
    /// Copies this store's prefixes into `prefixes`, leaving existing entries
    /// alone.
    pub fn share_prefixes_with(&self, prefixes: &mut HashMap<String, String>) {
        for (prefix, uri) in &self.prefixes {
            prefixes.insert(prefix.clone(), uri.clone());
        }
    }

    /// Expands a lexical term to its full IRI using `prefixes`, then this store's
    /// own. Returns the term unchanged when no prefix applies.
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
                eprintln!("KOLIBRIE_OPERATION_FAILED");
                term.to_string()
            }
        } else {
            term.to_string()
        }
    }

    /// A new store containing both stores' data.
    ///
    /// The two dictionaries assigned ids independently, so `other`'s ids are
    /// translated into the merged dictionary rather than assumed compatible —
    /// which is why this returns a third store instead of mutating either.
    pub fn union(&mut self, other: &SparqlDatabase) -> Self {
        let self_dict = self.dictionary.read().unwrap();
        let other_dict = other.dictionary.read().unwrap();
        let mut merged_dictionary = self_dict.clone();
        let self_quoted_triples = self.quoted_triple_store.read().unwrap();
        let other_quoted_triples = other.quoted_triple_store.read().unwrap();
        let mut merged_quoted_triples = self_quoted_triples.clone();
        let mut translated_ids = HashMap::new();

        // Preserve the complete lexical dictionary, not only terms currently
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

        // Graph names and every term in the other database must be translated
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
            ml_context: self.ml_context.clone(),
            train_neural_relation_decls: self.train_neural_relation_decls.clone(),
            neural_model_artifacts: self.neural_model_artifacts.clone(),
            neural_materialized_triples: self.neural_materialized_triples.clone(),
            ml_predict_materialized_triples: self.ml_predict_materialized_triples.clone(),
            probability_seeds: merged_seeds,
            // Statistics are rebuilt on first use
            cached_stats: None,
            exact_stats_generation: 0,
            planning_stats: None,
            planning_stats_policy: self.planning_stats_policy,
            stats_epoch: 0,
            stats_rebuild_count: 0,
            stats_rebuild_nanos: 0,
            quoted_triple_store: Arc::new(RwLock::new(merged_quoted_triples)),
            implicit_neural_materialization: self.implicit_neural_materialization,
        }
    }

    /// Runs a query and renders the result as text, for the demo server.
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

    /// The status line and body for one SPARQL query.
    ///
    /// Returns the two separately so that [`Self::handle_http_request`] composes
    /// exactly one response. This used to return a bare body on success and a
    /// complete HTTP response on an ML refusal, which the caller then wrapped in a
    /// second `200 OK` — so a refusal reached the client as a success whose body was
    /// another HTTP response.
    fn http_query_result(&mut self, query: &str) -> (&'static str, String) {
        match crate::execute_query::execute_sparql_query(query, self) {
            Ok(rows) => (
                "200 OK",
                rows.into_iter()
                    .map(|row| row.join("\t"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            Err(error) => {
                // Matched on the variant rather than on a rendered string. This is the
                // same status table the HTTP server keeps in `ml_error_response`: two
                // copies, in two crates, that have to be changed together.
                use crate::execute_query::QueryError;
                use crate::ml_policy::MlError;
                match &error {
                    QueryError::Ml(MlError::Disabled) => {
                        ("503 Service Unavailable", "ML_FEATURE_DISABLED".to_string())
                    }
                    QueryError::Ml(MlError::Forbidden) => {
                        ("403 Forbidden", "ML_FORBIDDEN".to_string())
                    }
                    QueryError::Ml(MlError::InvalidArtifact) => {
                        ("403 Forbidden", "ML_INVALID_ARTIFACT".to_string())
                    }
                    QueryError::Ml(MlError::InvalidConfiguration) => (
                        "503 Service Unavailable",
                        "ML_INVALID_CONFIGURATION".to_string(),
                    ),
                    // The message is deliberately not sent: a parse failure carries the
                    // offending query text.
                    _ => ("400 Bad Request", "QUERY_EXECUTION_FAILED".to_string()),
                }
            }
        }
    }

    /// The status line and body for one SPARQL Update.
    ///
    /// A failed update used to answer `200 OK` with the body `Update Failed`.
    fn http_update_result(&mut self, update: &str) -> (&'static str, String) {
        if let Ok(summary) = self.execute_update(update) {
            return (
                "200 OK",
                format!(
                    "Update Successful (inserted {}, deleted {})",
                    summary.inserted_quads, summary.deleted_quads
                ),
            );
        }
        // Historical standalone INSERT/DELETE aliases parse into the same update operation
        if crate::execute_query::execute_sparql_update_compat(update, self).is_ok() {
            return ("200 OK", "Update Successful".to_string());
        }
        ("400 Bad Request", "Update Failed".to_string())
    }

    /// Executes one supported standard SPARQL Update form and reports what changed
    pub fn execute_update(
        &mut self,
        update: &str,
    ) -> Result<crate::execute_query::UpdateSummary, String> {
        crate::execute_query::execute_sparql_update(update, self)
    }

    /// Applies an update and renders the outcome as text, for the demo server.
    ///
    /// The body only. [`Self::handle_http_request`] pairs the same outcome with a
    /// status line.
    pub fn handle_update(&mut self, update: &str) -> String {
        self.http_update_result(update).1
    }

    /// Answers one HTTP request against this store, for [`crate::utils::run_server`].
    ///
    /// A complete HTTP response, status line included, which
    /// [`crate::utils::handle_connection`] writes verbatim. It used to return the
    /// body alone and leave the caller to add `200 OK`, so every failure — a syntax
    /// error, a refused update, an ML refusal — reached the client as a success.
    ///
    /// Enforces the ML policy: a request naming a model the host has not approved
    /// is refused rather than run.
    pub fn handle_http_request(&mut self, request: &str) -> String {
        let context = self.ml_context.for_http();
        let (status, body) = crate::execute_query::with_ml_context(self, &context, |db| {
            db.handle_http_request_inner(request)
        });
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    fn handle_http_request_inner(&mut self, request: &str) -> (&'static str, String) {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        req.parse(request.as_bytes()).unwrap();

        match req.method.unwrap() {
            "GET" => {
                let url = Url::parse(&("http://localhost".to_owned() + req.path.unwrap())).unwrap();
                let query_pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
                if let Some(query) = query_pairs.get("query") {
                    return self.http_query_result(query);
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
                            return self.http_query_result(body);
                        }
                    } else if content_type == b"application/x-www-form-urlencoded" {
                        // URL-encoded POST query or update
                        if let Some(body) = request.split("\r\n\r\n").nth(1) {
                            let params = parse_form_urlencoded(body);

                            if let Some(query) = params.get("query") {
                                return self.http_query_result(query);
                            } else if let Some(update) = params.get("update") {
                                return self.http_update_result(update);
                            }
                        }
                    } else if content_type == b"application/sparql-update" {
                        // Direct POST update
                        if let Some(body) = request.split("\r\n\r\n").nth(1) {
                            return self.http_update_result(body);
                        }
                    }
                }
            }
            _ => {}
        }

        ("400 Bad Request", "Bad Request".to_string())
    }

    /// Return the decoded graph size for diagnostics
    pub fn debug_print_triples(&self) {
        println!(
            "TRIPLE_COUNT {}",
            self.query_default_triples(None, None, None).len()
        );
    }

    // Create user defined function
    /// Registers a user-defined function callable from `FILTER` expressions.
    ///
    /// Receives arguments as decoded text and returns text. Re-registering a name
    /// replaces the previous function.
    pub fn register_udf<F>(&mut self, name: &str, f: F)
    where
        F: Fn(Vec<&str>) -> String + Send + Sync + 'static,
    {
        self.udfs.insert(name.to_string(), ClonableFn::new(f));
    }

    // ── Dataset index ───────────────────────────────────────────────────────────
    //
    // The last field to leave the public API. These are deliberately named
    // operations rather than a `dataset_index()` getter: §4 has this surface becoming
    // `QuadSource` (scan / insert / graphs / len), so the shape callers depend on
    // should already be that rather than a borrow of `DatasetIndex`, which would
    // freeze the representation the storage trait is supposed to abstract.
    //
    // Most of them are thin, and that is the point — 33 of the 46 external call sites
    // only wanted a triple count.
    //
    // The mutating ones deliberately do NOT call `invalidate_stats_cache`, matching
    // `add_triple`/`add_quad`/`delete_*` above. Statistics detect a changed index
    // through `DatasetIndex::generation`, not through being told — the two tests at
    // the bottom of this file exist to prove exactly that. Invalidating here would
    // force a rebuild the generation check avoids, and would change the behaviour the
    // rewritten call sites had when they wrote to the field directly.

    /// Number of triples in the default graph.
    pub fn default_triple_count(&self) -> usize {
        self.dataset_index.len_default()
    }

    /// Number of quads across every graph, including named ones.
    pub fn quad_count(&self) -> usize {
        self.dataset_index.count_quads()
    }

    /// Every quad in the store, across all graphs.
    pub fn all_quads(&self) -> Vec<Quad> {
        self.dataset_index.all_quads()
    }

    /// Every graph in the store, including the default graph.
    pub fn graphs(&self) -> Vec<GraphId> {
        self.dataset_index.graphs()
    }

    /// The named graphs, excluding the default graph.
    pub fn named_graphs(&self) -> Vec<GraphId> {
        self.dataset_index.named_graphs()
    }

    /// Creates `graph` if it does not exist. Returns whether it was created.
    pub fn create_graph(&mut self, graph: GraphId) -> bool {
        self.dataset_index.create_graph(graph)
    }

    /// Inserts a triple into the default graph, reporting whether it was new.
    ///
    /// [`Self::add_triple`] does the same without the answer; this exists because a
    /// caller deduplicating derived facts needs to know.
    pub fn insert_triple(&mut self, triple: &Triple) -> bool {
        self.dataset_index.insert_triple(triple)
    }

    /// Removes every quad and graph, leaving the dictionary intact.
    pub fn clear_dataset(&mut self) {
        self.dataset_index.clear();
    }

    // ── Term dictionary ─────────────────────────────────────────────────────────

    /// Read access to the term dictionary.
    pub fn dictionary(&self) -> DictionaryRef<'_> {
        DictionaryRef(self.dictionary.read().unwrap())
    }

    /// Write access to the term dictionary, for interning new terms.
    pub fn dictionary_mut(&self) -> DictionaryMut<'_> {
        DictionaryMut(self.dictionary.write().unwrap())
    }

    /// Whether `self` and `other` encode terms through the same dictionary, as
    /// opposed to holding separate ones.
    ///
    /// Replaces `Arc::ptr_eq` on the raw field: it states the property callers
    /// actually test — whether two databases are isolated — without the handle
    /// having to be public.
    pub fn shares_dictionary_with(&self, other: &SparqlDatabase) -> bool {
        Arc::ptr_eq(&self.dictionary, &other.dictionary)
    }

    /// The dictionary handle, for sharing one term encoding across engines.
    ///
    /// This is the one place `Arc<RwLock<_>>` still reaches the public API, and it is
    /// a deliberate, named escape hatch rather than an oversight. Sharing an encoding
    /// is a real capability: the HTTP server and two examples hand the database's
    /// dictionary to a `Reasoner` so derived terms decode consistently, and
    /// `Reasoner::dictionary` is itself typed `Arc<RwLock<Dictionary>>` in another
    /// crate, so no guard-based signature can express it.
    ///
    /// P5 says `Arc<RwLock<_>>` must not appear in public signatures. Confining the
    /// violation to two documented methods leaves one place to change when §4 moves
    /// encoding behind `TermEncoder` and makes sharing a storage concern — against
    /// 96 call sites if the field had stayed public. Prefer [`Self::dictionary`] and
    /// [`Self::dictionary_mut`] for everything else.
    pub fn dictionary_handle(&self) -> Arc<RwLock<Dictionary>> {
        Arc::clone(&self.dictionary)
    }

    /// Adopts `handle` as this database's dictionary, replacing its own.
    ///
    /// See [`Self::dictionary_handle`] for why this takes the raw handle.
    pub fn set_dictionary_handle(&mut self, handle: Arc<RwLock<Dictionary>>) {
        self.dictionary = handle;
    }

    // ── Accessors for fields sealed in stage 2a ─────────────────────────────────
    //
    // Twelve fields left the public API here. They stay `pub(crate)` rather than
    // becoming fully private: the goal is that no *consumer* depends on the layout,
    // and changing the representation later is then a within-crate refactor. Making
    // them private as well would mean accessors for ~250 internal sites, which buys
    // nothing a consumer can observe.
    //
    // Each accessor below matches what the 79 external call sites actually did —
    // measured by sealing the fields and reading the compiler's E0616 list, not by
    // grepping for field names. Grep over-counted by up to 2x, because `model_decls`,
    // `probability_seeds` and friends are also fields on CombinedQuery, Rule,
    // CompiledProgram and Reasoner.

    /// The ML execution policy: which models are approved, and where artifacts live.
    pub fn ml_context(&self) -> &crate::ml_policy::MlExecutionContext {
        &self.ml_context
    }

    /// Replaces the ML execution policy.
    pub fn set_ml_context(&mut self, context: crate::ml_policy::MlExecutionContext) {
        self.ml_context = context;
    }

    /// When planning statistics are rebuilt rather than reused.
    pub fn planning_stats_policy(&self) -> PlanningStatsPolicy {
        self.planning_stats_policy
    }

    /// Sets when planning statistics are rebuilt.
    pub fn set_planning_stats_policy(&mut self, policy: PlanningStatsPolicy) {
        self.planning_stats_policy = policy;
    }

    /// How many times planning statistics have been rebuilt. Used by tests to assert
    /// that a policy actually avoided work.
    pub fn stats_rebuild_count(&self) -> u64 {
        self.stats_rebuild_count
    }

    /// Prefix bindings, for resolving abbreviated terms.
    pub fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    /// Binds `prefix` to `uri`, replacing any existing binding.
    pub fn add_prefix(&mut self, prefix: impl Into<String>, uri: impl Into<String>) {
        self.prefixes.insert(prefix.into(), uri.into());
    }

    /// Merges a set of prefix bindings in, replacing any that collide.
    ///
    /// The parsers produce prefixes as a map, so callers had been reaching for
    /// `prefixes.extend(..)` on the field directly.
    pub fn extend_prefixes(&mut self, bindings: impl IntoIterator<Item = (String, String)>) {
        self.prefixes.extend(bindings);
    }

    /// Declared ML models, by name.
    pub fn model_decls(&self) -> &HashMap<String, ModelDecl> {
        &self.model_decls
    }

    /// Declared neural relations, by predicate.
    pub fn neural_relation_decls(&self) -> &HashMap<String, NeuralRelationDecl> {
        &self.neural_relation_decls
    }

    /// Resolved artifact paths for trained models, by model name.
    pub fn neural_model_artifacts(&self) -> &HashMap<String, String> {
        &self.neural_model_artifacts
    }

    /// Triples materialised by neural relations, by predicate.
    pub fn neural_materialized_triples(&self) -> &HashMap<String, Vec<Triple>> {
        &self.neural_materialized_triples
    }

    /// Takes the triples materialised for `predicate`, so they can be retracted.
    pub fn take_neural_materialized(&mut self, predicate: &str) -> Option<Vec<Triple>> {
        self.neural_materialized_triples.remove(predicate)
    }

    /// Per-triple prior probabilities seeded into probabilistic reasoning.
    pub fn probability_seeds(&self) -> &HashMap<Triple, f64> {
        &self.probability_seeds
    }

    /// Maps a short rule name to its expanded IRI.
    pub fn add_rule_mapping(&mut self, name: impl Into<String>, expanded: impl Into<String>) {
        self.rule_map.insert(name.into(), expanded.into());
    }

    /// Read access to the quoted-triple store.
    ///
    /// Returns a guard rather than the `Arc<RwLock<_>>` itself. P5 forbids
    /// `Arc<RwLock<_>>` in public signatures, and a getter handing back the `Arc`
    /// would seal the field while freezing both the representation and the locking
    /// strategy — the same encapsulation-in-name-only that stage 1 avoided.
    pub fn quoted_triples(&self) -> QuotedTriplesRef<'_> {
        QuotedTriplesRef(self.quoted_triple_store.read().unwrap())
    }

    /// Write access to the quoted-triple store. See [`Self::quoted_triples`].
    pub fn quoted_triples_mut(&self) -> QuotedTriplesMut<'_> {
        QuotedTriplesMut(self.quoted_triple_store.write().unwrap())
    }

    /// Whether `self` and `other` share one quoted-triple store, as opposed to
    /// holding separate ones.
    ///
    /// Replaces `Arc::ptr_eq` on the raw field: it states the property a caller
    /// actually cares about — whether two databases are isolated — without the
    /// handle having to be public.
    pub fn shares_quoted_triples_with(&self, other: &SparqlDatabase) -> bool {
        Arc::ptr_eq(&self.quoted_triple_store, &other.quoted_triple_store)
    }

    // ── Accessors for fields sealed in stage 1 ──────────────────────────────────
    //
    // These three fields had no consumer outside this crate, so they are private and
    // reached through the operations below. The operations are deliberately specific
    // rather than `&mut` getters: handing out `&mut HashMap` would seal the field in
    // name only and still freeze the representation, which is the thing P4 exists to
    // avoid. Five further fields were used only inside this file and needed no
    // accessor at all.

    /// Looks up a registered user-defined function.
    pub(crate) fn udf(&self, name: &str) -> Option<&ClonableFn> {
        self.udfs.get(name)
    }

    /// Records the triples materialised for `cache_key`, so a rerun can undo them.
    //
    // Gated to match its only caller, `ml_predict_runtime`. Without this the default
    // build reports both methods dead — the same thing the step-4a demotions surfaced
    // for `ml_feature_loader`.
    #[cfg(feature = "ml")]
    pub(crate) fn set_ml_materialized(&mut self, cache_key: String, triples: Vec<Triple>) {
        self.ml_predict_materialized_triples
            .insert(cache_key, triples);
    }

    /// Takes the triples previously materialised for `cache_key`, if any.
    #[cfg(feature = "ml")]
    pub(crate) fn take_ml_materialized(&mut self, cache_key: &str) -> Option<Vec<Triple>> {
        self.ml_predict_materialized_triples.remove(cache_key)
    }

    /// Whether neural relations are materialised implicitly during query execution.
    pub(crate) fn implicit_neural_materialization(&self) -> bool {
        self.implicit_neural_materialization
    }

    /// Sets the flag and returns its previous value, so a caller can restore it.
    pub(crate) fn set_implicit_neural_materialization(&mut self, enabled: bool) -> bool {
        std::mem::replace(&mut self.implicit_neural_materialization, enabled)
    }

    /// Rebuilds every graph-scoped index, keeping named graphs distinct from the default
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

#[cfg(test)]
mod tests {
    use super::*;

    // These two tests live inside the crate because they deliberately write to
    // `dataset_index` without going through the public helpers, and the field is now
    // private. Both assert that statistics detect a change they were never told
    // about — which is a property of the generation check in
    // `planning_snapshot_is_usable`, not of the invalidation call.
    //
    // Rewriting them to use `replace_dataset_index` would have kept them compiling
    // and silently stopped testing that: it calls `invalidate_stats_cache` itself, so
    // the generation path would never be exercised. They were moved here instead.

    fn insert(database: &mut SparqlDatabase, index: u32) {
        database.add_triple_parts(&format!("urn:s{index}"), "urn:p", &format!("{index}"));
    }

    #[test]
    fn exact_statistics_notice_a_write_that_bypassed_the_helpers() {
        let mut database = SparqlDatabase::new();
        for index in 0..3 {
            insert(&mut database, index);
        }
        assert_eq!(database.get_or_build_stats().total_triples, 3);

        // Insert straight into the index; one lock guard avoids a self-deadlock.
        let quad = {
            let mut dictionary = database.dictionary_mut();
            Quad {
                subject: dictionary.encode("urn:d"),
                predicate: dictionary.encode("urn:p"),
                object: dictionary.encode("4"),
                graph: GraphId::Default,
            }
        };
        assert!(database.dataset_index.insert_quad(&quad));

        assert_eq!(database.get_or_build_stats().total_triples, 4);
    }

    #[test]
    fn a_replacement_index_with_a_lower_generation_is_not_mistaken_for_fresh() {
        let mut database = SparqlDatabase::new();
        for index in 0..200 {
            insert(&mut database, index);
        }
        database.set_planning_stats_policy(PlanningStatsPolicy::Bounded {
            minimum_batch: 1_000,
            refresh_fraction: 0.90,
        });
        let first = database.get_or_build_planning_stats();
        assert_eq!(first.total_triples, 200);

        let mut replacement = DatasetIndex::new();
        replacement.insert_quad(&Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Default,
        });
        // Generation 1, lower than the original 200: direct assignment, bypassing
        // replace_dataset_index so no invalidation is signalled.
        database.dataset_index = replacement;

        let refreshed = database.get_or_build_planning_stats();
        assert!(!Arc::ptr_eq(&first, &refreshed));
        assert_eq!(refreshed.total_triples, 1);
        assert_eq!(database.get_or_build_stats().total_triples, 1);
    }
}
