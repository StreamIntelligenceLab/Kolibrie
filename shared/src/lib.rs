/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The vocabulary every Kolibrie engine shares.
//!
//! This crate holds the types that the SPARQL engine, the Datalog reasoner and the
//! temporal (DatalogMTL) reasoner all have to agree on, and nothing that belongs to
//! only one of them. If a type lives here, two or more engines speak it; if only one
//! does, it belongs in that engine's crate.
//!
//! # Terms are integers, not strings
//!
//! The single most important thing to know: RDF terms are **not** stored as strings
//! during evaluation. A [`Dictionary`](dictionary::Dictionary) interns each term to a
//! `u32`, and a [`Triple`](triple::Triple) is three such ids. Joins, indexes and
//! pattern matching all work on integers; decoding back to text happens at the edges.
//!
//! The `u32` space is split:
//!
//! | range | meaning |
//! |---|---|
//! | `0 .. 0x8000_0000` | ordinary terms, assigned by `Dictionary` |
//! | `0x8000_0000 ..` | quoted triples (RDF-star), held by [`QuotedTripleStore`] |
//!
//! The top bit is the tag — see
//! [`is_quoted_triple_id`](quoted_triple_store::is_quoted_triple_id). An id is
//! therefore only meaningful together with the dictionary that produced it, which is
//! why engines sharing data must share a dictionary.
//!
//! [`QuotedTripleStore`]: quoted_triple_store::QuotedTripleStore

/// Quad indexes over several orderings, for answering patterns without scanning.
pub mod dataset_index;
/// Interning RDF terms to `u32` ids, and back.
pub mod dictionary;
/// Incremental maintenance of Sentential Decision Diagrams.
pub mod diff_sdd;
/// Join algorithms shared by the query engine and the reasoners.
pub mod join_algorithm;
/// Matching a triple pattern against a fact, and building a fact from a pattern.
pub mod pattern;
/// Hybrid exact/approximate probabilistic inference.
pub mod hybrid;
/// Provenance semirings: how a derived fact's annotation is computed.
pub mod provenance;
/// The parsed shape of a query: patterns, filters, modifiers and ML clauses.
pub mod query;
/// Storage for RDF-star quoted triples, which live in a tagged id range.
pub mod quoted_triple_store;
/// Rules: premises, conclusion, filters, and safety checking.
pub mod rule;
/// An index from triple pattern to the rules whose premises could match it.
pub mod rule_index;
/// Sentential Decision Diagrams, for weighted model counting.
pub mod sdd;
/// Declaring which facts carry a prior probability.
pub mod seed_spec;
/// Per-fact provenance annotations, kept alongside the facts.
pub mod tag_store;
/// Terms and triple patterns: the variable-or-constant vocabulary.
pub mod terms;
/// An encoded RDF triple: three dictionary ids.
pub mod triple;
