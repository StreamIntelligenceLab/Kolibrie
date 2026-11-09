/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The vocabulary every Kolibrie engine shares

/// Quad indexes over several orderings, for answering patterns without scanning
pub mod dataset_index;
/// Interning RDF terms to `u32` ids, and back
pub mod dictionary;
/// Incremental maintenance of Sentential Decision Diagrams
pub mod diff_sdd;
pub mod hybrid;
/// Join algorithms shared by the query engine and the reasoners
pub mod join_algorithm;
pub mod pattern;
/// Provenance semirings: how a derived fact's annotation is computed
pub mod provenance;
pub mod quad_source;
pub mod query;
/// Storage for RDF-star quoted triples, which live in a tagged id range
pub mod quoted_triple_store;
/// RDF term identity: how a term is spelled so that one term is one string
pub mod rdf_term;
/// Rules: premises, conclusion, filters, and safety checking
pub mod rule;
/// An index from triple pattern to the rules whose premises could match it
pub mod rule_index;
/// Sentential Decision Diagrams, for weighted model counting
pub mod sdd;
/// Declaring which facts carry a prior probability
pub mod seed_spec;
/// Per-fact provenance annotations, kept alongside the facts
pub mod tag_store;
pub mod term_encoder;
/// Terms and triple patterns: the variable-or-constant vocabulary
pub mod terms;
/// An encoded RDF triple: three dictionary ids
pub mod triple;
