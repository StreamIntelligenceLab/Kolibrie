/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The RDF store, [`KolibrieDB`], split by concern:
//!
//! - `terms`: term encoding, the dictionary and the quoted-triple store
//! - `prefixes`: prefix bindings and resolving abbreviated terms
//! - `dataset`: adding, removing, counting and merging triples and quads
//! - `planning_stats`: statistics the planner costs with, and when they are rebuilt
//! - `serialize`: writing RDF/XML, N-Triples, N-Quads and Turtle
//! - `parse_rdf_xml`, `parse_turtle`, `parse_n3`, `parse_ntriples`: reading those formats
//! - `extensions`: user-defined functions, rule names, ML and probabilistic state

mod dataset;
mod extensions;
mod parse_n3;
mod parse_ntriples;
mod parse_rdf_xml;
mod parse_turtle;
mod planning_stats;
mod prefixes;
mod serialize;
mod terms;
#[cfg(test)]
mod tests;

pub use planning_stats::{PlanningStatsPolicy, PlanningStatsSnapshot};
pub(crate) use terms::reencode_term_id;
pub use terms::{DictionaryMut, DictionaryRef, QuotedTriplesMut, QuotedTriplesRef};

use crate::stats::DatabaseStats;
use crate::utils::ClonableFn;
use shared::dataset_index::DatasetIndex;
use shared::query::{ModelDecl, NeuralRelationDecl, TrainNeuralRelationDecl};
use shared::quoted_triple_store::QuotedTripleStore;
use shared::term_encoder::SharedTerms;
use shared::triple::Triple;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// The RDF store: encoded triples, the dictionary that encodes them, indexes over several
/// orderings, and the statistics the planner costs with
#[derive(Debug, Clone)]
pub struct KolibrieDB {
    pub(crate) ml_context: crate::ml_policy::MlExecutionContext,
    pub(crate) dataset_index: DatasetIndex,
    pub(crate) dictionary: SharedTerms,
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
    training_seed: Option<u64>,
}

impl KolibrieDB {
    /// Initialize with host-selected ML authority
    pub fn with_ml_context(context: crate::ml_policy::MlExecutionContext) -> Self {
        let mut database = Self::new();
        database.ml_context = context;
        database
    }
    /// An empty store with ML disabled and the default statistics policy
    pub fn new() -> Self {
        Self {
            ml_context: crate::ml_policy::MlExecutionContext::disabled(),
            dataset_index: DatasetIndex::new(),
            dictionary: SharedTerms::new(),
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
            training_seed: None,
        }
    }
}
