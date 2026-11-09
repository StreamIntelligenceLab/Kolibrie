/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! User-defined functions, rule names, and the ML and probabilistic state hung off the store

use super::KolibrieDB;
use crate::utils::ClonableFn;
use shared::query::{ModelDecl, NeuralRelationDecl};
use shared::triple::Triple;
use std::collections::HashMap;

impl KolibrieDB {
    /// Registers a user-defined function callable from `FILTER` expressions
    pub fn register_udf<F>(&mut self, name: &str, f: F)
    where
        F: Fn(Vec<&str>) -> String + Send + Sync + 'static,
    {
        self.udfs.insert(name.to_string(), ClonableFn::new(f));
    }

    /// Looks up a registered user-defined function
    pub(crate) fn udf(&self, name: &str) -> Option<&ClonableFn> {
        self.udfs.get(name)
    }

    /// The ML execution policy: which models are approved, and where artifacts live
    pub fn ml_context(&self) -> &crate::ml_policy::MlExecutionContext {
        &self.ml_context
    }

    /// Replaces the ML execution policy
    pub fn set_ml_context(&mut self, context: crate::ml_policy::MlExecutionContext) {
        self.ml_context = context;
    }

    /// Declared ML models, by name
    pub fn model_decls(&self) -> &HashMap<String, ModelDecl> {
        &self.model_decls
    }

    /// Declared neural relations, by predicate
    pub fn neural_relation_decls(&self) -> &HashMap<String, NeuralRelationDecl> {
        &self.neural_relation_decls
    }

    /// Resolved artifact paths for trained models, by model name
    pub fn neural_model_artifacts(&self) -> &HashMap<String, String> {
        &self.neural_model_artifacts
    }

    /// Triples materialised by neural relations, by predicate
    pub fn neural_materialized_triples(&self) -> &HashMap<String, Vec<Triple>> {
        &self.neural_materialized_triples
    }

    /// Takes the triples materialised for `predicate`, so they can be retracted
    pub fn take_neural_materialized(&mut self, predicate: &str) -> Option<Vec<Triple>> {
        self.neural_materialized_triples.remove(predicate)
    }

    /// Per-triple prior probabilities seeded into probabilistic reasoning
    pub fn probability_seeds(&self) -> &HashMap<Triple, f64> {
        &self.probability_seeds
    }

    /// Maps a short rule name to its expanded IRI
    pub fn add_rule_mapping(&mut self, name: impl Into<String>, expanded: impl Into<String>) {
        self.rule_map.insert(name.into(), expanded.into());
    }

    /// Records the triples materialised for `cache_key`, so a rerun can undo them
    #[cfg(feature = "ml")]
    pub(crate) fn set_ml_materialized(&mut self, cache_key: String, triples: Vec<Triple>) {
        self.ml_predict_materialized_triples
            .insert(cache_key, triples);
    }

    /// Takes the triples previously materialised for `cache_key`, if any
    #[cfg(feature = "ml")]
    pub(crate) fn take_ml_materialized(&mut self, cache_key: &str) -> Option<Vec<Triple>> {
        self.ml_predict_materialized_triples.remove(cache_key)
    }

    /// Whether neural relations are materialised implicitly during query execution
    pub(crate) fn implicit_neural_materialization(&self) -> bool {
        self.implicit_neural_materialization
    }

    /// Sets the flag and returns its previous value, so a caller can restore it
    pub(crate) fn set_implicit_neural_materialization(&mut self, enabled: bool) -> bool {
        std::mem::replace(&mut self.implicit_neural_materialization, enabled)
    }

    /// The seed a `TRAIN NEURAL RELATION` declaration initialises weights from
    pub fn training_seed(&self) -> Option<u64> {
        self.training_seed
    }

    /// Makes declarative training reproducible by fixing its seed
    pub fn set_training_seed(&mut self, seed: Option<u64>) {
        self.training_seed = seed;
    }
}
