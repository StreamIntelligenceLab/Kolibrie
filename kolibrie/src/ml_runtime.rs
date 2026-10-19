/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The four points at which query evaluation gives the ML layer a chance to act.
//!
//! # Why this exists
//!
//! Evaluating a query has to let models run: a `SELECT` over a neural relation has
//! to derive its facts before matching, a `TRAIN` clause has to train, and a
//! top-level `ML.PREDICT` has to predict. Those operations need the ML runtime,
//! which sits above query evaluation and depends on it.
//!
//! So `execute_query` and `rule_execution` used to call up into
//! [`neural_relations`](crate::neural_relations) directly, and the five modules
//! involved formed the crate's last dependency cycle. They now call through this
//! trait, which names only [`SparqlDatabase`], [`QueryError`] and the declaration
//! types — all of which sit below both layers.
//!
//! # Not optional
//!
//! [`MlRuntime::materialize_for_patterns`] runs before
//! every `SELECT` and every update's `WHERE`. It is the step that derives a neural
//! relation's facts on demand, so a query over one returns nothing without it. This
//! is a hook in the evaluation path, not an extension point a host may ignore.
//!
//! # Who implements it
//!
//! Two implementations, chosen by the `ml` feature, and the crate root picks between
//! them, in `lib.rs`. A caller never installs one, which is why
//! moving these calls behind a trait changed no behaviour.

use crate::query_error::QueryError;
use crate::sparql_database::SparqlDatabase;
use shared::query::{CombinedRule, MLPredictClause, TrainNeuralRelationDecl};
use shared::triple::Triple;
use std::collections::HashMap;

/// What query evaluation needs from the ML layer.
///
/// Every method takes the store separately from `&self` so that an implementation
/// holds no borrow of it; the implementations are stateless unit types.
pub trait MlRuntime: Send + Sync {
    /// Derives the facts of any neural relation these patterns mention.
    ///
    /// Called before matching, once per `SELECT` and once per update `WHERE`.
    /// Succeeds without doing anything when no pattern names a neural relation.
    fn materialize_for_patterns(
        &self,
        database: &mut SparqlDatabase,
        patterns: &[(&str, &str, &str)],
        prefixes: &HashMap<String, String>,
    ) -> Result<(), String>;

    /// Runs one `TRAIN NEURAL RELATION` declaration.
    fn execute_train_decl(
        &self,
        database: &mut SparqlDatabase,
        declaration: &TrainNeuralRelationDecl,
    ) -> Result<(), String>;

    /// Runs a request whose top level is an `ML.PREDICT`.
    ///
    /// Returns a [`QueryError`] rather than a string so that a policy refusal stays
    /// a [`QueryError::Ml`] and keeps its `ML_*` code; flattening it would change
    /// the status an HTTP caller is given.
    fn execute_program(
        &self,
        database: &mut SparqlDatabase,
        program: &str,
    ) -> Result<(), QueryError>;

    /// Runs an `ML.PREDICT` clause inside a rule, rewriting the rule in place.
    ///
    /// Returns a [`QueryError`] for the same reason
    /// [`execute_program`](Self::execute_program) does: without the `ml` feature
    /// this is a policy refusal and has to stay a [`QueryError::Ml`], not become a
    /// generic execution failure.
    fn execute_predict_clause(
        &self,
        database: &mut SparqlDatabase,
        clause: &MLPredictClause<'_>,
        rule: &mut CombinedRule<'_>,
        prefixes: &HashMap<String, String>,
    ) -> Result<Vec<Triple>, QueryError>;
}
