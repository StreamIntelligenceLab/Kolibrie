/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stand-ins for [`neural_relations`](crate::neural_relations) when the `ml` feature
//! is off.
//!
//! Every entry point here mirrors the real one's signature and refuses with
//! `ML_FEATURE_DISABLED`, so the rest of the crate compiles without PyO3 and a caller
//! gets a clear error rather than a missing symbol.
//!
//! The refusals are deliberately conditional where they can be: a request that
//! declares no models and no neural relations succeeds, because it asked for nothing
//! the feature provides. Only a request that actually needs a model fails.

pub use crate::ml_syntax::lower_ml_predict_alias;
use crate::sparql_database::SparqlDatabase;
use shared::query::{ModelDecl, NeuralRelationDecl, TrainNeuralRelationDecl};
use std::collections::HashMap;

/// Accepts an empty set of declarations and refuses any non-empty one.
pub fn register_neural_declarations_checked(
    _db: &mut SparqlDatabase,
    _prefixes: &HashMap<String, String>,
    models: &[ModelDecl],
    relations: &[NeuralRelationDecl],
    training: &[TrainNeuralRelationDecl],
) -> Result<(), String> {
    if models.is_empty() && relations.is_empty() && training.is_empty() {
        Ok(())
    } else {
        Err("ML_FEATURE_DISABLED".into())
    }
}
/// As [`register_neural_declarations_checked`], reporting to stderr instead of
/// returning an error.
pub fn register_neural_declarations(
    db: &mut SparqlDatabase,
    prefixes: &HashMap<String, String>,
    models: &[ModelDecl],
    relations: &[NeuralRelationDecl],
    training: &[TrainNeuralRelationDecl],
) {
    if register_neural_declarations_checked(db, prefixes, models, relations, training).is_err() {
        eprintln!("ML_FEATURE_DISABLED");
    }
}
/// Always fails: training needs the `ml` feature.
pub fn execute_train_decl(
    _db: &mut SparqlDatabase,
    _train: &TrainNeuralRelationDecl,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("ML_FEATURE_DISABLED".into())
}
/// Succeeds when implicit materialisation is off or no neural relations are
/// declared, and otherwise refuses.
pub fn materialize_neural_relations_for_patterns(
    db: &mut SparqlDatabase,
    _patterns: &[(&str, &str, &str)],
    _prefixes: &HashMap<String, String>,
) -> Result<(), String> {
    if !db.implicit_neural_materialization() || db.neural_relation_decls.is_empty() {
        Ok(())
    } else {
        Err("ML_FEATURE_DISABLED".into())
    }
}
/// Always fails: running a neural program needs the `ml` feature.
pub fn execute_neural_program(_db: &mut SparqlDatabase, _program: &str) -> Result<(), String> {
    Err("ML_FEATURE_DISABLED".into())
}
