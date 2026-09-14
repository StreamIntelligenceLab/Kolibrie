/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub use crate::ml_syntax::lower_ml_predict_alias;
use crate::sparql_database::SparqlDatabase;
use shared::query::{ModelDecl, NeuralRelationDecl, TrainNeuralRelationDecl};
use std::collections::HashMap;

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
pub fn execute_train_decl(
    _db: &mut SparqlDatabase,
    _train: &TrainNeuralRelationDecl,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("ML_FEATURE_DISABLED".into())
}
pub fn materialize_neural_relations_for_patterns(
    db: &mut SparqlDatabase,
    _patterns: &[(&str, &str, &str)],
    _prefixes: &HashMap<String, String>,
) -> Result<(), String> {
    if db.neural_relation_decls.is_empty() {
        Ok(())
    } else {
        Err("ML_FEATURE_DISABLED".into())
    }
}
pub fn execute_neural_program(_db: &mut SparqlDatabase, _program: &str) -> Result<(), String> {
    Err("ML_FEATURE_DISABLED".into())
}
