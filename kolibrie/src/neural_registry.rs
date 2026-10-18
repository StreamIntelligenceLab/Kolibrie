/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Recording model, neural relation and training declarations on a store.
//!
//! Registration is not execution. Putting a declaration on a store needs the store,
//! the ML policy and the declaration types, and none of the machinery that later
//! runs a model, so it sits below [`neural_relations`](crate::neural_relations)
//! rather than inside it. `parser`, `execute_query` and `program` all register
//! declarations and none of them should have to reach into the ML runtime to do it.
//!
//! # Not gated on `ml`
//!
//! Unlike the runtime, this compiles either way. With the feature off
//! [`require_local`](crate::ml_policy::MlExecutionContext::require_local) already
//! refuses with `ML_FEATURE_DISABLED`, and a request declaring nothing never reaches
//! it, so one implementation gives both behaviours and the stub that used to mirror
//! this in `neural_relations_disabled` is gone.

use crate::sparql_database::SparqlDatabase;
#[cfg(feature = "ml")]
use ml::OutputType;
#[cfg(feature = "ml")]
use shared::query::NeuralOutputKind;
use shared::query::{
    ModelArch, ModelDecl, NeuralRelationDecl, TrainNeuralRelationDecl, TrainingDataSource,
};
use std::collections::HashMap;

/// The default artifact filename for a model.
///
/// Every non-alphanumeric character becomes `_`, so the result is a safe filename
/// — but two model names differing only in punctuation collide. A host that cares
/// should name artifacts explicitly.
pub fn default_model_artifact_path(model_name: &str) -> String {
    let sanitized: String = model_name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    format!("{}_model.bin", sanitized)
}

pub(crate) fn normalize_term(
    database: &SparqlDatabase,
    prefixes: &HashMap<String, String>,
    term: &str,
) -> String {
    if term.starts_with('?') {
        term.to_string()
    } else {
        database.resolve_query_term(term, prefixes)
    }
}

pub(crate) fn normalize_triple(
    database: &SparqlDatabase,
    prefixes: &HashMap<String, String>,
    triple: &(String, String, String),
) -> (String, String, String) {
    (
        normalize_term(database, prefixes, &triple.0),
        normalize_term(database, prefixes, &triple.1),
        normalize_term(database, prefixes, &triple.2),
    )
}

/// Registers model, relation and training declarations on a store.
///
/// Reports failures to stderr. Prefer
/// [`register_neural_declarations_checked`] where the caller can handle an error.
pub fn register_neural_declarations(
    database: &mut SparqlDatabase,
    prefixes: &HashMap<String, String>,
    model_decls: &[ModelDecl],
    neural_relation_decls: &[NeuralRelationDecl],
    train_neural_relation_decls: &[TrainNeuralRelationDecl],
) {
    if register_neural_declarations_checked(
        database,
        prefixes,
        model_decls,
        neural_relation_decls,
        train_neural_relation_decls,
    )
    .is_err()
    {
        eprintln!("ML_REGISTRATION_REJECTED");
    }
}

/// Registers declarations, returning an error rather than reporting it.
///
/// Checks the ML policy for every model named, so a set containing one unapproved
/// model registers none of them.
pub fn register_neural_declarations_checked(
    database: &mut SparqlDatabase,
    prefixes: &HashMap<String, String>,
    model_decls: &[ModelDecl],
    neural_relation_decls: &[NeuralRelationDecl],
    train_neural_relation_decls: &[TrainNeuralRelationDecl],
) -> Result<(), String> {
    if !model_decls.is_empty()
        || !neural_relation_decls.is_empty()
        || !train_neural_relation_decls.is_empty()
    {
        database
            .ml_context
            .require_local()
            .map_err(|e| e.to_string())?;
    }
    for name in model_decls
        .iter()
        .map(|d| d.name.as_str())
        .chain(neural_relation_decls.iter().map(|d| d.model_name.as_str()))
    {
        crate::ml_policy::validate_model_name(name).map_err(|e| e.to_string())?;
    }
    for train in train_neural_relation_decls {
        if let Some(path) = &train.save_path {
            database
                .ml_context
                .local_artifact(path)
                .map_err(|e| e.to_string())?;
        }
    }
    for decl in model_decls {
        database.model_decls.insert(decl.name.clone(), decl.clone());
    }

    for decl in neural_relation_decls {
        let mut normalized = decl.clone();
        normalized.predicate = normalize_term(database, prefixes, &decl.predicate);
        normalized.input_patterns = decl
            .input_patterns
            .iter()
            .map(|triple| normalize_triple(database, prefixes, triple))
            .collect();
        normalized.anchor_var = normalize_term(database, prefixes, &decl.anchor_var);
        database
            .neural_relation_decls
            .insert(normalized.predicate.clone(), normalized);
    }

    for decl in train_neural_relation_decls {
        let mut normalized = decl.clone();
        normalized.predicate = normalize_term(database, prefixes, &decl.predicate);
        normalized.target_triple = normalize_triple(database, prefixes, &decl.target_triple);
        if let TrainingDataSource::GraphPattern(patterns) = &decl.data_source {
            normalized.data_source = TrainingDataSource::GraphPattern(
                patterns
                    .iter()
                    .map(|triple| normalize_triple(database, prefixes, triple))
                    .collect(),
            );
        }
        if let Some(path) = &normalized.save_path {
            if let Some(relation) = database.neural_relation_decls.get(&normalized.predicate) {
                database
                    .neural_model_artifacts
                    .insert(relation.model_name.clone(), path.clone());
            }
        }
        database
            .train_neural_relation_decls
            .insert(normalized.predicate.clone(), normalized);
    }
    Ok(())
}

#[cfg(feature = "ml")]
pub(crate) fn model_output_type(model_decl: &ModelDecl) -> OutputType {
    match &model_decl.output_kind {
        NeuralOutputKind::Exclusive { labels } => OutputType::Categorical(labels.len()),
        NeuralOutputKind::Binary { .. } => OutputType::Binary,
    }
}

pub(crate) fn model_hidden_layers(model_decl: &ModelDecl) -> &[usize] {
    match &model_decl.arch {
        ModelArch::Mlp { hidden_layers } => hidden_layers,
    }
}
