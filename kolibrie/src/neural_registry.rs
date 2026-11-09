/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Recording model, neural relation and training declarations on a store

use crate::database::KolibrieDB;
#[cfg(feature = "ml")]
use ml::OutputType;
#[cfg(feature = "ml")]
use shared::query::ModelArch;
#[cfg(feature = "ml")]
use shared::query::NeuralOutputKind;
use shared::query::{ModelDecl, NeuralRelationDecl, TrainNeuralRelationDecl, TrainingDataSource};
use std::collections::HashMap;

/// The default artifact filename for a model
pub fn default_model_artifact_path(model_name: &str) -> String {
    let sanitized: String = model_name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    format!("{}_model.bin", sanitized)
}

pub(crate) fn normalize_term(
    database: &KolibrieDB,
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
    database: &KolibrieDB,
    prefixes: &HashMap<String, String>,
    triple: &(String, String, String),
) -> (String, String, String) {
    (
        normalize_term(database, prefixes, &triple.0),
        normalize_term(database, prefixes, &triple.1),
        normalize_term(database, prefixes, &triple.2),
    )
}

/// Registers model, relation and training declarations on a store
pub fn register_neural_declarations(
    database: &mut KolibrieDB,
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

/// Registers declarations, returning an error rather than reporting it
pub fn register_neural_declarations_checked(
    database: &mut KolibrieDB,
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

#[cfg(feature = "ml")]
pub(crate) fn model_hidden_layers(model_decl: &ModelDecl) -> &[usize] {
    match &model_decl.arch {
        ModelArch::Mlp { hidden_layers } => hidden_layers,
    }
}
