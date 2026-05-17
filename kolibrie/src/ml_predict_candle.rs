/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::error::Error;

use ml::{MlpNeuralPredicate, OutputType};
use shared::query::{MLPredictClause, NeuralOutputKind, NeuralRelationDecl};

use crate::neural_relations::{model_hidden_layers, model_output_type};
use crate::sparql_database::SparqlDatabase;

type CandleResult<T> = Result<T, Box<dyn Error>>;

/// Successful Candle `ML.PREDICT` dispatch with row-aligned outputs
pub struct CandleDispatch {
    pub predictions: Vec<String>,
    pub probabilities: Vec<f64>,
    pub output_kind: NeuralOutputKind,
    pub normalized_predicate: String,
    pub relation: NeuralRelationDecl,
}

/// Try Candle for an `ML.PREDICT` clause and resolve the target predicate
pub fn try_candle_predict(
    database: &mut SparqlDatabase,
    ml_predict: &MLPredictClause<'_>,
    conclusion: &[(&str, &str, &str)],
    rule_prefixes: &HashMap<String, String>,
    input_rows: &[HashMap<String, u32>],
) -> CandleResult<Option<CandleDispatch>> {
    let output_var = ml_predict.output.trim_start_matches('?');

    // Find the ML output predicate from the conclusion template
    let predicate_raw = match find_object_position_predicate(conclusion, output_var) {
        Some(p) => p,
        None => return Ok(None),
    };
    let normalized_predicate = database.resolve_query_term(predicate_raw, rule_prefixes);

    // Use Python fallback when the relation is not registered
    let relation = match database.neural_relation_decls.get(&normalized_predicate) {
        Some(r) => r.clone(),
        None => return Ok(None),
    };

    // Require a MODEL declaration
    let model_decl = database
        .model_decls
        .get(&relation.model_name)
        .cloned()
        .ok_or_else(|| {
            format!(
                "NEURAL RELATION <{}> is registered for model '{}' but no MODEL declaration exists",
                normalized_predicate, relation.model_name
            )
        })?;

    // Check that ML.PREDICT names the registered model
    if relation.model_name != ml_predict.model {
        return Err(format!(
            "ML.PREDICT names model \"{}\" but NEURAL RELATION for <{}> uses model \"{}\"",
            ml_predict.model, normalized_predicate, relation.model_name
        )
        .into());
    }

    // Require a trained artifact
    let artifact_path = database
        .neural_model_artifacts
        .get(&relation.model_name)
        .cloned()
        .ok_or_else(|| {
            format!(
                "Model '{}' is registered but has not been trained yet (no artifact in neural_model_artifacts)",
                relation.model_name
            )
        })?;

    // Build features from the registered feature vars
    let features = build_features_for_rows(database, &relation, input_rows)?;

    // Load the model and run inference
    let output_type: OutputType = model_output_type(&model_decl);
    let hidden = model_hidden_layers(&model_decl);
    let model = MlpNeuralPredicate::load(
        relation.feature_vars.len(),
        hidden,
        output_type,
        &artifact_path,
    )?;

    let probs = if features.is_empty() {
        Vec::new()
    } else {
        let (_tracked, probs) = model.forward_with_grads(&features)?;
        probs
    };

    // Map probabilities to labels
    let (predictions, probabilities) = map_probs_to_labels(&probs, &model_decl.output_kind);

    Ok(Some(CandleDispatch {
        predictions,
        probabilities,
        output_kind: model_decl.output_kind.clone(),
        normalized_predicate,
        relation,
    }))
}

/// Resolve by model name for the physical operator runtime path
///
/// Requires exactly one relation for the model, otherwise callers fall back to Python
pub fn try_candle_predict_by_model_name(
    database: &mut SparqlDatabase,
    model_name: &str,
    input_rows: &[HashMap<String, u32>],
) -> CandleResult<Option<CandleDispatch>> {
    // Find the relation bound to this model name
    let matching: Vec<&NeuralRelationDecl> = database
        .neural_relation_decls
        .values()
        .filter(|r| r.model_name == model_name)
        .collect();
    let relation = match matching.len() {
        1 => matching[0].clone(),
        _ => return Ok(None),
    };
    let normalized_predicate = relation.predicate.clone();

    let model_decl = match database.model_decls.get(&relation.model_name).cloned() {
        Some(m) => m,
        None => return Ok(None),
    };
    let artifact_path = match database.neural_model_artifacts.get(&relation.model_name).cloned() {
        Some(p) => p,
        None => return Ok(None),
    };

    let features = build_features_for_rows(database, &relation, input_rows)?;
    let output_type: OutputType = model_output_type(&model_decl);
    let hidden = model_hidden_layers(&model_decl);
    let model = MlpNeuralPredicate::load(
        relation.feature_vars.len(),
        hidden,
        output_type,
        &artifact_path,
    )?;

    let probs = if features.is_empty() {
        Vec::new()
    } else {
        let (_tracked, probs) = model.forward_with_grads(&features)?;
        probs
    };
    let (predictions, probabilities) = map_probs_to_labels(&probs, &model_decl.output_kind);

    Ok(Some(CandleDispatch {
        predictions,
        probabilities,
        output_kind: model_decl.output_kind.clone(),
        normalized_predicate,
        relation,
    }))
}

fn find_object_position_predicate<'a>(
    conclusion: &[(&'a str, &'a str, &'a str)],
    output_var: &str,
) -> Option<&'a str> {
    for (_, predicate, object) in conclusion {
        let obj_stripped = object.trim_start_matches('?');
        if obj_stripped == output_var {
            return Some(predicate);
        }
    }
    None
}

fn build_features_for_rows(
    database: &SparqlDatabase,
    relation: &NeuralRelationDecl,
    input_rows: &[HashMap<String, u32>],
) -> CandleResult<Vec<Vec<f64>>> {
    let dict = database.dictionary.read().unwrap();
    let mut features: Vec<Vec<f64>> = Vec::with_capacity(input_rows.len());
    for (row_idx, row) in input_rows.iter().enumerate() {
        let mut feature_vec: Vec<f64> = Vec::with_capacity(relation.feature_vars.len());
        for var in &relation.feature_vars {
            let key = var.trim_start_matches('?');
            let id = row
                .get(key)
                .or_else(|| row.get(var.as_str()))
                .copied()
                .ok_or_else(|| {
                    format!(
                        "Row {}: feature variable {} not present in INPUT query result",
                        row_idx, var
                    )
                })?;
            let decoded = dict.decode(id).ok_or_else(|| {
                format!(
                    "Row {}: dictionary has no entry for id {} bound to feature var {}",
                    row_idx, id, var
                )
            })?;
            let term = decoded.to_string();
            let value = crate::ml_feature_loader::rdf_term_to_f64(&term).map_err(|err| {
                format!(
                    "Row {}: feature var {} value {:?} is not numeric: {}",
                    row_idx, var, term, err
                )
            })?;
            feature_vec.push(value);
        }
        features.push(feature_vec);
    }
    drop(dict);
    Ok(features)
}

fn map_probs_to_labels(
    probs: &[Vec<f64>],
    output_kind: &NeuralOutputKind,
) -> (Vec<String>, Vec<f64>) {
    let mut predictions = Vec::with_capacity(probs.len());
    let mut probabilities = Vec::with_capacity(probs.len());
    match output_kind {
        NeuralOutputKind::Exclusive { labels } => {
            for row in probs {
                let (argmax_idx, argmax_prob) = row
                    .iter()
                    .enumerate()
                    .fold((0usize, f64::NEG_INFINITY), |acc, (idx, p)| {
                        if *p > acc.1 { (idx, *p) } else { acc }
                    });
                let label = labels
                    .get(argmax_idx)
                    .cloned()
                    .unwrap_or_else(|| argmax_idx.to_string());
                predictions.push(label);
                probabilities.push(argmax_prob);
            }
        }
        NeuralOutputKind::Binary { positive_literal } => {
            for row in probs {
                predictions.push(positive_literal.clone());
                probabilities.push(row.first().copied().unwrap_or(0.0));
            }
        }
    }
    (predictions, probabilities)
}
