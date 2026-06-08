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

use crate::ml_feature_loader::query_training_rows;
use shared::query::{CombinedRule, MLPredictClause};
use shared::triple::Triple;

use crate::ml_predict_candle::try_candle_predict;
use crate::sparql_database::SparqlDatabase;

type MlResult<T> = Result<T, Box<dyn Error>>;

/// Predicted row with original INPUT bindings and decoded output
pub struct PredictedRow {
    pub bindings: HashMap<String, u32>,
    pub prediction_literal: String,
    pub probability: Option<f64>,
}

pub(crate) struct MlConclusionMeta {
    pub normalized_predicate: String,
    pub cache_key: String,
    /// Indices of conclusion triples that reference the ML output var
    pub ml_conclusion_indices: Vec<usize>,
}

/// Resolve metadata for `ML.PREDICT` conclusion handling
///
/// Checks output position, single-predicate use, and referenced output vars
pub(crate) fn resolve_ml_conclusion_metadata(
    rule: &CombinedRule<'_>,
    ml_output_var: &str,
    rule_prefixes: &HashMap<String, String>,
    database: &SparqlDatabase,
) -> MlResult<MlConclusionMeta> {
    let out_stripped = ml_output_var.trim_start_matches('?');
    let mut ml_indices: Vec<usize> = Vec::new();
    let mut normalized_predicate: Option<String> = None;
    let mut bad_position: Option<String> = None;

    for (idx, (s, p, o)) in rule.conclusion.iter().enumerate() {
        let s_stripped = s.trim_start_matches('?');
        let p_stripped = p.trim_start_matches('?');
        let o_stripped = o.trim_start_matches('?');
        let in_subject = s.starts_with('?') && s_stripped == out_stripped;
        let in_predicate = p.starts_with('?') && p_stripped == out_stripped;
        let in_object = o.starts_with('?') && o_stripped == out_stripped;

        if in_subject || in_predicate {
            bad_position = Some(format!("({}, {}, {})", s, p, o));
            continue;
        }
        if in_object {
            ml_indices.push(idx);
            let normalized = database.resolve_query_term(p, rule_prefixes);
            match &normalized_predicate {
                None => normalized_predicate = Some(normalized),
                Some(existing) if existing == &normalized => {}
                Some(existing) => {
                    return Err(format!(
                        "ML.PREDICT output variable {} used across multiple conclusion predicates: {} and {} — not supported",
                        ml_output_var, existing, normalized
                    )
                    .into());
                }
            }
        }
    }

    if ml_indices.is_empty() {
        if let Some(bad) = bad_position {
            return Err(format!(
                "ML.PREDICT output variable {} must appear in object position of a conclusion triple in v1; found only in subject/predicate position of {}",
                ml_output_var, bad
            )
            .into());
        }
        return Err(format!(
            "ML.PREDICT OUTPUT {} is not referenced by any conclusion triple",
            ml_output_var
        )
        .into());
    }

    let normalized_predicate = normalized_predicate.unwrap();
    let cache_key = format!(
        "{}::{}::{}",
        rule.head.predicate, normalized_predicate, out_stripped
    );

    Ok(MlConclusionMeta {
        normalized_predicate,
        cache_key,
        ml_conclusion_indices: ml_indices,
    })
}

/// Execute `ML.PREDICT` and materialize its conclusion triples
pub fn execute_ml_predict_clause<'a>(
    ml_predict: &MLPredictClause<'_>,
    rule: &mut CombinedRule<'a>,
    database: &mut SparqlDatabase,
    rule_prefixes: &HashMap<String, String>,
) -> MlResult<Vec<Triple>> {
    let out_var = ml_predict.output;
    let meta = resolve_ml_conclusion_metadata(rule, out_var, rule_prefixes, database)?;

    // Run the INPUT query with native ids
    let input_rows = run_input_query(ml_predict.input_raw, database, rule_prefixes)?;

    // Clean up and strip ML templates even with no input
    if input_rows.is_empty() {
        purge_previous_materialization(database, &meta.cache_key);
        strip_ml_conclusions(rule, out_var);
        database
            .ml_predict_materialized_triples
            .insert(meta.cache_key, Vec::new());
        return Ok(Vec::new());
    }

    // Try Candle first
    let candle = try_candle_predict(
        database,
        ml_predict,
        &rule.conclusion,
        rule_prefixes,
        &input_rows,
    )?;

    let (predictions, probabilities, emit_prob_var) = match candle {
        Some(dispatch) => {
            let emit_prob = matches!(
                dispatch.output_kind,
                shared::query::NeuralOutputKind::Binary { .. }
            );
            (dispatch.predictions, dispatch.probabilities, emit_prob)
        }
        None => {
            // Fall back to Python
            let preds = run_python_ml_dispatch(
                database,
                ml_predict.model,
                &input_rows,
                &ml_predict.input_select,
            )?;
            (preds, Vec::new(), false)
        }
    };

    if predictions.len() != input_rows.len() {
        return Err(format!(
            "ML dispatch returned {} predictions for {} input rows (positional mismatch)",
            predictions.len(),
            input_rows.len()
        )
        .into());
    }

    let predicted_rows: Vec<PredictedRow> = input_rows
        .into_iter()
        .enumerate()
        .map(|(idx, bindings)| PredictedRow {
            bindings,
            prediction_literal: predictions[idx].clone(),
            probability: if emit_prob_var {
                probabilities.get(idx).copied()
            } else {
                None
            },
        })
        .collect();

    let prob_var_owned = if emit_prob_var {
        Some(format!("{}_prob", out_var.trim_start_matches('?')))
    } else {
        None
    };

    // Materialize shared ML conclusions
    let inserted = materialize_ml_conclusions(
        database,
        out_var,
        prob_var_owned.as_deref(),
        &mut rule.conclusion,
        rule_prefixes,
        &predicted_rows,
        &meta.cache_key,
        &meta.normalized_predicate,
        &meta.ml_conclusion_indices,
    )?;

    Ok(inserted)
}

/// Run the INPUT query through the optimizer and return id-encoded rows
fn run_input_query(
    input_raw: &str,
    database: &mut SparqlDatabase,
    rule_prefixes: &HashMap<String, String>,
) -> MlResult<Vec<HashMap<String, u32>>> {
    let mut query_with_prefixes = String::new();
    for (prefix, uri) in rule_prefixes {
        let prefix_decl = format!("PREFIX {}:", prefix);
        if !input_raw.contains(&prefix_decl) {
            query_with_prefixes.push_str(&format!("PREFIX {}: <{}>\n", prefix, uri));
        }
    }
    query_with_prefixes.push_str(input_raw);

    let rows = query_training_rows(database, &query_with_prefixes)
        .map_err(|err| format!("failed to execute ML.PREDICT INPUT query: {err}"))?;

    Ok(rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(var, term)| {
                    let id = database.encode_term_star(&term);
                    (var, id)
                })
                .collect::<HashMap<String, u32>>()
        })
        .collect())
}

/// Remove old materialized triples for this cache key
fn purge_previous_materialization(database: &mut SparqlDatabase, cache_key: &str) {
    if let Some(old) = database.ml_predict_materialized_triples.remove(cache_key) {
        for triple in old {
            database.delete_triple(&triple);
        }
    }
}

/// Strip ML conclusion triples from the rule
fn strip_ml_conclusions<'a>(rule: &mut CombinedRule<'a>, ml_output_var: &str) {
    let out_stripped = ml_output_var.trim_start_matches('?');
    rule.conclusion.retain(|(s, p, o)| {
        let matches = |slot: &str| slot.starts_with('?') && slot.trim_start_matches('?') == out_stripped;
        !(matches(s) || matches(p) || matches(o))
    });
}

/// Materialize predictions and strip consumed conclusion templates
#[allow(clippy::too_many_arguments)]
pub fn materialize_ml_conclusions<'a>(
    database: &mut SparqlDatabase,
    ml_output_var: &str,
    prob_var: Option<&str>,
    conclusion: &mut Vec<(&'a str, &'a str, &'a str)>,
    rule_prefixes: &HashMap<String, String>,
    predicted_rows: &[PredictedRow],
    cache_key: &str,
    normalized_predicate: &str,
    ml_conclusion_indices: &[usize],
) -> MlResult<Vec<Triple>> {
    let out_stripped = ml_output_var.trim_start_matches('?');

    // Check all non-ML variables in ML templates exist in row bindings
    if let Some(first_row) = predicted_rows.first() {
        let row_keys = &first_row.bindings;
        for &idx in ml_conclusion_indices {
            let (s, p, o) = conclusion[idx];
            for slot in [s, p, o] {
                if !slot.starts_with('?') {
                    continue;
                }
                let name = slot.trim_start_matches('?');
                if name == out_stripped {
                    continue;
                }
                if !row_keys.contains_key(name) {
                    return Err(format!(
                        "Variable {} in ML conclusion not bound by INPUT query — add {} to INPUT SELECT",
                        slot, slot
                    )
                    .into());
                }
            }
        }
    }

    // Remove stale materialization for this cache key
    purge_previous_materialization(database, cache_key);

    // Copy ML templates before mutating the database
    let ml_templates: Vec<(String, String, String)> = ml_conclusion_indices
        .iter()
        .map(|&idx| {
            let (s, p, o) = conclusion[idx];
            (s.to_string(), p.to_string(), o.to_string())
        })
        .collect();

    let mut inserted: Vec<Triple> = Vec::with_capacity(predicted_rows.len() * ml_templates.len());

    for row in predicted_rows {
        for (s_tmpl, p_tmpl, o_tmpl) in &ml_templates {
            let subject = substitute_slot(s_tmpl, out_stripped, row, database, rule_prefixes)?;
            let predicate = substitute_slot(p_tmpl, out_stripped, row, database, rule_prefixes)?;
            let object = substitute_slot(o_tmpl, out_stripped, row, database, rule_prefixes)?;

            let triple = encode_triple(database, &subject, &predicate, &object);
            database.add_triple(triple.clone());
            inserted.push(triple);
        }

        // Add the companion probability triple when requested
        if let Some(prob_var_name) = prob_var {
            let prob_value = row.probability.unwrap_or(0.0);
            let companion_predicate = format!("{}_prob", normalized_predicate);
            // Use the first ML conclusion subject as the probability anchor
            if let Some((s_tmpl, _, _)) = ml_templates.first() {
                let subject = substitute_slot(s_tmpl, out_stripped, row, database, rule_prefixes)?;
                let triple = encode_triple(
                    database,
                    &subject,
                    &companion_predicate,
                    &prob_value.to_string(),
                );
                database.add_triple(triple.clone());
                inserted.push(triple);
            }
            let _ = prob_var_name; // name is derived; kept arg for API clarity
        }
    }

    // Track triples for rerun cleanup
    database
        .ml_predict_materialized_triples
        .insert(cache_key.to_string(), inserted.clone());

    // Strip ML templates from the rule
    conclusion.retain(|(s, p, o)| {
        let matches = |slot: &str| slot.starts_with('?') && slot.trim_start_matches('?') == out_stripped;
        !(matches(s) || matches(p) || matches(o))
    });

    Ok(inserted)
}

fn substitute_slot(
    slot: &str,
    ml_output_var_stripped: &str,
    row: &PredictedRow,
    database: &SparqlDatabase,
    rule_prefixes: &HashMap<String, String>,
) -> MlResult<String> {
    if slot.starts_with('?') {
        let name = slot.trim_start_matches('?');
        if name == ml_output_var_stripped {
            return Ok(row.prediction_literal.clone());
        }
        let id = row
            .bindings
            .get(name)
            .copied()
            .ok_or_else(|| format!("Variable {} not bound in INPUT row", slot))?;
        let dict = database.dictionary.read().unwrap();
        let value = dict
            .decode(id)
            .ok_or_else(|| format!("Dictionary has no entry for id {} ({})", id, slot))?
            .to_string();
        Ok(value)
    } else {
        Ok(database.resolve_query_term(slot, rule_prefixes))
    }
}

fn encode_triple(database: &mut SparqlDatabase, subject: &str, predicate: &str, object: &str) -> Triple {
    Triple {
        subject: database.encode_term_star(subject),
        predicate: database.encode_term_star(predicate),
        object: database.encode_term_star(object),
    }
}

/// Run the existing Python MLHandler path and return decoded predictions
pub(crate) fn run_python_ml_dispatch(
    database: &SparqlDatabase,
    model: &str,
    input_rows: &[HashMap<String, u32>],
    input_select: &[(&str, &str, Option<&str>)],
) -> MlResult<Vec<String>> {
    use ml::generate_ml_models;
    use ml::MLHandler;

    if input_rows.is_empty() {
        return Ok(Vec::new());
    }

    let input_variables: Vec<String> = input_select
        .iter()
        .filter(|(var, _, _)| var.starts_with('?'))
        .map(|(var, _, _)| (*var).to_string())
        .collect();

    let feature_matrix = filter_numeric_features(input_rows, &input_variables, database);

    let model_dir = {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let ml_dir = path.join("ml");
            if ml_dir.exists() && ml_dir.is_dir() {
                break ml_dir.join("examples").join("models");
            }
            if !path.pop() {
                break std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("models");
            }
        }
    };
    std::fs::create_dir_all(&model_dir)?;

    let models_exist = std::fs::read_dir(&model_dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let path = entry.path();
            path.is_file()
                && path.extension().map_or(false, |ext| ext == "pkl")
                && path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map_or(false, |stem| stem.ends_with("_predictor"))
        })
        .count()
        >= 1;

    if !models_exist {
        let script_name = format!("{}.py", model);
        let predictor_script = model_dir
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join(&script_name))
            .unwrap_or_else(|| std::path::PathBuf::from(&script_name));
        if let Some(script_path) = predictor_script.to_str() {
            generate_ml_models(&model_dir, script_path)?;
        }
    }

    let mut ml_handler = MLHandler::new()?;
    let model_ids = ml_handler.discover_and_load_models(&model_dir, model)?;
    if model_ids.is_empty() {
        return Err("No valid models found with TTL schemas".into());
    }
    let best_model_name = ml_handler.best_model.clone().unwrap_or_else(|| model_ids[0].clone());

    let result = ml_handler.predict(&best_model_name, feature_matrix)?;
    Ok(result.predictions.iter().map(f64::to_string).collect())
}

/// Pick numeric INPUT variables and return rows aligned with `input_rows`
pub(crate) fn filter_numeric_features(
    input_rows: &[HashMap<String, u32>],
    input_variables: &[String],
    database: &SparqlDatabase,
) -> Vec<Vec<f64>> {
    let Some(first_row) = input_rows.first() else {
        return Vec::new();
    };

    let dict = database.dictionary.read().unwrap();
    let numeric_vars: Vec<String> = input_variables
        .iter()
        .filter(|var| {
            let stripped = var.strip_prefix('?').unwrap_or(var);
            first_row
                .get(stripped)
                .and_then(|&id| dict.decode(id))
                .map(|s| s.parse::<f64>().is_ok())
                .unwrap_or(false)
        })
        .cloned()
        .collect();

    let result: Vec<Vec<f64>> = input_rows
        .iter()
        .map(|row| {
            numeric_vars
                .iter()
                .filter_map(|var| {
                    let stripped = var.strip_prefix('?').unwrap_or(var);
                    row.get(stripped)
                        .and_then(|&id| dict.decode(id))
                        .and_then(|s| s.parse::<f64>().ok())
                })
                .collect()
        })
        .collect();
    drop(dict);
    result
}
