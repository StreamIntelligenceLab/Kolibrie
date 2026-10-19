/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Predicates whose facts come from a model rather than from data.
//!
//! A neural relation declares that querying some predicate should run a model over
//! the rows a pattern selects. Declaring one is not authority to run it — every entry
//! point here checks the [`MlExecutionContext`](crate::ml_policy::MlExecutionContext)
//! first, and an unapproved model is refused before anything executes.
//!
//! # Argmax here, distributions in `program`
//!
//! These entry points bind the single most likely label. A full distribution needs
//! the probabilistic machinery in [`program`](crate::program), which can represent
//! competing labels as a mutually exclusive group; see
//! [`DISTRIBUTION_REQUIRES_PROGRAM`].

use std::collections::HashMap;
use std::error::Error;

use ml::MlpNeuralPredicate;
use shared::query::{
    MLPredictClause, ModelDecl, NeuralOutputKind, NeuralRelationDecl, TrainNeuralRelationDecl,
    TrainingDataSource,
};
use shared::triple::Triple;

use crate::execute_ml_train::{
    build_ground_reasoner_from_db, execute_ml_training_owned, OwnedNeuralCallSpec,
    OwnedNeuralChoice, OwnedNeuralGroupType, OwnedNeuralTrainingClause,
};
use crate::execute_query::query_training_rows;
use crate::ml_feature_loader::build_feature_vec;
use crate::parser::parse_combined_query;
// Re-exported, not redefined: the implementation lives one layer down in
// `neural_registry`, and `kolibrie::neural_relations::register_neural_declarations`
// stays a working path for callers that already use it.
pub use crate::neural_registry::register_neural_declarations;

use crate::neural_registry::{default_model_artifact_path, model_hidden_layers, model_output_type};
use crate::sparql_database::SparqlDatabase;

type NeuralResult<T> = Result<T, Box<dyn Error>>;

/// Message explaining that `OUTPUT ?v DISTRIBUTION` is only supported through
/// [`program`](crate::program), because representing competing labels as mutually
/// exclusive needs the probabilistic machinery these entry points do not have.
pub const DISTRIBUTION_REQUIRES_PROGRAM: &str =
    "ML.PREDICT ... OUTPUT ?v DISTRIBUTION runs only through kolibrie::program; legacy entry points predict the argmax";

fn push_unique(vars: &mut Vec<String>, value: String) {
    if !vars.iter().any(|existing| existing == &value) {
        vars.push(value);
    }
}

fn build_select_query(patterns: &[(String, String, String)], vars: &[String]) -> String {
    let select_vars = vars.join(" ");

    let format_term = |term: &str| {
        if term.starts_with('?')
            || term.starts_with('<')
            || term.starts_with('"')
            || term.starts_with("<<")
            || (term.contains(':') && !term.starts_with("http://") && !term.starts_with("https://"))
        {
            term.to_string()
        } else if term.starts_with("http://") || term.starts_with("https://") {
            format!("<{}>", term)
        } else {
            term.to_string()
        }
    };

    let where_body = patterns
        .iter()
        .map(|(s, p, o)| format!("{} {} {} .", format_term(s), format_term(p), format_term(o)))
        .collect::<Vec<_>>()
        .join("\n    ");
    format!("SELECT {} WHERE {{\n    {}\n}}", select_vars, where_body)
}

fn resolve_model_components(
    database: &SparqlDatabase,
    predicate: &str,
) -> Result<(NeuralRelationDecl, ModelDecl), String> {
    let relation = database
        .neural_relation_decls
        .get(predicate)
        .cloned()
        .ok_or_else(|| format!("No NEURAL RELATION registered for predicate {}", predicate))?;
    let model = database
        .model_decls
        .get(&relation.model_name)
        .cloned()
        .ok_or_else(|| {
            format!(
                "No MODEL declaration registered for {}",
                relation.model_name
            )
        })?;
    Ok((relation, model))
}

/// Resolves a parsed training declaration into owned, prefix-expanded form.
pub fn lower_train_decl_to_owned(
    database: &SparqlDatabase,
    train_decl: &TrainNeuralRelationDecl,
) -> Result<OwnedNeuralTrainingClause, String> {
    let (relation, model) = resolve_model_components(database, &train_decl.predicate)?;

    let training_query = match &train_decl.data_source {
        TrainingDataSource::Query(query) => query.clone(),
        TrainingDataSource::GraphPattern(patterns) => {
            let mut vars = Vec::new();
            push_unique(&mut vars, relation.anchor_var.clone());
            for feature in &relation.feature_vars {
                push_unique(&mut vars, feature.clone());
            }
            push_unique(&mut vars, train_decl.label_var.clone());
            for term in [
                &train_decl.target_triple.0,
                &train_decl.target_triple.1,
                &train_decl.target_triple.2,
            ] {
                if term.starts_with('?') {
                    push_unique(&mut vars, term.clone());
                }
            }

            let mut query_patterns = relation.input_patterns.clone();
            query_patterns.extend(patterns.clone());
            build_select_query(&query_patterns, &vars)
        }
    };

    let neural_call = match &model.output_kind {
        NeuralOutputKind::Exclusive { labels } => OwnedNeuralCallSpec {
            feature_vars: relation.feature_vars.clone(),
            group_type: OwnedNeuralGroupType::Exclusive {
                choices: labels
                    .iter()
                    .enumerate()
                    .map(|(idx, label)| OwnedNeuralChoice {
                        triple_template: (
                            relation.anchor_var.clone(),
                            relation.predicate.clone(),
                            label.clone(),
                        ),
                        prob_var: format!("?p{}", idx),
                    })
                    .collect(),
            },
        },
        NeuralOutputKind::Binary { positive_literal } => OwnedNeuralCallSpec {
            feature_vars: relation.feature_vars.clone(),
            group_type: OwnedNeuralGroupType::Independent {
                fact_template: (
                    relation.anchor_var.clone(),
                    relation.predicate.clone(),
                    positive_literal.clone(),
                ),
                prob_var: "?p0".to_string(),
            },
        },
    };

    let save_path = train_decl
        .save_path
        .clone()
        .or_else(|| database.neural_model_artifacts.get(&model.name).cloned())
        .or_else(|| Some(default_model_artifact_path(&model.name)));

    Ok(OwnedNeuralTrainingClause {
        model_name: model.name,
        neural_calls: vec![neural_call],
        training_data_raw: training_query,
        label_var: train_decl.label_var.clone(),
        target_triple: train_decl.target_triple.clone(),
        loss: train_decl.loss,
        optimizer: train_decl.optimizer,
        learning_rate: train_decl.learning_rate,
        epochs: train_decl.epochs,
        batch_size: train_decl.batch_size,
        save_path,
    })
}

/// Trains the model behind a neural relation and writes its artifact.
///
/// Requires trusted-local authority: the artifact path is resolved through the ML
/// context, so a declaration naming a path cannot write outside the directory the
/// host granted.
pub fn execute_train_decl(
    database: &mut SparqlDatabase,
    train_decl: &TrainNeuralRelationDecl,
) -> NeuralResult<()> {
    database.ml_context.require_local()?;
    let owned_clause = lower_train_decl_to_owned(database, train_decl)?;
    let base_reasoner = build_ground_reasoner_from_db(database, None);
    execute_ml_training_owned(&owned_clause, &base_reasoner, database)?;

    if let Some(relation) = database.neural_relation_decls.get(&train_decl.predicate) {
        if let Some(save_path) = &owned_clause.save_path {
            database
                .neural_model_artifacts
                .insert(relation.model_name.clone(), save_path.clone());
        }
    }
    database
        .train_neural_relation_decls
        .insert(train_decl.predicate.clone(), train_decl.clone());
    Ok(())
}

fn run_ml_predict_input_query(
    database: &mut SparqlDatabase,
    input_raw: &str,
    prefixes: &HashMap<String, String>,
) -> NeuralResult<Vec<HashMap<String, String>>> {
    let mut query_with_prefixes = String::new();
    for (prefix, uri) in prefixes {
        let prefix_decl = format!("PREFIX {}:", prefix);
        if !input_raw.contains(&prefix_decl) {
            query_with_prefixes.push_str(&format!("PREFIX {}: <{}>\n", prefix, uri));
        }
    }
    query_with_prefixes.push_str(input_raw);

    query_training_rows(database, &query_with_prefixes)
}

fn encode_prediction_rows(
    database: &mut SparqlDatabase,
    rows: &[HashMap<String, String>],
) -> Vec<HashMap<String, u32>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|(var, term)| (var.clone(), database.encode_term_star(term)))
                .collect::<HashMap<String, u32>>()
        })
        .collect()
}

fn resolve_unique_relation_for_model(
    database: &SparqlDatabase,
    model_name: &str,
) -> NeuralResult<NeuralRelationDecl> {
    let matching: Vec<NeuralRelationDecl> = database
        .neural_relation_decls
        .values()
        .filter(|relation| relation.model_name == model_name)
        .cloned()
        .collect();

    match matching.len() {
        1 => Ok(matching[0].clone()),
        0 => Err(format!(
            "Top-level ML.PREDICT MODEL \"{}\" does not match any registered NEURAL RELATION",
            model_name
        )
        .into()),
        count => Err(format!(
            "Top-level ML.PREDICT MODEL \"{}\" matches {} NEURAL RELATION declarations; use an unambiguous model-to-relation mapping",
            model_name, count
        )
        .into()),
    }
}

/// Runs an `ML.PREDICT` that appears at the top level of a request rather than
/// attached to a rule.
pub fn execute_top_level_ml_predict(
    database: &mut SparqlDatabase,
    ml_predict: &MLPredictClause<'_>,
    prefixes: &HashMap<String, String>,
) -> NeuralResult<()> {
    database.ml_context.require_local()?;
    crate::ml_policy::validate_model_name(ml_predict.model)?;
    if ml_predict.distribution {
        return Err(DISTRIBUTION_REQUIRES_PROGRAM.into());
    }
    let relation = resolve_unique_relation_for_model(database, ml_predict.model)?;
    let rows = run_ml_predict_input_query(database, ml_predict.input_raw, prefixes)?;

    remove_materialized_triples(database, &relation.predicate);
    if rows.is_empty() {
        return Ok(());
    }

    let encoded_rows = encode_prediction_rows(database, &rows);
    let dispatch = crate::ml_predict_candle::try_candle_predict_by_model_name(
        database,
        ml_predict.model,
        &encoded_rows,
    )?
    .ok_or_else(|| {
        format!(
            "Top-level ML.PREDICT MODEL \"{}\" could not be dispatched to a trained NEURAL RELATION",
            ml_predict.model
        )
    })?;

    let anchor_key = relation.anchor_var.trim_start_matches('?');
    let mut generated = Vec::new();
    for (row, prediction) in rows.iter().zip(dispatch.predictions.iter()) {
        let anchor = row
            .get(anchor_key)
            .or_else(|| row.get(&relation.anchor_var))
            .ok_or_else(|| format!("Missing anchor variable {}", relation.anchor_var))?;
        let triple = Triple {
            subject: database.encode_term_star(anchor),
            predicate: database.encode_term_star(&relation.predicate),
            object: database.encode_term_star(prediction),
        };
        database.add_triple(triple.clone());
        generated.push(triple);
    }

    database
        .neural_materialized_triples
        .insert(relation.predicate.clone(), generated);
    Ok(())
}

/// Parses and runs a program's neural declarations against a store.
pub fn execute_neural_program(
    database: &mut SparqlDatabase,
    program: &str,
) -> Result<(), crate::execute_query::QueryError> {
    crate::execute_query::validate_query_policy(program, database)?;
    database.register_prefixes_from_query(program);

    let (_rest, combined) = parse_combined_query(program).map_err(|_| {
        crate::execute_query::QueryError::Parse("Failed to parse neural program".to_string())
    })?;

    if !combined.rules.is_empty() {
        return Err(crate::execute_query::QueryError::Unsupported(
            "execute_neural_program only accepts MODEL / NEURAL RELATION / TRAIN NEURAL RELATION declarations and top-level ML.PREDICT"
                .to_string(),
        ));
    }
    if combined
        .ml_predict
        .as_ref()
        .is_some_and(|predict| predict.distribution)
    {
        return Err(crate::execute_query::QueryError::Unsupported(
            DISTRIBUTION_REQUIRES_PROGRAM.to_string(),
        ));
    }

    for (prefix, uri) in &combined.prefixes {
        database.prefixes.insert(prefix.clone(), uri.clone());
    }

    let mut prefixes = combined.prefixes.clone();
    database.share_prefixes_with(&mut prefixes);

    register_neural_declarations(
        database,
        &prefixes,
        &combined.model_decls,
        &combined.neural_relation_decls,
        &combined.train_neural_relation_decls,
    );

    let normalized_trains: Vec<TrainNeuralRelationDecl> = combined
        .train_neural_relation_decls
        .iter()
        .filter_map(|decl| {
            let predicate = database.resolve_query_term(&decl.predicate, &prefixes);
            database
                .train_neural_relation_decls
                .get(&predicate)
                .cloned()
        })
        .collect();

    for train_decl in &normalized_trains {
        execute_train_decl(database, train_decl)
            .map_err(|err| crate::execute_query::QueryError::Execution(err.to_string()))?;
        materialize_neural_relation(database, &train_decl.predicate)
            .map_err(|err| crate::execute_query::QueryError::Execution(err.to_string()))?;
    }

    if let Some(ml_predict) = &combined.ml_predict {
        execute_top_level_ml_predict(database, ml_predict, &prefixes)
            .map_err(|err| crate::execute_query::QueryError::Execution(err.to_string()))?;
    }

    Ok(())
}

fn remove_materialized_triples(database: &mut SparqlDatabase, predicate: &str) {
    if let Some(old_triples) = database.neural_materialized_triples.remove(predicate) {
        for triple in old_triples {
            database.delete_triple(&triple);
        }
    }
}

/// Runs one neural relation's model and adds the predicted facts to the store.
pub fn materialize_neural_relation(
    database: &mut SparqlDatabase,
    predicate: &str,
) -> NeuralResult<()> {
    database.ml_context.require_local()?;
    let (relation, model_decl) = resolve_model_components(database, predicate)?;
    let artifact_path = database
        .neural_model_artifacts
        .get(&model_decl.name)
        .cloned()
        .ok_or_else(|| {
            format!(
                "No trained artifact available for MODEL {}",
                model_decl.name
            )
        })?;

    let mut vars = Vec::new();
    push_unique(&mut vars, relation.anchor_var.clone());
    for feature in &relation.feature_vars {
        push_unique(&mut vars, feature.clone());
    }
    let select_query = build_select_query(&relation.input_patterns, &vars);
    let rows = query_training_rows(database, &select_query)?;
    if rows.is_empty() {
        remove_materialized_triples(database, predicate);
        return Ok(());
    }

    let feature_refs: Vec<&str> = relation.feature_vars.iter().map(String::as_str).collect();
    let features = rows
        .iter()
        .map(|row| build_feature_vec(row, &feature_refs))
        .collect::<Result<Vec<_>, _>>()?;
    let model = MlpNeuralPredicate::load(
        relation.feature_vars.len(),
        model_hidden_layers(&model_decl),
        model_output_type(&model_decl),
        database
            .ml_context
            .local_artifact(&artifact_path)?
            .to_str()
            .ok_or("invalid artifact path")?,
    )?;
    let (_tracked, probs) = model.forward_with_grads(&features)?;

    remove_materialized_triples(database, predicate);
    let mut generated = Vec::new();

    match &model_decl.output_kind {
        NeuralOutputKind::Exclusive { labels } => {
            for (row, row_probs) in rows.iter().zip(probs.iter()) {
                let anchor = row
                    .get(relation.anchor_var.trim_start_matches('?'))
                    .or_else(|| row.get(&relation.anchor_var))
                    .ok_or_else(|| format!("Missing anchor variable {}", relation.anchor_var))?;
                let best_idx = row_probs
                    .iter()
                    .enumerate()
                    .max_by(|(_, left), (_, right)| left.partial_cmp(right).unwrap())
                    .map(|(idx, _)| idx)
                    .unwrap_or(0);
                let triple = Triple {
                    subject: database.encode_term_star(anchor),
                    predicate: database.encode_term_star(&relation.predicate),
                    object: database.encode_term_star(&labels[best_idx]),
                };
                database.add_triple(triple.clone());
                generated.push(triple);
            }
        }
        NeuralOutputKind::Binary { positive_literal } => {
            for (row, row_probs) in rows.iter().zip(probs.iter()) {
                if row_probs.first().copied().unwrap_or(0.0) < 0.5 {
                    continue;
                }
                let anchor = row
                    .get(relation.anchor_var.trim_start_matches('?'))
                    .or_else(|| row.get(&relation.anchor_var))
                    .ok_or_else(|| format!("Missing anchor variable {}", relation.anchor_var))?;
                let triple = Triple {
                    subject: database.encode_term_star(anchor),
                    predicate: database.encode_term_star(&relation.predicate),
                    object: database.encode_term_star(positive_literal),
                };
                database.add_triple(triple.clone());
                generated.push(triple);
            }
        }
    }

    database
        .neural_materialized_triples
        .insert(predicate.to_string(), generated);
    Ok(())
}

/// Materialises whichever neural relations the given patterns could match.
///
/// This is the implicit path: querying a neural predicate runs its model without
/// the query saying so. [`SparqlDatabase`](crate::sparql_database::SparqlDatabase)
/// can switch it off, and [`execute_program`](crate::program::execute_program) does
/// exactly that so a program's own declarations are the only thing invoking a model.
pub fn materialize_neural_relations_for_patterns(
    database: &mut SparqlDatabase,
    patterns: &[(&str, &str, &str)],
    prefixes: &HashMap<String, String>,
) -> Result<(), String> {
    if !database.implicit_neural_materialization() {
        return Ok(());
    }
    for (_, predicate, _) in patterns {
        let resolved = database.resolve_query_term(predicate, prefixes);
        if database.neural_relation_decls.contains_key(&resolved) {
            materialize_neural_relation(database, &resolved).map_err(|err| err.to_string())?;
        }
    }
    Ok(())
}

pub use crate::ml_syntax::lower_ml_predict_alias;

#[cfg(test)]
mod tests {
    mod ml_local {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/common/ml_local.rs"
        ));
    }
    use super::*;
    use crate::execute_query::execute;
    use crate::rule_execution::process_rule_definition;
    use shared::query::{LossFn, ModelArch, OptimizerKind};

    fn populate_multiclass_db(db: &mut SparqlDatabase) {
        for (idx, label, features) in [
            ("s0", "A", [1.0, 0.0, 0.0]),
            ("s1", "A", [1.0, 0.0, 0.0]),
            ("s2", "B", [0.0, 1.0, 0.0]),
            ("s3", "B", [0.0, 1.0, 0.0]),
            ("s4", "C", [0.0, 0.0, 1.0]),
            ("s5", "C", [0.0, 0.0, 1.0]),
        ] {
            db.add_triple_parts(idx, "http://example.org/x0", &features[0].to_string());
            db.add_triple_parts(idx, "http://example.org/x1", &features[1].to_string());
            db.add_triple_parts(idx, "http://example.org/x2", &features[2].to_string());
            db.add_triple_parts(idx, "http://example.org/gold", label);
        }
    }

    fn populate_binary_db(db: &mut SparqlDatabase) {
        for (idx, label, features) in [
            ("t0", "1", [1.0, 1.0]),
            ("t1", "1", [1.0, 1.0]),
            ("t2", "0", [0.0, 0.0]),
            ("t3", "0", [0.0, 0.0]),
        ] {
            db.add_triple_parts(idx, "http://example.org/x0", &features[0].to_string());
            db.add_triple_parts(idx, "http://example.org/x1", &features[1].to_string());
            db.add_triple_parts(idx, "http://example.org/gold", label);
        }
    }

    #[test]
    fn relation_driven_training_query_is_built_from_input_and_data() {
        let mut db = ml_local::database();
        db.prefixes
            .insert("ex".to_string(), "http://example.org/".to_string());
        let prefixes = db.prefixes.clone();
        register_neural_declarations(
            &mut db,
            &prefixes,
            &[ModelDecl {
                name: "digit_model".to_string(),
                arch: ModelArch::Mlp {
                    hidden_layers: vec![8, 4],
                },
                output_kind: NeuralOutputKind::Exclusive {
                    labels: vec!["A".to_string(), "B".to_string()],
                },
            }],
            &[NeuralRelationDecl {
                predicate: "ex:pred".to_string(),
                model_name: "digit_model".to_string(),
                input_patterns: vec![
                    (
                        "?sample".to_string(),
                        "ex:x0".to_string(),
                        "?x0".to_string(),
                    ),
                    (
                        "?sample".to_string(),
                        "ex:x1".to_string(),
                        "?x1".to_string(),
                    ),
                ],
                feature_vars: vec!["?x0".to_string(), "?x1".to_string()],
                anchor_var: "?sample".to_string(),
            }],
            &[],
        );
        let owned = lower_train_decl_to_owned(
            &db,
            &TrainNeuralRelationDecl {
                predicate: "http://example.org/pred".to_string(),
                data_source: TrainingDataSource::GraphPattern(vec![(
                    "?sample".to_string(),
                    "ex:gold".to_string(),
                    "?label".to_string(),
                )]),
                label_var: "?label".to_string(),
                target_triple: (
                    "?sample".to_string(),
                    "http://example.org/pred".to_string(),
                    "?label".to_string(),
                ),
                loss: LossFn::CrossEntropy,
                optimizer: OptimizerKind::Adam,
                learning_rate: 0.01,
                epochs: 5,
                batch_size: 2,
                save_path: Some("kolibrie_first_class_relation_query.bin".to_string()),
            },
        )
        .unwrap();
        assert!(owned
            .training_data_raw
            .contains("?sample <http://example.org/x0> ?x0"));
        assert!(owned.training_data_raw.contains("?sample ex:gold ?label"));
    }

    #[test]
    fn first_class_neural_relation_executes_in_query_where_clause() {
        let mut db = ml_local::database();
        populate_multiclass_db(&mut db);

        let query = r#"
PREFIX ex: <http://example.org/>

MODEL "digit_model" {
    ARCH MLP { HIDDEN [16, 8] }
    OUTPUT EXCLUSIVE { "A", "B", "C" }
}

NEURAL RELATION ex:predictedDigit USING MODEL "digit_model" {
    INPUT {
        ?sample ex:x0 ?x0 .
        ?sample ex:x1 ?x1 .
        ?sample ex:x2 ?x2 .
    }
    FEATURES { ?x0, ?x1, ?x2 }
}

TRAIN NEURAL RELATION ex:predictedDigit {
    DATA {
        ?sample ex:gold ?label .
    }
    LABEL ?label
    TARGET { ?sample ex:predictedDigit ?label }
    LOSS cross_entropy
    OPTIMIZER adam
    LEARNING_RATE 0.1
    EPOCHS 80
    BATCH_SIZE 4
    SAVE_TO "kolibrie_first_class_digit.bin"
}

SELECT ?sample
WHERE {
    ?sample ex:predictedDigit A .
}
        "#;

        let results = execute(query, &mut db)
            .expect("query should run")
            .into_rows();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn query_fallback_training_executes_and_materializes_relation() {
        let mut db = ml_local::database();
        populate_multiclass_db(&mut db);
        db.prefixes
            .insert("ex".to_string(), "http://example.org/".to_string());
        let prefixes = db.prefixes.clone();

        register_neural_declarations(
            &mut db,
            &prefixes,
            &[ModelDecl {
                name: "digit_model".to_string(),
                arch: ModelArch::Mlp {
                    hidden_layers: vec![16, 8],
                },
                output_kind: NeuralOutputKind::Exclusive {
                    labels: vec!["A".to_string(), "B".to_string(), "C".to_string()],
                },
            }],
            &[NeuralRelationDecl {
                predicate: "ex:predictedDigit".to_string(),
                model_name: "digit_model".to_string(),
                input_patterns: vec![
                    (
                        "?sample".to_string(),
                        "ex:x0".to_string(),
                        "?x0".to_string(),
                    ),
                    (
                        "?sample".to_string(),
                        "ex:x1".to_string(),
                        "?x1".to_string(),
                    ),
                    (
                        "?sample".to_string(),
                        "ex:x2".to_string(),
                        "?x2".to_string(),
                    ),
                ],
                feature_vars: vec!["?x0".to_string(), "?x1".to_string(), "?x2".to_string()],
                anchor_var: "?sample".to_string(),
            }],
            &[],
        );

        let train_decl = TrainNeuralRelationDecl {
            predicate: "http://example.org/predictedDigit".to_string(),
            data_source: TrainingDataSource::Query(
                "SELECT ?sample ?x0 ?x1 ?x2 ?label WHERE { ?sample ex:x0 ?x0 . ?sample ex:x1 ?x1 . ?sample ex:x2 ?x2 . ?sample ex:gold ?label . }"
                    .to_string(),
            ),
            label_var: "?label".to_string(),
            target_triple: (
                "?sample".to_string(),
                "http://example.org/predictedDigit".to_string(),
                "?label".to_string(),
            ),
            loss: LossFn::CrossEntropy,
            optimizer: OptimizerKind::Adam,
            learning_rate: 0.1,
            epochs: 80,
            batch_size: 4,
            save_path: Some("kolibrie_first_class_query_fallback.bin".to_string()),
        };

        execute_train_decl(&mut db, &train_decl).unwrap();
        materialize_neural_relation(&mut db, "http://example.org/predictedDigit").unwrap();
        assert_eq!(
            db.neural_materialized_triples
                .get("http://example.org/predictedDigit")
                .map(|triples| triples.len())
                .unwrap_or_default(),
            6
        );
    }

    #[test]
    fn first_class_binary_neural_relation_executes_in_rule_where_clause() {
        let mut db = ml_local::database();
        populate_binary_db(&mut db);

        // Use a linear model to avoid random convergence failures
        let rule = r#"
PREFIX ex: <http://example.org/>

MODEL "fraud_model" {
    ARCH MLP { HIDDEN [] }
    OUTPUT BINARY { true }
}

NEURAL RELATION ex:isFraud USING MODEL "fraud_model" {
    INPUT {
        ?sample ex:x0 ?x0 .
        ?sample ex:x1 ?x1 .
    }
    FEATURES { ?x0, ?x1 }
}

TRAIN NEURAL RELATION ex:isFraud {
    DATA {
        ?sample ex:gold ?label .
    }
    LABEL ?label
    TARGET { ?sample ex:isFraud true }
    LOSS binary_cross_entropy
    OPTIMIZER adam
    LEARNING_RATE 0.1
    EPOCHS 80
    BATCH_SIZE 2
    SAVE_TO "kolibrie_first_class_binary.bin"
}

RULE :FlagFraud :-
CONSTRUCT {
    ?sample ex:flagged true .
}
WHERE {
    ?sample ex:isFraud true .
}
        "#;

        let (_rule, inferred) = process_rule_definition(rule, &mut db).unwrap();
        assert_eq!(inferred.len(), 2);
    }
}
