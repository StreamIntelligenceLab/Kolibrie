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

use datalog::reasoning::materialisation::sdd_seed_materialise::infer_new_facts_with_sdd_seed_specs;
use datalog::reasoning::Reasoner;
use ml::{MlpNeuralPredicate, OutputType};
use rand::seq::SliceRandom;
use shared::diff_sdd::wmc_gradient;
use shared::provenance::Provenance;
use shared::query::LossFn;
use shared::rule::Rule;
use shared::seed_spec::{ExclusiveChoice, SeedSpec};
use shared::triple::Triple;

use crate::ml_feature_loader::{build_feature_vec, query_training_rows, rdf_term_to_f64, RdfTerm};
use crate::sparql_database::SparqlDatabase;

type TrainResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone)]
pub struct OwnedNeuralChoice {
    pub triple_template: (String, String, String),
    pub prob_var: String,
}

#[derive(Debug, Clone)]
pub enum OwnedNeuralGroupType {
    Exclusive { choices: Vec<OwnedNeuralChoice> },
    Independent { fact_template: (String, String, String), prob_var: String },
}

#[derive(Debug, Clone)]
pub struct OwnedNeuralCallSpec {
    pub feature_vars: Vec<String>,
    pub group_type: OwnedNeuralGroupType,
}

#[derive(Debug, Clone)]
pub struct OwnedNeuralTrainingClause {
    pub model_name: String,
    pub neural_calls: Vec<OwnedNeuralCallSpec>,
    pub training_data_raw: String,
    pub label_var: String,
    pub target_triple: (String, String, String),
    pub loss: LossFn,
    pub optimizer: shared::query::OptimizerKind,
    pub learning_rate: f64,
    pub epochs: usize,
    pub batch_size: usize,
    pub save_path: Option<String>,
}

pub fn execute_ml_training_owned(
    clause: &OwnedNeuralTrainingClause,
    base_reasoner: &Reasoner,
    db: &mut SparqlDatabase,
) -> TrainResult<MlpNeuralPredicate> {
    let rows = query_training_rows(db, &clause.training_data_raw)?;
    if rows.is_empty() {
        return Err("training data query returned no rows".into());
    }
    if clause.neural_calls.is_empty() {
        return Err("neural training requires at least one neural call".into());
    }

    let expected_dim = clause.neural_calls[0].feature_vars.len();
    if expected_dim == 0 {
        return Err("neural relation calls must declare at least one feature variable".into());
    }

    let first_group = &clause.neural_calls[0].group_type;
    let output_type = match first_group {
        OwnedNeuralGroupType::Exclusive { choices } => OutputType::Categorical(choices.len()),
        OwnedNeuralGroupType::Independent { .. } => OutputType::Binary,
    };
    let output_dim = match output_type {
        OutputType::Binary => 1,
        OutputType::Categorical(size) => size,
    };

    for call in &clause.neural_calls {
        if call.feature_vars.len() != expected_dim {
            return Err("all neural relation calls in one training clause must have equal feature dimensions".into());
        }
        match (&call.group_type, output_type) {
            (OwnedNeuralGroupType::Exclusive { choices }, OutputType::Categorical(size)) if choices.len() == size => {}
            (OwnedNeuralGroupType::Independent { .. }, OutputType::Binary) => {}
            _ => return Err("mixing Exclusive and Independent neural calls is not supported in neural training v2".into()),
        }
    }

    let model = MlpNeuralPredicate::new(expected_dim, &[64, 32], output_type)?;
    let var_to_col = build_var_to_col_maps(clause, output_dim);

    for _epoch in 0..clause.epochs {
        let mut epoch_rows = rows.clone();
        epoch_rows.shuffle(&mut rand::rng());

        for batch in epoch_rows.chunks(clause.batch_size.max(1)) {
            model.zero_grads();

            let mut tracked_tensors = Vec::with_capacity(clause.neural_calls.len());
            let mut detached_probs = Vec::with_capacity(clause.neural_calls.len());
            for call in &clause.neural_calls {
                let feature_refs: Vec<&str> = call.feature_vars.iter().map(String::as_str).collect();
                let features: Vec<Vec<f64>> = batch
                    .iter()
                    .map(|row| build_feature_vec(row, &feature_refs))
                    .collect::<Result<_, _>>()?;
                let (tracked, probs) = model.forward_with_grads(&features)?;
                tracked_tensors.push(tracked);
                detached_probs.push(probs);
            }

            let mut per_call_grad_batches: Vec<Vec<HashMap<u32, f64>>> =
                (0..clause.neural_calls.len()).map(|_| vec![HashMap::new(); batch.len()]).collect();

            for (sample_idx, row) in batch.iter().enumerate() {
                let mut local_reasoner = base_reasoner.clone();
                let seeds = build_seed_specs_for_row(clause, &detached_probs, sample_idx, row, db, output_dim)?;
                let (_facts, tag_store) = infer_new_facts_with_sdd_seed_specs(&mut local_reasoner, seeds);

                let target = instantiate_triple(
                    (
                        clause.target_triple.0.as_str(),
                        clause.target_triple.1.as_str(),
                        clause.target_triple.2.as_str(),
                    ),
                    row,
                    db,
                )?;
                let has_target = !local_reasoner
                    .index_manager
                    .query(Some(target.subject), Some(target.predicate), Some(target.object))
                    .is_empty();

                let explicit_tag = if has_target && tag_store.has_explicit_tag(&target) {
                    Some(tag_store.get_tag(&target))
                } else {
                    None
                };
                let p_q = match explicit_tag {
                    Some(tag) => tag_store.provenance().recover_probability(&tag),
                    None if has_target => 1.0,
                    None => 0.0,
                };

                let d_loss_d_pq = loss_gradient(clause.loss, p_q, row, &clause.label_var)?;
                if let Some(tag) = explicit_tag {
                    let provenance = tag_store.provenance().clone();
                    let mut manager = provenance.manager().lock().unwrap();
                    let grads = wmc_gradient(&mut manager, tag);
                    let scaled: HashMap<u32, f64> = grads
                        .into_iter()
                        .map(|(var, grad)| (var, grad * d_loss_d_pq))
                        .collect();
                    for call_grads in &mut per_call_grad_batches {
                        call_grads[sample_idx] = scaled.clone();
                    }
                }
            }

            for (call_idx, tracked) in tracked_tensors.iter().enumerate() {
                model.surrogate_backward(tracked, &per_call_grad_batches[call_idx], &var_to_col[call_idx])?;
            }
            model.optimizer_step(clause.optimizer, clause.learning_rate);
        }
    }

    if let Some(path) = &clause.save_path {
        model.save(path)?;
    }

    Ok(model)
}

fn build_var_to_col_maps(
    clause: &OwnedNeuralTrainingClause,
    output_dim: usize,
) -> Vec<HashMap<u32, usize>> {
    clause
        .neural_calls
        .iter()
        .enumerate()
        .map(|(call_idx, call)| {
            let base_var = (call_idx * output_dim) as u32;
            match &call.group_type {
                OwnedNeuralGroupType::Exclusive { choices } => choices
                    .iter()
                    .enumerate()
                    .map(|(choice_idx, _)| (base_var + choice_idx as u32, choice_idx))
                    .collect(),
                OwnedNeuralGroupType::Independent { .. } => HashMap::from([(base_var, 0usize)]),
            }
        })
        .collect()
}

fn build_seed_specs_for_row(
    clause: &OwnedNeuralTrainingClause,
    detached_probs: &[Vec<Vec<f64>>],
    sample_idx: usize,
    row: &HashMap<String, RdfTerm>,
    db: &SparqlDatabase,
    output_dim: usize,
) -> TrainResult<Vec<SeedSpec>> {
    clause
        .neural_calls
        .iter()
        .enumerate()
        .map(|(call_idx, call)| {
            let base_var = (call_idx * output_dim) as u32;
            match &call.group_type {
                OwnedNeuralGroupType::Exclusive { choices } => {
                    let choice_specs = choices
                        .iter()
                        .enumerate()
                        .map(|(choice_idx, choice)| {
                            Ok(ExclusiveChoice {
                                triple: instantiate_triple(
                                    (
                                        choice.triple_template.0.as_str(),
                                        choice.triple_template.1.as_str(),
                                        choice.triple_template.2.as_str(),
                                    ),
                                    row,
                                    db,
                                )?,
                                prob: detached_probs[call_idx][sample_idx][choice_idx],
                                choice_id: base_var + choice_idx as u32,
                            })
                        })
                        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
                    Ok(SeedSpec::ExclusiveGroup {
                        group_id: call_idx as u32,
                        choices: choice_specs,
                    })
                }
                OwnedNeuralGroupType::Independent { fact_template, .. } => Ok(SeedSpec::Independent {
                    triple: instantiate_triple(
                        (
                            fact_template.0.as_str(),
                            fact_template.1.as_str(),
                            fact_template.2.as_str(),
                        ),
                        row,
                        db,
                    )?,
                    prob: detached_probs[call_idx][sample_idx][0],
                    seed_id: base_var,
                }),
            }
        })
        .collect()
}

fn instantiate_triple(
    template: (&str, &str, &str),
    row: &HashMap<String, RdfTerm>,
    db: &SparqlDatabase,
) -> TrainResult<Triple> {
    let subject = instantiate_term(template.0, row, db)?;
    let predicate = instantiate_term(template.1, row, db)?;
    let object = instantiate_term(template.2, row, db)?;
    Ok(Triple {
        subject: db.encode_term_star(&subject),
        predicate: db.encode_term_star(&predicate),
        object: db.encode_term_star(&object),
    })
}

fn instantiate_term(
    term: &str,
    row: &HashMap<String, RdfTerm>,
    db: &SparqlDatabase,
) -> TrainResult<String> {
    if term.starts_with('?') {
        let key = term.trim_start_matches('?');
        row.get(key)
            .cloned()
            .or_else(|| row.get(term).cloned())
            .ok_or_else(|| format!("Missing row binding for variable {}", term).into())
    } else if term.starts_with('<') && term.ends_with('>') {
        Ok(term.trim_start_matches('<').trim_end_matches('>').to_string())
    } else if term.contains(':') && !term.starts_with("http://") && !term.starts_with("https://") {
        let mut parts = term.splitn(2, ':');
        let prefix = parts.next().unwrap_or_default();
        let local = parts.next().unwrap_or_default();
        if let Some(base) = db.prefixes.get(prefix) {
            Ok(format!("{}{}", base, local))
        } else {
            Ok(term.to_string())
        }
    } else {
        Ok(term.to_string())
    }
}

fn loss_gradient(
    loss: LossFn,
    p_q: f64,
    row: &HashMap<String, RdfTerm>,
    label_var: &str,
) -> TrainResult<f64> {
    let p = p_q.clamp(1e-15, 1.0 - 1e-15);
    match loss {
        LossFn::CrossEntropy | LossFn::Nll => Ok(-1.0 / p.max(1e-15)),
        LossFn::Mse => {
            let label = row
                .get(label_var.trim_start_matches('?'))
                .or_else(|| row.get(label_var))
                .ok_or_else(|| format!("Missing label variable {}", label_var))?;
            let label_f64 = rdf_term_to_f64(label)?;
            Ok(2.0 * (p_q - label_f64))
        }
        LossFn::BinaryCrossEntropy => {
            let label = row
                .get(label_var.trim_start_matches('?'))
                .or_else(|| row.get(label_var))
                .ok_or_else(|| format!("Missing label variable {}", label_var))?;
            let label_f64 = rdf_term_to_f64(label)?;
            Ok(-(label_f64 / p) + ((1.0 - label_f64) / (1.0 - p)))
        }
    }
}

pub fn build_ground_reasoner_from_db(db: &SparqlDatabase, extra_rule: Option<Rule>) -> Reasoner {
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = db.dictionary.clone();
    for triple in &db.triples {
        reasoner.index_manager.insert(triple);
    }
    if let Some(rule) = extra_rule {
        reasoner.add_rule(rule);
    }
    reasoner
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::query::{LossFn, OptimizerKind};
    use shared::rule::FilterCondition;
    use shared::terms::Term;

    fn make_clause(
        training_data_raw: &str,
        neural_calls: Vec<OwnedNeuralCallSpec>,
        target_triple: (&str, &str, &str),
        label_var: &str,
    ) -> OwnedNeuralTrainingClause {
        OwnedNeuralTrainingClause {
            model_name: "test".to_string(),
            neural_calls,
            training_data_raw: training_data_raw.to_string(),
            label_var: label_var.to_string(),
            target_triple: (
                target_triple.0.to_string(),
                target_triple.1.to_string(),
                target_triple.2.to_string(),
            ),
            loss: LossFn::CrossEntropy,
            optimizer: OptimizerKind::Adam,
            learning_rate: 0.1,
            epochs: 80,
            batch_size: 4,
            save_path: None,
        }
    }

    #[test]
    fn neural_train_exclusive_3class() {
        let mut db = SparqlDatabase::new();
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

        let query = "SELECT ?sensor ?x0 ?x1 ?x2 ?label WHERE { ?sensor <http://example.org/x0> ?x0 . ?sensor <http://example.org/x1> ?x1 . ?sensor <http://example.org/x2> ?x2 . ?sensor <http://example.org/gold> ?label . }";
        let clause = make_clause(
            query,
            vec![OwnedNeuralCallSpec {
                feature_vars: vec!["?x0".to_string(), "?x1".to_string(), "?x2".to_string()],
                group_type: OwnedNeuralGroupType::Exclusive {
                    choices: vec![
                        OwnedNeuralChoice {
                            triple_template: ("?sensor".to_string(), "http://example.org/pred".to_string(), "A".to_string()),
                            prob_var: "?p0".to_string(),
                        },
                        OwnedNeuralChoice {
                            triple_template: ("?sensor".to_string(), "http://example.org/pred".to_string(), "B".to_string()),
                            prob_var: "?p1".to_string(),
                        },
                        OwnedNeuralChoice {
                            triple_template: ("?sensor".to_string(), "http://example.org/pred".to_string(), "C".to_string()),
                            prob_var: "?p2".to_string(),
                        },
                    ],
                },
            }],
            ("?sensor", "http://example.org/pred", "?label"),
            "?label",
        );

        let base_reasoner = build_ground_reasoner_from_db(&db, None);
        let model = execute_ml_training_owned(&clause, &base_reasoner, &mut db).unwrap();

        let eval_rows = query_training_rows(&mut db, query).unwrap();
        let mut correct_probs = Vec::new();
        for row in &eval_rows {
            let features = build_feature_vec(row, &["?x0", "?x1", "?x2"]).unwrap();
            let (_tracked, probs) = model.forward_with_grads(&[features]).unwrap();
            let label = row.get("label").unwrap();
            let idx = match label.as_str() {
                "A" => 0,
                "B" => 1,
                "C" => 2,
                _ => panic!("unexpected label"),
            };
            correct_probs.push(probs[0][idx]);
        }
        let avg: f64 = correct_probs.iter().sum::<f64>() / correct_probs.len() as f64;
        assert!(avg > 0.9, "expected avg correct prob > 0.9, got {}", avg);
    }

    #[test]
    fn neural_train_two_group_grad_flow() {
        let mut db = SparqlDatabase::new();
        for sample in ["g0", "g1", "g2", "g3"] {
            db.add_triple_parts(sample, "http://example.org/lx0", "0");
            db.add_triple_parts(sample, "http://example.org/lx1", "1");
            db.add_triple_parts(sample, "http://example.org/rx0", "0");
            db.add_triple_parts(sample, "http://example.org/rx1", "1");
            db.add_triple_parts(sample, "http://example.org/label", "yes");
        }

        let mut base_reasoner = build_ground_reasoner_from_db(&db, None);
        let mut dict = base_reasoner.dictionary.write().unwrap();
        let sample = Term::Variable("sample".to_string());
        let left_pred = Term::Constant(dict.encode("http://example.org/left"));
        let right_pred = Term::Constant(dict.encode("http://example.org/right"));
        let ok_pred = Term::Constant(dict.encode("http://example.org/ok"));
        let left_value = Term::Constant(dict.encode("L1"));
        let right_value = Term::Constant(dict.encode("R1"));
        let yes_value = Term::Constant(dict.encode("yes"));
        drop(dict);
        base_reasoner.add_rule(Rule {
            premise: vec![
                (sample.clone(), left_pred, left_value),
                (sample.clone(), right_pred, right_value),
            ],
            negative_premise: vec![],
            filters: Vec::<FilterCondition>::new(),
            conclusion: vec![(sample, ok_pred, yes_value)],
        });

        let query = "SELECT ?sample ?lx0 ?lx1 ?rx0 ?rx1 ?label WHERE { ?sample <http://example.org/lx0> ?lx0 . ?sample <http://example.org/lx1> ?lx1 . ?sample <http://example.org/rx0> ?rx0 . ?sample <http://example.org/rx1> ?rx1 . ?sample <http://example.org/label> ?label . }";
        let clause = make_clause(
            query,
            vec![
                OwnedNeuralCallSpec {
                    feature_vars: vec!["?lx0".to_string(), "?lx1".to_string()],
                    group_type: OwnedNeuralGroupType::Exclusive {
                        choices: vec![
                            OwnedNeuralChoice {
                                triple_template: ("?sample".to_string(), "http://example.org/left".to_string(), "L0".to_string()),
                                prob_var: "?p0".to_string(),
                            },
                            OwnedNeuralChoice {
                                triple_template: ("?sample".to_string(), "http://example.org/left".to_string(), "L1".to_string()),
                                prob_var: "?p1".to_string(),
                            },
                        ],
                    },
                },
                OwnedNeuralCallSpec {
                    feature_vars: vec!["?rx0".to_string(), "?rx1".to_string()],
                    group_type: OwnedNeuralGroupType::Exclusive {
                        choices: vec![
                            OwnedNeuralChoice {
                                triple_template: ("?sample".to_string(), "http://example.org/right".to_string(), "R0".to_string()),
                                prob_var: "?p0".to_string(),
                            },
                            OwnedNeuralChoice {
                                triple_template: ("?sample".to_string(), "http://example.org/right".to_string(), "R1".to_string()),
                                prob_var: "?p1".to_string(),
                            },
                        ],
                    },
                },
            ],
            ("?sample", "http://example.org/ok", "?label"),
            "?label",
        );

        let model = execute_ml_training_owned(&clause, &base_reasoner, &mut db).unwrap();
        let rows = query_training_rows(&mut db, query).unwrap();
        let row = &rows[0];
        let (_tracked_left, left_probs) = model
            .forward_with_grads(&[build_feature_vec(row, &["?lx0", "?lx1"]).unwrap()])
            .unwrap();
        let (_tracked_right, right_probs) = model
            .forward_with_grads(&[build_feature_vec(row, &["?rx0", "?rx1"]).unwrap()])
            .unwrap();
        assert!(left_probs[0][1] > 0.8, "left call did not receive useful gradient");
        assert!(right_probs[0][1] > 0.8, "right call did not receive useful gradient");
    }
}
