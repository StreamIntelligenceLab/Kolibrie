/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Running a `RULE` definition, and the `RETRIEVE` clause, against a store.
//!
//! These used to sit at the end of [`parser`](crate::parser), which is why a module
//! whose job is turning text into an AST also validated the ML policy, registered
//! declarations, ran training, drove the Datalog reasoner and built and fired
//! `CSPARQLWindow`s. Everything in `parser` above them was, and is, pure parsing.
//!
//! Splitting them out leaves `parser` depending on nothing in this crate, and lets
//! the stream and ML layers be called from above rather than reached up into from
//! below.

use crate::execute_query::QueryError;
use crate::parser::{convert_combined_rule, convert_triple_pattern, parse_combined_query};
use crate::rsp::r2s::{Relation2StreamOperator, StreamOperator};
use crate::rsp::s2r::{
    CSPARQLWindow, ContentContainer, Report, ReportStrategy, Tick, WindowTriple,
};
use crate::sparql_database::SparqlDatabase;
use datalog::reasoning::Reasoner;
use shared::hybrid::*;
use shared::provenance::Provenance;
use shared::query::*;
use shared::rule::Rule;
use shared::terms::*;
use shared::triple::Triple;
use std::collections::HashMap;

/// Parses a rule and runs it against `database`, returning the rule and the facts
/// it derived.
///
/// The whole pipeline in one call: policy check, prefix registration, parse, lower
/// against the store's dictionary, then derive. Prefixes declared in `rule_input`
/// are registered on the store as a side effect, so later queries can use them.
///
/// The ML policy is enforced first: a rule naming a model the host has not approved
/// is refused before anything is parsed or derived.
pub fn process_rule_definition(
    rule_input: &str,
    database: &mut SparqlDatabase,
) -> Result<(Rule, Vec<Triple>), QueryError> {
    crate::execute_query::validate_query_policy(rule_input, database)?;
    // First, register any prefixes from the rule with the database
    database.register_prefixes_from_query(rule_input);

    let parse_result = parse_combined_query(rule_input);

    if let Ok((_rest, combined)) = parse_result {
        let single = combined.single_rule().map_err(QueryError::Unsupported)?;
        if combined.ml_predict.is_some() {
            return Err(QueryError::Unsupported(
                "process_rule_definition does not execute a top-level ML.PREDICT; use kolibrie::program"
                    .to_string(),
            ));
        }
        if single
            .and_then(|rule| rule.ml_predict.as_ref())
            .is_some_and(|predict| predict.distribution)
        {
            return Err(QueryError::Unsupported(
                "in-rule ML.PREDICT ... DISTRIBUTION is not supported; write a top-level ML.PREDICT in a program"
                    .to_string(),
            ));
        }
        for (prefix, uri) in &combined.prefixes {
            database.prefixes.insert(prefix.clone(), uri.clone());
        }

        let mut rule_prefixes = combined.prefixes.clone();
        database.share_prefixes_with(&mut rule_prefixes);
        crate::neural_registry::register_neural_declarations_checked(
            database,
            &rule_prefixes,
            &combined.model_decls,
            &combined.neural_relation_decls,
            &combined.train_neural_relation_decls,
        )
        .map_err(QueryError::Execution)?;

        let normalized_trains: Vec<TrainNeuralRelationDecl> = combined
            .train_neural_relation_decls
            .iter()
            .filter_map(|decl| {
                let normalized_pred = database.resolve_query_term(&decl.predicate, &rule_prefixes);
                database
                    .train_neural_relation_decls
                    .get(&normalized_pred)
                    .cloned()
            })
            .collect();
        for train_decl in &normalized_trains {
            crate::ml_runtime_for_build()
                .execute_train_decl(database, train_decl)
                .map_err(QueryError::Execution)?;
        }

        #[allow(unused_mut)] // ML-enabled execution can rewrite this rule.
        let mut rule = combined
            .single_rule()
            .map_err(QueryError::Unsupported)?
            .cloned()
            .ok_or_else(|| QueryError::Parse("Failed to parse rule definition".to_string()))?;

        crate::ml_runtime_for_build()
            .materialize_for_patterns(database, &rule.body.0, &rule_prefixes)
            .map_err(QueryError::Execution)?;

        // Execute ML.PREDICT before converting the rule for Datalog
        if let Some(ml_predict) = rule.ml_predict.clone() {
            // No `cfg` needed any more: the build's runtime refuses this itself when
            // the feature is off, with the same `QUERY_POLICY` code as before.
            crate::ml_runtime_for_build().execute_predict_clause(
                database,
                &ml_predict,
                &mut rule,
                &rule_prefixes,
            )?;
        }

        let mut kg = Reasoner::new();
        kg.dictionary = database.dictionary.clone();
        for triple in database.query_default_triples(None, None, None) {
            kg.dataset_index.insert(&triple);
        }
        kg.probability_seeds = database.probability_seeds.clone();

        let mut dict = kg.dictionary.write().unwrap();
        let dynamic_rule = convert_combined_rule(rule.clone(), &mut dict, &rule_prefixes);
        drop(dict);
        database.dictionary = kg.dictionary.clone();

        // Check if this rule has windowing - if so, set up RSP processing
        if !rule.window_clause.is_empty() {
            let mut all_stream_results: Vec<Triple> = Vec::new();
            let mut rsp_windows: Vec<CSPARQLWindow<WindowTriple>> = Vec::new();

            // Set up stream operator based on parsed stream type
            let stream_operator = match &rule.stream_type {
                Some(StreamType::RStream) => StreamOperator::RSTREAM,
                Some(StreamType::IStream) => StreamOperator::ISTREAM,
                Some(StreamType::DStream) => StreamOperator::DSTREAM,
                _ => StreamOperator::RSTREAM, // Default
            };

            // Create a window for each window clause
            for window_clause in &rule.window_clause {
                let mut rsp_window =
                    create_rsp_window(&window_clause.window_spec).map_err(QueryError::Execution)?;

                // Process existing triples through the window
                let mut current_time = 1;
                let default_triples = database.query_default_triples(None, None, None);
                for triple in default_triples.iter() {
                    let dict = database.dictionary.read().unwrap();
                    let window_triple = WindowTriple {
                        s: dict.decode(triple.subject).unwrap_or("").to_string(),
                        p: dict.decode(triple.predicate).unwrap_or("").to_string(),
                        o: dict.decode(triple.object).unwrap_or("").to_string(),
                    };
                    drop(dict);

                    // Add to window
                    rsp_window.add_to_window(window_triple, current_time);
                    current_time += 1;
                }

                // Register a callback to process windowed results
                let kg_clone = kg.clone();
                let rule_clone = dynamic_rule.clone();
                let _stream_op_clone = stream_operator.clone();

                rsp_window.register_callback(Box::new(
                    move |content: ContentContainer<WindowTriple>| {
                        // Convert window content back to Knowledge Graph format
                        let mut window_kg = kg_clone.clone();
                        for window_triple in content.iter() {
                            window_kg.add_abox_triple(
                                &window_triple.s,
                                &window_triple.p,
                                &window_triple.o,
                            );
                        }

                        // Apply the rule to windowed data
                        window_kg.add_rule(rule_clone.clone());
                        window_kg.infer_new_facts_semi_naive();
                    },
                ));

                rsp_windows.push(rsp_window);
            }

            // Add the rule to the main knowledge graph
            kg.add_rule(dynamic_rule.clone());

            // For immediate processing, also infer from current data
            let inferred_facts = kg.infer_new_facts_semi_naive();

            // Apply stream operator to results
            let eval_time = database
                .query_default_triples(None, None, None)
                .len()
                .saturating_add(1);

            for _window_clause in &rsp_windows {
                let mut r2s_operator = Relation2StreamOperator::new(stream_operator.clone(), 0);
                let stream_results = r2s_operator.eval(inferred_facts.clone(), eval_time);

                // Add inferred facts to the database
                for triple in stream_results.iter() {
                    database.add_triple(triple.clone());
                    all_stream_results.push(triple.clone());
                }
            }

            // Register rule predicates
            register_rule_predicates(&dynamic_rule, database);

            return Ok((dynamic_rule, all_stream_results));
        }

        // Non-windowed rule processing
        // Check if this is a provenance-annotated rule
        if rule.prob_annotation.is_some() {
            let ann = rule.prob_annotation.as_ref().unwrap();

            kg.add_rule(dynamic_rule.clone());
            register_rule_predicates(&dynamic_rule, database);

            // Choose provenance based on annotation, then materialize tags as RDF-star
            let provenance_type = ann.combination;
            let inferred_facts = match provenance_type {
                "minmax" | "min" => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::MinMaxProbability);
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                "addmult" | "independent" => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::AddMultProbability);
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                "boolean" => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::BooleanProvenance);
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                // Full derivation lineage with certified top-k lower bounds and
                // selective exact SDD escalation.
                "hybrid" => {
                    let config = ann.hybrid_config.as_ref().ok_or_else(|| {
                        QueryError::Execution(
                            "PROB(provenance=hybrid) requires a valid threshold and hybrid configuration"
                                .to_string(),
                        )
                    })?;
                    let snapshot = SeedSnapshot::from_probability_seeds(&kg.probability_seeds)
                        .map_err(|error| QueryError::Execution(error.to_string()))?;
                    let (facts, results, _lineage) = kg
                        .infer_new_facts_with_hybrid(snapshot, config)
                        .map_err(|error| QueryError::Execution(error.to_string()))?;
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star =
                        encode_hybrid_results_as_rdf_star(&results, &mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                // Exact proof-formula provenance (WMC via Shannon expansion)
                "wmc" => {
                    let (facts, tag_store) = kg
                        .infer_new_facts_with_provenance(shared::provenance::WmcProvenance::new());
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star =
                        tag_store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                // SDD-based exact proof-formula provenance (WMC via SDD)
                "sdd" => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::sdd::SddProvenance::new());
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star =
                        tag_store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                // Top-K proof-tracking provenance.
                // k is read from the threshold field (default 5).
                // Syntax: PROB(combination=topk) or PROB(combination=topk, threshold=10)
                "topk" => {
                    let k = ann.threshold.map(|t| t as usize).unwrap_or(5);
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::TopKProofs::new(k));
                    let diagnostics: HashMap<Triple, HybridProbabilityResult> = facts
                        .iter()
                        .map(|triple| {
                            let estimate = tag_store
                                .provenance()
                                .recover_probability(&tag_store.get_tag(triple));
                            (
                                triple.clone(),
                                HybridProbabilityResult::UnsafeApproximation {
                                    estimate,
                                    reason: HybridReason::DiagnosticOnly,
                                    metrics: HybridMetrics {
                                        k_used: k,
                                        ..HybridMetrics::default()
                                    },
                                },
                            )
                        })
                        .collect();
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star =
                        encode_hybrid_results_as_rdf_star(&diagnostics, &mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                _ => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::MinMaxProbability);
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
            };

            for triple in inferred_facts.iter() {
                database.add_triple(triple.clone());
            }

            Ok((dynamic_rule, inferred_facts))
        } else {
            kg.add_rule(dynamic_rule.clone());

            // Register rule predicates
            register_rule_predicates(&dynamic_rule, database);

            // Infer new facts based on the rule
            let inferred_facts = kg.infer_new_facts_semi_naive();

            // Add inferred facts to the database
            for triple in inferred_facts.iter() {
                database.add_triple(triple.clone());
            }

            Ok((dynamic_rule, inferred_facts))
        }
    } else {
        Err(QueryError::Parse(
            "Failed to parse rule definition".to_string(),
        ))
    }
}

// Add this function to handle RETRIEVE clause processing
/// Resolves a `RETRIEVE` clause against `database`, returning the triples that
/// describe the matching streams.
pub fn process_retrieve_clause(
    retrieve_clause: &RetrieveClause,
    database: &mut SparqlDatabase,
) -> Result<Vec<Triple>, String> {
    // Convert graph patterns to triple patterns for matching
    let mut retrieved_triples = Vec::new();

    for pattern in &retrieve_clause.graph_pattern {
        // Create a temporary knowledge graph to match patterns
        let mut kg = Reasoner::new();
        let default_triples = database.query_default_triples(None, None, None);
        for triple in default_triples.iter() {
            let dict = database.dictionary.read().unwrap();
            let subject = dict.decode(triple.subject).map(|s| s.to_string());
            let predicate = dict.decode(triple.predicate).map(|p| p.to_string());
            let object = dict.decode(triple.object).map(|o| o.to_string());
            drop(dict);

            if let (Some(s), Some(p), Some(o)) = (subject, predicate, object) {
                kg.add_abox_triple(&s, &p, &o);
            }
        }

        // Match the pattern against the knowledge graph
        let mut dict = database.dictionary.write().unwrap();
        let pattern_converted = convert_triple_pattern(*pattern, &mut dict, &database.prefixes);
        drop(dict);

        // Find matching triples based on the pattern
        for triple in default_triples.iter() {
            if matches_pattern(&pattern_converted, triple) {
                retrieved_triples.push(triple.clone());
            }
        }
    }

    Ok(retrieved_triples)
}

// Helper function to check if a triple matches a pattern
fn matches_pattern(pattern: &TriplePattern, triple: &Triple) -> bool {
    // Check subject match
    let subject_match = match &pattern.0 {
        Term::Variable(_) | Term::QuotedTriple(_) => true,
        Term::Constant(code) => *code == triple.subject,
    };

    // Check predicate match
    let predicate_match = match &pattern.1 {
        Term::Variable(_) | Term::QuotedTriple(_) => true,
        Term::Constant(code) => *code == triple.predicate,
    };

    // Check object match
    let object_match = match &pattern.2 {
        Term::Variable(_) | Term::QuotedTriple(_) => true,
        Term::Constant(code) => *code == triple.object,
    };

    subject_match && predicate_match && object_match
}

// Helper function to create RSP window from parsed specification
fn create_rsp_window(window_spec: &WindowSpec) -> Result<CSPARQLWindow<WindowTriple>, String> {
    // Create report strategy
    let mut report = Report::new();

    let report_strategy = match window_spec.report_strategy {
        Some("NON_EMPTY_CONTENT") => ReportStrategy::NonEmptyContent,
        Some("ON_CONTENT_CHANGE") => ReportStrategy::OnContentChange,
        Some("ON_WINDOW_CLOSE") => ReportStrategy::OnWindowClose,
        Some("PERIODIC") => ReportStrategy::Periodic(5), // Default period
        _ => ReportStrategy::OnWindowClose,              // Default
    };
    report.add(report_strategy);

    // Create tick strategy
    let tick = match window_spec.tick {
        Some("TIME_DRIVEN") => Tick::TimeDriven,
        Some("TUPLE_DRIVEN") => Tick::TupleDriven,
        Some("BATCH_DRIVEN") => Tick::BatchDriven,
        _ => Tick::TimeDriven, // Default
    };

    // Handle different window types
    match window_spec.window_type {
        WindowType::Sliding => {
            let slide = window_spec.slide.unwrap_or(1);
            Ok(CSPARQLWindow::new(
                window_spec.width,
                slide,
                report,
                tick,
                String::default(),
            ))
        }
        WindowType::Tumbling => {
            // Tumbling window: slide = width
            Ok(CSPARQLWindow::new(
                window_spec.width,
                window_spec.width,
                report,
                tick,
                String::default(),
            ))
        }
        WindowType::Range => {
            // Range window: slide = 1 (continuous)
            Ok(CSPARQLWindow::new(
                window_spec.width,
                1,
                report,
                tick,
                String::default(),
            ))
        }
    }
}

// Helper function to register rule predicates
fn register_rule_predicates(rule: &Rule, database: &mut SparqlDatabase) {
    for conclusion in &rule.conclusion {
        if let Term::Constant(code) = conclusion.1 {
            let dict = database.dictionary.read().unwrap();
            let expanded = dict.decode(code).unwrap_or_else(|| "").to_string();
            drop(dict);
            let local = if let Some(idx) = expanded.rfind('#') {
                &expanded[idx + 1..]
            } else if let Some(idx) = expanded.rfind(':') {
                &expanded[idx + 1..]
            } else {
                &expanded
            };
            let rule_key = local.to_lowercase();
            database.rule_map.insert(rule_key, expanded.to_string());
        }
    }
}
