/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use datalog::reasoning::materialisation::sdd_seed_materialise::try_infer_new_facts_with_sdd_seed_specs;
use datalog::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::provenance::Provenance;
use shared::query::{
    CombinedQuery, CombinedRule, FilterExpression, ModelDecl, NeuralOutputKind,
    NeuralRelationDecl, SparqlOperation, TrainNeuralRelationDecl, TrainingDataSource,
};
use shared::quoted_triple_store::QuotedTripleStore;
use shared::rule::{FilterCondition, Rule};
use shared::sdd::{SddBudgetError, SddId, SddLimits, SddProvenance};
use shared::seed_spec::{ExclusiveChoice, SeedSpec};
use shared::tag_store::TagStore;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;

use crate::error_handler::format_parse_error;
use crate::ml_policy::MlExecutionContext;
use crate::parser::parse_combined_query;
use crate::sparql_database::{reencode_term_id, SparqlDatabase};

pub const DISTRIBUTION_TOLERANCE: f64 = 1e-7;
pub const PROB_VALUE_IRI: &str = "http://www.w3.org/ns/prob#value";
const XSD_DOUBLE_IRI: &str = "http://www.w3.org/2001/XMLSchema#double";

#[derive(Debug, Clone, PartialEq)]
pub enum ProgramError {
    Parse(String),
    Unsupported(String),
    Policy(String),
    Snapshot(String),
    Coverage(String),
    Resource(ResourceError),
    Execution(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceError {
    SeedLimit { requested: usize, limit: usize },
    IdOverflow,
    NodeBudgetExceeded,
    DeadlineExceeded,
}

impl fmt::Display for ProgramError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(message) => write!(f, "PROGRAM_PARSE_ERROR: {message}"),
            Self::Unsupported(message) => write!(f, "PROGRAM_UNSUPPORTED: {message}"),
            Self::Policy(message) => write!(f, "PROGRAM_POLICY: {message}"),
            Self::Snapshot(message) => write!(f, "PROGRAM_INVALID_SNAPSHOT: {message}"),
            Self::Coverage(message) => write!(f, "PROGRAM_INCOMPLETE_COVERAGE: {message}"),
            Self::Resource(error) => write!(f, "PROGRAM_RESOURCE_LIMIT: {error:?}"),
            Self::Execution(message) => write!(f, "PROGRAM_EXECUTION_FAILED: {message}"),
        }
    }
}

impl std::error::Error for ProgramError {}

impl From<SddBudgetError> for ProgramError {
    fn from(error: SddBudgetError) -> Self {
        match error {
            SddBudgetError::NodeBudgetExceeded => Self::Resource(ResourceError::NodeBudgetExceeded),
            SddBudgetError::DeadlineExceeded => Self::Resource(ResourceError::DeadlineExceeded),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProgramLimits {
    pub max_seed_variables: usize,
    pub max_sdd_nodes: usize,
    pub deadline: Option<Duration>,
}

impl Default for ProgramLimits {
    fn default() -> Self {
        Self {
            max_seed_variables: 1 << 20,
            max_sdd_nodes: 1 << 24,
            deadline: Some(Duration::from_secs(600)),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProgramOptions {
    pub training_seed: Option<u64>,
    pub frozen_predictions: Option<PredictionSnapshot>,
    pub required_choices: Option<Vec<(String, String)>>,
    pub retain_provenance: bool,
    pub explain: bool,
    pub prediction_batch_size: Option<usize>,
    pub limits: ProgramLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleMode {
    Deterministic,
    Sdd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LexTerm {
    Variable(String),
    Constant(String),
}

type LexPattern = (LexTerm, LexTerm, LexTerm);

#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub name: String,
    premise: Vec<LexPattern>,
    filters: Vec<FilterCondition>,
    conclusion: Vec<LexPattern>,
}

impl CompiledRule {
    fn encode(&self, dictionary: &mut Dictionary) -> Rule {
        Rule {
            premise: self.premise.iter().map(|p| encode_pattern(p, dictionary)).collect(),
            negative_premise: Vec::new(),
            filters: self.filters.clone(),
            conclusion: self.conclusion.iter().map(|p| encode_pattern(p, dictionary)).collect(),
        }
    }
}

fn encode_term(term: &LexTerm, dictionary: &mut Dictionary) -> Term {
    match term {
        LexTerm::Variable(name) => Term::Variable(name.clone()),
        LexTerm::Constant(value) => Term::Constant(dictionary.encode(value)),
    }
}

fn encode_pattern(pattern: &LexPattern, dictionary: &mut Dictionary) -> TriplePattern {
    (
        encode_term(&pattern.0, dictionary),
        encode_term(&pattern.1, dictionary),
        encode_term(&pattern.2, dictionary),
    )
}

#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "ml"), allow(dead_code))]
struct CompiledPrediction {
    model: String,
    query: String,
    columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CompiledProgram {
    prefixes: HashMap<String, String>,
    model_decls: Vec<ModelDecl>,
    relation_decls: Vec<NeuralRelationDecl>,
    train_decls: Vec<TrainNeuralRelationDecl>,
    prediction: Option<CompiledPrediction>,
    rules: Vec<CompiledRule>,
    mode: Option<RuleMode>,
}

impl CompiledProgram {
    pub fn rules(&self) -> &[CompiledRule] {
        &self.rules
    }

    pub fn mode(&self) -> Option<RuleMode> {
        self.mode
    }

    pub fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    pub fn has_prediction(&self) -> bool {
        self.prediction.is_some()
    }

    pub fn has_training(&self) -> bool {
        !self.train_decls.is_empty()
    }

    pub fn datalog_rules(&self, dictionary: &mut Dictionary) -> Vec<Rule> {
        self.rules.iter().map(|rule| rule.encode(dictionary)).collect()
    }
}

pub fn compile_program(
    text: &str,
    policy: &MlExecutionContext,
) -> Result<CompiledProgram, ProgramError> {
    let (remaining, combined) = parse_combined_query(text)
        .map_err(|error| ProgramError::Parse(format_parse_error(text, error)))?;
    if !remaining.trim().is_empty() {
        return Err(ProgramError::Parse(format!(
            "unexpected trailing input: {}",
            remaining.trim()
        )));
    }
    if combined.retrieve_clause.is_some() {
        return Err(ProgramError::Unsupported("RETRIEVE is not part of a program".into()));
    }
    if combined.register_clause.is_some() {
        return Err(ProgramError::Unsupported("REGISTER is not part of a program".into()));
    }
    if combined.sparql.is_some() {
        return Err(ProgramError::Unsupported(
            "SELECT and Update statements are not part of a program; query the result with query_program_result".into(),
        ));
    }
    let prefixes = combined.prefixes.clone();
    check_program_policy(&combined, policy)?;

    let model_decls = combined
        .model_decls
        .iter()
        .map(|decl| normalize_model_decl(decl, &prefixes))
        .collect::<Result<Vec<_>, _>>()?;
    let relation_decls = combined
        .neural_relation_decls
        .iter()
        .map(|decl| normalize_relation_decl(decl, &prefixes))
        .collect::<Result<Vec<_>, _>>()?;
    let train_decls = combined
        .train_neural_relation_decls
        .iter()
        .map(|decl| normalize_train_decl(decl, &prefixes))
        .collect::<Result<Vec<_>, _>>()?;

    let prediction = match &combined.ml_predict {
        Some(ml_predict) => {
            if !ml_predict.distribution {
                return Err(ProgramError::Unsupported(
                    "a program ML.PREDICT must use OUTPUT ?v DISTRIBUTION; argmax prediction stays on the legacy entry points".into(),
                ));
            }
            Some(compile_prediction(ml_predict.model, ml_predict.input_raw, &prefixes)?)
        }
        None => None,
    };

    let mut rules = Vec::with_capacity(combined.rules.len());
    let mut mode = None;
    for rule in &combined.rules {
        let (compiled, rule_mode) = lower_rule(rule, text, &prefixes)?;
        match mode {
            None => mode = Some(rule_mode),
            Some(existing) if existing != rule_mode => {
                return Err(ProgramError::Unsupported(
                    "a program mixes plain rules with PROB(combination=sdd) rules; use one mode for every rule".into(),
                ))
            }
            Some(_) => {}
        }
        rules.push(compiled);
    }

    Ok(CompiledProgram {
        prefixes,
        model_decls,
        relation_decls,
        train_decls,
        prediction,
        rules,
        mode,
    })
}

fn check_program_policy(
    combined: &CombinedQuery<'_>,
    policy: &MlExecutionContext,
) -> Result<(), ProgramError> {
    let uses_ml = !combined.model_decls.is_empty()
        || !combined.neural_relation_decls.is_empty()
        || !combined.train_neural_relation_decls.is_empty()
        || combined.ml_predict.is_some();
    if !uses_ml {
        return Ok(());
    }
    if !cfg!(feature = "ml") {
        return Err(ProgramError::Policy("ML_FEATURE_DISABLED".into()));
    }
    policy
        .require_local()
        .map_err(|error| ProgramError::Policy(error.to_string()))?;
    for name in combined
        .model_decls
        .iter()
        .map(|decl| decl.name.as_str())
        .chain(combined.neural_relation_decls.iter().map(|decl| decl.model_name.as_str()))
        .chain(combined.ml_predict.iter().map(|predict| predict.model))
    {
        crate::ml_policy::validate_model_name(name)
            .map_err(|error| ProgramError::Policy(error.to_string()))?;
    }
    for train in &combined.train_neural_relation_decls {
        if let Some(path) = &train.save_path {
            policy
                .local_artifact(path)
                .map_err(|error| ProgramError::Policy(error.to_string()))?;
        }
    }
    Ok(())
}

fn normalize_term(term: &str, prefixes: &HashMap<String, String>) -> Result<String, ProgramError> {
    let term = term.trim();
    if term.starts_with(['?', '$']) {
        return Ok(format!("?{}", &term[1..]));
    }
    if let Some(iri) = term.strip_prefix('<').and_then(|rest| rest.strip_suffix('>')) {
        return Ok(iri.to_string());
    }
    if let Some(literal) = term.strip_prefix('"').and_then(|rest| rest.strip_suffix('"')) {
        return Ok(literal.to_string());
    }
    expand_prefixed(term, prefixes)
}

fn expand_prefixed(term: &str, prefixes: &HashMap<String, String>) -> Result<String, ProgramError> {
    if term.starts_with("<<") || term.starts_with("_:") || term.starts_with('[') {
        return Err(ProgramError::Unsupported(format!(
            "quoted triples and blank nodes are not supported in programs: `{term}`"
        )));
    }
    match term.split_once(':') {
        Some((_, rest)) if rest.starts_with("//") => Ok(term.to_string()),
        Some((prefix, local)) => prefixes
            .get(prefix)
            .map(|namespace| format!("{namespace}{local}"))
            .ok_or_else(|| {
                ProgramError::Unsupported(format!("undeclared prefix `{prefix}:` in `{term}`"))
            }),
        None => Ok(term.to_string()),
    }
}

fn normalize_model_decl(
    decl: &ModelDecl,
    prefixes: &HashMap<String, String>,
) -> Result<ModelDecl, ProgramError> {
    let output_kind = match &decl.output_kind {
        NeuralOutputKind::Exclusive { labels } => NeuralOutputKind::Exclusive {
            labels: labels
                .iter()
                .map(|label| normalize_term(label, prefixes))
                .collect::<Result<_, _>>()?,
        },
        NeuralOutputKind::Binary { positive_literal } => NeuralOutputKind::Binary {
            positive_literal: normalize_term(positive_literal, prefixes)?,
        },
    };
    Ok(ModelDecl {
        name: decl.name.clone(),
        arch: decl.arch.clone(),
        output_kind,
    })
}

fn normalize_triple(
    triple: &(String, String, String),
    prefixes: &HashMap<String, String>,
) -> Result<(String, String, String), ProgramError> {
    Ok((
        normalize_term(&triple.0, prefixes)?,
        normalize_term(&triple.1, prefixes)?,
        normalize_term(&triple.2, prefixes)?,
    ))
}

fn normalize_relation_decl(
    decl: &NeuralRelationDecl,
    prefixes: &HashMap<String, String>,
) -> Result<NeuralRelationDecl, ProgramError> {
    let normalized = NeuralRelationDecl {
        predicate: normalize_term(&decl.predicate, prefixes)?,
        model_name: decl.model_name.clone(),
        input_patterns: decl
            .input_patterns
            .iter()
            .map(|triple| normalize_triple(triple, prefixes))
            .collect::<Result<_, _>>()?,
        feature_vars: decl
            .feature_vars
            .iter()
            .map(|var| normalize_term(var, prefixes))
            .collect::<Result<_, _>>()?,
        anchor_var: normalize_term(&decl.anchor_var, prefixes)?,
    };
    if normalized.feature_vars.is_empty()
        || normalized.feature_vars.iter().any(|var| !var.starts_with('?'))
    {
        return Err(ProgramError::Unsupported(format!(
            "NEURAL RELATION {} must list feature variables",
            normalized.predicate
        )));
    }
    let bound: HashSet<&str> = normalized
        .input_patterns
        .iter()
        .flat_map(|(s, p, o)| [s.as_str(), p.as_str(), o.as_str()])
        .filter(|term| term.starts_with('?'))
        .collect();
    for var in normalized.feature_vars.iter().chain([&normalized.anchor_var]) {
        if !bound.contains(var.as_str()) {
            return Err(ProgramError::Unsupported(format!(
                "NEURAL RELATION {} uses {var}, which its INPUT does not bind",
                normalized.predicate
            )));
        }
    }
    Ok(normalized)
}

fn normalize_train_decl(
    decl: &TrainNeuralRelationDecl,
    prefixes: &HashMap<String, String>,
) -> Result<TrainNeuralRelationDecl, ProgramError> {
    let data_source = match &decl.data_source {
        TrainingDataSource::GraphPattern(patterns) => TrainingDataSource::GraphPattern(
            patterns
                .iter()
                .map(|triple| normalize_triple(triple, prefixes))
                .collect::<Result<_, _>>()?,
        ),
        TrainingDataSource::Query(_) => {
            return Err(ProgramError::Unsupported(
                "program training reads a DATA graph pattern, not a free-form query".into(),
            ))
        }
    };
    Ok(TrainNeuralRelationDecl {
        predicate: normalize_term(&decl.predicate, prefixes)?,
        data_source,
        label_var: normalize_term(&decl.label_var, prefixes)?,
        target_triple: normalize_triple(&decl.target_triple, prefixes)?,
        loss: decl.loss,
        optimizer: decl.optimizer,
        learning_rate: decl.learning_rate,
        epochs: decl.epochs,
        batch_size: decl.batch_size,
        save_path: decl.save_path.clone(),
    })
}

fn compile_prediction(
    model: &str,
    input_raw: &str,
    prefixes: &HashMap<String, String>,
) -> Result<CompiledPrediction, ProgramError> {
    let mut query = String::new();
    let mut names: Vec<_> = prefixes.iter().collect();
    names.sort();
    for (prefix, namespace) in names {
        if !input_raw.contains(&format!("PREFIX {prefix}:")) {
            query.push_str(&format!("PREFIX {prefix}: <{namespace}>\n"));
        }
    }
    query.push_str(input_raw);

    let (remaining, parsed) = parse_combined_query(&query)
        .map_err(|error| ProgramError::Parse(format_parse_error(&query, error)))?;
    let select = match (&parsed.sparql, remaining.trim().is_empty()) {
        (Some(SparqlOperation::Select(select)), true)
            if parsed.rules.is_empty()
                && parsed.ml_predict.is_none()
                && parsed.model_decls.is_empty()
                && parsed.neural_relation_decls.is_empty()
                && parsed.train_neural_relation_decls.is_empty() =>
        {
            select
        }
        _ => {
            return Err(ProgramError::Unsupported(
                "ML.PREDICT INPUT must contain exactly one plain SELECT query".into(),
            ))
        }
    };
    let mut columns = Vec::with_capacity(select.variables.len());
    for (kind, variable, alias) in &select.variables {
        if *kind != "VAR" || alias.is_some() || *variable == "*" {
            return Err(ProgramError::Unsupported(
                "ML.PREDICT INPUT must project plain variables".into(),
            ));
        }
        columns.push(variable.trim_start_matches(['?', '$']).to_string());
    }
    Ok(CompiledPrediction {
        model: model.to_string(),
        query,
        columns,
    })
}

fn lower_rule(
    rule: &CombinedRule<'_>,
    source: &str,
    prefixes: &HashMap<String, String>,
) -> Result<(CompiledRule, RuleMode), ProgramError> {
    let name = expand_prefixed(rule.head.predicate, prefixes).unwrap_or_else(|_| rule.head.predicate.to_string());
    let reject = |what: &str| -> Result<(CompiledRule, RuleMode), ProgramError> {
        Err(ProgramError::Unsupported(format!("RULE {name}: {what}")))
    };
    if rule.stream_type.is_some() || !rule.window_clause.is_empty() || !rule.window_blocks.is_empty() {
        return reject("windows and streams are not supported in programs");
    }
    if !rule.negated_body.is_empty() {
        return reject("negation (NOT) is not supported in programs");
    }
    if rule.body.2.is_some() {
        return reject("VALUES is not supported in program rules");
    }
    if !rule.body.3.is_empty() {
        return reject("BIND is not supported in program rules; state derived values as data");
    }
    if !rule.body.4.is_empty() {
        return reject("subqueries are not supported in program rules");
    }
    if let Some(ml_predict) = &rule.ml_predict {
        if ml_predict.distribution {
            return reject("in-rule ML.PREDICT ... DISTRIBUTION is not supported; write the top-level ML.PREDICT before the RULE blocks");
        }
        return reject("ML.PREDICT attached to a RULE is not supported in programs; write the top-level ML.PREDICT before the RULE blocks");
    }
    if rule.body.0.is_empty() {
        return reject("WHERE must contain at least one triple pattern");
    }
    if rule.conclusion.is_empty() {
        return reject("CONSTRUCT must contain at least one triple");
    }

    let mode = match &rule.prob_annotation {
        None => RuleMode::Deterministic,
        Some(annotation)
            if annotation.combination == "sdd"
                && annotation.threshold.is_none()
                && annotation.confidence.is_none()
                && annotation.hybrid_config.is_none() =>
        {
            RuleMode::Sdd
        }
        Some(annotation) if annotation.combination != "sdd" => {
            return reject(&format!(
                "PROB(combination={}) is not supported; programs use PROB(combination=sdd)",
                annotation.combination
            ))
        }
        Some(_) => return reject("PROB(combination=sdd) takes no further options in programs"),
    };

    let premise = rule
        .body
        .0
        .iter()
        .map(|pattern| lower_pattern(*pattern, source, prefixes))
        .collect::<Result<Vec<_>, _>>()?;
    let conclusion = rule
        .conclusion
        .iter()
        .map(|pattern| lower_pattern(*pattern, source, prefixes))
        .collect::<Result<Vec<_>, _>>()?;

    let bound: HashSet<&str> = premise
        .iter()
        .flat_map(|(s, p, o)| [s, p, o])
        .filter_map(|term| match term {
            LexTerm::Variable(name) => Some(name.as_str()),
            LexTerm::Constant(_) => None,
        })
        .collect();
    for term in conclusion.iter().flat_map(|(s, p, o)| [s, p, o]) {
        if let LexTerm::Variable(variable) = term {
            if !bound.contains(variable.as_str()) {
                return reject(&format!(
                    "CONSTRUCT variable ?{variable} is not bound by WHERE; rules cannot create blank nodes"
                ));
            }
        }
    }

    let mut filters = Vec::new();
    for filter in &rule.body.1 {
        lower_filter(filter, &bound, &mut filters)
            .map_err(|what| ProgramError::Unsupported(format!("RULE {name}: {what}")))?;
    }

    Ok((
        CompiledRule {
            name,
            premise,
            filters,
            conclusion,
        },
        mode,
    ))
}

fn lower_pattern(
    pattern: (&str, &str, &str),
    source: &str,
    prefixes: &HashMap<String, String>,
) -> Result<LexPattern, ProgramError> {
    Ok((
        lower_rule_term(pattern.0, source, prefixes)?,
        lower_rule_term(pattern.1, source, prefixes)?,
        lower_rule_term(pattern.2, source, prefixes)?,
    ))
}

fn lower_rule_term(
    token: &str,
    source: &str,
    prefixes: &HashMap<String, String>,
) -> Result<LexTerm, ProgramError> {
    if token.starts_with(['?', '$']) {
        return Ok(LexTerm::Variable(token[1..].to_string()));
    }
    let start = token.as_ptr() as usize;
    let base = source.as_ptr() as usize;
    let preceding = if start > base && start <= base + source.len() {
        source[..start - base].chars().next_back()
    } else {
        None
    };
    match preceding {
        Some('<') | Some('"') | Some('\'') if !token.starts_with('<') => {
            Ok(LexTerm::Constant(token.to_string()))
        }
        _ => expand_prefixed(token, prefixes).map(LexTerm::Constant),
    }
}

fn lower_filter(
    filter: &FilterExpression<'_>,
    bound: &HashSet<&str>,
    output: &mut Vec<FilterCondition>,
) -> Result<(), String> {
    match filter {
        FilterExpression::And(left, right) => {
            lower_filter(left, bound, output)?;
            lower_filter(right, bound, output)
        }
        FilterExpression::Comparison(variable, operator, value) => {
            let Some(name) = variable.strip_prefix(['?', '$']) else {
                return Err("FILTER must compare a variable with a number".into());
            };
            if !bound.contains(name) {
                return Err(format!("FILTER variable ?{name} is not bound by WHERE"));
            }
            if !matches!(*operator, "<" | ">" | "<=" | ">=" | "=" | "!=") {
                return Err(format!("FILTER operator {operator} is not supported"));
            }
            let number = value.trim().trim_matches('"');
            if number.parse::<f64>().map_or(true, |parsed| !parsed.is_finite()) {
                return Err("FILTER must compare a variable with a finite number".into());
            }
            output.push(FilterCondition {
                variable: name.to_string(),
                operator: operator.to_string(),
                value: number.to_string(),
            });
            Ok(())
        }
        _ => Err("only numeric comparisons joined by && are supported in program FILTERs".into()),
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ChoiceRow {
    probabilities: Vec<f64>,
    features: Option<Vec<f64>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeuralSnapshotMetadata {
    pub relation: String,
    pub model: String,
    pub artifact: String,
    pub artifact_sha256: String,
    pub feature_vars: Vec<String>,
    pub labels: Vec<String>,
    pub input_dim: usize,
    pub rows: usize,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PredictionSnapshot {
    domains: BTreeMap<String, Vec<String>>,
    groups: BTreeMap<(String, String), ChoiceRow>,
    independent: BTreeMap<(String, String, String), f64>,
    neural: Vec<NeuralSnapshotMetadata>,
}

impl PredictionSnapshot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty() && self.independent.is_empty()
    }

    pub fn choice_groups(&self) -> usize {
        self.groups.len()
    }

    pub fn independent_facts(&self) -> usize {
        self.independent.len()
    }

    pub fn labels(&self, relation: &str) -> Option<&[String]> {
        self.domains.get(relation).map(Vec::as_slice)
    }

    pub fn distribution(&self, relation: &str, anchor: &str) -> Option<&[f64]> {
        self.groups
            .get(&(relation.to_string(), anchor.to_string()))
            .map(|row| row.probabilities.as_slice())
    }

    pub fn anchors(&self, relation: &str) -> Vec<&str> {
        self.groups
            .keys()
            .filter(|(rel, _)| rel == relation)
            .map(|(_, anchor)| anchor.as_str())
            .collect()
    }

    pub fn neural_metadata(&self) -> &[NeuralSnapshotMetadata] {
        &self.neural
    }

    pub fn declare_exclusive_relation(
        &mut self,
        relation: &str,
        labels: &[String],
    ) -> Result<(), ProgramError> {
        check_plain_term(relation)?;
        if labels.is_empty() {
            return Err(ProgramError::Snapshot(format!("relation {relation} declares no labels")));
        }
        let mut seen = HashSet::new();
        for label in labels {
            check_plain_term(label)?;
            if !seen.insert(label) {
                return Err(ProgramError::Snapshot(format!(
                    "relation {relation} repeats label {label}"
                )));
            }
        }
        if self.independent.keys().any(|(_, predicate, _)| predicate == relation) {
            return Err(ProgramError::Snapshot(format!(
                "relation {relation} already carries independent facts"
            )));
        }
        match self.domains.get(relation) {
            Some(existing) if existing.as_slice() != labels => Err(ProgramError::Snapshot(format!(
                "relation {relation} was declared with a different label order or domain"
            ))),
            Some(_) => Ok(()),
            None => {
                self.domains.insert(relation.to_string(), labels.to_vec());
                Ok(())
            }
        }
    }

    pub fn add_distribution(
        &mut self,
        relation: &str,
        anchor: &str,
        probabilities: &[f64],
        features: Option<&[f64]>,
    ) -> Result<(), ProgramError> {
        check_plain_term(anchor)?;
        let labels = self.domains.get(relation).ok_or_else(|| {
            ProgramError::Snapshot(format!("relation {relation} has no declared label domain"))
        })?;
        if probabilities.len() != labels.len() {
            return Err(ProgramError::Snapshot(format!(
                "{anchor}: expected {} probabilities for {relation}, got {}",
                labels.len(),
                probabilities.len()
            )));
        }
        for probability in probabilities {
            check_probability(anchor, *probability)?;
        }
        let total: f64 = probabilities.iter().sum();
        if (total - 1.0).abs() > DISTRIBUTION_TOLERANCE {
            return Err(ProgramError::Snapshot(format!(
                "{anchor}: probabilities for {relation} sum to {total}, outside 1 ± {DISTRIBUTION_TOLERANCE}"
            )));
        }
        if let Some(features) = features {
            if features.iter().any(|value| !value.is_finite()) {
                return Err(ProgramError::Snapshot(format!("{anchor}: non-finite feature")));
            }
        }
        let row = ChoiceRow {
            probabilities: probabilities.to_vec(),
            features: features.map(<[f64]>::to_vec),
        };
        let key = (relation.to_string(), anchor.to_string());
        match self.groups.get(&key) {
            Some(existing) if *existing == row => Ok(()),
            Some(_) => Err(ProgramError::Snapshot(format!(
                "{anchor}: conflicting features or distributions for {relation}"
            ))),
            None => {
                self.groups.insert(key, row);
                Ok(())
            }
        }
    }

    pub fn add_independent(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
        probability: f64,
    ) -> Result<(), ProgramError> {
        for term in [subject, predicate, object] {
            check_plain_term(term)?;
        }
        check_probability(subject, probability)?;
        if self.domains.contains_key(predicate) {
            return Err(ProgramError::Snapshot(format!(
                "{subject} {predicate} {object}: an independent fact overlaps exclusive relation {predicate}"
            )));
        }
        let key = (subject.to_string(), predicate.to_string(), object.to_string());
        match self.independent.get(&key) {
            Some(existing) if existing.to_bits() == probability.to_bits() => Ok(()),
            Some(_) => Err(ProgramError::Snapshot(format!(
                "{subject} {predicate} {object}: conflicting independent probabilities"
            ))),
            None => {
                self.independent.insert(key, probability);
                Ok(())
            }
        }
    }

    pub fn merge(&mut self, other: &PredictionSnapshot) -> Result<(), ProgramError> {
        for (relation, labels) in &other.domains {
            self.declare_exclusive_relation(relation, labels)?;
        }
        for ((relation, anchor), row) in &other.groups {
            self.add_distribution(relation, anchor, &row.probabilities, row.features.as_deref())?;
        }
        for ((subject, predicate, object), probability) in &other.independent {
            self.add_independent(subject, predicate, object, *probability)?;
        }
        for metadata in &other.neural {
            if !self.neural.contains(metadata) {
                self.neural.push(metadata.clone());
            }
        }
        Ok(())
    }

    fn has_choice(&self, relation: &str, anchor: &str) -> bool {
        self.groups
            .contains_key(&(relation.to_string(), anchor.to_string()))
    }
}

fn check_plain_term(term: &str) -> Result<(), ProgramError> {
    if term.is_empty() || term.starts_with("<<") {
        return Err(ProgramError::Snapshot(format!(
            "`{term}` is not a supported snapshot term"
        )));
    }
    Ok(())
}

fn check_probability(context: &str, probability: f64) -> Result<(), ProgramError> {
    if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
        return Err(ProgramError::Snapshot(format!(
            "{context}: probability {probability} is not a finite value in [0, 1]"
        )));
    }
    Ok(())
}

fn checked_seed_count(
    counts: impl IntoIterator<Item = usize>,
    limit: usize,
) -> Result<u32, ProgramError> {
    let mut total = 0usize;
    for count in counts {
        total = total
            .checked_add(count)
            .ok_or(ProgramError::Resource(ResourceError::IdOverflow))?;
    }
    if total > limit {
        return Err(ProgramError::Resource(ResourceError::SeedLimit {
            requested: total,
            limit,
        }));
    }
    u32::try_from(total).map_err(|_| ProgramError::Resource(ResourceError::IdOverflow))
}

fn allocate_seed_specs(
    snapshot: &PredictionSnapshot,
    dictionary: &mut Dictionary,
    limit: usize,
) -> Result<Vec<SeedSpec>, ProgramError> {
    let group_sizes = snapshot.groups.values().map(|row| row.probabilities.len());
    let total = checked_seed_count(
        std::iter::once(snapshot.independent.len()).chain(group_sizes),
        limit,
    )?;
    let mut specs = Vec::with_capacity(snapshot.independent.len() + snapshot.groups.len());
    let mut next = 0u32;
    let mut take_id = || -> Result<u32, ProgramError> {
        let id = next;
        next = next
            .checked_add(1)
            .ok_or(ProgramError::Resource(ResourceError::IdOverflow))?;
        Ok(id)
    };
    for ((subject, predicate, object), probability) in &snapshot.independent {
        specs.push(SeedSpec::Independent {
            triple: Triple {
                subject: dictionary.encode(subject),
                predicate: dictionary.encode(predicate),
                object: dictionary.encode(object),
            },
            prob: *probability,
            seed_id: take_id()?,
        });
    }
    for (group_index, ((relation, anchor), row)) in snapshot.groups.iter().enumerate() {
        let labels = &snapshot.domains[relation];
        let subject = dictionary.encode(anchor);
        let predicate = dictionary.encode(relation);
        let mut choices = Vec::with_capacity(labels.len());
        for (label, probability) in labels.iter().zip(&row.probabilities) {
            choices.push(ExclusiveChoice {
                triple: Triple {
                    subject,
                    predicate,
                    object: dictionary.encode(label),
                },
                prob: *probability,
                choice_id: take_id()?,
            });
        }
        specs.push(SeedSpec::ExclusiveGroup {
            group_id: u32::try_from(group_index)
                .map_err(|_| ProgramError::Resource(ResourceError::IdOverflow))?,
            choices,
        });
    }
    debug_assert_eq!(next, total);
    Ok(specs)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coverage {
    pub prediction_rows: usize,
    pub predicted_choices: usize,
    pub required_choices: usize,
    pub confirmed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrainingReport {
    pub model: String,
    pub relation: String,
    pub artifact: String,
    pub artifact_sha256: String,
    pub seed: Option<u64>,
    pub data_fetch: Duration,
    pub learning: Duration,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProgramTimings {
    pub validation: Duration,
    pub training: Duration,
    pub prediction_input: Duration,
    pub neural_inference: Duration,
    pub prediction_publication: Duration,
    pub data_preparation: Duration,
    pub logical_inference: Duration,
    pub probability_recovery: Duration,
    pub result_publication: Duration,
    pub artifact_publication: Duration,
    pub total: Duration,
}

pub struct ProgramResult {
    dataset: SparqlDatabase,
    hypotheses: HashMap<Triple, f64>,
    asserted: HashSet<Triple>,
    snapshot: PredictionSnapshot,
    coverage: Coverage,
    training: Vec<TrainingReport>,
    timings: ProgramTimings,
    provenance: Option<TagStore<SddProvenance>>,
    mode: Option<RuleMode>,
    derived: usize,
}

impl fmt::Debug for ProgramResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProgramResult")
            .field("hypotheses", &self.hypotheses.len())
            .field("derived", &self.derived)
            .field("coverage", &self.coverage)
            .field("mode", &self.mode)
            .finish()
    }
}

pub struct PreparedAnswers {
    provenance: SddProvenance,
    tags: Vec<Option<SddId>>,
}

impl PreparedAnswers {
    pub fn recover(&self) -> Vec<f64> {
        self.tags
            .iter()
            .map(|tag| match tag {
                Some(tag) => self.provenance.recover_probability(tag),
                None => 0.0,
            })
            .collect()
    }
}

impl ProgramResult {
    pub fn dataset(&self) -> &SparqlDatabase {
        &self.dataset
    }

    pub fn snapshot(&self) -> &PredictionSnapshot {
        &self.snapshot
    }

    pub fn coverage(&self) -> &Coverage {
        &self.coverage
    }

    pub fn training(&self) -> &[TrainingReport] {
        &self.training
    }

    pub fn timings(&self) -> &ProgramTimings {
        &self.timings
    }

    pub fn mode(&self) -> Option<RuleMode> {
        self.mode
    }

    pub fn derived_facts(&self) -> usize {
        self.derived
    }

    pub fn hypothesis_count(&self) -> usize {
        self.hypotheses.len()
    }

    fn lookup(&self, subject: &str, predicate: &str, object: &str) -> Option<Triple> {
        let dictionary = self.dataset.dictionary.read().unwrap();
        Some(Triple {
            subject: *dictionary.string_to_id.get(subject)?,
            predicate: *dictionary.string_to_id.get(predicate)?,
            object: *dictionary.string_to_id.get(object)?,
        })
    }

    pub fn fact_probability(&self, subject: &str, predicate: &str, object: &str) -> Option<f64> {
        let triple = self.lookup(subject, predicate, object)?;
        if let Some(probability) = self.hypotheses.get(&triple) {
            Some(*probability)
        } else if self.asserted.contains(&triple) {
            Some(1.0)
        } else {
            None
        }
    }

    pub fn answer_probability(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
    ) -> Result<f64, ProgramError> {
        match self.fact_probability(subject, predicate, object) {
            Some(probability) => Ok(probability),
            None if self.coverage.confirmed => Ok(0.0),
            None => Err(ProgramError::Coverage(format!(
                "{subject} {predicate} {object} is absent and prediction coverage is not confirmed"
            ))),
        }
    }

    pub fn answer_set(&self, predicate: &str) -> Vec<(String, String, f64)> {
        let Some(predicate_id) = self
            .dataset
            .dictionary
            .read()
            .unwrap()
            .string_to_id
            .get(predicate)
            .copied()
        else {
            return Vec::new();
        };
        let mut answers: Vec<_> = self
            .hypotheses
            .iter()
            .map(|(triple, probability)| (triple, *probability))
            .chain(self.asserted.iter().map(|triple| (triple, 1.0)))
            .filter(|(triple, _)| triple.predicate == predicate_id)
            .map(|(triple, probability)| {
                (
                    self.dataset.decode_any(triple.subject).unwrap_or_default(),
                    self.dataset.decode_any(triple.object).unwrap_or_default(),
                    probability,
                )
            })
            .collect();
        answers.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        answers
    }

    pub fn prepare_answers(
        &self,
        answers: &[(String, String, String)],
    ) -> Result<PreparedAnswers, ProgramError> {
        let tags = self.provenance.as_ref().ok_or_else(|| {
            ProgramError::Execution("provenance was not retained; set retain_provenance".into())
        })?;
        let mut prepared = Vec::with_capacity(answers.len());
        for (subject, predicate, object) in answers {
            let tag = match self.lookup(subject, predicate, object) {
                Some(triple) if self.hypotheses.contains_key(&triple) || self.asserted.contains(&triple) => {
                    Some(tags.get_tag(&triple))
                }
                _ if self.coverage.confirmed => None,
                _ => {
                    return Err(ProgramError::Coverage(format!(
                        "{subject} {predicate} {object} is absent and prediction coverage is not confirmed"
                    )))
                }
            };
            prepared.push(tag);
        }
        Ok(PreparedAnswers {
            provenance: tags.provenance().clone(),
            tags: prepared,
        })
    }
}

#[derive(Default)]
pub struct PublishedProgramResult {
    current: Option<ProgramResult>,
}

impl PublishedProgramResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn current(&self) -> Option<&ProgramResult> {
        self.current.as_ref()
    }

    pub fn current_mut(&mut self) -> Option<&mut ProgramResult> {
        self.current.as_mut()
    }

    pub fn publish(
        &mut self,
        outcome: Result<ProgramResult, ProgramError>,
    ) -> Result<&mut ProgramResult, ProgramError> {
        let result = outcome?;
        Ok(self.current.insert(result))
    }
}

pub fn query_program_result(
    result: &mut ProgramResult,
    select: &str,
) -> Result<Vec<Vec<String>>, ProgramError> {
    let (remaining, parsed) = parse_combined_query(select)
        .map_err(|error| ProgramError::Parse(format_parse_error(select, error)))?;
    let plain_select = remaining.trim().is_empty()
        && matches!(parsed.sparql, Some(SparqlOperation::Select(_)))
        && parsed.rules.is_empty()
        && parsed.ml_predict.is_none()
        && parsed.model_decls.is_empty()
        && parsed.neural_relation_decls.is_empty()
        && parsed.train_neural_relation_decls.is_empty()
        && parsed.retrieve_clause.is_none()
        && parsed.register_clause.is_none();
    if !plain_select {
        return Err(ProgramError::Unsupported(
            "query_program_result accepts exactly one plain SELECT query".into(),
        ));
    }
    crate::execute_query::execute_sparql_query(select, &mut result.dataset)
        .map_err(ProgramError::Execution)
}

pub fn parse_probability_literal(term: &str) -> Option<f64> {
    let lexical = term.strip_prefix('"')?;
    let (value, datatype) = lexical.split_once('"')?;
    if datatype != format!("^^<{XSD_DOUBLE_IRI}>") {
        return None;
    }
    value.parse().ok()
}

fn probability_literal(probability: f64) -> String {
    format!("\"{probability}\"^^<{XSD_DOUBLE_IRI}>")
}

struct Deadline(Option<Instant>);

impl Deadline {
    fn check(&self) -> Result<(), ProgramError> {
        match self.0 {
            Some(deadline) if Instant::now() >= deadline => {
                Err(ProgramError::Resource(ResourceError::DeadlineExceeded))
            }
            _ => Ok(()),
        }
    }
}

pub fn execute_program(
    asserted_input: &mut SparqlDatabase,
    program: &CompiledProgram,
    options: &ProgramOptions,
) -> Result<ProgramResult, ProgramError> {
    let previous = std::mem::replace(&mut asserted_input.implicit_neural_materialization, false);
    let outcome = run_program(asserted_input, program, options);
    asserted_input.implicit_neural_materialization = previous;
    outcome
}

fn run_program(
    input: &mut SparqlDatabase,
    program: &CompiledProgram,
    options: &ProgramOptions,
) -> Result<ProgramResult, ProgramError> {
    let started = Instant::now();
    let deadline = Deadline(options.limits.deadline.map(|limit| started + limit));
    let mut timings = ProgramTimings::default();
    deadline.check()?;

    let phase = Instant::now();
    let neural_plan = plan_neural(input, program, options)?;
    timings.validation = phase.elapsed();
    deadline.check()?;

    let mut snapshot = PredictionSnapshot::new();
    if let Some(frozen) = &options.frozen_predictions {
        snapshot.merge(frozen)?;
    }
    let mut coverage = Coverage {
        prediction_rows: 0,
        predicted_choices: 0,
        required_choices: 0,
        confirmed: false,
    };
    let neural_run = run_neural(input, program, options, neural_plan, &mut snapshot, &mut coverage, &mut timings, &deadline)?;

    let input_seeds = input.probability_seeds.clone();
    for (triple, probability) in &input_seeds {
        let decode = |id| {
            input
                .decode_any(id)
                .ok_or_else(|| ProgramError::Snapshot(format!("tagged input term {id} is missing")))
        };
        snapshot.add_independent(
            &decode(triple.subject)?,
            &decode(triple.predicate)?,
            &decode(triple.object)?,
            *probability,
        )?;
    }

    if let Some(required) = &options.required_choices {
        let missing: Vec<_> = required
            .iter()
            .filter(|(relation, anchor)| !snapshot.has_choice(relation, anchor))
            .collect();
        if !missing.is_empty() {
            return Err(ProgramError::Coverage(format!(
                "{} required choice(s) have no distribution, first {} {}",
                missing.len(),
                missing[0].0,
                missing[0].1
            )));
        }
        coverage.required_choices = required.len();
    }
    coverage.confirmed = options.required_choices.is_some()
        || (program.prediction.is_none() && snapshot.groups.is_empty());

    let mode = program.mode;
    if mode == Some(RuleMode::Deterministic) && !snapshot.is_empty() {
        return Err(ProgramError::Unsupported(
            "plain rules cannot run over uncertain inputs; annotate every rule with PROB(combination=sdd)".into(),
        ));
    }
    deadline.check()?;

    let phase = Instant::now();
    let dictionary = Arc::new(RwLock::new(Dictionary::new()));
    let quoted = Arc::new(RwLock::new(QuotedTripleStore::new()));
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(&dictionary);
    let asserted = copy_asserted_input(
        input,
        program,
        &snapshot,
        &input_seeds,
        &dictionary,
        &quoted,
        &mut reasoner,
    );
    let seeds = {
        let mut dict = dictionary.write().unwrap();
        for rule in &program.rules {
            reasoner.add_rule(rule.encode(&mut dict));
        }
        if mode == Some(RuleMode::Deterministic) {
            Vec::new()
        } else {
            allocate_seed_specs(&snapshot, &mut dict, options.limits.max_seed_variables)?
        }
    };
    let mut seed_triples = Vec::new();
    for spec in &seeds {
        match spec {
            SeedSpec::Independent { triple, .. } => seed_triples.push(triple.clone()),
            SeedSpec::ExclusiveGroup { choices, .. } => {
                seed_triples.extend(choices.iter().map(|choice| choice.triple.clone()))
            }
        }
    }
    for triple in &seed_triples {
        if asserted.contains(triple) {
            let dict = dictionary.read().unwrap();
            return Err(ProgramError::Snapshot(format!(
                "probabilistic seed {} is also asserted as a certain fact",
                dict.decode_triple(triple)
            )));
        }
    }
    timings.data_preparation = phase.elapsed();
    deadline.check()?;

    let phase = Instant::now();
    let mut hypotheses = HashMap::new();
    let mut provenance = None;
    let derived;
    match mode {
        Some(RuleMode::Sdd) => {
            let limits = SddLimits {
                max_nodes: options.limits.max_sdd_nodes,
                deadline: deadline.0,
            };
            let (facts, tags) = try_infer_new_facts_with_sdd_seed_specs(&mut reasoner, seeds, limits)?;
            timings.logical_inference = phase.elapsed();
            derived = facts.len();

            let phase = Instant::now();
            let semiring = tags.provenance().clone();
            for triple in seed_triples.iter().chain(facts.iter()) {
                let tag = tags.get_tag(triple);
                if tag == semiring.zero() {
                    continue;
                }
                hypotheses.insert(triple.clone(), semiring.recover_probability(&tag));
            }
            timings.probability_recovery = phase.elapsed();
            provenance = Some(tags);
        }
        Some(RuleMode::Deterministic) => {
            let facts = reasoner.infer_new_facts_semi_naive();
            timings.logical_inference = phase.elapsed();
            deadline.check()?;
            derived = facts.len();
            hypotheses.extend(facts.into_iter().map(|triple| (triple, 1.0)));
        }
        None => {
            derived = 0;
            for spec in &seeds {
                match spec {
                    SeedSpec::Independent { triple, prob, .. } => {
                        hypotheses.insert(triple.clone(), *prob);
                    }
                    SeedSpec::ExclusiveGroup { choices, .. } => {
                        for choice in choices {
                            hypotheses.insert(choice.triple.clone(), choice.prob);
                        }
                    }
                }
            }
        }
    }
    deadline.check()?;

    let phase = Instant::now();
    let mut dataset = SparqlDatabase::new();
    dataset.dictionary = Arc::clone(&dictionary);
    dataset.quoted_triple_store = Arc::clone(&quoted);
    dataset.prefixes = program.prefixes.clone();
    dataset.implicit_neural_materialization = false;
    {
        let mut dict = dictionary.write().unwrap();
        let mut qt = quoted.write().unwrap();
        let prob_value = dict.encode(PROB_VALUE_IRI);
        let mut annotations = Vec::with_capacity(hypotheses.len());
        for (triple, probability) in &hypotheses {
            annotations.push(Triple {
                subject: qt.encode(triple.subject, triple.predicate, triple.object),
                predicate: prob_value,
                object: dict.encode(&probability_literal(*probability)),
            });
        }
        if options.explain {
            if let Some(tags) = &provenance {
                annotations.extend(tags.encode_as_rdf_star_with_explanation(&mut dict, &mut qt));
            }
        }
        drop(qt);
        drop(dict);
        for triple in hypotheses.keys().cloned().chain(annotations) {
            dataset.add_triple(triple);
        }
    }
    timings.result_publication = phase.elapsed();
    deadline.check()?;

    let phase = Instant::now();
    let training = publish_neural(input, program, neural_run)?;
    timings.artifact_publication = phase.elapsed();
    timings.total = started.elapsed();

    Ok(ProgramResult {
        dataset,
        hypotheses,
        asserted,
        snapshot,
        coverage,
        training,
        timings,
        provenance: if options.retain_provenance { provenance } else { None },
        mode,
        derived,
    })
}

fn copy_asserted_input(
    input: &SparqlDatabase,
    program: &CompiledProgram,
    snapshot: &PredictionSnapshot,
    input_seeds: &HashMap<Triple, f64>,
    dictionary: &Arc<RwLock<Dictionary>>,
    quoted: &Arc<RwLock<QuotedTripleStore>>,
    reasoner: &mut Reasoner,
) -> HashSet<Triple> {
    let mut asserted = HashSet::new();
    let mut predicates: HashSet<&str> = snapshot
        .domains
        .keys()
        .map(String::as_str)
        .chain(snapshot.independent.keys().map(|(_, predicate, _)| predicate.as_str()))
        .collect();
    let mut copy_all = false;
    for rule in &program.rules {
        for (_, predicate, _) in &rule.premise {
            match predicate {
                LexTerm::Constant(value) => {
                    predicates.insert(value.as_str());
                }
                LexTerm::Variable(_) => copy_all = true,
            }
        }
    }
    let source_triples: Vec<Triple> = if copy_all {
        input.query_default_triples(None, None, None)
    } else {
        let ids: Vec<u32> = {
            let dict = input.dictionary.read().unwrap();
            predicates
                .iter()
                .filter_map(|predicate| dict.string_to_id.get(*predicate).copied())
                .collect()
        };
        ids.into_iter()
            .flat_map(|id| input.query_default_triples(None, Some(id), None))
            .collect()
    };

    let source_dictionary = input.dictionary.read().unwrap();
    let source_quoted = input.quoted_triple_store.read().unwrap();
    let mut target_dictionary = dictionary.write().unwrap();
    let mut target_quoted = quoted.write().unwrap();
    let mut translated = HashMap::new();
    for triple in source_triples {
        if input_seeds.contains_key(&triple) {
            continue;
        }
        let mut remap = |id| {
            reencode_term_id(
                id,
                &source_dictionary,
                &source_quoted,
                &mut target_dictionary,
                &mut target_quoted,
                &mut translated,
            )
        };
        let copied = Triple {
            subject: remap(triple.subject),
            predicate: remap(triple.predicate),
            object: remap(triple.object),
        };
        reasoner.insert_ground_triple(copied.clone());
        asserted.insert(copied);
    }
    asserted
}

#[cfg(not(feature = "ml"))]
struct NeuralPlan;

#[cfg(not(feature = "ml"))]
struct NeuralRun;

#[cfg(not(feature = "ml"))]
fn plan_neural(
    _input: &SparqlDatabase,
    program: &CompiledProgram,
    _options: &ProgramOptions,
) -> Result<NeuralPlan, ProgramError> {
    if program.prediction.is_some()
        || !program.train_decls.is_empty()
        || !program.model_decls.is_empty()
        || !program.relation_decls.is_empty()
    {
        return Err(ProgramError::Policy("ML_FEATURE_DISABLED".into()));
    }
    Ok(NeuralPlan)
}

#[cfg(not(feature = "ml"))]
#[allow(clippy::too_many_arguments)]
fn run_neural(
    _input: &mut SparqlDatabase,
    _program: &CompiledProgram,
    _options: &ProgramOptions,
    _plan: NeuralPlan,
    _snapshot: &mut PredictionSnapshot,
    _coverage: &mut Coverage,
    _timings: &mut ProgramTimings,
    _deadline: &Deadline,
) -> Result<NeuralRun, ProgramError> {
    Ok(NeuralRun)
}

#[cfg(not(feature = "ml"))]
fn publish_neural(
    _input: &mut SparqlDatabase,
    _program: &CompiledProgram,
    _run: NeuralRun,
) -> Result<Vec<TrainingReport>, ProgramError> {
    Ok(Vec::new())
}

#[cfg(feature = "ml")]
struct TrainPlan {
    decl: TrainNeuralRelationDecl,
    relation: NeuralRelationDecl,
    model: ModelDecl,
    save_path: String,
}

#[cfg(feature = "ml")]
struct PredictPlan {
    relation: NeuralRelationDecl,
    model: ModelDecl,
    labels: Vec<String>,
    artifact: Option<String>,
}

#[cfg(feature = "ml")]
struct NeuralPlan {
    models: HashMap<String, ModelDecl>,
    relations: HashMap<String, NeuralRelationDecl>,
    trains: Vec<TrainPlan>,
    predict: Option<PredictPlan>,
}

#[cfg(feature = "ml")]
struct StagedModel {
    report: TrainingReport,
    save_path: String,
    bytes: Vec<u8>,
}

#[cfg(feature = "ml")]
struct NeuralRun {
    staged: Vec<StagedModel>,
}

#[cfg(feature = "ml")]
fn ml_error(error: impl fmt::Display) -> ProgramError {
    ProgramError::Execution(error.to_string())
}

#[cfg(feature = "ml")]
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(feature = "ml")]
fn exclusive_labels(model: &ModelDecl, prefixes: &HashMap<String, String>) -> Result<Vec<String>, ProgramError> {
    match &model.output_kind {
        NeuralOutputKind::Exclusive { labels } => labels
            .iter()
            .map(|label| normalize_term(label, prefixes))
            .collect(),
        NeuralOutputKind::Binary { positive_literal } => {
            Ok(vec![normalize_term(positive_literal, prefixes)?])
        }
    }
}

#[cfg(feature = "ml")]
fn plan_neural(
    input: &SparqlDatabase,
    program: &CompiledProgram,
    options: &ProgramOptions,
) -> Result<NeuralPlan, ProgramError> {
    use shared::query::LossFn;

    let uses_ml = program.prediction.is_some()
        || !program.train_decls.is_empty()
        || !program.model_decls.is_empty()
        || !program.relation_decls.is_empty();
    let mut models = input.model_decls.clone();
    let mut relations = input.neural_relation_decls.clone();
    if uses_ml {
        input
            .ml_context
            .require_local()
            .map_err(|error| ProgramError::Policy(error.to_string()))?;
    }
    for decl in &program.model_decls {
        models.insert(decl.name.clone(), decl.clone());
    }
    for decl in &program.relation_decls {
        relations.insert(decl.predicate.clone(), decl.clone());
    }
    let mut prefixes = input.prefixes.clone();
    prefixes.extend(program.prefixes.clone());

    for relation in &program.relation_decls {
        if !models.contains_key(&relation.model_name) {
            return Err(ProgramError::Unsupported(format!(
                "NEURAL RELATION {} uses undeclared MODEL \"{}\"",
                relation.predicate, relation.model_name
            )));
        }
    }

    let mut trains = Vec::new();
    let mut trained_models = HashSet::new();
    for decl in &program.train_decls {
        let relation = relations.get(&decl.predicate).cloned().ok_or_else(|| {
            ProgramError::Unsupported(format!("TRAIN names unknown NEURAL RELATION {}", decl.predicate))
        })?;
        let model = models.get(&relation.model_name).cloned().ok_or_else(|| {
            ProgramError::Unsupported(format!("MODEL \"{}\" is not declared", relation.model_name))
        })?;
        let (subject, predicate, object) = &decl.target_triple;
        if subject != &relation.anchor_var || predicate != &relation.predicate {
            return Err(ProgramError::Unsupported(format!(
                "TRAIN {}: TARGET must be {{ {} <{}> ... }} for digit-supervised training",
                decl.predicate, relation.anchor_var, relation.predicate
            )));
        }
        match &model.output_kind {
            NeuralOutputKind::Exclusive { .. } => {
                if object != &decl.label_var || !matches!(decl.loss, LossFn::CrossEntropy | LossFn::Nll) {
                    return Err(ProgramError::Unsupported(format!(
                        "TRAIN {}: exclusive models are trained with LOSS cross_entropy and TARGET object {}",
                        decl.predicate, decl.label_var
                    )));
                }
            }
            NeuralOutputKind::Binary { positive_literal } => {
                if object != &normalize_term(positive_literal, &prefixes)? || decl.loss != LossFn::BinaryCrossEntropy {
                    return Err(ProgramError::Unsupported(format!(
                        "TRAIN {}: binary models are trained with LOSS binary_cross_entropy and the positive TARGET",
                        decl.predicate
                    )));
                }
            }
        }
        if decl.epochs == 0 || decl.batch_size == 0 || !decl.learning_rate.is_finite() || decl.learning_rate <= 0.0 {
            return Err(ProgramError::Unsupported(format!(
                "TRAIN {}: EPOCHS, BATCH_SIZE and LEARNING_RATE must be positive",
                decl.predicate
            )));
        }
        if !trained_models.insert(model.name.clone()) {
            return Err(ProgramError::Unsupported(format!(
                "MODEL \"{}\" is trained twice in one program",
                model.name
            )));
        }
        let save_path = decl
            .save_path
            .clone()
            .unwrap_or_else(|| crate::neural_relations::default_model_artifact_path(&model.name));
        let path = input
            .ml_context
            .local_artifact(&save_path)
            .map_err(|error| ProgramError::Policy(error.to_string()))?;
        if path.exists() {
            return Err(ProgramError::Policy(format!(
                "artifact {save_path} already exists; artifacts are never overwritten"
            )));
        }
        trains.push(TrainPlan {
            decl: decl.clone(),
            relation,
            model,
            save_path,
        });
    }

    let predict = match &program.prediction {
        None => None,
        Some(prediction) => {
            let matching: Vec<_> = relations
                .values()
                .filter(|relation| relation.model_name == prediction.model)
                .cloned()
                .collect();
            let relation = match matching.as_slice() {
                [relation] => relation.clone(),
                [] => {
                    return Err(ProgramError::Unsupported(format!(
                        "ML.PREDICT MODEL \"{}\" has no NEURAL RELATION",
                        prediction.model
                    )))
                }
                _ => {
                    return Err(ProgramError::Unsupported(format!(
                        "ML.PREDICT MODEL \"{}\" matches {} NEURAL RELATIONs; the relation must be unambiguous",
                        prediction.model,
                        matching.len()
                    )))
                }
            };
            let model = models.get(&prediction.model).cloned().ok_or_else(|| {
                ProgramError::Unsupported(format!("MODEL \"{}\" is not declared", prediction.model))
            })?;
            for var in relation.feature_vars.iter().chain([&relation.anchor_var]) {
                let name = var.trim_start_matches('?');
                if !prediction.columns.iter().any(|column| column == name) {
                    return Err(ProgramError::Unsupported(format!(
                        "ML.PREDICT INPUT must SELECT {var} for NEURAL RELATION {}",
                        relation.predicate
                    )));
                }
            }
            let artifact = if trained_models.contains(&model.name) {
                None
            } else {
                let artifact = input.neural_model_artifacts.get(&model.name).cloned().ok_or_else(|| {
                    ProgramError::Unsupported(format!(
                        "MODEL \"{}\" has no trained artifact; add TRAIN NEURAL RELATION",
                        model.name
                    ))
                })?;
                input
                    .ml_context
                    .local_artifact(&artifact)
                    .map_err(|error| ProgramError::Policy(error.to_string()))?;
                Some(artifact)
            };
            if let Some(size) = options.prediction_batch_size {
                if size == 0 {
                    return Err(ProgramError::Unsupported("prediction_batch_size must be positive".into()));
                }
            }
            let labels = exclusive_labels(&model, &prefixes)?;
            Some(PredictPlan {
                relation,
                model,
                labels,
                artifact,
            })
        }
    };

    Ok(NeuralPlan {
        models,
        relations,
        trains,
        predict,
    })
}

#[cfg(feature = "ml")]
#[allow(clippy::too_many_arguments)]
fn run_neural(
    input: &mut SparqlDatabase,
    program: &CompiledProgram,
    options: &ProgramOptions,
    plan: NeuralPlan,
    snapshot: &mut PredictionSnapshot,
    coverage: &mut Coverage,
    timings: &mut ProgramTimings,
    deadline: &Deadline,
) -> Result<NeuralRun, ProgramError> {
    use crate::neural_relations::{model_hidden_layers, model_output_type};
    use ml::MlpNeuralPredicate;

    let phase = Instant::now();
    let mut staged = Vec::new();
    let mut in_memory: HashMap<String, MlpNeuralPredicate> = HashMap::new();
    for train in &plan.trains {
        deadline.check()?;
        let mut view = SparqlDatabase::new();
        view.model_decls = plan.models.clone();
        view.neural_relation_decls = plan.relations.clone();
        let mut clause = crate::neural_relations::lower_train_decl_to_owned(&view, &train.decl)
            .map_err(ProgramError::Execution)?;
        clause.save_path = None;
        let (model, neural_timings) = crate::execute_ml_train::execute_supervised_training(
            &clause,
            model_hidden_layers(&train.model),
            input,
            options.training_seed,
        )
        .map_err(ml_error)?;
        let bytes = model.to_bytes().map_err(ml_error)?;
        let model = MlpNeuralPredicate::from_bytes(
            train.relation.feature_vars.len(),
            model_hidden_layers(&train.model),
            model_output_type(&train.model),
            &bytes,
        )
        .map_err(ml_error)?;
        let report = TrainingReport {
            model: train.model.name.clone(),
            relation: train.relation.predicate.clone(),
            artifact: train.save_path.clone(),
            artifact_sha256: sha256_hex(&bytes),
            seed: options.training_seed,
            data_fetch: neural_timings.data_fetch,
            learning: neural_timings.learning,
        };
        in_memory.insert(train.model.name.clone(), model);
        staged.push(StagedModel {
            report,
            save_path: train.save_path.clone(),
            bytes,
        });
    }
    timings.training = phase.elapsed();
    deadline.check()?;

    if let (Some(predict), Some(prediction)) = (&plan.predict, &program.prediction) {
        let loaded;
        let (model, artifact, artifact_sha256) = match &predict.artifact {
            None => {
                let staged_model = staged
                    .iter()
                    .find(|staged| staged.report.model == predict.model.name)
                    .ok_or_else(|| ProgramError::Execution("staged model is missing".into()))?;
                (
                    &in_memory[&predict.model.name],
                    staged_model.save_path.clone(),
                    staged_model.report.artifact_sha256.clone(),
                )
            }
            Some(artifact) => {
                let path = input
                    .ml_context
                    .local_artifact(artifact)
                    .map_err(|error| ProgramError::Policy(error.to_string()))?;
                let bytes = std::fs::read(&path).map_err(ml_error)?;
                loaded = MlpNeuralPredicate::from_bytes(
                    predict.relation.feature_vars.len(),
                    model_hidden_layers(&predict.model),
                    model_output_type(&predict.model),
                    &bytes,
                )
                .map_err(ml_error)?;
                (&loaded, artifact.clone(), sha256_hex(&bytes))
            }
        };

        let phase = Instant::now();
        let rows = crate::execute_query::execute_sparql_query(&prediction.query, input)
            .map_err(ProgramError::Execution)?;
        let column = |var: &str| {
            let name = var.trim_start_matches('?');
            prediction.columns.iter().position(|column| column == name)
        };
        let anchor_column = column(&predict.relation.anchor_var)
            .ok_or_else(|| ProgramError::Execution("anchor column is missing".into()))?;
        let feature_columns = predict
            .relation
            .feature_vars
            .iter()
            .map(|var| column(var).ok_or_else(|| ProgramError::Execution(format!("{var} is missing"))))
            .collect::<Result<Vec<_>, _>>()?;
        let mut anchors = Vec::with_capacity(rows.len());
        let mut features = Vec::with_capacity(rows.len());
        for row in &rows {
            if row.len() != prediction.columns.len() {
                return Err(ProgramError::Execution("prediction input row has the wrong width".into()));
            }
            anchors.push(row[anchor_column].clone());
            features.push(
                feature_columns
                    .iter()
                    .map(|index| crate::ml_feature_loader::rdf_term_to_f64(&row[*index]).map_err(ml_error))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        timings.prediction_input = phase.elapsed();
        deadline.check()?;

        let phase = Instant::now();
        let mut probabilities = Vec::with_capacity(features.len());
        if !features.is_empty() {
            let batch = options.prediction_batch_size.unwrap_or(features.len());
            for chunk in features.chunks(batch) {
                probabilities.extend(model.predict(chunk).map_err(ml_error)?);
            }
        }
        timings.neural_inference = phase.elapsed();
        if probabilities.len() != rows.len() {
            return Err(ProgramError::Execution(format!(
                "the model returned {} predictions for {} input rows",
                probabilities.len(),
                rows.len()
            )));
        }

        let phase = Instant::now();
        let relation = &predict.relation.predicate;
        let mut predicted = PredictionSnapshot::new();
        match &predict.model.output_kind {
            NeuralOutputKind::Exclusive { .. } => {
                predicted.declare_exclusive_relation(relation, &predict.labels)?;
                for ((anchor, row), probs) in anchors.iter().zip(&features).zip(&probabilities) {
                    predicted.add_distribution(relation, anchor, probs, Some(row))?;
                }
            }
            NeuralOutputKind::Binary { .. } => {
                for (anchor, probs) in anchors.iter().zip(&probabilities) {
                    let [probability] = probs.as_slice() else {
                        return Err(ProgramError::Snapshot("a binary model must return one probability".into()));
                    };
                    predicted.add_independent(anchor, relation, &predict.labels[0], *probability)?;
                }
            }
        }
        predicted.neural.push(NeuralSnapshotMetadata {
            relation: relation.clone(),
            model: predict.model.name.clone(),
            artifact,
            artifact_sha256,
            feature_vars: predict.relation.feature_vars.clone(),
            labels: predict.labels.clone(),
            input_dim: predict.relation.feature_vars.len(),
            rows: rows.len(),
        });
        coverage.prediction_rows = rows.len();
        coverage.predicted_choices = predicted.groups.len() + predicted.independent.len();
        snapshot.merge(&predicted)?;
        timings.prediction_publication = phase.elapsed();
    }

    Ok(NeuralRun { staged })
}

#[cfg(feature = "ml")]
fn publish_neural(
    input: &mut SparqlDatabase,
    program: &CompiledProgram,
    run: NeuralRun,
) -> Result<Vec<TrainingReport>, ProgramError> {
    if program.model_decls.is_empty()
        && program.relation_decls.is_empty()
        && program.train_decls.is_empty()
    {
        return Ok(Vec::new());
    }
    for staged in &run.staged {
        input
            .ml_context
            .save_local_artifact(&staged.save_path, &staged.bytes)
            .map_err(|error| ProgramError::Policy(error.to_string()))?;
    }
    let prefixes = program.prefixes.clone();
    crate::neural_relations::register_neural_declarations_checked(
        input,
        &prefixes,
        &program.model_decls,
        &program.relation_decls,
        &[],
    )
    .map_err(ProgramError::Policy)?;
    for (staged, decl) in run.staged.iter().zip(&program.train_decls) {
        input
            .neural_model_artifacts
            .insert(staged.report.model.clone(), staged.save_path.clone());
        let mut registered = decl.clone();
        registered.save_path = Some(staged.save_path.clone());
        input
            .train_neural_relation_decls
            .insert(registered.predicate.clone(), registered);
    }
    Ok(run.staged.into_iter().map(|staged| staged.report).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_seed_counts_fail_before_allocation() {
        assert_eq!(
            checked_seed_count([10, 5], 12),
            Err(ProgramError::Resource(ResourceError::SeedLimit {
                requested: 15,
                limit: 12
            }))
        );
        assert_eq!(checked_seed_count([10, 5], 15), Ok(15));
        assert_eq!(
            checked_seed_count([usize::MAX, 1], usize::MAX),
            Err(ProgramError::Resource(ResourceError::IdOverflow))
        );
        assert_eq!(
            checked_seed_count([u32::MAX as usize + 1], usize::MAX),
            Err(ProgramError::Resource(ResourceError::IdOverflow))
        );
    }

    #[test]
    fn allocated_seed_ids_are_dense_across_independent_and_exclusive_seeds() {
        let mut snapshot = PredictionSnapshot::new();
        let labels = vec!["a".to_string(), "b".to_string()];
        snapshot.declare_exclusive_relation("r", &labels).unwrap();
        snapshot.add_distribution("r", "x", &[0.25, 0.75], None).unwrap();
        snapshot.add_distribution("r", "y", &[1.0, 0.0], None).unwrap();
        snapshot.add_independent("s", "p", "o", 0.5).unwrap();
        let mut dictionary = Dictionary::new();
        let specs = allocate_seed_specs(&snapshot, &mut dictionary, 100).unwrap();
        let mut ids = Vec::new();
        for spec in &specs {
            match spec {
                SeedSpec::Independent { seed_id, .. } => ids.push(*seed_id),
                SeedSpec::ExclusiveGroup { choices, .. } => {
                    ids.extend(choices.iter().map(|choice| choice.choice_id))
                }
            }
        }
        assert_eq!(ids, vec![0, 1, 2, 3, 4]);
        assert!(matches!(
            allocate_seed_specs(&snapshot, &mut dictionary, 4),
            Err(ProgramError::Resource(ResourceError::SeedLimit { requested: 5, limit: 4 }))
        ));
    }
}
