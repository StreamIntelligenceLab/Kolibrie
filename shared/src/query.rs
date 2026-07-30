/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterExpression<'a> {
    Comparison(&'a str, &'a str, &'a str),
    And(Box<FilterExpression<'a>>, Box<FilterExpression<'a>>),
    Or(Box<FilterExpression<'a>>, Box<FilterExpression<'a>>),
    Not(Box<FilterExpression<'a>>),
    ArithmeticExpr(Box<ArithmeticExpression<'a>>),
    FunctionCall(&'a str, Vec<&'a str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArithmeticExpression<'a> {
    Operand(&'a str), // Variable, literal, or number
    Add(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    Subtract(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    Multiply(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    Divide(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
}

impl<'a> ArithmeticExpression<'a> {
    /// Evaluate the expression. `resolve` maps variable strings (e.g. `?x`) to f64 values.
    pub fn evaluate<F: Fn(&str) -> Option<f64>>(&self, resolve: &F) -> Result<f64, String> {
        match self {
            Self::Operand(s) => {
                if s.starts_with(['?', '$']) {
                    resolve(s).ok_or_else(|| format!("Variable '{}' not found or not numeric", s))
                } else {
                    s.parse::<f64>()
                        .map_err(|_| format!("Cannot parse '{}' as number", s))
                }
            }
            Self::Add(l, r) => Ok(l.evaluate(resolve)? + r.evaluate(resolve)?),
            Self::Subtract(l, r) => Ok(l.evaluate(resolve)? - r.evaluate(resolve)?),
            Self::Multiply(l, r) => Ok(l.evaluate(resolve)? * r.evaluate(resolve)?),
            Self::Divide(l, r) => {
                let rv = r.evaluate(resolve)?;
                if rv == 0.0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(l.evaluate(resolve)? / rv)
                }
            }
        }
    }
}

// Define the Value enum to represent terms or UNDEF in VALUES clause
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Term(String),
    Undef,
}

// Define the ValuesClause struct to hold variables and their corresponding values
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuesClause<'a> {
    pub variables: Vec<&'a str>,
    pub values: Vec<Vec<Value>>,
}

/// A source-borrowed triple pattern as it appears in SPARQL text.
///
/// The three slices retain their lexical spelling, including variable sigils,
/// IRI brackets, literal quotes/suffixes, blank-node prefixes, and RDF-star
/// quoted-triple delimiters. Resolution and dictionary encoding happen only
/// when this syntax tree is lowered into the query plan.
pub type LexicalTriplePattern<'a> = (&'a str, &'a str, &'a str);

/// A source-borrowed quad used by update data blocks and templates.
/// `graph == None` denotes the default graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexicalQuadPattern<'a> {
    pub graph: Option<&'a str>,
    pub triple: LexicalTriplePattern<'a>,
}

/// The existing tuple representation used by BIND.
pub type BindClause<'a> = (&'a str, Vec<&'a str>, &'a str);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertClause<'a> {
    pub quads: Vec<LexicalQuadPattern<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteClause<'a> {
    pub quads: Vec<LexicalQuadPattern<'a>>,
}

/// Recursive graph-pattern algebra shared by SELECT and update WHERE clauses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupGraphPattern<'a> {
    /// The empty group graph pattern. It evaluates to one solution mapping.
    Unit,
    Bgp(Vec<LexicalTriplePattern<'a>>),
    Join(Vec<GroupGraphPattern<'a>>),
    /// UNION preserves branch multiplicity; DISTINCT is a SELECT modifier.
    Union(Vec<GroupGraphPattern<'a>>),
    Graph {
        /// Raw IRI, prefixed name, or variable lexeme following GRAPH.
        name: &'a str,
        pattern: Box<GroupGraphPattern<'a>>,
    },
    Filter(FilterExpression<'a>),
    Bind(BindClause<'a>),
    Values(ValuesClause<'a>),
    SubQuery(Box<SubQuery<'a>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubQuery<'a> {
    pub query: SelectQuery<'a>,
}

#[derive(Debug, Clone)]
pub struct RuleHead<'a> {
    pub predicate: &'a str,
}

#[derive(Debug, Clone)]
pub struct MLPredictClause<'a> {
    pub model: &'a str,
    pub input_raw: &'a str, // Raw input query string
    pub input_select: Vec<(&'a str, &'a str, Option<&'a str>)>, // Parsed SELECT variables
    pub input_where: Vec<(&'a str, &'a str, &'a str)>, // Parsed WHERE patterns
    pub input_filters: Vec<FilterExpression<'a>>, // Parsed FILTER conditions
    pub output: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LossFn {
    CrossEntropy,
    Nll,
    Mse,
    BinaryCrossEntropy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerKind {
    Adam,
    Sgd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelArch {
    Mlp { hidden_layers: Vec<usize> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NeuralOutputKind {
    Exclusive { labels: Vec<String> },
    Binary { positive_literal: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelDecl {
    pub name: String,
    pub arch: ModelArch,
    pub output_kind: NeuralOutputKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeuralRelationDecl {
    pub predicate: String,
    pub model_name: String,
    pub input_patterns: Vec<(String, String, String)>,
    pub feature_vars: Vec<String>,
    pub anchor_var: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrainingDataSource {
    GraphPattern(Vec<(String, String, String)>),
    Query(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrainNeuralRelationDecl {
    pub predicate: String,
    pub data_source: TrainingDataSource,
    pub label_var: String,
    pub target_triple: (String, String, String),
    pub loss: LossFn,
    pub optimizer: OptimizerKind,
    pub learning_rate: f64,
    pub epochs: usize,
    pub batch_size: usize,
    pub save_path: Option<String>,
}

// Add new structs for windowing support
#[derive(Clone, Debug)]
pub struct WindowClause<'a> {
    pub window_iri: &'a str,
    pub stream_iri: &'a str,
    pub window_spec: WindowSpec<'a>,
    /// Per-window sync policy; `None` means use the engine-level default.
    pub policy: Option<SyncPolicy>,
}

#[derive(Clone, Debug)]
pub struct WindowSpec<'a> {
    pub window_type: WindowType,
    pub width: usize,
    pub slide: Option<usize>,
    pub report_strategy: Option<&'a str>,
    pub tick: Option<&'a str>,
}

#[derive(Clone, Debug)]
pub enum WindowType {
    Range,
    Tumbling,
    Sliding,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Fallback {
    Steal,
    Drop,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SyncPolicy {
    /// Emit immediately using stale data from non-firing windows (τ=0, fallback=steal)
    Steal,
    /// Wait until all windows have fired in the current cycle (τ=∞)
    Wait,
    /// Wait up to `duration`; on expiry apply `fallback`
    Timeout {
        duration: Duration,
        fallback: Fallback,
    },
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Wait
    }
}

#[derive(Clone, Debug)]
pub enum StreamType<'a> {
    RStream,
    IStream,
    DStream,
    Custom(&'a str),
}

#[derive(Debug, Clone)]
pub struct RegisterClause<'a> {
    pub stream_type: StreamType<'a>,
    pub output_stream_iri: &'a str,
    pub query: RSPQLSelectQuery<'a>,
}

#[derive(Debug, Clone)]
pub struct RSPQLSelectQuery<'a> {
    pub variables: Vec<(&'a str, &'a str, Option<&'a str>)>,
    pub window_clause: Vec<WindowClause<'a>>,
    pub where_clause: (
        Vec<(&'a str, &'a str, &'a str)>,
        Vec<FilterExpression<'a>>,
        Option<ValuesClause<'a>>,
        Vec<(&'a str, Vec<&'a str>, &'a str)>,
        Vec<SubQuery<'a>>,
    ),
    pub window_blocks: Vec<WindowBlock<'a>>,
}

#[derive(Debug, Clone)]
pub struct WindowBlock<'a> {
    pub window_name: &'a str,
    pub patterns: Vec<(&'a str, &'a str, &'a str)>,
}

/// Probability annotation for a RULE.
/// Parsed from: PROB(combination=independent, threshold=0.3, confidence=0.9)
#[derive(Clone, Debug)]
pub struct ProbAnnotation<'a> {
    pub combination: &'a str,
    pub threshold: Option<f64>,
    pub confidence: Option<f64>,
    /// Fully validated policy for `provenance=hybrid`.
    pub hybrid_config: Option<crate::hybrid::HybridConfig>,
}

// Modified CombinedRule to include windowing
#[derive(Clone, Debug)]
pub struct CombinedRule<'a> {
    pub head: RuleHead<'a>,
    pub stream_type: Option<StreamType<'a>>,
    pub window_clause: Vec<WindowClause<'a>>,
    pub model_decls: Vec<ModelDecl>,
    pub neural_relation_decls: Vec<NeuralRelationDecl>,
    pub train_neural_relation_decls: Vec<TrainNeuralRelationDecl>,
    pub body: (
        Vec<(&'a str, &'a str, &'a str)>, // triple patterns from WHERE
        Vec<FilterExpression<'a>>,        // filters
        Option<ValuesClause<'a>>,
        Vec<(&'a str, Vec<&'a str>, &'a str)>, // BIND clauses
        Vec<SubQuery<'a>>,                     // subqueries
    ),
    /// Negated body atoms parsed from `NOT triple_pattern` clauses in WHERE.
    pub negated_body: Vec<(&'a str, &'a str, &'a str)>,
    pub conclusion: Vec<(&'a str, &'a str, &'a str)>,
    pub ml_predict: Option<MLPredictClause<'a>>, // new field for ML.PREDICT clause
    pub prob_annotation: Option<ProbAnnotation<'a>>, // probabilistic rule annotation
}

// Add these new enums and structs
#[derive(Debug, Clone, PartialEq)]
pub enum RetrieveMode {
    Some,
    Every,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamState {
    Latent,
    Active,
}

#[derive(Debug, Clone)]
pub struct RetrieveClause<'a> {
    pub mode: RetrieveMode,
    pub state: StreamState,
    pub variable: &'a str,
    pub from_iri: &'a str,
    pub graph_pattern: Vec<(&'a str, &'a str, &'a str)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCondition<'a> {
    pub variable: &'a str,
    pub direction: SortDirection,
}

/// A SELECT query in Kolibrie's supported SPARQL fragment.
///
/// Projection entries retain the historical `(kind, variable, alias)` shape:
/// ordinary variables use `"VAR"` and aggregates use their canonical
/// uppercase function name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectQuery<'a> {
    pub distinct: bool,
    pub variables: Vec<(&'a str, &'a str, Option<&'a str>)>,
    /// Graph IRIs used to form the replacement default graph.
    pub from: Vec<&'a str>,
    /// Graph IRIs visible to GRAPH in the replacement dataset.
    pub from_named: Vec<&'a str>,
    pub pattern: GroupGraphPattern<'a>,
    pub group_vars: Vec<&'a str>,
    pub order_conditions: Vec<OrderCondition<'a>>,
    pub limit: Option<usize>,
}

/// The six SPARQL Update forms supported by Kolibrie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateOperation<'a> {
    InsertData(InsertClause<'a>),
    DeleteData(DeleteClause<'a>),
    InsertWhere {
        insert: InsertClause<'a>,
        where_pattern: GroupGraphPattern<'a>,
    },
    /// `DELETE { template } WHERE { pattern }`
    DeleteWhere {
        delete: DeleteClause<'a>,
        where_pattern: GroupGraphPattern<'a>,
    },
    DeleteInsertWhere {
        delete: DeleteClause<'a>,
        insert: InsertClause<'a>,
        where_pattern: GroupGraphPattern<'a>,
    },
    /// `DELETE WHERE { pattern }`; the parsed quad block is both the template
    /// and the WHERE graph pattern.
    DeleteWhereShorthand {
        delete: DeleteClause<'a>,
        where_pattern: GroupGraphPattern<'a>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparqlOperation<'a> {
    Select(SelectQuery<'a>),
    Update(UpdateOperation<'a>),
}

#[derive(Debug, Clone)]
pub struct CombinedQuery<'a> {
    pub prefixes: HashMap<String, String>,
    pub retrieve_clause: Option<RetrieveClause<'a>>,
    pub register_clause: Option<RegisterClause<'a>>,
    pub model_decls: Vec<ModelDecl>,
    pub neural_relation_decls: Vec<NeuralRelationDecl>,
    pub train_neural_relation_decls: Vec<TrainNeuralRelationDecl>,
    pub rule: Option<CombinedRule<'a>>,
    pub ml_predict: Option<MLPredictClause<'a>>,
    /// The single standard-SPARQL syntax tree. Extension-only requests leave
    /// this as `None`; recognized standard syntax never falls through to an
    /// extension parser.
    pub sparql: Option<SparqlOperation<'a>>,
}
