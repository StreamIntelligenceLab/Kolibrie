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

#[derive(Debug, Clone)]
pub enum FilterExpression<'a> {
    Comparison(&'a str, &'a str, &'a str),
    And(Box<FilterExpression<'a>>, Box<FilterExpression<'a>>),
    Or(Box<FilterExpression<'a>>, Box<FilterExpression<'a>>),
    Not(Box<FilterExpression<'a>>),
    ArithmeticExpr(Box<ArithmeticExpression<'a>>),
    FunctionCall(&'a str, Vec<&'a str>),
}

#[derive(Debug, Clone)]
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
                if s.starts_with('?') {
                    resolve(s).ok_or_else(|| format!("Variable '{}' not found or not numeric", s))
                } else {
                    s.parse::<f64>().map_err(|_| format!("Cannot parse '{}' as number", s))
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
#[derive(Debug, Clone)]
pub enum Value {
    Term(String),
    Undef,
}

// Define the ValuesClause struct to hold variables and their corresponding values
#[derive(Debug, Clone)]
pub struct ValuesClause<'a> {
    pub variables: Vec<&'a str>,
    pub values: Vec<Vec<Value>>,
}

// Define the InsertClause struct to hold triple patterns for the INSERT clause
#[derive(Debug, Clone)]
pub struct InsertClause<'a> {
    pub triples: Vec<(&'a str, &'a str, &'a str)>,
}

// Define the DeleteClause struct to hold triple patterns for the DELETE clause
#[derive(Debug, Clone)]
pub struct DeleteClause<'a> {
    pub triples: Vec<(&'a str, &'a str, &'a str)>,
}

#[derive(Debug, Clone)]
pub struct SubQuery<'a> {
    pub variables: Vec<(&'a str, &'a str, Option<&'a str>)>, // SELECT variables
    pub patterns: Vec<(&'a str, &'a str, &'a str)>,          // WHERE patterns
    pub filters: Vec<FilterExpression<'a>>,           // FILTER conditions
    pub binds: Vec<(&'a str, Vec<&'a str>, &'a str)>,        // BIND clauses
    pub _values_clause: Option<ValuesClause<'a>>,            // VALUES clause
    pub limit: Option<usize>, // Add LIMIT support
}

#[derive(Debug, Clone)]
pub struct RuleHead<'a> {
    pub predicate: &'a str,
}

#[derive(Debug, Clone)]
pub struct MLPredictClause<'a> {
    pub model: &'a str,
    pub input_raw: &'a str,                                 // Raw input query string
    pub input_select: Vec<(&'a str, &'a str, Option<&'a str>)>, // Parsed SELECT variables
    pub input_where: Vec<(&'a str, &'a str, &'a str)>,      // Parsed WHERE patterns
    pub input_filters: Vec<FilterExpression<'a>>,    // Parsed FILTER conditions
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
    Timeout { duration: Duration, fallback: Fallback },
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
        Vec<FilterExpression<'a>>, // filters
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

#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderCondition<'a> {
    pub variable: &'a str,
    pub direction: SortDirection,
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
    pub sparql: (
        Option<InsertClause<'a>>,
        Vec<(&'a str, &'a str, Option<&'a str>)>,
        Vec<(&'a str, &'a str, &'a str)>,
        Vec<FilterExpression<'a>>,
        Vec<&'a str>,
        HashMap<String, String>,
        Option<ValuesClause<'a>>,
        Vec<(&'a str, Vec<&'a str>, &'a str)>,
        Vec<SubQuery<'a>>,
        Option<usize>,
        Vec<WindowBlock<'a>>,
        Vec<OrderCondition<'a>>,
    ),
    pub delete_clause: Option<DeleteClause<'a>>,
}

// ---------------------------------------------------------------------------
// Strict SPARQL query/update syntax tree
// ---------------------------------------------------------------------------

/// A term in the strict SPARQL parser.
///
/// The variants deliberately retain slices into the source query so parsing
/// remains allocation-light and callers can report useful source locations.
/// IRI values do not include `<` and `>`, variable and blank-node values do
/// not include their `?`/`$` and `_:` prefixes, prefixed names retain their
/// complete lexical form, and literals retain their complete lexical form
/// (quotes, escapes, language tag, and datatype suffix included).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparqlTerm<'a> {
    Variable(&'a str),
    Iri(&'a str),
    PrefixedName(&'a str),
    BlankNode(&'a str),
    Literal(&'a str),
    QuotedTriple(&'a str),
    /// The predicate abbreviation `a`.
    A,
}

/// The name following `GRAPH`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SparqlGraphName<'a> {
    /// The IRI value without angle brackets.
    Iri(&'a str),
    /// The complete prefixed name, for example `ex:graph`.
    PrefixedName(&'a str),
    /// The variable name without its `?` or `$` sigil.
    Variable(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparqlTriplePattern<'a> {
    pub subject: SparqlTerm<'a>,
    pub predicate: SparqlTerm<'a>,
    pub object: SparqlTerm<'a>,
}

/// Recursive graph-pattern algebra for Kolibrie's SELECT/WHERE fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupGraphPattern<'a> {
    /// The empty group pattern. It evaluates to one solution mapping.
    Empty,
    Bgp(Vec<SparqlTriplePattern<'a>>),
    Join(Vec<GroupGraphPattern<'a>>),
    Union(Vec<GroupGraphPattern<'a>>),
    Graph {
        name: SparqlGraphName<'a>,
        pattern: Box<GroupGraphPattern<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparqlProjection<'a> {
    All,
    /// Variable names without their `?` or `$` sigils.
    Variables(Vec<&'a str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictSelectQuery<'a> {
    pub prefixes: HashMap<String, String>,
    pub distinct: bool,
    pub projection: SparqlProjection<'a>,
    /// Named graphs visible to `GRAPH` when explicit `FROM NAMED` clauses are
    /// present. An empty vector means no dataset replacement was requested.
    pub from_named: Vec<SparqlGraphName<'a>>,
    pub pattern: GroupGraphPattern<'a>,
    pub limit: Option<usize>,
}

/// A template/data quad. `graph == None` denotes the default graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparqlQuadPattern<'a> {
    pub graph: Option<SparqlGraphName<'a>>,
    pub triple: SparqlTriplePattern<'a>,
}

/// The deliberately scoped SPARQL 1.1 Update operation family supported by
/// Kolibrie's strict parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrictUpdateOperation<'a> {
    InsertData(Vec<SparqlQuadPattern<'a>>),
    DeleteData(Vec<SparqlQuadPattern<'a>>),
    Modify {
        /// Empty for INSERT-only MODIFY.
        delete: Vec<SparqlQuadPattern<'a>>,
        /// Empty for DELETE-only MODIFY.
        insert: Vec<SparqlQuadPattern<'a>>,
        where_pattern: GroupGraphPattern<'a>,
    },
    DeleteWhere {
        template: Vec<SparqlQuadPattern<'a>>,
        where_pattern: GroupGraphPattern<'a>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictUpdateRequest<'a> {
    pub prefixes: HashMap<String, String>,
    pub operation: StrictUpdateOperation<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrictSparqlRequest<'a> {
    Select(StrictSelectQuery<'a>),
    Update(StrictUpdateRequest<'a>),
}

/// A source-positioned error returned by
/// `kolibrie::parser::parse_strict_sparql`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictSparqlParseError {
    pub offset: usize,
    pub message: String,
}

impl std::fmt::Display for StrictSparqlParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SPARQL syntax error at byte {}: {}",
            self.offset, self.message
        )
    }
}

impl std::error::Error for StrictSparqlParseError {}
