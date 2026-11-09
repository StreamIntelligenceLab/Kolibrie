/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The parsed shape of a request, before anything is resolved

use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
/// A `FILTER` condition, as written
pub enum FilterExpression<'a> {
    /// `(left, operator, right)`, all lexical — for example `("?x", ">", "5")`
    Comparison(&'a str, &'a str, &'a str),
    /// Both operands must hold
    And(Box<FilterExpression<'a>>, Box<FilterExpression<'a>>),
    /// Either operand may hold
    Or(Box<FilterExpression<'a>>, Box<FilterExpression<'a>>),
    /// Negation of the inner condition
    Not(Box<FilterExpression<'a>>),
    /// An arithmetic expression used where a truth value is expected
    ArithmeticExpr(Box<ArithmeticExpression<'a>>),
    /// `(function name, arguments)`, resolved against the host's registered user-defined functions
    /// at execution time rather than at parse time
    FunctionCall(&'a str, Vec<&'a str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// An arithmetic expression over variables and numeric literals
pub enum ArithmeticExpression<'a> {
    /// A variable (leading `?` or `$`) or a numeric literal
    Operand(&'a str),
    /// Sum
    Add(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    /// Difference
    Subtract(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    /// Product
    Multiply(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    /// Quotient
    Divide(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
}

impl<'a> ArithmeticExpression<'a> {
    /// Evaluate the expression
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

/// One cell of a `VALUES` block
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// A term, in its lexical spelling
    Term(String),
    /// `UNDEF`: this row leaves the variable unbound rather than binding it to a value, so it does
    /// not constrain the join at that position
    Undef,
}

/// An inline `VALUES` table supplying candidate bindings
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuesClause<'a> {
    /// Column headings, in order
    pub variables: Vec<&'a str>,
    /// Rows, each the same length as `variables`
    pub values: Vec<Vec<Value>>,
}

/// A source-borrowed triple pattern as it appears in SPARQL text
pub type LexicalTriplePattern<'a> = (&'a str, &'a str, &'a str);

/// A source-borrowed quad used by update data blocks and templates
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexicalQuadPattern<'a> {
    /// The graph, or `None` for the default graph
    pub graph: Option<&'a str>,
    /// The triple itself
    pub triple: LexicalTriplePattern<'a>,
}

/// The existing tuple representation used by BIND
pub type BindClause<'a> = (&'a str, Vec<&'a str>, &'a str);

/// Everything a `WHERE` clause declares, as written
#[derive(Debug, Clone, Default)]
pub struct WhereClause<'a> {
    /// Triple patterns the clause matches
    pub patterns: Vec<LexicalTriplePattern<'a>>,
    /// `FILTER` expressions constraining them
    pub filters: Vec<FilterExpression<'a>>,
    /// An inline `VALUES` table, if the clause has one
    pub values: Option<ValuesClause<'a>>,
    /// `BIND` clauses computing new variables
    pub binds: Vec<BindClause<'a>>,
    /// Nested `SELECT` subqueries
    pub subqueries: Vec<SubQuery<'a>>,
    /// RSP-QL `WINDOW` blocks
    pub window_blocks: Vec<WindowBlock<'a>>,
    /// Patterns under `NOT`, which must *not* match
    pub negated_patterns: Vec<LexicalTriplePattern<'a>>,
}

/// The quads an update inserts: either literal data or a template to instantiate
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertClause<'a> {
    /// Quads to insert
    pub quads: Vec<LexicalQuadPattern<'a>>,
}

/// The quads an update deletes: either literal data or a template to instantiate
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteClause<'a> {
    /// Quads to delete, possibly a template containing variables
    pub quads: Vec<LexicalQuadPattern<'a>>,
}

/// Recursive graph-pattern algebra shared by SELECT and update WHERE clauses
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupGraphPattern<'a> {
    /// The empty group graph pattern
    Unit,
    /// A basic graph pattern: triples that must all match, joined on shared variable names
    Bgp(Vec<LexicalTriplePattern<'a>>),
    /// Conjunction of sub-patterns
    Join(Vec<GroupGraphPattern<'a>>),
    /// UNION preserves branch multiplicity; DISTINCT is a SELECT modifier
    Union(Vec<GroupGraphPattern<'a>>),
    /// `GRAPH`: evaluate the inner pattern against one named graph, or against each in turn when
    /// the name is a variable
    Graph {
        /// Raw IRI, prefixed name, or variable lexeme following GRAPH
        name: &'a str,
        /// Pattern to evaluate within that graph
        pattern: Box<GroupGraphPattern<'a>>,
    },
    /// A condition restricting the solutions of the enclosing group
    Filter(FilterExpression<'a>),
    /// `BIND`: computes a new binding from existing ones
    Bind(BindClause<'a>),
    /// An inline `VALUES` table
    Values(ValuesClause<'a>),
    /// A nested SELECT, evaluated independently and joined with the enclosing group
    SubQuery(Box<SubQuery<'a>>),
}

/// A nested SELECT appearing inside a graph pattern
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubQuery<'a> {
    /// The nested query
    pub query: SelectQuery<'a>,
}

/// The name of a `RULE`, as written after the `RULE` keyword
#[derive(Debug, Clone)]
pub struct RuleHead<'a> {
    /// The rule's identifying predicate, lexical
    pub predicate: &'a str,
}

/// An `ML.PREDICT` clause: run a model over the rows a query selects
#[derive(Debug, Clone)]
pub struct MLPredictClause<'a> {
    /// Name of the model to invoke, which must be declared and approved
    pub model: &'a str,
    /// The input query exactly as written
    pub input_raw: &'a str,
    /// Projection of the input query, as `(kind, variable, alias)`
    pub input_select: Vec<(&'a str, &'a str, Option<&'a str>)>,
    /// Triple patterns of the input query's WHERE clause
    pub input_where: Vec<(&'a str, &'a str, &'a str)>,
    /// Filters of the input query's WHERE clause
    pub input_filters: Vec<FilterExpression<'a>>,
    /// Variable the prediction binds to
    pub output: &'a str,
    /// Whether to bind the full distribution over labels rather than the single most likely one
    pub distribution: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Training objective for a neural relation
pub enum LossFn {
    /// Cross-entropy, for a distribution over mutually exclusive labels
    CrossEntropy,
    /// Negative log likelihood, expecting log-probabilities as input
    Nll,
    /// Mean squared error, for regression
    Mse,
    /// Binary cross-entropy, for a single yes/no output
    BinaryCrossEntropy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Which optimiser trains a neural relation
pub enum OptimizerKind {
    /// Adam
    Adam,
    /// Plain stochastic gradient descent
    Sgd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A model's architecture
pub enum ModelArch {
    /// A multilayer perceptron
    Mlp {
        /// Width of each hidden layer, in order
        hidden_layers: Vec<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// What a model's output means, which decides how a prediction becomes facts
pub enum NeuralOutputKind {
    /// A distribution over mutually exclusive labels, at most one of which holds
    Exclusive {
        /// The labels, in the order the model's outputs correspond to
        labels: Vec<String>,
    },
    /// A single probability that one fact holds
    Binary {
        /// The term asserted when the model answers positively
        positive_literal: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A `MODEL` declaration: what a model is called, its shape, and how to read its output
pub struct ModelDecl {
    /// Name rules refer to it by
    pub name: String,
    /// Architecture to instantiate
    pub arch: ModelArch,
    /// How to interpret the output vector
    pub output_kind: NeuralOutputKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A declaration that a predicate's facts come from a model rather than from data
pub struct NeuralRelationDecl {
    /// The predicate this relation defines
    pub predicate: String,
    /// Model to invoke; must name a [`ModelDecl`]
    pub model_name: String,
    /// Patterns selecting the rows to predict over
    pub input_patterns: Vec<(String, String, String)>,
    /// Variables forming the feature vector, in model input order
    pub feature_vars: Vec<String>,
    /// Variable identifying the subject a prediction is about, so the derived fact can be attached
    /// to the right entity
    pub anchor_var: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Where training examples come from
pub enum TrainingDataSource {
    /// Triple patterns selecting the examples
    GraphPattern(Vec<(String, String, String)>),
    /// A full query, as text, selecting them
    Query(String),
}

#[derive(Debug, Clone, PartialEq)]
/// A `TRAIN` declaration: fit the model behind a neural relation
pub struct TrainNeuralRelationDecl {
    /// The neural relation being trained; must be declared
    pub predicate: String,
    /// Where the training examples come from
    pub data_source: TrainingDataSource,
    /// Variable holding each example's ground-truth label
    pub label_var: String,
    /// Pattern identifying what is being predicted about each example
    pub target_triple: (String, String, String),
    /// Objective to minimise
    pub loss: LossFn,
    /// Optimiser to use
    pub optimizer: OptimizerKind,
    /// Step size
    pub learning_rate: f64,
    /// Passes over the training data
    pub epochs: usize,
    /// Examples per gradient step
    pub batch_size: usize,
    /// Where to write the fitted weights
    pub save_path: Option<String>,
}

/// A `FROM NAMED WINDOW` clause: one window over one stream
#[derive(Clone, Debug)]
pub struct WindowClause<'a> {
    /// Name the WHERE clause refers to this window by
    pub window_iri: &'a str,
    /// Stream the window observes
    pub stream_iri: &'a str,
    /// Width, slide and reporting behaviour
    pub window_spec: WindowSpec<'a>,
    /// Per-window sync policy; `None` means use the engine-level default
    pub policy: Option<SyncPolicy>,
}

/// How a window selects from its stream
#[derive(Clone, Debug)]
pub struct WindowSpec<'a> {
    /// Range, tumbling or sliding
    pub window_type: WindowType,
    /// Window width, in the stream's time unit
    pub width: usize,
    /// How far the window advances each step
    pub slide: Option<usize>,
    /// When results are reported, as written
    pub report_strategy: Option<&'a str>,
    /// What advances time — arriving events or the wall clock
    pub tick: Option<&'a str>,
}

/// The three window shapes
#[derive(Clone, Debug)]
pub enum WindowType {
    /// A time range, sliding by its slide parameter
    Range,
    /// Non-overlapping: each event falls in exactly one window
    Tumbling,
    /// Overlapping: an event may fall in several windows
    Sliding,
}

/// What to do when a [`SyncPolicy::Timeout`] expires before every window has fired
#[derive(Clone, Debug, PartialEq)]
pub enum Fallback {
    /// Emit anyway, reusing the last content of windows that have not fired
    Steal,
    /// Emit nothing for this cycle
    Drop,
}

#[derive(Clone, Debug, PartialEq)]
/// When a query joining several windows is allowed to emit
pub enum SyncPolicy {
    /// Emit immediately using stale data from non-firing windows (τ=0, fallback=steal)
    Steal,
    /// Wait until all windows have fired in the current cycle (τ=∞)
    Wait,
    /// Wait up to `duration`; on expiry apply `fallback`
    Timeout {
        /// How long to wait for the remaining windows
        duration: Duration,
        /// What to do if they have still not fired
        fallback: Fallback,
    },
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Wait
    }
}

/// The relation-to-stream operator: how a query's answers become an output stream
#[derive(Clone, Debug)]
pub enum StreamType<'a> {
    /// Every solution in the current window, each time it is evaluated
    RStream,
    /// Only solutions that were absent from the previous evaluation
    IStream,
    /// Only solutions present previously and now gone
    DStream,
    /// A host-defined operator, named lexically
    Custom(&'a str),
}

/// A `REGISTER` clause: a standing query whose answers are published as a stream
#[derive(Debug, Clone)]
pub struct RegisterClause<'a> {
    /// Which solutions to publish each evaluation
    pub stream_type: StreamType<'a>,
    /// Name the output stream is published under
    pub output_stream_iri: &'a str,
    /// The continuous query
    pub query: RSPQLSelectQuery<'a>,
}

/// A continuous SELECT over one or more windows
#[derive(Debug, Clone)]
pub struct RSPQLSelectQuery<'a> {
    /// Projection, as `(kind, variable, alias)`
    pub variables: Vec<(&'a str, &'a str, Option<&'a str>)>,
    /// The windows this query reads
    pub window_clause: Vec<WindowClause<'a>>,
    /// The WHERE clause, as a five-tuple of `(triple patterns, filters, VALUES, BIND clauses,
    /// subqueries)`
    pub where_clause: (
        Vec<(&'a str, &'a str, &'a str)>,
        Vec<FilterExpression<'a>>,
        Option<ValuesClause<'a>>,
        Vec<(&'a str, Vec<&'a str>, &'a str)>,
        Vec<SubQuery<'a>>,
    ),
    /// Patterns scoped to a named window, for queries that address windows individually rather than
    /// joining them in one flat WHERE
    pub window_blocks: Vec<WindowBlock<'a>>,
}

/// Patterns evaluated against one named window
#[derive(Debug, Clone)]
pub struct WindowBlock<'a> {
    /// The window these patterns read, matching a [`WindowClause::window_iri`]
    pub window_name: &'a str,
    /// Triple patterns to match within that window
    pub patterns: Vec<(&'a str, &'a str, &'a str)>,
}

/// Probability annotation for a RULE
#[derive(Clone, Debug)]
pub struct ProbAnnotation<'a> {
    /// How premise probabilities combine: `independent`, `sdd`, `hybrid` and so on, as written
    pub combination: &'a str,
    /// Minimum probability a derived fact needs to be kept
    pub threshold: Option<f64>,
    /// Confidence demanded of an approximate method before its answer is accepted
    pub confidence: Option<f64>,
    /// Fully validated policy for `provenance=hybrid`
    pub hybrid_config: Option<crate::hybrid::HybridConfig>,
}

/// One `RULE` block: a Datalog rule, optionally streaming, probabilistic or neural
#[derive(Clone, Debug)]
pub struct CombinedRule<'a> {
    /// The rule's name
    pub head: RuleHead<'a>,
    /// Output stream operator, for a rule that publishes continuously
    pub stream_type: Option<StreamType<'a>>,
    /// Windows this rule reads, for a rule over streams
    pub window_clause: Vec<WindowClause<'a>>,
    /// Models declared on this rule
    pub model_decls: Vec<ModelDecl>,
    /// Neural relations declared on this rule
    pub neural_relation_decls: Vec<NeuralRelationDecl>,
    /// Training declarations on this rule
    pub train_neural_relation_decls: Vec<TrainNeuralRelationDecl>,
    /// The rule body, as a five-tuple of `(triple patterns, filters, VALUES, BIND clauses,
    /// subqueries)`
    pub body: (
        Vec<(&'a str, &'a str, &'a str)>,
        Vec<FilterExpression<'a>>,
        Option<ValuesClause<'a>>,
        Vec<(&'a str, Vec<&'a str>, &'a str)>,
        Vec<SubQuery<'a>>,
    ),
    /// Negated body atoms parsed from `NOT triple_pattern` clauses in WHERE
    pub negated_body: Vec<(&'a str, &'a str, &'a str)>,
    /// Patterns scoped to individual named windows
    pub window_blocks: Vec<WindowBlock<'a>>,
    /// Facts derived when the body matches — the `CONSTRUCT` template
    pub conclusion: Vec<(&'a str, &'a str, &'a str)>,
    /// An `ML.PREDICT` clause supplying bindings from a model
    pub ml_predict: Option<MLPredictClause<'a>>,
    /// `PROB(...)` annotation making this rule probabilistic
    pub prob_annotation: Option<ProbAnnotation<'a>>,
}

/// How many matches a `RETRIEVE` clause wants
#[derive(Debug, Clone, PartialEq)]
pub enum RetrieveMode {
    /// Any one match suffices
    Some,
    /// Every match is required
    Every,
}

/// Which streams a `RETRIEVE` clause considers
#[derive(Debug, Clone, PartialEq)]
pub enum StreamState {
    /// Declared but not yet producing
    Latent,
    /// Currently producing
    Active,
}

/// A `RETRIEVE` clause: discover streams matching a description
#[derive(Debug, Clone)]
pub struct RetrieveClause<'a> {
    /// Whether one match or all matches are wanted
    pub mode: RetrieveMode,
    /// Whether to consider latent or active streams
    pub state: StreamState,
    /// Variable the discovered stream binds to
    pub variable: &'a str,
    /// Catalogue to search
    pub from_iri: &'a str,
    /// Patterns a stream's description must satisfy
    pub graph_pattern: Vec<(&'a str, &'a str, &'a str)>,
}

/// Direction of an `ORDER BY` condition
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending
    Asc,
    /// Descending
    Desc,
}

/// One `ORDER BY` key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCondition<'a> {
    /// Variable to sort on
    pub variable: &'a str,
    /// Which way
    pub direction: SortDirection,
}

/// A SELECT query in Kolibrie's supported SPARQL fragment
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectQuery<'a> {
    /// Whether duplicate solutions are removed
    pub distinct: bool,
    /// Projection, as `(kind, variable, alias)` — see the type's own note
    pub variables: Vec<(&'a str, &'a str, Option<&'a str>)>,
    /// Graph IRIs used to form the replacement default graph
    pub from: Vec<&'a str>,
    /// Graph IRIs visible to GRAPH in the replacement dataset
    pub from_named: Vec<&'a str>,
    /// The WHERE clause
    pub pattern: GroupGraphPattern<'a>,
    /// `GROUP BY` keys
    pub group_vars: Vec<&'a str>,
    /// `ORDER BY` keys, in precedence order
    pub order_conditions: Vec<OrderCondition<'a>>,
    /// `LIMIT`
    pub limit: Option<usize>,
}

/// The six SPARQL Update forms supported by Kolibrie
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateOperation<'a> {
    /// `INSERT DATA`: ground quads, no pattern matching
    InsertData(InsertClause<'a>),
    /// `DELETE DATA`: ground quads, no pattern matching
    DeleteData(DeleteClause<'a>),
    /// `INSERT { template } WHERE { pattern }`
    InsertWhere {
        /// Template instantiated once per solution
        insert: InsertClause<'a>,
        /// Pattern producing the solutions
        where_pattern: GroupGraphPattern<'a>,
    },
    /// `DELETE { template } WHERE { pattern }`
    DeleteWhere {
        /// Template instantiated once per solution
        delete: DeleteClause<'a>,
        /// Pattern producing the solutions
        where_pattern: GroupGraphPattern<'a>,
    },
    /// `DELETE { ... } INSERT { ... } WHERE { ... }`
    DeleteInsertWhere {
        /// Template of quads to remove
        delete: DeleteClause<'a>,
        /// Template of quads to add
        insert: InsertClause<'a>,
        /// Pattern producing the solutions
        where_pattern: GroupGraphPattern<'a>,
    },
    /// `DELETE WHERE { pattern }`; the parsed quad block is both the template and the WHERE graph
    /// pattern
    DeleteWhereShorthand {
        /// The quad block, doubling as the delete template
        delete: DeleteClause<'a>,
        /// The same block, as the pattern that selects what to remove
        where_pattern: GroupGraphPattern<'a>,
    },
}

/// A standard-SPARQL request: a query or an update, never both
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparqlOperation<'a> {
    /// A SELECT, which returns solutions and changes nothing
    Select(SelectQuery<'a>),
    /// An update, which changes the store and returns nothing
    Update(UpdateOperation<'a>),
}

/// Everything one parsed request contains
#[derive(Debug, Clone)]
pub struct CombinedQuery<'a> {
    /// `PREFIX` bindings, expanded form by short name
    pub prefixes: HashMap<String, String>,
    /// A `RETRIEVE` clause discovering streams
    pub retrieve_clause: Option<RetrieveClause<'a>>,
    /// A `REGISTER` clause defining a standing query
    pub register_clause: Option<RegisterClause<'a>>,
    /// Top-level model declarations
    pub model_decls: Vec<ModelDecl>,
    /// Top-level neural relation declarations
    pub neural_relation_decls: Vec<NeuralRelationDecl>,
    /// Top-level training declarations
    pub train_neural_relation_decls: Vec<TrainNeuralRelationDecl>,
    /// `RULE` blocks, in source order
    pub rules: Vec<CombinedRule<'a>>,
    /// A top-level `ML.PREDICT`, as opposed to one attached to a rule
    pub ml_predict: Option<MLPredictClause<'a>>,
    /// The single standard-SPARQL syntax tree
    pub sparql: Option<SparqlOperation<'a>>,
}

impl<'a> CombinedQuery<'a> {
    /// The sole rule, for entry points that execute exactly one
    pub fn single_rule(&self) -> Result<Option<&CombinedRule<'a>>, String> {
        match self.rules.as_slice() {
            [] => Ok(None),
            [rule] => Ok(Some(rule)),
            rules => Err(format!(
                "this entry point executes one RULE block, but the request contains {}; use the program API for multiple rules",
                rules.len()
            )),
        }
    }
}
