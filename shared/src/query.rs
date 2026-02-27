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
    ArithmeticExpr(&'a str),
}

#[derive(Debug, Clone)]
pub enum ArithmeticExpression<'a> {
    Operand(&'a str), // Variable, literal, or number
    Add(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    Subtract(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    Multiply(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
    Divide(Box<ArithmeticExpression<'a>>, Box<ArithmeticExpression<'a>>),
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
    pub arguments: Vec<&'a str>,
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

// Modified CombinedRule to include windowing
#[derive(Clone, Debug)]
pub struct CombinedRule<'a> {
    pub head: RuleHead<'a>,
    pub stream_type: Option<StreamType<'a>>,
    pub window_clause: Vec<WindowClause<'a>>,
    pub body: (
        Vec<(&'a str, &'a str, &'a str)>, // triple patterns from WHERE
        Vec<FilterExpression<'a>>, // filters
        Option<ValuesClause<'a>>,
        Vec<(&'a str, Vec<&'a str>, &'a str)>, // BIND clauses
        Vec<SubQuery<'a>>,                     // subqueries
    ),
    pub conclusion: Vec<(&'a str, &'a str, &'a str)>,
    pub ml_predict: Option<MLPredictClause<'a>>, // new field for ML.PREDICT clause
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
    pub rule: Option<CombinedRule<'a>>,
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
}
