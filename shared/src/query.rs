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

#[derive(Clone, Debug)]
pub enum StreamType<'a> {
    RStream,
    IStream, 
    DStream,
    Custom(&'a str),
}

// Modified CombinedRule to include windowing
#[derive(Clone, Debug)]
pub struct CombinedRule<'a> {
    pub head: RuleHead<'a>,
    pub stream_type: Option<StreamType<'a>>,
    pub window_clause: Option<WindowClause<'a>>,
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

#[derive(Debug, Clone)]
pub struct CombinedQuery<'a> {
    pub prefixes: HashMap<String, String>,
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
    ),
}