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

#[derive(Debug, Clone)]
pub struct CombinedRule<'a> {
    pub head: RuleHead<'a>,
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