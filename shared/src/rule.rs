use crate::terms::TriplePattern;

#[derive(Debug, Clone)]
pub struct FilterCondition {
    pub variable: String,
    pub operator: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Rule {
    pub premise: Vec<TriplePattern>,
    pub filters: Vec<FilterCondition>,
    pub conclusion: Vec<TriplePattern>,
}