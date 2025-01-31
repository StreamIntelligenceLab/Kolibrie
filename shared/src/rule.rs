use crate::terms::TriplePattern;

#[derive(Debug, Clone)]
pub struct Rule {
    pub premise: Vec<TriplePattern>,
    pub conclusion: TriplePattern,
}