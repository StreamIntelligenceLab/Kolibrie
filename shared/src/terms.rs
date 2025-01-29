#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Term {
    Variable(String),
    Constant(u32),
}

pub type TriplePattern = (Term, Term, Term);

#[derive(Debug)]
pub enum UnresolvedTerm {
    Var(String),
    Prefixed(String),
}

pub type UnresolvedTriple = (UnresolvedTerm, UnresolvedTerm, UnresolvedTerm);