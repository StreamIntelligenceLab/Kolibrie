#[derive(PartialEq, Debug, Clone, Eq, PartialOrd, Ord)]
pub struct Triple {
    pub subject: u32,
    pub predicate: u32,
    pub object: u32,
}

#[derive(PartialEq, Debug, Clone, Eq, PartialOrd, Ord)]
pub struct TimestampedTriple {
    pub triple: Triple,
    pub timestamp: u64,
}
