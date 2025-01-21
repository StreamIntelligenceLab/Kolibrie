use std::collections::{BTreeMap, HashMap};

/// An enum specifying each type of index we want to maintain.
/// Helps keep things type-safe and easy to reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    SubjectPredicate,
    PredicateSubject,
    SubjectObject,
    ObjectSubject,
    PredicateObject,
    ObjectPredicate,
}

/// A structure to encapsulate one specific index (e.g., S-P, P-S, etc.).
/// We store `BTreeMap<(u64, u64), Vec<u64>>` to handle a key pair -> list of values.
#[derive(Debug, Clone)]
pub struct TripleIndex {
    pub index: BTreeMap<(u32, u32), Vec<u32>>,
    pub index_type: IndexType,
}

impl TripleIndex {
    /// Create a new TripleIndex of the given type.
    pub fn new(index_type: IndexType) -> Self {
        Self {
            index: BTreeMap::new(),
            index_type,
        }
    }

    /// Insert a single (key -> value) into the index.
    /// key is (u64, u64), and value is u64 (representing subject/predicate/object).
    pub fn insert(&mut self, key: (u32, u32), value: u32) {
        self.index.entry(key).or_default().push(value);
    }

    /// Retrieve the list of values for a given key, if it exists.
    pub fn get(&self, key: &(u32, u32)) -> Option<&Vec<u32>> {
        self.index.get(key)
    }

    /// Clear out all data in this index.
    pub fn clear(&mut self) {
        self.index.clear();
    }

    /// Sort and deduplicate all values in the index for more efficient queries.
    pub fn optimize(&mut self) {
        for values in self.index.values_mut() {
            values.sort_unstable();
            values.dedup();
        }
    }
}

/// A manager that holds multiple TripleIndex objects—one per IndexType.
/// This centralizes all indexing logic so that `SparqlDatabase` can just delegate to it.
#[derive(Debug, Clone)]
pub struct IndexManager {
    indexes: HashMap<IndexType, TripleIndex>,
}

impl IndexManager {
    /// Initialize one `TripleIndex` for each `IndexType`.
    pub fn new() -> Self {
        let mut indexes = HashMap::new();
        indexes.insert(IndexType::SubjectPredicate, TripleIndex::new(IndexType::SubjectPredicate));
        indexes.insert(IndexType::PredicateSubject, TripleIndex::new(IndexType::PredicateSubject));
        indexes.insert(IndexType::SubjectObject,    TripleIndex::new(IndexType::SubjectObject));
        indexes.insert(IndexType::ObjectSubject,    TripleIndex::new(IndexType::ObjectSubject));
        indexes.insert(IndexType::PredicateObject,  TripleIndex::new(IndexType::PredicateObject));
        indexes.insert(IndexType::ObjectPredicate,  TripleIndex::new(IndexType::ObjectPredicate));

        Self { indexes }
    }

    /// Get an immutable reference to a specific index.
    pub fn get_index(&self, index_type: IndexType) -> Option<&TripleIndex> {
        self.indexes.get(&index_type)
    }

    /// Get a mutable reference to a specific index.
    pub fn get_index_mut(&mut self, index_type: IndexType) -> Option<&mut TripleIndex> {
        self.indexes.get_mut(&index_type)
    }

    /// Clear all indexes at once (useful if you need to rebuild everything).
    pub fn clear_all(&mut self) {
        for index in self.indexes.values_mut() {
            index.clear();
        }
    }

    /// Sort and deduplicate the values in every index.
    pub fn optimize_all(&mut self) {
        for index in self.indexes.values_mut() {
            index.optimize();
        }
    }
}