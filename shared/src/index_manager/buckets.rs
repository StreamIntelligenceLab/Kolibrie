use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

/// A single bucket mapped to a specific access pattern.
/// It only stores triples that match its designated `TriplePattern`.
#[derive(Debug, Clone)]
pub struct Bucket {
    pub pattern: TriplePattern,
    pub data: HashSet<Triple>,
}

impl Bucket {
    pub fn new(pattern: TriplePattern) -> Self {
        Self {
            pattern,
            data: HashSet::new(),
        }
    }

    /// Checks if a given triple matches this bucket's specific pattern.
    pub fn matches(&self, triple: &Triple) -> bool {
        let (s, p, o) = &self.pattern;
        
        let s_match = match s { Constant(c) => triple.subject == *c, Variable(_) => true };
        let p_match = match p { Constant(c) => triple.predicate == *c, Variable(_) => true };
        let o_match = match o { Constant(c) => triple.object == *c, Variable(_) => true };
        
        s_match && p_match && o_match
    }
}

/// The main indexer that manages multiple isolated buckets.
#[derive(Debug, Clone)]
pub struct BucketIndex {
    pub buckets: Vec<Bucket>,
}

impl BucketIndex {
    pub fn new(patterns: Vec<TriplePattern>) -> Self {
        let mut unique_patterns: Vec<TriplePattern> = Vec::new();

        // Deduplicate patterns before creating buckets.
        // We consider any Variable equivalent to any other Variable, 
        // and Constants equivalent if their inner values match.
        for p in patterns {
            let is_duplicate = unique_patterns.iter().any(|existing| Self::patterns_equivalent(existing, &p));
            if !is_duplicate {
                unique_patterns.push(p);
            }
        }

        println!("--- BucketIndex Initialization ---");
        println!("Requested patterns: {}, Unique buckets created: {}", unique_patterns.len(), unique_patterns.len());
        if unique_patterns.is_empty() {
            println!("WARNING: BucketIndex initialized with 0 patterns! No data will be stored.");
        }

        let buckets = unique_patterns.into_iter().enumerate().map(|(i, pat)| {
            println!("  Bucket [{}]: {:?}", i, pat);
            Bucket::new(pat)
        }).collect();

        Self { buckets }
    }

    /// Helper to check if two TriplePatterns are semantically equivalent.
    /// This prevents creating separate buckets for (?s, ?p, ?o) and (?x, ?y, ?z)
    /// if the Variable inner IDs differ.
    fn patterns_equivalent(p1: &TriplePattern, p2: &TriplePattern) -> bool {
        let match_term = |t1: &Term, t2: &Term| -> bool {
            match (t1, t2) {
                (Constant(c1), Constant(c2)) => c1 == c2,
                (Variable(_), Variable(_)) => true, // All variables are treated as equivalent "unbound" slots
                _ => false,
            }
        };

        match_term(&p1.0, &p2.0) && match_term(&p1.1, &p2.1) && match_term(&p1.2, &p2.2)
    }

    /// Determines if a bucket's pattern "covers" the query pattern.
    /// A bucket covers a query if the bucket is EQUAL to or MORE GENERAL than the query.
    /// If the bucket is more specific than the query, it is unsafe to use.
    fn bucket_covers_query(bucket_pat: &TriplePattern, q_s: Option<u32>, q_p: Option<u32>, q_o: Option<u32>) -> bool {
        let (b_s, b_p, b_o) = bucket_pat;

        let s_safe = match b_s { Variable(_) => true, Constant(c) => q_s == Some(*c) };
        let p_safe = match b_p { Variable(_) => true, Constant(c) => q_p == Some(*c) };
        let o_safe = match b_o { Variable(_) => true, Constant(c) => q_o == Some(*c) };

        s_safe && p_safe && o_safe
    }

    /// Checks if a bucket pattern is an exact match for the query options.
    fn is_exact_match(bucket_pat: &TriplePattern, q_s: Option<u32>, q_p: Option<u32>, q_o: Option<u32>) -> bool {
        let (b_s, b_p, b_o) = bucket_pat;
        
        let s_match = match b_s { Constant(c) => q_s == Some(*c), Variable(_) => q_s.is_none() };
        let p_match = match b_p { Constant(c) => q_p == Some(*c), Variable(_) => q_p.is_none() };
        let o_match = match b_o { Constant(c) => q_o == Some(*c), Variable(_) => q_o.is_none() };
        
        s_match && p_match && o_match
    }
}

impl TripleIndex for BucketIndex {
    fn clone_empty(&self) -> Box<dyn TripleIndex> {
        // Re-create the index using the exact same bucket patterns, but empty.
        let patterns = self.buckets.iter().map(|b| b.pattern.clone()).collect();
        Box::new(BucketIndex::new(patterns))
    }

    fn clone_box(&self) -> Box<dyn TripleIndex> {
        Box::new(self.clone())
    }

    fn triple_count(&self) -> usize {
        // Because triples might be stored twice or more across different buckets, 
        // we must deduplicate them to get the true logical count.
        let mut unique = HashSet::new();
        for bucket in &self.buckets {
            for triple in &bucket.data {
                unique.insert(triple.clone());
            }
        }
        unique.len()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport { sp: false, so: false, po: false, ps: false, os: false, op: false }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        let mut inserted_anywhere = false;
        
        for bucket in &mut self.buckets {
            if bucket.matches(triple) {
                if bucket.data.insert(triple.clone()) {
                    inserted_anywhere = true;
                }
            }
        }
        
        inserted_anywhere
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        let mut deleted_anywhere = false;
        for bucket in &mut self.buckets {
            if bucket.data.remove(triple) {
                deleted_anywhere = true;
            }
        }
        deleted_anywhere
    }

    fn build_from_triples(&mut self, triples: &[Triple]) {
        self.clear();
        
        println!("Building BucketIndex with {} triples across {} buckets...", triples.len(), self.buckets.len());
        
        if self.buckets.is_empty() {
            println!("WARNING: Cannot build from triples because 0 buckets exist!");
            return;
        }

        let mut insert_count = 0;
        for triple in triples {
            if self.insert(triple) {
                insert_count += 1;
            }
        }
        
        println!("Finished building. {}/{} triples matched at least one bucket.", insert_count, triples.len());
        self.optimize();
    }

    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        // Step 1: Look for an EXACT match bucket first (no extra filtering needed)
        if let Some(exact_bucket) = self.buckets.iter().find(|b| Self::is_exact_match(&b.pattern, s, p, o)) {
            return exact_bucket.data.iter().cloned().collect();
        }

        // Step 2: Look for a "covering" bucket (more general than the query)
        if let Some(covering_bucket) = self.buckets.iter().find(|b| Self::bucket_covers_query(&b.pattern, s, p, o)) {
            return covering_bucket.data.iter()
                .filter(|t| {
                    (s.is_none() || s == Some(t.subject)) &&
                    (p.is_none() || p == Some(t.predicate)) &&
                    (o.is_none() || o == Some(t.object))
                })
                .cloned()
                .collect();
        }

        // Step 3: If no bucket covers this query, it is unsafe.
        eprintln!("Warning: Query {:?} {:?} {:?} is too general for the existing buckets. Returning empty.", s, p, o);
        Vec::new()
    }

    fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
        let (s, p, o) = pattern;
        let sub = match s { Constant(x) => Some(*x), Variable(_) => None };
        let pre = match p { Constant(x) => Some(*x), Variable(_) => None };
        let obj = match o { Constant(x) => Some(*x), Variable(_) => None };
        
        self.query(sub, pre, obj)
    }

    fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.data.clear();
        }
    }

    fn scan_sp(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_so(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_po(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }

    fn optimize(&mut self) {
        for bucket in &mut self.buckets {
            bucket.data.shrink_to_fit();
        }
    }
}