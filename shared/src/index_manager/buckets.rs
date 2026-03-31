use serde::{Serialize, Deserialize};

use std::collections::{HashSet, HashMap};

use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

// ── helpers ──────────────────────────────────────────────────────────────────

fn get_triple_field(triple: &Triple, pos: usize) -> u32 {
    match pos {
        0 => triple.subject,
        1 => triple.predicate,
        2 => triple.object,
        _ => panic!("invalid position {pos}"),
    }
}

fn is_one_constant_pattern(pattern: &TriplePattern) -> bool {
    let (s, p, o) = pattern;
    matches!(
        (s, p, o),
        (Constant(_), Variable(_), Variable(_))
        | (Variable(_), Constant(_), Variable(_))
        | (Variable(_), Variable(_), Constant(_))
    )
}

// ── TwoWayData ────────────────────────────────────────────────────────────────

/// Bidirectional index for buckets with exactly one bound constant.
///
/// Given a pattern like `(?s, C, ?o)`:
///   - `pos_a = 0` (subject), `pos_b = 2` (object), `const_pos = 1`, `const_val = C`
///   - `forward`:  subject  → { objects  … }
///   - `backward`: object   → { subjects … }
///
/// A query that binds `pos_a` (e.g. `bound_s, C, ?o`) is served by a single
/// `forward` lookup instead of iterating the whole bucket.
#[derive(Debug, Clone)]
pub struct TwoWayData {
    pos_a:     usize,
    pos_b:     usize,
    const_pos: usize,
    const_val: u32,
    forward:   HashMap<u32, HashSet<u32>>,   // pos_a_val → { pos_b_val, … }
    backward:  HashMap<u32, HashSet<u32>>,   // pos_b_val → { pos_a_val, … }
}

impl TwoWayData {
    fn from_pattern(pattern: &TriplePattern) -> Self {
        let (s, p, o) = pattern;
        let mut free      = Vec::new();
        let mut const_pos = 0;
        let mut const_val = 0u32;

        for (i, term) in [s, p, o].iter().enumerate() {
            match term {
                Variable(_)  => free.push(i),
                Constant(c)  => { const_pos = i; const_val = *c; }
            }
        }

        assert_eq!(free.len(), 2, "TwoWayData requires exactly one constant");

        Self {
            pos_a: free[0], pos_b: free[1],
            const_pos, const_val,
            forward: HashMap::new(), backward: HashMap::new(),
        }
    }

    fn build_triple(&self, a: u32, b: u32) -> Triple {
        let mut vals = [0u32; 3];
        vals[self.pos_a]    = a;
        vals[self.pos_b]    = b;
        vals[self.const_pos] = self.const_val;
        Triple { subject: vals[0], predicate: vals[1], object: vals[2] }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        let a = get_triple_field(triple, self.pos_a);
        let b = get_triple_field(triple, self.pos_b);
        let inserted = self.forward.entry(a).or_default().insert(b);
        self.backward.entry(b).or_default().insert(a);
        inserted
    }

    fn remove(&mut self, triple: &Triple) -> bool {
        let a = get_triple_field(triple, self.pos_a);
        let b = get_triple_field(triple, self.pos_b);

        let removed = if let Some(set) = self.forward.get_mut(&a) {
            let r = set.remove(&b);
            if set.is_empty() { self.forward.remove(&a); }
            r
        } else { false };

        if removed {
            if let Some(set) = self.backward.get_mut(&b) {
                set.remove(&a);
                if set.is_empty() { self.backward.remove(&b); }
            }
        }

        removed
    }

    /// Query using `q[0..=2]` = `[s, p, o]` as `Option<u32>`.
    /// The constant position is already guaranteed to match by the time we get here.
    fn query(&self, q: [Option<u32>; 3]) -> Vec<Triple> {
        let qa = q[self.pos_a];
        let qb = q[self.pos_b];

        match (qa, qb) {
            // One free dimension bound → single hashmap lookup, O(output)
            (Some(a), None) => {
                self.forward.get(&a).map_or(Vec::new(), |bs| {
                    bs.iter().map(|&b| self.build_triple(a, b)).collect()
                })
            }
            (None, Some(b)) => {
                self.backward.get(&b).map_or(Vec::new(), |as_| {
                    as_.iter().map(|&a| self.build_triple(a, b)).collect()
                })
            }
            // Both free dimensions bound → existence check
            (Some(a), Some(b)) => {
                if self.forward.get(&a).map_or(false, |bs| bs.contains(&b)) {
                    vec![self.build_triple(a, b)]
                } else {
                    Vec::new()
                }
            }
            // Nothing extra bound → dump everything
            (None, None) => {
                self.forward.iter()
                    .flat_map(|(&a, bs)| bs.iter().map(move |&b| (a, b)))
                    .map(|(a, b)| self.build_triple(a, b))
                    .collect()
            }
        }
    }

    fn triple_count(&self) -> usize {
        self.forward.values().map(|s| s.len()).sum()
    }

    fn clear(&mut self) {
        self.forward.clear();
        self.backward.clear();
    }

    fn shrink_to_fit(&mut self) {
        for s in self.forward.values_mut()  { s.shrink_to_fit(); }
        for s in self.backward.values_mut() { s.shrink_to_fit(); }
        self.forward.shrink_to_fit();
        self.backward.shrink_to_fit();
    }
}

// ── BucketData ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum BucketData {
    /// Patterns with 0, 2, or 3 constants – a flat set is fine.
    Simple(HashSet<Triple>),
    /// Pattern with exactly 1 constant – bidirectional maps for O(output) lookups.
    TwoWay(TwoWayData),
}

// ── Bucket ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Bucket {
    pub pattern: TriplePattern,
    pub data:    BucketData,
}

impl Bucket {
    pub fn new(pattern: TriplePattern) -> Self {
        let data = if is_one_constant_pattern(&pattern) {
            BucketData::TwoWay(TwoWayData::from_pattern(&pattern))
        } else {
            BucketData::Simple(HashSet::new())
        };
        Self { pattern, data }
    }

    pub fn matches(&self, triple: &Triple) -> bool {
        let (s, p, o) = &self.pattern;
        let s_ok = match s { Constant(c) => triple.subject   == *c, Variable(_) => true };
        let p_ok = match p { Constant(c) => triple.predicate == *c, Variable(_) => true };
        let o_ok = match o { Constant(c) => triple.object    == *c, Variable(_) => true };
        s_ok && p_ok && o_ok
    }

    pub fn insert(&mut self, triple: &Triple) -> bool {
        match &mut self.data {
            BucketData::Simple(set) => set.insert(triple.clone()),
            BucketData::TwoWay(tw) => tw.insert(triple),
        }
    }

    pub fn remove(&mut self, triple: &Triple) -> bool {
        match &mut self.data {
            BucketData::Simple(set) => set.remove(triple),
            BucketData::TwoWay(tw) => tw.remove(triple),
        }
    }

    pub fn triple_count(&self) -> usize {
        match &self.data {
            BucketData::Simple(set) => set.len(),
            BucketData::TwoWay(tw)  => tw.triple_count(),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.data {
            BucketData::Simple(set) => set.clear(),
            BucketData::TwoWay(tw)  => tw.clear(),
        }
    }

    pub fn shrink_to_fit(&mut self) {
        match &mut self.data {
            BucketData::Simple(set) => set.shrink_to_fit(),
            BucketData::TwoWay(tw)  => tw.shrink_to_fit(),
        }
    }

    /// Return triples that match the given optional bindings.
    /// Callers must ensure the bucket covers the query (i.e. `bucket_covers_query` passed).
    pub fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        match &self.data {
            BucketData::Simple(set) => {
                set.iter()
                    .filter(|t| {
                        (s.is_none() || s == Some(t.subject))   &&
                        (p.is_none() || p == Some(t.predicate)) &&
                        (o.is_none() || o == Some(t.object))
                    })
                    .cloned()
                    .collect()
            }
            BucketData::TwoWay(tw) => tw.query([s, p, o]),
        }
    }
}

// ── BucketIndex ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BucketIndex {
    pub buckets: Vec<Bucket>,
}

impl BucketIndex {
    pub fn new(patterns: Vec<TriplePattern>) -> Self {
        let mut unique_patterns: Vec<TriplePattern> = Vec::new();

        for p in patterns {
            let is_dup = unique_patterns.iter().any(|e| Self::patterns_equivalent(e, &p));
            if !is_dup { unique_patterns.push(p); }
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

    fn patterns_equivalent(p1: &TriplePattern, p2: &TriplePattern) -> bool {
        let match_term = |t1: &Term, t2: &Term| match (t1, t2) {
            (Constant(c1), Constant(c2)) => c1 == c2,
            (Variable(_),  Variable(_))  => true,
            _                            => false,
        };
        match_term(&p1.0, &p2.0) && match_term(&p1.1, &p2.1) && match_term(&p1.2, &p2.2)
    }

    fn bucket_covers_query(bucket_pat: &TriplePattern, q_s: Option<u32>, q_p: Option<u32>, q_o: Option<u32>) -> bool {
        let (b_s, b_p, b_o) = bucket_pat;
        let s_safe = match b_s { Variable(_) => true, Constant(c) => q_s == Some(*c) };
        let p_safe = match b_p { Variable(_) => true, Constant(c) => q_p == Some(*c) };
        let o_safe = match b_o { Variable(_) => true, Constant(c) => q_o == Some(*c) };
        s_safe && p_safe && o_safe
    }

    fn is_exact_match(bucket_pat: &TriplePattern, q_s: Option<u32>, q_p: Option<u32>, q_o: Option<u32>) -> bool {
        let (b_s, b_p, b_o) = bucket_pat;
        let s_ok = match b_s { Constant(c) => q_s == Some(*c), Variable(_) => q_s.is_none() };
        let p_ok = match b_p { Constant(c) => q_p == Some(*c), Variable(_) => q_p.is_none() };
        let o_ok = match b_o { Constant(c) => q_o == Some(*c), Variable(_) => q_o.is_none() };
        s_ok && p_ok && o_ok
    }
}

impl TripleIndex for BucketIndex {
    fn clone_empty(&self) -> Box<dyn TripleIndex> {
        let patterns = self.buckets.iter().map(|b| b.pattern.clone()).collect();
        Box::new(BucketIndex::new(patterns))
    }

    fn clone_box(&self) -> Box<dyn TripleIndex> {
        Box::new(self.clone())
    }

    fn triple_count(&self) -> usize {
        // Buckets may overlap, so deduplicate.
        // For TwoWay buckets we reconstruct triples on the fly; this is the one
        // place where the extra memory of TwoWay costs a bit more to count.
        let mut unique: HashSet<Triple> = HashSet::new();
        for bucket in &self.buckets {
            // query(None,None,None) works for both Simple and TwoWay
            unique.extend(bucket.query(None, None, None));
        }
        unique.len()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport { sp: false, so: false, po: false, ps: false, os: false, op: false }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        let mut inserted_anywhere = false;
        for bucket in &mut self.buckets {
            if bucket.matches(triple) && bucket.insert(triple) {
                inserted_anywhere = true;
            }
        }
        inserted_anywhere
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        let mut deleted_anywhere = false;
        for bucket in &mut self.buckets {
            if bucket.remove(triple) {
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
            if self.insert(triple) { insert_count += 1; }
        }

        println!("Finished building. {}/{} triples matched at least one bucket.", insert_count, triples.len());
        self.optimize();
    }

    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        // Exact match: the bucket pattern mirrors the query exactly, no extra filtering.
        if let Some(b) = self.buckets.iter().find(|b| Self::is_exact_match(&b.pattern, s, p, o)) {
            return b.query(s, p, o);
        }

        // Covering match: bucket is more general; Bucket::query() handles the filtering,
        // and TwoWay buckets do it in O(output) via a hashmap lookup.
        if let Some(b) = self.buckets.iter().find(|b| Self::bucket_covers_query(&b.pattern, s, p, o)) {
            return b.query(s, p, o);
        }

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
        for bucket in &mut self.buckets { bucket.clear(); }
    }

    fn scan_sp(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_so(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_po(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }

    fn optimize(&mut self) {
        for bucket in &mut self.buckets { bucket.shrink_to_fit(); }
    }
}
