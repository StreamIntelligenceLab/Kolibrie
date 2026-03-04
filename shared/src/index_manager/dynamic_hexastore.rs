//hexastore, but only builds indexes if they would be used
//uses heuristic to determine when index is valuable
//Step 1: dynamic hexastore is initialized with an array of access patterns represented as Triples
//(with bound and or unbound variables, eg: (s, p, ?o) could be determined with sp scan or ps scan)
//Step 2: dynamic hexastore chooses initial necessary indexes, eg if all triples can be solved with
//ps scan, then only having pso index is sufficient.
//Step 3: dynamic hexastore creates index pools per necessary index. And assigns an index pool to
//every access pattern (eg p and s bound, only p bound, ...)
//(An index pool is a pool of indexes with the only rule being that if you join all indexes in a
//pool, every triple in the current window must be present. We use this to switch between used
//indexes dynamically later on.
//Step 4: now we ingest data, when building the index from data or adding a data entry, we want to
//add this entry to every pool exactly once. This means every pool has at most 1 active index to
//which we insert. At first this will just be the only index present in the pool (like pso in our
//example). 
//Step 5: every set time interval we also check a heuristic to determine if we should switch the
//index used in a pool. To do this, we use the number of unique subjects, predicates and objects
//from the existing stats implementation which has the cardinalities of each of these.
//The heuristic works as follows: We determine a cost as an overhead value (which is static and set
//at build time) + the amount of hashset/map lookups. You can guess the amount of hashset/map
//lookups as follows: if you scan for example an spo index for a certain predicate, you will do #s
//lookups to find the s number of po maps. If you scan for a bound subject (predicate and object
//not bound) on this same index, you will only have to do 1 lookup to find the po map that has all
//entries. Having only a bound object, will necessitate a full table scan: first find every po map
//(#s lookups), then find every o set, (#p lookups or #s*#p lookups in total), then for every set
//check if the bound object is present (#s*#p*1 total lookups) We then also guess the cost of
//maintaining an index as follows: MAP_OVERHEAD * (1 + #s) + SET_OVERHEAD * (#s * #p) + SPACE_OVERHEAD * (#s * #p * #o)
//Then we find the set of indexes to maintain that minimizes cost = sum of cost heurists for each
//access pattern passed during initialisation + sum of cost heuristics for every index in that set
//There will already be existing indexes, since we at first initialize these based on which access
//patterns we have exclusively (not taking into account data cardinalities, since we dont know that
//yet at initialisation). To successfully transition from the old indexes without having to do an
//expensive complete copy operation between the indexes, we do the following:
//1) Create a new pool for every index in the new index set
//2) For every access pattern, use the heuristics to find the best pool to assign it to.
//3) For every new pool, find the old pool that creates the lowest total cost across all access
//   patterns assigned to the new pool.
//4) Any unassigned old pools are deleted.
//5) Of the old pool(s) that were assigned to a new pool, check if the desired index of the new
//   pool is present in the old pool (doesnt matter if active or not).
//   -  If so: we dont have to create any new indexes for that pool, and the old pool just becomes
//   the new pool, only (possibly) changing the active index.
//   -  If not: create a new index of the type that we want for the new pool, set that to the
//   active index, and maintain the indexes of the old pool in the new pool to maintain the data
//6) In the data deletion function, that deletes a triple from the triplestore, (attempt to) delete
//   the triple from every index in every pool. Also check for every index if the index is empty
//   after this. If so, and it is not the active index, delete that index from the pool.
// By adding to only one index per pool and deleting unilaterally, eventually, unless we keep
// switching active indexes, each pool will converge to have one index.
//
//
//
//
//



use std::collections::HashSet;
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

// ─── Cost heuristic constants ───────────────────────────────────────────────
/// Fixed overhead per HashMap maintained in an index (models allocation, cache pressure, etc.)
const MAP_OVERHEAD: f64 = 5.0;
/// Fixed overhead per HashSet maintained in an index
const SET_OVERHEAD: f64 = 2.0;
/// Per-triple space overhead (models memory occupied by each stored value)
const SPACE_OVERHEAD: f64 = 0.01;

// ─── Index type enum ────────────────────────────────────────────────────────
/// The six possible triple-index orderings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    SPO, // subject → predicate → object
    SOP, // subject → object   → predicate
    PSO, // predicate → subject → object
    POS, // predicate → object  → subject
    OSP, // object → subject   → predicate
    OPS, // object → predicate → subject
}

impl IndexType {
    /// All six permutations.
    const ALL: [IndexType; 6] = [
        IndexType::SPO, IndexType::SOP,
        IndexType::PSO, IndexType::POS,
        IndexType::OSP, IndexType::OPS,
    ];

    /// Create a fresh, empty `Box<dyn TripleIndex>` for this type.
    fn create_empty(&self) -> Box<dyn TripleIndex> {
        match self {
            IndexType::SPO => Box::new(SPOSingleIndex::new()),
            IndexType::SOP => Box::new(SOPSingleIndex::new()),
            IndexType::PSO => Box::new(PSOSingleIndex::new()),
            IndexType::POS => Box::new(POSSingleIndex::new()),
            IndexType::OSP => Box::new(OSPSingleIndex::new()),
            IndexType::OPS => Box::new(OPSSingleIndex::new()),
        }
    }

    /// Which two-key scan does this index natively support?
    fn native_scan(&self) -> ScanKind {
        match self {
            IndexType::SPO => ScanKind::SP,
            IndexType::SOP => ScanKind::SO,
            IndexType::PSO => ScanKind::PS,
            IndexType::POS => ScanKind::PO,
            IndexType::OSP => ScanKind::OS,
            IndexType::OPS => ScanKind::OP,
        }
    }
}

// ─── Access pattern helpers ─────────────────────────────────────────────────
/// The six possible two-key scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ScanKind { SP, SO, PS, PO, OS, OP }

/// Compact representation of which components of a triple pattern are bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BoundPattern {
    s: bool,
    p: bool,
    o: bool,
}

impl BoundPattern {
    fn from_triple_pattern(pat: &TriplePattern) -> Self {
        Self {
            s: matches!(pat.0, Term::Constant(_)),
            p: matches!(pat.1, Term::Constant(_)),
            o: matches!(pat.2, Term::Constant(_)),
        }
    }

    /// Which scans can serve this pattern efficiently (≤ 2 lookups to reach the leaf set)?
    fn efficient_scans(&self) -> Vec<ScanKind> {
        match (self.s, self.p, self.o) {
            // Two bound: exactly one native scan is ideal
            (true, true, false) => vec![ScanKind::SP, ScanKind::PS],
            (true, false, true) => vec![ScanKind::SO, ScanKind::OS],
            (false, true, true) => vec![ScanKind::PO, ScanKind::OP],
            // One bound: indexes where the bound component is the first key
            (true, false, false) => vec![ScanKind::SP, ScanKind::SO],
            (false, true, false) => vec![ScanKind::PS, ScanKind::PO],
            (false, false, true) => vec![ScanKind::OS, ScanKind::OP],
            // Fully bound or fully unbound: any index works
            _ => vec![ScanKind::SP, ScanKind::SO, ScanKind::PS, ScanKind::PO, ScanKind::OS, ScanKind::OP],
        }
    }

    /// Which IndexTypes can serve this pattern efficiently?
    fn efficient_index_types(&self) -> Vec<IndexType> {
        self.efficient_scans()
            .into_iter()
            .map(|sk| match sk {
                ScanKind::SP => IndexType::SPO,
                ScanKind::SO => IndexType::SOP,
                ScanKind::PS => IndexType::PSO,
                ScanKind::PO => IndexType::POS,
                ScanKind::OS => IndexType::OSP,
                ScanKind::OP => IndexType::OPS,
            })
            .collect()
    }
}

// ─── Cardinality snapshot (from DatabaseStats or estimated) ─────────────────
/// Lightweight snapshot of the unique-count statistics needed by the heuristic.
#[derive(Debug, Clone)]
pub struct CardinalitySnapshot {
    pub num_subjects: f64,
    pub num_predicates: f64,
    pub num_objects: f64,
}

impl CardinalitySnapshot {
    /// Build from raw counts.
    pub fn from_stats(
        _total_triples: u64,
        unique_subjects: usize,
        unique_predicates: usize,
        unique_objects: usize,
    ) -> Self {
        // Use at least 1.0 to avoid division-by-zero in cost formulas
        Self {
            num_subjects: (unique_subjects as f64).max(1.0),
            num_predicates: (unique_predicates as f64).max(1.0),
            num_objects: (unique_objects as f64).max(1.0),
        }
    }

    /// A default snapshot when we have no data yet.
    fn unknown() -> Self {
        Self { num_subjects: 1.0, num_predicates: 1.0, num_objects: 1.0 }
    }
}

// ─── Cost heuristic functions ───────────────────────────────────────────────

/// Estimate the query cost (number of hash lookups) for scanning `idx` to answer `pat`.
/// Lower is better.
fn query_cost(idx: IndexType, pat: BoundPattern, card: &CardinalitySnapshot) -> f64 {
    let s = card.num_subjects;
    let p = card.num_predicates;
    let o = card.num_objects;

    // The three levels of the nested HashMap for this index type:
    let (l1, l2, _l3) = index_level_sizes(idx, s, p, o);

    match (pat.s, pat.p, pat.o) {
        // Fully bound – always 3 lookups regardless of index
        (true, true, true) => 3.0,

        // Two bound
        (true, true, false) => two_bound_cost(idx, 's', 'p', l1, l2),
        (true, false, true) => two_bound_cost(idx, 's', 'o', l1, l2),
        (false, true, true) => two_bound_cost(idx, 'p', 'o', l1, l2),

        // One bound
        (true, false, false) => one_bound_cost(idx, 's', l1, l2),
        (false, true, false) => one_bound_cost(idx, 'p', l1, l2),
        (false, false, true) => one_bound_cost(idx, 'o', l1, l2),

        // No bound – full scan: iterate all level-1, all level-2
        (false, false, false) => l1 * l2,
    }
}

/// Return (#level1, #level2-per-l1, #level3-per-l2) for an index type given cardinalities.
fn index_level_sizes(idx: IndexType, s: f64, p: f64, o: f64) -> (f64, f64, f64) {
    match idx {
        IndexType::SPO => (s, p, o),
        IndexType::SOP => (s, o, p),
        IndexType::PSO => (p, s, o),
        IndexType::POS => (p, o, s),
        IndexType::OSP => (o, s, p),
        IndexType::OPS => (o, p, s),
    }
}

/// Map a component char ('s','p','o') to the position (0,1,2) in the given index type.
fn component_position(idx: IndexType, comp: char) -> usize {
    let order = match idx {
        IndexType::SPO => ['s', 'p', 'o'],
        IndexType::SOP => ['s', 'o', 'p'],
        IndexType::PSO => ['p', 's', 'o'],
        IndexType::POS => ['p', 'o', 's'],
        IndexType::OSP => ['o', 's', 'p'],
        IndexType::OPS => ['o', 'p', 's'],
    };
    order.iter().position(|&c| c == comp).unwrap_or(2)
}

/// Cost when exactly two components are bound.
fn two_bound_cost(
    idx: IndexType, a: char, b: char,
    l1: f64, l2: f64,
) -> f64 {
    let pos_a = component_position(idx, a);
    let pos_b = component_position(idx, b);

    match (pos_a, pos_b) {
        // Both bound components match first two keys → 2 lookups (ideal)
        (0, 1) | (1, 0) => 2.0,
        // First key bound, third key bound → 1 lookup + iterate level-2
        (0, 2) | (2, 0) => 1.0 + l2,
        // Second key bound, third key bound → iterate level-1, then 1 lookup each
        (1, 2) | (2, 1) => l1 + l1,
        _ => l1 * l2, // fallback
    }
}

/// Cost when exactly one component is bound.
fn one_bound_cost(
    idx: IndexType, comp: char,
    l1: f64, l2: f64,
) -> f64 {
    let pos = component_position(idx, comp);
    match pos {
        0 => 1.0 + l2,           // first key bound → 1 lookup, iterate second level
        1 => l1,                  // second key bound → iterate first level, 1 lookup each
        2 => l1 * l2,            // third key bound → full scan of first two levels
        _ => l1 * l2,
    }
}

/// Estimate the maintenance cost of keeping an index alive.
fn maintenance_cost(idx: IndexType, card: &CardinalitySnapshot) -> f64 {
    let (l1, l2, l3) = index_level_sizes(idx, card.num_subjects, card.num_predicates, card.num_objects);
    MAP_OVERHEAD * (1.0 + l1) + SET_OVERHEAD * (l1 * l2) + SPACE_OVERHEAD * (l1 * l2 * l3)
}

// ─── Index Pool ─────────────────────────────────────────────────────────────

/// A pool of indexes that collectively cover all triples in the store.
/// Exactly one index is *active* (receives inserts); the rest are legacy
/// indexes being drained through deletions.
#[derive(Debug, Clone)]
struct IndexPool {
    /// The index type that this pool ideally maintains.
    desired_type: IndexType,
    /// Index of the active (insert-target) index in `indexes`.
    active_idx: usize,
    /// The types corresponding to each index in `indexes`.
    types: Vec<IndexType>,
    /// The actual index implementations.
    indexes: Vec<Box<dyn TripleIndex>>,
}

impl IndexPool {
    fn new(desired: IndexType) -> Self {
        let idx = desired.create_empty();
        Self {
            desired_type: desired,
            active_idx: 0,
            types: vec![desired],
            indexes: vec![idx],
        }
    }

    /// Insert a triple into the active index only.
    fn insert(&mut self, triple: &Triple) -> bool {
        self.indexes[self.active_idx].insert(triple)
    }

    /// Delete a triple from *every* index in the pool.
    /// Also garbage-collect empty non-active indexes.
    fn delete(&mut self, triple: &Triple) -> bool {
        let mut any_deleted = false;
        for idx in &mut self.indexes {
            if idx.delete(triple) {
                any_deleted = true;
            }
        }
        // Garbage-collect empty non-active indexes (walk backwards for safe removal)
        let mut i = self.indexes.len();
        while i > 0 {
            i -= 1;
            if i != self.active_idx && self.indexes[i].triple_count() == 0 {
                self.indexes.remove(i);
                self.types.remove(i);
                // Adjust active_idx if it was shifted
                if self.active_idx > i {
                    self.active_idx -= 1;
                }
            }
        }
        any_deleted
    }

    /// Does this pool contain an index of the given type?
    fn contains_type(&self, t: IndexType) -> bool {
        self.types.contains(&t)
    }

    /// Switch the active index to the given type.
    /// If the type already exists in the pool, just change the active pointer.
    /// Otherwise, create a new empty index of that type and make it active.
    fn switch_active(&mut self, new_type: IndexType) {
        if let Some(pos) = self.types.iter().position(|&t| t == new_type) {
            self.active_idx = pos;
        } else {
            let new_idx = new_type.create_empty();
            self.indexes.push(new_idx);
            self.types.push(new_type);
            self.active_idx = self.indexes.len() - 1;
        }
        self.desired_type = new_type;
    }

    /// Query: for correctness we must query ALL indexes in the pool and merge
    /// results (new inserts go to the active index but older data may still
    /// reside in legacy indexes).
    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        if self.indexes.len() == 1 {
            return self.indexes[0].query(s, p, o);
        }
        // Merge results from all indexes, deduplicate
        let mut seen = HashSet::new();
        let mut results = Vec::new();
        for idx in &self.indexes {
            for triple in idx.query(s, p, o) {
                if seen.insert((triple.subject, triple.predicate, triple.object)) {
                    results.push(triple);
                }
            }
        }
        results
    }

    fn clear(&mut self) {
        for idx in &mut self.indexes {
            idx.clear();
        }
    }

    fn triple_count(&self) -> usize {
        // The union of all indexes; must deduplicate.
        self.query(None, None, None).len()
    }

    /// Helper: merge scan results from all indexes that support the scan.
    fn merge_scan<F>(&self, scan_fn: F) -> Option<Vec<u32>>
    where
        F: Fn(&Box<dyn TripleIndex>) -> Option<&HashSet<u32>>,
    {
        let mut merged: Option<HashSet<u32>> = None;
        for idx in &self.indexes {
            if let Some(set) = scan_fn(idx) {
                match &mut merged {
                    Some(m) => m.extend(set.iter().copied()),
                    None => merged = Some(set.clone()),
                }
            }
        }
        merged.map(|s| s.into_iter().collect())
    }
}

// ─── DynamicHexastoreIndex ──────────────────────────────────────────────────

/// A dynamic hexastore that only builds the index permutations that are
/// actually needed by the registered access patterns, and can switch
/// the active index within each pool based on data-cardinality heuristics.
#[derive(Debug, Clone)]
pub struct DynamicHexastoreIndex {
    /// The access patterns this index was initialised with.
    access_patterns: Vec<TriplePattern>,
    /// One pool per selected index permutation.
    pools: Vec<IndexPool>,
    /// For each access pattern (by position in `access_patterns`), which pool index serves it.
    pattern_to_pool: Vec<usize>,
    /// How many triples have been inserted since last re-evaluation.
    inserts_since_eval: usize,
    /// How often (in inserts) to re-evaluate the heuristic.
    eval_interval: usize,
    /// Latest cardinality snapshot (updated during re-evaluation).
    latest_card: CardinalitySnapshot,
}

impl DynamicHexastoreIndex {
    // ── Construction ────────────────────────────────────────────────────

    /// Create a new `DynamicHexastoreIndex` tailored to the given access patterns.
    ///
    /// `eval_interval` controls how often (in number of inserts) the heuristic
    /// is re-evaluated.  A typical value is 10 000.
    pub fn new(access_patterns: Vec<TriplePattern>, eval_interval: usize) -> Self {
        let eval_interval = eval_interval.max(1);
        let card = CardinalitySnapshot::unknown();

        // Step 2: choose initial indexes purely from access patterns
        let needed = Self::initial_index_set(&access_patterns);

        // Step 3: create pools & assign patterns
        let pools: Vec<IndexPool> = needed.iter().map(|&t| IndexPool::new(t)).collect();
        let pattern_to_pool = Self::assign_patterns_to_pools(&access_patterns, &pools, &card);

        Self {
            access_patterns,
            pools,
            pattern_to_pool,
            inserts_since_eval: 0,
            eval_interval,
            latest_card: card,
        }
    }

    /// Convenience: create with default eval interval.
    pub fn with_patterns(access_patterns: Vec<TriplePattern>) -> Self {
        Self::new(access_patterns, 10_000)
    }

    // ── Step 2: choose initial indexes ──────────────────────────────────

    fn initial_index_set(patterns: &[TriplePattern]) -> Vec<IndexType> {
        if patterns.is_empty() {
            // Fallback: just use SPO
            return vec![IndexType::SPO];
        }

        let mut needed: HashSet<IndexType> = HashSet::new();

        for pat in patterns {
            let bp = BoundPattern::from_triple_pattern(pat);
            let candidates = bp.efficient_index_types();
            if candidates.len() == 1 {
                // Only one good index for this pattern → must include it
                needed.insert(candidates[0]);
            }
        }

        // For patterns that have multiple candidates, try to reuse already-selected indexes
        for pat in patterns {
            let bp = BoundPattern::from_triple_pattern(pat);
            let candidates = bp.efficient_index_types();
            if candidates.len() > 1 {
                // Prefer an already-needed index
                let reuse = candidates.iter().find(|c| needed.contains(c));
                if reuse.is_none() {
                    // Pick the first candidate
                    needed.insert(candidates[0]);
                }
            }
        }

        if needed.is_empty() {
            needed.insert(IndexType::SPO);
        }

        needed.into_iter().collect()
    }

    // ── Step 3: assign patterns to pools ────────────────────────────────

    fn assign_patterns_to_pools(
        patterns: &[TriplePattern],
        pools: &[IndexPool],
        card: &CardinalitySnapshot,
    ) -> Vec<usize> {
        patterns
            .iter()
            .map(|pat| {
                let bp = BoundPattern::from_triple_pattern(pat);
                // Find the pool whose desired_type gives the lowest query cost
                pools
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| {
                        let ca = query_cost(a.desired_type, bp, card);
                        let cb = query_cost(b.desired_type, bp, card);
                        ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(i, _)| i)
                    .unwrap_or(0)
            })
            .collect()
    }

    // ── Step 5: re-evaluate with heuristic ──────────────────────────────

    /// Update the cardinality snapshot and optionally re-evaluate which
    /// indexes to maintain.
    pub fn update_cardinalities(&mut self, card: CardinalitySnapshot) {
        self.latest_card = card;
    }

    /// Force a re-evaluation of the index set based on the current cardinalities.
    pub fn reevaluate(&mut self) {
        // Clone the snapshot so we don't hold an immutable borrow on `self`
        // while calling `transition_pools(&mut self, …)`.
        let card = self.latest_card.clone();
        let patterns = &self.access_patterns;

        // Find the optimal set of index types
        let best_set = Self::find_best_index_set(patterns, &card);

        // Transition pools
        self.transition_pools(best_set, &card);
    }

    /// Find the set of IndexTypes that minimises total cost.
    fn find_best_index_set(patterns: &[TriplePattern], card: &CardinalitySnapshot) -> Vec<IndexType> {
        // We enumerate subsets of IndexType::ALL that can cover every pattern.
        // Since there are only 6 types, 2^6 = 64 subsets — perfectly tractable.

        let all = IndexType::ALL;
        let n = all.len();
        let mut best_cost = f64::MAX;
        let mut best_set: Vec<IndexType> = vec![IndexType::SPO]; // fallback

        let bound_patterns: Vec<BoundPattern> = patterns
            .iter()
            .map(|p| BoundPattern::from_triple_pattern(p))
            .collect();

        for mask in 1u32..(1 << n) {
            let set: Vec<IndexType> = (0..n)
                .filter(|&i| mask & (1 << i) != 0)
                .map(|i| all[i])
                .collect();

            // Check that every pattern can be served by at least one index in the set
            let covers_all = bound_patterns.iter().all(|bp| {
                let efficient = bp.efficient_index_types();
                // Either a native efficient index is in the set,
                // or at least some index exists (fallback to full scan)
                efficient.iter().any(|e| set.contains(e)) || !set.is_empty()
            });

            if !covers_all {
                continue;
            }

            // Compute total cost = query costs + maintenance costs
            let query_total: f64 = bound_patterns
                .iter()
                .map(|bp| {
                    // Pick the best index from the set for this pattern
                    set.iter()
                        .map(|&idx| query_cost(idx, *bp, card))
                        .fold(f64::MAX, f64::min)
                })
                .sum();

            let maint_total: f64 = set.iter().map(|&idx| maintenance_cost(idx, card)).sum();

            let total = query_total + maint_total;
            if total < best_cost {
                best_cost = total;
                best_set = set;
            }
        }

        best_set
    }

    /// Transition from old pools to new pools following the transition steps.
    fn transition_pools(&mut self, new_types: Vec<IndexType>, card: &CardinalitySnapshot) {
        // Step 1: create new pool descriptors
        let mut new_pools: Vec<IndexPool> = new_types.iter().map(|&t| IndexPool::new(t)).collect();

        // Step 2: assign every access pattern to the best new pool
        let new_assignment = Self::assign_patterns_to_pools(
            &self.access_patterns,
            &new_pools,
            card,
        );

        // Step 3: for every new pool, find the best matching old pool
        for (new_pool_idx, new_pool) in new_pools.iter_mut().enumerate() {
            // Collect access patterns assigned to this new pool
            let assigned_pats: Vec<usize> = new_assignment
                .iter()
                .enumerate()
                .filter(|(_, &pool_idx)| pool_idx == new_pool_idx)
                .map(|(pat_idx, _)| pat_idx)
                .collect();

            if assigned_pats.is_empty() {
                continue;
            }

            // Find the old pool that minimizes cost for these patterns
            let best_old = self.pools.iter().enumerate().min_by(|(_, a), (_, b)| {
                let cost_a: f64 = assigned_pats.iter().map(|&pi| {
                    let bp = BoundPattern::from_triple_pattern(&self.access_patterns[pi]);
                    query_cost(a.desired_type, bp, card)
                }).sum();
                let cost_b: f64 = assigned_pats.iter().map(|&pi| {
                    let bp = BoundPattern::from_triple_pattern(&self.access_patterns[pi]);
                    query_cost(b.desired_type, bp, card)
                }).sum();
                cost_a.partial_cmp(&cost_b).unwrap_or(std::cmp::Ordering::Equal)
            });

            if let Some((old_idx, _)) = best_old {
                // Step 5: check if the desired index type already exists in the old pool
                let old_pool = &self.pools[old_idx];
                if old_pool.contains_type(new_pool.desired_type) {
                    // Reuse the old pool, just switch active
                    let mut reused = old_pool.clone();
                    reused.switch_active(new_pool.desired_type);
                    *new_pool = reused;
                } else {
                    // Create a new index of the desired type, keep old indexes for data
                    let mut merged = old_pool.clone();
                    merged.switch_active(new_pool.desired_type);
                    *new_pool = merged;
                }
            }
        }

        // Step 4: old pools not assigned to any new pool are simply dropped
        self.pools = new_pools;
        self.pattern_to_pool = new_assignment;
    }

    /// Called after each insert to possibly trigger re-evaluation.
    fn maybe_reevaluate(&mut self) {
        self.inserts_since_eval += 1;
        if self.inserts_since_eval >= self.eval_interval {
            self.inserts_since_eval = 0;
            self.reevaluate();
        }
    }
}

// ─── TripleIndex implementation ─────────────────────────────────────────────

impl TripleIndex for DynamicHexastoreIndex {
    fn clone_empty(&self) -> Box<dyn TripleIndex> {
        Box::new(DynamicHexastoreIndex::new(
            self.access_patterns.clone(),
            self.eval_interval,
        ))
    }

    fn clone_box(&self) -> Box<dyn TripleIndex> {
        Box::new(self.clone())
    }

    fn triple_count(&self) -> usize {
        // Any single pool contains all triples (by the pool invariant).
        if self.pools.is_empty() {
            return 0;
        }
        self.pools[0].triple_count()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        // Report the union of all pools' native scans
        let mut support = AccessPatternSupport {
            sp: false, so: false, po: false,
            ps: false, os: false, op: false,
        };
        for pool in &self.pools {
            match pool.desired_type.native_scan() {
                ScanKind::SP => support.sp = true,
                ScanKind::SO => support.so = true,
                ScanKind::PO => support.po = true,
                ScanKind::PS => support.ps = true,
                ScanKind::OS => support.os = true,
                ScanKind::OP => support.op = true,
            }
        }
        support
    }

    // ── Mutation ────────────────────────────────────────────────────────

    fn insert(&mut self, triple: &Triple) -> bool {
        if self.pools.is_empty() {
            return false;
        }

        // Step 4: add to every pool exactly once (via each pool's active index)
        let mut any_new = false;
        for pool in &mut self.pools {
            if pool.insert(triple) {
                any_new = true;
            }
        }

        if any_new {
            self.maybe_reevaluate();
        }

        any_new
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        // Step 6: delete from every index in every pool
        let mut any_deleted = false;
        for pool in &mut self.pools {
            if pool.delete(triple) {
                any_deleted = true;
            }
        }
        any_deleted
    }

    fn clear(&mut self) {
        for pool in &mut self.pools {
            pool.clear();
        }
    }

    fn build_from_triples(&mut self, triples: &[Triple]) {
        self.clear();
        // Simple insert loop (pools handle the routing)
        for triple in triples {
            for pool in &mut self.pools {
                pool.insert(triple);
            }
        }
        // After bulk load, gather rough cardinalities and reevaluate
        let mut subjects = HashSet::new();
        let mut predicates = HashSet::new();
        let mut objects = HashSet::new();
        for t in triples {
            subjects.insert(t.subject);
            predicates.insert(t.predicate);
            objects.insert(t.object);
        }
        self.latest_card = CardinalitySnapshot::from_stats(
            triples.len() as u64,
            subjects.len(),
            predicates.len(),
            objects.len(),
        );
        self.reevaluate();
    }

    // ── Query ───────────────────────────────────────────────────────────

    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        // Any pool contains all the data, so we pick the best pool for this query shape.
        if self.pools.is_empty() {
            return Vec::new();
        }

        let bp = BoundPattern {
            s: s.is_some(),
            p: p.is_some(),
            o: o.is_some(),
        };

        // Find the pool with the lowest query cost for this pattern
        let best_pool = self.pools
            .iter()
            .min_by(|a, b| {
                let ca = query_cost(a.desired_type, bp, &self.latest_card);
                let cb = query_cost(b.desired_type, bp, &self.latest_card);
                ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap();

        best_pool.query(s, p, o)
    }

    fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
        let (s, p, o) = pattern;
        let sub = match s { Term::Constant(x) => Some(*x), Term::Variable(_) => None };
        let pre = match p { Term::Constant(x) => Some(*x), Term::Variable(_) => None };
        let obj = match o { Term::Constant(x) => Some(*x), Term::Variable(_) => None };
        self.query(sub, pre, obj)
    }

    // ── Two-key scans ───────────────────────────────────────────────────
    // These delegate to the pool whose desired_type natively supports the scan.
    // Because pools may have multiple internal indexes, the pool merges results.
    // We return None if no pool supports it natively with a single index
    // (the engine will fall back to query() + filter).

    fn scan_sp(&self, s: u32, p: u32) -> Option<&HashSet<u32>> {
        for pool in &self.pools {
            if pool.indexes.len() == 1 && pool.desired_type == IndexType::SPO {
                return pool.indexes[0].scan_sp(s, p);
            }
        }
        None
    }

    fn scan_so(&self, s: u32, o: u32) -> Option<&HashSet<u32>> {
        for pool in &self.pools {
            if pool.indexes.len() == 1 && pool.desired_type == IndexType::SOP {
                return pool.indexes[0].scan_so(s, o);
            }
        }
        None
    }

    fn scan_po(&self, p: u32, o: u32) -> Option<&HashSet<u32>> {
        for pool in &self.pools {
            if pool.indexes.len() == 1 && pool.desired_type == IndexType::POS {
                return pool.indexes[0].scan_po(p, o);
            }
        }
        None
    }

    fn scan_ps(&self, p: u32, s: u32) -> Option<&HashSet<u32>> {
        for pool in &self.pools {
            if pool.indexes.len() == 1 && pool.desired_type == IndexType::PSO {
                return pool.indexes[0].scan_ps(p, s);
            }
        }
        None
    }

    fn scan_os(&self, o: u32, s: u32) -> Option<&HashSet<u32>> {
        for pool in &self.pools {
            if pool.indexes.len() == 1 && pool.desired_type == IndexType::OSP {
                return pool.indexes[0].scan_os(o, s);
            }
        }
        None
    }

    fn scan_op(&self, o: u32, p: u32) -> Option<&HashSet<u32>> {
        for pool in &self.pools {
            if pool.indexes.len() == 1 && pool.desired_type == IndexType::OPS {
                return pool.indexes[0].scan_op(o, p);
            }
        }
        None
    }

    // ── Bulk / optimisation ─────────────────────────────────────────────

    fn optimize(&mut self) {
        for pool in &mut self.pools {
            for idx in &mut pool.indexes {
                idx.optimize();
            }
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::triple::Triple;

    fn make_triple(s: u32, p: u32, o: u32) -> Triple {
        Triple { subject: s, predicate: p, object: o }
    }

    #[test]
    fn test_insert_and_query_basic() {
        // Access pattern: (?s, p=1, ?o) — only predicate bound
        let patterns = vec![
            (Term::Variable("s".into()), Term::Constant(1), Term::Variable("o".into())),
        ];
        let mut idx = DynamicHexastoreIndex::with_patterns(patterns);

        assert!(idx.insert(&make_triple(10, 1, 100)));
        assert!(idx.insert(&make_triple(20, 1, 200)));
        assert!(idx.insert(&make_triple(30, 2, 300)));

        let result = idx.query(None, Some(1), None);
        assert_eq!(result.len(), 2);

        let result_all = idx.query(None, None, None);
        assert_eq!(result_all.len(), 3);
    }

    #[test]
    fn test_delete_removes_from_all_pools() {
        let patterns = vec![
            (Term::Constant(1), Term::Variable("p".into()), Term::Variable("o".into())),
            (Term::Variable("s".into()), Term::Constant(2), Term::Variable("o".into())),
        ];
        let mut idx = DynamicHexastoreIndex::with_patterns(patterns);

        idx.insert(&make_triple(1, 2, 3));
        idx.insert(&make_triple(1, 2, 4));
        assert_eq!(idx.triple_count(), 2);

        assert!(idx.delete(&make_triple(1, 2, 3)));
        assert_eq!(idx.triple_count(), 1);

        // Deleting non-existent triple returns false
        assert!(!idx.delete(&make_triple(99, 99, 99)));
    }

    #[test]
    fn test_duplicate_insert_returns_false() {
        let patterns = vec![
            (Term::Variable("s".into()), Term::Variable("p".into()), Term::Variable("o".into())),
        ];
        let mut idx = DynamicHexastoreIndex::with_patterns(patterns);

        assert!(idx.insert(&make_triple(1, 2, 3)));
        assert!(!idx.insert(&make_triple(1, 2, 3)));
        assert_eq!(idx.triple_count(), 1);
    }

    #[test]
    fn test_build_from_triples() {
        let patterns = vec![
            (Term::Variable("s".into()), Term::Constant(1), Term::Variable("o".into())),
            (Term::Constant(10), Term::Variable("p".into()), Term::Variable("o".into())),
        ];
        let mut idx = DynamicHexastoreIndex::with_patterns(patterns);

        let triples: Vec<Triple> = (0..100)
            .map(|i| make_triple(i % 10, i % 5, i))
            .collect();
        idx.build_from_triples(&triples);

        assert_eq!(idx.triple_count(), 100);

        // Query specific predicate
        let p1 = idx.query(None, Some(1), None);
        assert_eq!(p1.len(), 20); // i % 5 == 1 for i=1,6,11,...,96 → 20 triples
    }

    #[test]
    fn test_clear() {
        let patterns = vec![
            (Term::Variable("s".into()), Term::Variable("p".into()), Term::Variable("o".into())),
        ];
        let mut idx = DynamicHexastoreIndex::with_patterns(patterns);
        idx.insert(&make_triple(1, 2, 3));
        idx.insert(&make_triple(4, 5, 6));
        assert_eq!(idx.triple_count(), 2);

        idx.clear();
        assert_eq!(idx.triple_count(), 0);
    }

    #[test]
    fn test_get_matching_triples() {
        let patterns = vec![
            (Term::Constant(1), Term::Constant(2), Term::Variable("o".into())),
        ];
        let mut idx = DynamicHexastoreIndex::with_patterns(patterns);
        idx.insert(&make_triple(1, 2, 10));
        idx.insert(&make_triple(1, 2, 20));
        idx.insert(&make_triple(1, 3, 30));

        let pat = (Term::Constant(1), Term::Constant(2), Term::Variable("o".into()));
        let result = idx.get_matching_triples(&pat);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_reevaluate_does_not_lose_data() {
        let patterns = vec![
            (Term::Variable("s".into()), Term::Constant(1), Term::Variable("o".into())),
        ];
        let mut idx = DynamicHexastoreIndex::new(patterns, 5);

        // Insert enough to trigger re-evaluation
        for i in 0..20 {
            idx.insert(&make_triple(i, 1, i * 10));
        }
        // Update cardinalities and force reevaluate
        idx.update_cardinalities(CardinalitySnapshot {
            num_subjects: 20.0,
            num_predicates: 1.0,
            num_objects: 20.0,
        });
        idx.reevaluate();

        // Data should still be there
        assert_eq!(idx.triple_count(), 20);
        let result = idx.query(None, Some(1), None);
        assert_eq!(result.len(), 20);
    }

    #[test]
    fn test_supported_access_patterns() {
        // Pattern needs SP scan �� should report sp=true
        let patterns = vec![
            (Term::Constant(1), Term::Constant(2), Term::Variable("o".into())),
        ];
        let idx = DynamicHexastoreIndex::with_patterns(patterns);
        let support = idx.supported_access_patterns();
        // At least one of SP or PS should be supported
        assert!(support.sp || support.ps);
    }

    #[test]
    fn test_cost_functions() {
        let card = CardinalitySnapshot {
            num_subjects: 100.0,
            num_predicates: 10.0,
            num_objects: 50.0,
        };

        // Querying SPO for bound s,p should be cheap (2 lookups)
        let cost_sp = query_cost(IndexType::SPO, BoundPattern { s: true, p: true, o: false }, &card);
        assert_eq!(cost_sp, 2.0);

        // Querying SPO for bound o only should be expensive (full scan)
        let cost_o = query_cost(IndexType::SPO, BoundPattern { s: false, p: false, o: true }, &card);
        assert!(cost_o > 10.0);

        // OPS should be cheap for bound o
        let cost_o_ops = query_cost(IndexType::OPS, BoundPattern { s: false, p: false, o: true }, &card);
        assert!(cost_o_ops < cost_o);
    }

    #[test]
    fn test_clone_empty_and_clone_box() {
        let patterns = vec![
            (Term::Variable("s".into()), Term::Constant(1), Term::Variable("o".into())),
        ];
        let mut idx = DynamicHexastoreIndex::with_patterns(patterns);
        idx.insert(&make_triple(1, 1, 10));

        let empty = idx.clone_empty();
        assert_eq!(empty.triple_count(), 0);

        let cloned = idx.clone_box();
        assert_eq!(cloned.triple_count(), 1);
    }
}
