use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleTableIndex {
  pub table: HashSet<Triple>,
}

impl TripleIndex for SingleTableIndex {
  fn clone_empty(&self) -> Box<dyn TripleIndex> {
        Box::new(SingleTableIndex::new())
    }

    fn clone_box(&self) -> Box<dyn TripleIndex> {
        Box::new(self.clone())
    }

    fn triple_count(&self) -> usize {
        self.table.len()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        // no specialized access pattern supported
        AccessPatternSupport { sp: false, so: false, po: false, ps: false, os: false, op: false }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        // Insert returns true only when the triple was not present before.
        self.table.insert(triple.clone())
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        // HashSet::remove accepts &T and returns whether an element was removed.
        self.table.remove(triple)
    }

    fn build_from_triples(&mut self, triples: &[Triple]) {
        // simple replace-with-new-set strategy; keeps semantics consistent.
        use rayon::prelude::*;

        self.clear();

        if triples.is_empty() {
            return;
        }

        // Reserve a reasonable capacity (heuristic)
        self.table.reserve(triples.len());

        // If rayon is available, we can build partial sets and merge them.
        let num_threads = rayon::current_num_threads();
        if triples.len() >= 10_000 && num_threads > 1 {
            // parallel build
            let chunk_size = (triples.len() / num_threads).max(1_000);
            let partials: Vec<HashSet<Triple>> = triples
                .par_chunks(chunk_size)
                .map(|chunk| {
                    let mut local = HashSet::with_capacity(chunk.len());
                    for t in chunk {
                        local.insert(t.clone());
                    }
                    local
                })
                .collect();

            for part in partials {
                self.table.extend(part);
            }
        } else {
            // serial build
            self.table.extend(triples.iter().cloned());
        }

        self.optimize_post_build();
    }

    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        // brute-force scan across the table; acceptable because this index has no sub-indexes
        let mut results = Vec::new();

        for triple in &self.table {
            let matches = match (s, p, o) {
                (Some(ss), Some(pp), Some(oo)) => triple.subject == ss && triple.predicate == pp && triple.object == oo,
                (Some(ss), Some(pp), None) => triple.subject == ss && triple.predicate == pp,
                (Some(ss), None, Some(oo)) => triple.subject == ss && triple.object == oo,
                (None, Some(pp), Some(oo)) => triple.predicate == pp && triple.object == oo,
                (Some(ss), None, None) => triple.subject == ss,
                (None, Some(pp), None) => triple.predicate == pp,
                (None, None, Some(oo)) => triple.object == oo,
                (None, None, None) => true,
            };

            if matches {
                results.push(triple.clone());
            }
        }

        results
    }

    fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
        let (s, p, o) = pattern;
        let sub = match s { Constant(x) => Some(*x), Variable(_) => None, QuotedTriple(_) => None };
        let pre = match p { Constant(x) => Some(*x), Variable(_) => None, QuotedTriple(_) => None };
        let obj = match o { Constant(x) => Some(*x), Variable(_) => None, QuotedTriple(_) => None };
        self.query(sub, pre, obj)
    }

    fn clear(&mut self) {
        self.table.clear();
    }

    // no specialized scans possible; all return None
    fn scan_sp(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_so(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_po(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }

    fn optimize(&mut self) {
        // nothing complex to optimize here — just shrink to fit
        self.table.shrink_to_fit();
    }
}
impl SingleTableIndex {
  pub fn new() -> Self {
    Self { table: HashSet::new() }
  }

  /// merge another NoIndex into this one
  pub fn merge_from(&mut self, other: SingleTableIndex) {
    self.table.extend(other.table);
  }

  /// optimized single-triple insert used during parallel builds
  #[inline]
  fn insert_optimized(&mut self, triple: &Triple) -> bool {
    // returns true if inserted, false if already present
    self.table.insert(triple.clone())
  }

  fn optimize_post_build(&mut self) {
    self.table.shrink_to_fit();
  }
}
