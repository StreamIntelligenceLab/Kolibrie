use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OPSSingleIndex {
  // The six permutations, using HashMap of HashMap of HashSet.
  pub ops: HashMap<u32, HashMap<u32, HashSet<u32>>>,
}

impl TripleIndex for OPSSingleIndex {
  fn clone_empty(&self) -> Box<dyn TripleIndex> {
    Box::new(OPSSingleIndex::new())
  }

  fn clone_box(&self) -> Box<dyn TripleIndex> {
    Box::new(self.clone())
  }

  fn triple_count(&self) -> usize {
    self.ops.values()
      .map(|sub_map| sub_map.values().map(|objs| objs.len()).sum::<usize>())
      .sum()
  }    

  fn supported_access_patterns(&self) -> AccessPatternSupport {
    AccessPatternSupport {
      sp: false, so: false, po: false,
      ps: false, os: false, op: true
    }
  }

  /// Insert a single triple
  fn insert(&mut self, triple: &Triple) -> bool {
    let Triple { subject: s, predicate: p, object: o } = *triple;
    if let Some(pred_map) = self.ops.get(&o) {
      if let Some(subjects) = pred_map.get(&p) {
        if objects.contains(&s) {
          return false; // triple already stored
        }
      }
    }
    self.ops.entry(o).or_default().entry(p).or_default().insert(s);
    true
  }

  /// Delete a single triple
  fn delete(&mut self, triple: &Triple) -> bool {
    let Triple { subject: s, predicate: p, object: o } = *triple;

    let exists = self.ops
      .get(&o)
      .and_then(|pred_map| pred_map.get(&p))
      .map_or(false, |subjects| subjects.contains(&s));

    if !exists {
      return false; // triple doesn't exist
    }

    // Remove from index using helper function
    remove_from_index(&mut self.ops, s, p, o);
    true 
  }

  /// Bulk-build the index from a list of triples
  fn build_from_triples(&mut self, triples: &[Triple]) {
    for triple in triples {
      self.insert(triple);
    }
    use rayon::prelude::*;

    self.clear();

    if triples.is_empty() {
      return;
    }

    // Pre-allocate with capacity estimates
    let capacity = triples.len() / 100;

    self.ops.reserve(capacity);

    // Build indexes in parallel by creating partial indexes and merging
    let num_threads = rayon::current_num_threads();
    let chunk_size = (triples.len() / num_threads).max(10_000);

    let partial_indexes: Vec<PSOSingleIndex> = triples
      .par_chunks(chunk_size)
      .map(|chunk| {
        let mut local_index = OPSSingleIndex::new();

        // Pre-allocate local index
        let local_capacity = chunk.len() / 50;
        local_index.ops.reserve(local_capacity);

        // Insert triples into local index
        for triple in chunk {
          local_index.insert_optimized(triple);
        }

        local_index
      })
      .collect();

    // Sequentially merge partial indexes
    for partial_index in partial_indexes {
      self.merge_from(partial_index);
    }

    // Optimize memory layout after building
    self.optimize_post_build();
  }

  /// Query the index
  fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
    let mut results = Vec::new();

    match (s, p, o) {
      // Fully bound
      (Some(ss), Some(pp), Some(oo)) => {
        if let Some(pred_map) = self.ops.get(&oo) {
          if let Some(subjects) = pred_map.get(&pp) {
            if subjects.contains(&ss) {
              results.push(Triple { subject: ss, predicate: pp, object: oo });
            }
          }
        }
      }
      // (S, P, -)
      (Some(ss), Some(pp), None) => {
        for (&obj, pred_map) in &self.ops {
          if let Some(subjects) = pred_map.get(&pp) {
            if subjects.contains(&ss) {
              results.push(Triple { subject: ss, predicate: pp, object: obj });
            }
          }
        }
      }
      // (S, -, O)
      (Some(ss), None, Some(oo)) => {
        if let Some(pred_map) = self.ops.get(&oo) {
          for (&pred, subjects) in pred_map {
            if subjects.contains(&ss) {
              results.push(Triple { subject: ss, predicate: pred, object: oo });
            }
          }
        }
      }
      // (-, P, O)
      (None, Some(pp), Some(oo)) => {
        if let Some(pred_map) = self.ops.get(&oo) {
          if let Some(subjects) = pred_map.get(&pp) {
            for &subj in subjects {
              results.push(Triple { subject: subj, predicate: pp, object: oo });
            }
          }
        }
      }
      // (S, -, -)
      (Some(ss), None, None) => {
        for (&obj, pred_map) in self.ops {
          for (&pred, subjects) in pred_map {
            if subjects.contains(&ss) {
              results.push( Triple { subject: ss, predicate: pred, object: obj });
            }
          }
        }
      }
      // (-, P, -)
      (None, Some(pp), None) => {
        for (&obj, pred_map) in self.ops {
          if let Some(subjects) = pred_map.get(&pp) {
            for &subj in subjects {
              results.push(Triple { subject: subj, predicate: pp, object: obj });
            }
          }
        }
      }
      // (-, -, O)
      (None, None, Some(oo)) => {
        if let Some(pred_map) = self.ops.get(&oo) {
          for (&pred, subjects) in pred_map {
            for &subj in subjects {
              results.push(Triple { subject: subj, predicate: pred, object: oo });
            }
          }
        }
      }
      // (-, -, -) => all
      (None, None, None) => {
        for (&obj, pred_map) in self.ops {
          for (&pred, subjects) in pred_map {
            for &subj in subjects {
              results.push(Triple { subject: subj, predicate: pred, object: obj });
            }
          }
        }
      }
    }

    results
  }

  /// Return all triples that match a given `TriplePattern`
  fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
    let (s, p, o) = pattern;
    let sub = match s {
      Constant(x) => Some(*x),
      Variable(_) => None,
    };
    let pre = match p {
      Constant(x) => Some(*x),
      Variable(_) => None,
    };
    let obj = match o {
      Constant(x) => Some(*x),
      Variable(_) => None,
    };

    self.query(sub, pre, obj)
  }

  /// Clear all data in the indexes
  fn clear(&mut self) {
    self.ops.clear();
  }

  /// Scan using the Subject-Predicate index (spo)
  fn scan_sp(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
    return None;
  }

  /// Scan using the Subject-Object index (sop)
  fn scan_so(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
    return None;
  }

  /// Scan using the Predicate-Object index (pos)
  fn scan_po(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
    return None;
  }

  /// Scan using the Predicate-Subject index (pso)
  fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
    return None;
  }

  /// Scan using the Object-Subject index (osp)
  fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
    return None;
  }

  /// Scan using the Object-Predicate index (ops)
  fn scan_op(&self, o: u32, p: u32) -> Option<&HashSet<u32>> {
    self.ops
      .get(&o)
      .and_then(|pred_map| pred_map.get(&p));
  }

  fn optimize(&mut self) {
    use rayon::prelude::*;

    // Optimize PSO index
    self.ops.par_iter_mut().for_each(|(_, pred_map)| {
      pred_map.par_iter_mut().for_each(|(_, subj_set)| {
        subj_set.shrink_to_fit();
      });
      pred_map.shrink_to_fit();
    });
    self.ops.shrink_to_fit();
  }

}

impl OPSSingleIndex {
  pub fn new() -> Self {
    Self {
      ops: HashMap::new(),
    }
  }

  /// Efficiently merge another index into this one using parallel processing where possible
  pub fn merge_from(&mut self, other: OPSSingleIndex) {

    // Merge OPS index  
    for (o, pred_map) in other.ops {
      let entry = self.ops.entry(o).or_insert_with(HashMap::new);
      for (p, subj_set) in pred_map {
        entry.entry(p).or_insert_with(HashSet::new).extend(subj_set);
      }
    }
  }

  #[inline]
  fn insert_optimized(&mut self, triple: &Triple) -> bool {
    let Triple { subject: s, predicate: p, object: o } = *triple;

    // Check for duplicates
    if let Some(pred_map) = self.ops.get(&o) {
      if let Some(subjects) = pred_map.get(&p) {
        if subjects.contains(&s) {
          return false;
        }
      }
    }

    // Batch insert into all indexes
    self.ops.entry(o).or_insert_with(|| HashMap::with_capacity(16))
      .entry(p).or_insert_with(|| HashSet::with_capacity(8))
      .insert(s);

    true
  }

  fn optimize_post_build(&mut self) {
    use rayon::prelude::*;

    self.ops.par_iter_mut().for_each(|(_, pred_map)| {
      pred_map.shrink_to_fit();
      pred_map.par_iter_mut().for_each(|(_, subj_set)| {
        subj_set.shrink_to_fit();
      });
    });
    self.ops.shrink_to_fit();
  }
}
