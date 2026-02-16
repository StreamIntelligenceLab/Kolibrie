use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PSOSingleIndex {
    // The six permutations, using HashMap of HashMap of HashSet.
    pub pso: HashMap<u32, HashMap<u32, HashSet<u32>>>,
}

impl TripleIndex for PSOSingleIndex {
    fn clone_box(&self) -> Box<dyn TripleIndex> {
        Box::new(self.clone())
    }
    
    fn triple_count(&self) -> usize {
        self.pso.values()
            .map(|sub_map| sub_map.values().map(|objs| objs.len()).sum::<usize>())
            .sum()
    }    

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport {
            sp: false, so: false, po: false,
            ps: true, os: false, op: false 
        }
    }

    /// Insert a single triple into all six indexes
    fn insert(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(sub_map) = self.pso.get(&p) {
            if let Some(objects) = sub_map.get(&s) {
                if objects.contains(&o) {
                    return false; // triple already stored
                }
            }
        }
        self.pso.entry(p).or_default().entry(s).or_default().insert(o);
        true
    }

    /// Delete a single triple from all six indexes
    fn delete(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        
        let exists = self.pso
            .get(&p)
            .and_then(|sub_map| sub_map.get(&s))
            .map_or(false, |objects| objects.contains(&o));
        
        if !exists {
            return false; // triple doesn't exist
        }

        // Remove from all six indexes using helper function
        remove_from_index(&mut self.pso, p, s, o);
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
        
        self.pso.reserve(capacity);
        
        // Build indexes in parallel by creating partial indexes and merging
        let num_threads = rayon::current_num_threads();
        let chunk_size = (triples.len() / num_threads).max(10_000);
        
        let partial_indexes: Vec<PSOSingleIndex> = triples
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut local_index = PSOSingleIndex::new();
                
                // Pre-allocate local index
                let local_capacity = chunk.len() / 50;
                local_index.pso.reserve(local_capacity);
                
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
                if let Some(sub_map) = self.pso.get(&pp) {
                    if let Some(objects) = sub_map.get(&ss) {
                        if objects.contains(&oo) {
                            results.push(Triple { subject: ss, predicate: pp, object: oo });
                        }
                    }
                }
            }
            // (S, P, -)
            (Some(ss), Some(pp), None) => {
                if let Some(sub_map) = self.pso.get(&pp) {
                    if let Some(objects) = sub_map.get(&ss) {
                        for &obj in objects {
                            results.push(Triple { subject: ss, predicate: pp, object: obj });
                        }
                    }
                }
            }
            // (S, -, O)
            (Some(ss), None, Some(oo)) => {
                for (&pred, sub_map) in &self.pso {
                    if let Some(objects) = sub_map.get(&ss) {
                        if objects.contains(&oo) {
                            results.push(Triple { subject: ss, predicate: pred, object: oo })
                        }
                    }
                }
            }
            // (-, P, O)
            (None, Some(pp), Some(oo)) => {
                if let Some(sub_map) = self.pso.get(&pp) {
                    for (&sub, objects) in sub_map {
                        if objects.contains(&oo) {
                            results.push(Triple { subject: sub, predicate: pp, object: oo })
                        }
                    }
                }
            }
            // (S, -, -)
            (Some(ss), None, None) => {
                for (&pred, sub_map) in &self.pso {
                    if let Some(objects) = sub_map.get(&ss) {
                        for &obj in objects {
                            results.push(Triple { subject: ss, predicate: pred, object: obj })
                        }
                    }
                }
            }
            // (-, P, -)
            (None, Some(pp), None) => {
                if let Some(sub_map) = self.pso.get(&pp) {
                    for (&sub, objects) in sub_map {
                        for &obj in objects {
                            results.push(Triple { subject: sub, predicate: pp, object: obj })
                        }
                    }
                }
            }
            // (-, -, O)
            (None, None, Some(oo)) => {
                for (&pred, sub_map) in &self.pso {
                    for (&sub, objects) in sub_map {
                        if objects.contains(&oo) {
                            results.push(Triple { subject: sub, predicate: pred, object: oo });
                        }
                    }
                }
            }
            // (-, -, -) => all
            (None, None, None) => {
                for (&pred, sub_map) in &self.pso {
                    for (&sub, objects) in sub_map {
                        for &obj in objects {
                            results.push(Triple { subject: sub, predicate: pred, object: obj });
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
        self.pso.clear();
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
    fn scan_ps(&self, p: u32, s: u32) -> Option<&HashSet<u32>> {
        self.pso
            .get(&p)
            .and_then(|subj_map| subj_map.get(&s))
    }

    /// Scan using the Object-Subject index (osp)
    fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
        return None;
    }

    /// Scan using the Object-Predicate index (ops)
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
        return None;
    }

    fn optimize(&mut self) {
        use rayon::prelude::*;

        // Optimize PSO index
        self.pso.par_iter_mut().for_each(|(_, subj_map)| {
            subj_map.par_iter_mut().for_each(|(_, obj_set)| {
                obj_set.shrink_to_fit();
            });
            subj_map.shrink_to_fit();
        });
        self.pso.shrink_to_fit();
    }

}

impl PSOSingleIndex {
    pub fn new() -> Self {
        Self {
            pso: HashMap::new(),
        }
    }

    /// Efficiently merge another index into this one using parallel processing where possible
    pub fn merge_from(&mut self, other: PSOSingleIndex) {

        // Merge PSO index  
        for (p, subj_map) in other.pso {
            let entry = self.pso.entry(p).or_insert_with(HashMap::new);
            for (s, obj_set) in subj_map {
                entry.entry(s).or_insert_with(HashSet::new).extend(obj_set);
            }
        }
    }

    #[inline]
    fn insert_optimized(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        
        // Check for duplicates
        if let Some(sub_map) = self.pso.get(&p) {
            if let Some(objects) = sub_map.get(&s) {
                if objects.contains(&o) {
                    return false;
                }
            }
        }
        
        // Batch insert into all indexes
        self.pso.entry(p).or_insert_with(|| HashMap::with_capacity(16))
               .entry(s).or_insert_with(|| HashSet::with_capacity(8))
               .insert(o);

        true
    }

    fn optimize_post_build(&mut self) {
        use rayon::prelude::*;
        
        self.pso.par_iter_mut().for_each(|(_, subj_map)| {
            subj_map.shrink_to_fit();
            subj_map.par_iter_mut().for_each(|(_, obj_set)| {
                obj_set.shrink_to_fit();
            });
        });
        self.pso.shrink_to_fit();
    }
}