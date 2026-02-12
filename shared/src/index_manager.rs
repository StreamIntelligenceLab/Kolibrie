/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::terms::Term::*;
use crate::triple::Triple;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedIndex {
    // The six permutations, using HashMap of HashMap of HashSet.
    pub spo: HashMap<u32, HashMap<u32, HashSet<u32>>>,
    pub pos: HashMap<u32, HashMap<u32, HashSet<u32>>>,
    pub osp: HashMap<u32, HashMap<u32, HashSet<u32>>>,
    pub pso: HashMap<u32, HashMap<u32, HashSet<u32>>>,
    pub ops: HashMap<u32, HashMap<u32, HashSet<u32>>>,
    pub sop: HashMap<u32, HashMap<u32, HashSet<u32>>>,
}

impl UnifiedIndex {
    pub fn new() -> Self {
        Self {
            spo: HashMap::new(),
            pos: HashMap::new(),
            osp: HashMap::new(),
            pso: HashMap::new(),
            ops: HashMap::new(),
            sop: HashMap::new(),
        }
    }

    /// Insert a single triple into all six indexes
    pub fn insert(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(pred_map) = self.spo.get(&s) {
            if let Some(objects) = pred_map.get(&p) {
                if objects.contains(&o) {
                    return false; // triple already stored
                }
            }
        }
        self.spo.entry(s).or_default().entry(p).or_default().insert(o);
        self.pos.entry(p).or_default().entry(o).or_default().insert(s);
        self.osp.entry(o).or_default().entry(s).or_default().insert(p);
        self.pso.entry(p).or_default().entry(s).or_default().insert(o);
        self.ops.entry(o).or_default().entry(p).or_default().insert(s);
        self.sop.entry(s).or_default().entry(o).or_default().insert(p);
        true
    }

    /// Delete a single triple from all six indexes
    pub fn delete(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        
        let exists = self.spo
            .get(&s)
            .and_then(|pred_map| pred_map.get(&p))
            .map_or(false, |objects| objects.contains(&o));
        
        if !exists {
            return false; // triple doesn't exist
        }

        // Remove from all six indexes using helper function
        remove_from_index(&mut self.spo, s, p, o);
        remove_from_index(&mut self.pos, p, o, s);
        remove_from_index(&mut self.osp, o, s, p);
        remove_from_index(&mut self.pso, p, s, o);
        remove_from_index(&mut self.ops, o, p, s);
        remove_from_index(&mut self.sop, s, o, p);
        true 
    }

    /// Bulk-build the index from a list of triples
    pub fn build_from_triples(&mut self, triples: &[Triple]) {
        use rayon::prelude::*;
    
        self.clear();
        
        if triples.is_empty() {
            return;
        }
        
        // Pre-allocate with capacity estimates
        let capacity = triples.len() / 100;
        
        self.spo.reserve(capacity);
        self.pos.reserve(capacity);
        self.osp.reserve(capacity);
        self.pso.reserve(capacity);
        self.ops.reserve(capacity);
        self.sop.reserve(capacity);
        
        // Build indexes in parallel by creating partial indexes and merging
        let num_threads = rayon::current_num_threads();
        let chunk_size = (triples.len() / num_threads).max(10_000);
        
        let partial_indexes: Vec<UnifiedIndex> = triples
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut local_index = UnifiedIndex::new();
                
                // Pre-allocate local index
                let local_capacity = chunk.len() / 50;
                local_index.spo.reserve(local_capacity);
                local_index.pos.reserve(local_capacity);
                local_index.osp.reserve(local_capacity);
                local_index.pso.reserve(local_capacity);
                local_index.ops.reserve(local_capacity);
                local_index.sop.reserve(local_capacity);
                
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
    
    #[inline]
    fn insert_optimized(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        
        // Check for duplicates only in SPO index (most selective)
        if let Some(pred_map) = self.spo.get(&s) {
            if let Some(objects) = pred_map.get(&p) {
                if objects.contains(&o) {
                    return false;
                }
            }
        }
        
        // Batch insert into all indexes
        self.spo.entry(s).or_insert_with(|| HashMap::with_capacity(8))
               .entry(p).or_insert_with(|| HashSet::with_capacity(16))
               .insert(o);
        
        self.pos.entry(p).or_insert_with(|| HashMap::with_capacity(16))
               .entry(o).or_insert_with(|| HashSet::with_capacity(8))
               .insert(s);
        
        self.osp.entry(o).or_insert_with(|| HashMap::with_capacity(8))
               .entry(s).or_insert_with(|| HashSet::with_capacity(16))
               .insert(p);
        
        self.pso.entry(p).or_insert_with(|| HashMap::with_capacity(16))
               .entry(s).or_insert_with(|| HashSet::with_capacity(8))
               .insert(o);
        
        self.ops.entry(o).or_insert_with(|| HashMap::with_capacity(16))
               .entry(p).or_insert_with(|| HashSet::with_capacity(8))
               .insert(s);
        
        self.sop.entry(s).or_insert_with(|| HashMap::with_capacity(8))
               .entry(o).or_insert_with(|| HashSet::with_capacity(16))
               .insert(p);
        
        true
    }
    
    fn optimize_post_build(&mut self) {
        use rayon::prelude::*;
        
        // Parallelize the optimization of each index
        rayon::scope(|s| {
            s.spawn(|_| {
                // SPO index
                self.spo.par_iter_mut().for_each(|(_, pred_map)| {
                    pred_map.shrink_to_fit();
                    pred_map.par_iter_mut().for_each(|(_, obj_set)| {
                        obj_set.shrink_to_fit();
                    });
                });
                self.spo.shrink_to_fit();
            });
            
            s.spawn(|_| {
                // POS index
                self.pos.par_iter_mut().for_each(|(_, obj_map)| {
                    obj_map.shrink_to_fit();
                    obj_map.par_iter_mut().for_each(|(_, subj_set)| {
                        subj_set.shrink_to_fit();
                    });
                });
                self.pos.shrink_to_fit();
            });
            
            s.spawn(|_| {
                // OSP index
                self.osp.par_iter_mut().for_each(|(_, subj_map)| {
                    subj_map.shrink_to_fit();
                    subj_map.par_iter_mut().for_each(|(_, pred_set)| {
                        pred_set.shrink_to_fit();
                    });
                });
                self.osp.shrink_to_fit();
            });
            
            s.spawn(|_| {
                // PSO index
                self.pso.par_iter_mut().for_each(|(_, subj_map)| {
                    subj_map.shrink_to_fit();
                    subj_map.par_iter_mut().for_each(|(_, obj_set)| {
                        obj_set.shrink_to_fit();
                    });
                });
                self.pso.shrink_to_fit();
            });
            
            s.spawn(|_| {
                // OPS index
                self.ops.par_iter_mut().for_each(|(_, pred_map)| {
                    pred_map.shrink_to_fit();
                    pred_map.par_iter_mut().for_each(|(_, subj_set)| {
                        subj_set.shrink_to_fit();
                    });
                });
                self.ops.shrink_to_fit();
            });
            
            s.spawn(|_| {
                // SOP index
                self.sop.par_iter_mut().for_each(|(_, obj_map)| {
                    obj_map.shrink_to_fit();
                    obj_map.par_iter_mut().for_each(|(_, pred_set)| {
                        pred_set.shrink_to_fit();
                    });
                });
                self.sop.shrink_to_fit();
            });
        });
    }

    /// Query the index
    pub fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        let mut results = Vec::new();

        match (s, p, o) {
            // Fully bound
            (Some(ss), Some(pp), Some(oo)) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    if let Some(objects) = pred_map.get(&pp) {
                        if objects.contains(&oo) {
                            results.push(Triple { subject: ss, predicate: pp, object: oo });
                        }
                    }
                }
            }
            // (S, P, -)
            (Some(ss), Some(pp), None) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    if let Some(objects) = pred_map.get(&pp) {
                        for &obj in objects {
                            results.push(Triple { subject: ss, predicate: pp, object: obj });
                        }
                    }
                }
            }
            // (S, -, O)
            (Some(ss), None, Some(oo)) => {
                if let Some(obj_map) = self.sop.get(&ss) {
                    if let Some(predicates) = obj_map.get(&oo) {
                        for &pred in predicates {
                            results.push(Triple { subject: ss, predicate: pred, object: oo });
                        }
                    }
                }
            }
            // (-, P, O)
            (None, Some(pp), Some(oo)) => {
                if let Some(obj_map) = self.pos.get(&pp) {
                    if let Some(subjects) = obj_map.get(&oo) {
                        for &subj in subjects {
                            results.push(Triple { subject: subj, predicate: pp, object: oo });
                        }
                    }
                }
            }
            // (S, -, -)
            (Some(ss), None, None) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    for (&pred, objects) in pred_map {
                        for &obj in objects {
                            results.push(Triple { subject: ss, predicate: pred, object: obj });
                        }
                    }
                }
            }
            // (-, P, -)
            (None, Some(pp), None) => {
                if let Some(obj_map) = self.pso.get(&pp) {
                    for (&subj, objects) in obj_map {
                        for &obj in objects {
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
                for (&subj, pred_map) in &self.spo {
                    for (&pred, objects) in pred_map {
                        for &obj in objects {
                            results.push(Triple { subject: subj, predicate: pred, object: obj });
                        }
                    }
                }
            }
        }

        results
    }

    /// Return all triples that match a given `TriplePattern`
    pub fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
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
    pub fn clear(&mut self) {
        self.spo.clear();
        self.pos.clear();
        self.osp.clear();
        self.pso.clear();
        self.ops.clear();
        self.sop.clear();
    }

    /// Scan using the Subject-Predicate index (spo)
    pub fn scan_sp(&self, s: u32, p: u32) -> Option<&HashSet<u32>> {
        self.spo
            .get(&s)
            .and_then(|pred_map| pred_map.get(&p))
    }

    /// Scan using the Subject-Object index (sop)
    pub fn scan_so(&self, s: u32, o: u32) -> Option<&HashSet<u32>> {
        self.sop
            .get(&s)
            .and_then(|obj_map| obj_map.get(&o))
    }

    /// Scan using the Predicate-Object index (pos)
    pub fn scan_po(&self, p: u32, o: u32) -> Option<&HashSet<u32>> {
        self.pos
            .get(&p)
            .and_then(|obj_map| obj_map.get(&o))
    }

    pub fn scan_ps(&self, p: u32, s: u32) -> Option<&HashSet<u32>> {
        self.pso
            .get(&p)
            .and_then(|subj_map| subj_map.get(&s))
    }

    pub fn scan_os(&self, o: u32, s: u32) -> Option<&HashSet<u32>> {
        self.osp
            .get(&o)
            .and_then(|subj_map| subj_map.get(&s))
    }

    pub fn scan_op(&self, o: u32, p: u32) -> Option<&HashSet<u32>> {
        self.ops
            .get(&o)
            .and_then(|pred_map| pred_map.get(&p))
    }

    /// Efficiently merge another index into this one using parallel processing where possible
    pub fn merge_from(&mut self, other: UnifiedIndex) {
        // Merge SPO index
        for (s, pred_map) in other.spo {
            let entry = self.spo.entry(s).or_insert_with(HashMap::new);
            for (p, obj_set) in pred_map {
                entry.entry(p).or_insert_with(HashSet::new).extend(obj_set);
            }
        }
        
        // Merge PSO index  
        for (p, subj_map) in other.pso {
            let entry = self.pso.entry(p).or_insert_with(HashMap::new);
            for (s, obj_set) in subj_map {
                entry.entry(s).or_insert_with(HashSet::new).extend(obj_set);
            }
        }
        
        // Merge OPS index
        for (o, pred_map) in other.ops {
            let entry = self.ops.entry(o).or_insert_with(HashMap::new);
            for (p, subj_set) in pred_map {
                entry.entry(p).or_insert_with(HashSet::new).extend(subj_set);
            }
        }
        
        // Merge POS index
        for (p, obj_map) in other.pos {
            let entry = self.pos.entry(p).or_insert_with(HashMap::new);
            for (o, subj_set) in obj_map {
                entry.entry(o).or_insert_with(HashSet::new).extend(subj_set);
            }
        }
        
        // Merge OSP index
        for (o, subj_map) in other.osp {
            let entry = self.osp.entry(o).or_insert_with(HashMap::new);
            for (s, pred_set) in subj_map {
                entry.entry(s).or_insert_with(HashSet::new).extend(pred_set);
            }
        }
        
        // Merge SOP index
        for (s, obj_map) in other.sop {
            let entry = self.sop.entry(s).or_insert_with(HashMap::new);
            for (o, pred_set) in obj_map {
                entry.entry(o).or_insert_with(HashSet::new).extend(pred_set);
            }
        }
    }

    pub fn optimize(&mut self) {
        use rayon::prelude::*;
        
        // Optimize SPO index
        self.spo.par_iter_mut().for_each(|(_, pred_map)| {
            pred_map.par_iter_mut().for_each(|(_, obj_set)| {
                obj_set.shrink_to_fit();
            });
            pred_map.shrink_to_fit();
        });
        self.spo.shrink_to_fit();
        
        // Optimize PSO index
        self.pso.par_iter_mut().for_each(|(_, subj_map)| {
            subj_map.par_iter_mut().for_each(|(_, obj_set)| {
                obj_set.shrink_to_fit();
            });
            subj_map.shrink_to_fit();
        });
        self.pso.shrink_to_fit();
        
        // Optimize OPS index
        self.ops.par_iter_mut().for_each(|(_, pred_map)| {
            pred_map.par_iter_mut().for_each(|(_, subj_set)| {
                subj_set.shrink_to_fit();
            });
            pred_map.shrink_to_fit();
        });
        self.ops.shrink_to_fit();
        
        // Optimize POS index
        self.pos.par_iter_mut().for_each(|(_, obj_map)| {
            obj_map.par_iter_mut().for_each(|(_, subj_set)| {
                subj_set.shrink_to_fit();
            });
            obj_map.shrink_to_fit();
        });
        self.pos.shrink_to_fit();
        
        // Optimize OSP index
        self.osp.par_iter_mut().for_each(|(_, subj_map)| {
            subj_map.par_iter_mut().for_each(|(_, pred_set)| {
                pred_set.shrink_to_fit();
            });
            subj_map.shrink_to_fit();
        });
        self.osp.shrink_to_fit();
        
        // Optimize SOP index
        self.sop.par_iter_mut().for_each(|(_, obj_map)| {
            obj_map.par_iter_mut().for_each(|(_, pred_set)| {
                pred_set.shrink_to_fit();
            });
            obj_map.shrink_to_fit();
        });
        self.sop.shrink_to_fit();
    }
}

/// Helper function to remove a triple from a nested index structure and clean up empty collections
#[inline]
fn remove_from_index(
    index: &mut HashMap<u32, HashMap<u32, HashSet<u32>>>,
    key1: u32,
    key2: u32,
    value: u32,
) {
    if let Some(inner_map) = index.get_mut(&key1) {
        if let Some(set) = inner_map.get_mut(&key2) {
            set.remove(&value);
            // Clean up empty inner set
            if set.is_empty() {
                inner_map.remove(&key2);
            }
        }
        // Clean up empty inner map
        if inner_map.is_empty() {
            index.remove(&key1);
        }
    }
}
