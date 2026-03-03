use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SOPSingleIndex {
    pub sop: HashMap<u32, HashMap<u32, HashSet<u32>>>,
}

impl TripleIndex for SOPSingleIndex {
    fn clone_empty(&self) -> Box<dyn TripleIndex> { Box::new(SOPSingleIndex::new()) }
    fn clone_box(&self) -> Box<dyn TripleIndex> { Box::new(self.clone()) }

    fn triple_count(&self) -> usize {
        self.sop.values().map(|obj_map| obj_map.values().map(|ps| ps.len()).sum::<usize>()).sum()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport {
            sp: false, so: true, po: false,
            ps: false, os: false, op: false
        }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(obj_map) = self.sop.get(&s) {
            if let Some(preds) = obj_map.get(&o) {
                if preds.contains(&p) { return false; }
            }
        }
        self.sop.entry(s).or_default().entry(o).or_default().insert(p);
        true
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(obj_map) = self.sop.get_mut(&s) {
            if let Some(preds) = obj_map.get_mut(&o) {
                if preds.remove(&p) {
                    if preds.is_empty() { obj_map.remove(&o); }
                    if obj_map.is_empty() { self.sop.remove(&s); }
                    return true;
                }
            }
        }
        false
    }

    fn build_from_triples(&mut self, triples: &[Triple]) {
        use rayon::prelude::*;
        self.clear();
        if triples.is_empty() { return; }

        let capacity = (triples.len() / 100).max(1);
        self.sop.reserve(capacity);

        let num_threads = rayon::current_num_threads();
        let chunk_size = (triples.len() / num_threads).max(10_000);

        let partials: Vec<SOPSingleIndex> = triples
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut local = SOPSingleIndex::new();
                local.sop.reserve((chunk.len() / 50).max(1));
                for t in chunk { local.insert_optimized(t); }
                local
            })
            .collect();

        for p in partials { self.merge_from(p); }
        self.optimize_post_build();
    }

    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        let mut results = Vec::new();
        match (s, p, o) {
            (Some(ss), Some(pp), Some(oo)) => {
                if let Some(obj_map) = self.sop.get(&ss) {
                    if let Some(preds) = obj_map.get(&oo) {
                        if preds.contains(&pp) {
                            results.push(Triple { subject: ss, predicate: pp, object: oo });
                        }
                    }
                }
            }
            (Some(ss), Some(pp), None) => {
                if let Some(obj_map) = self.sop.get(&ss) {
                    for (&obj, preds) in obj_map {
                        if preds.contains(&pp) {
                            results.push(Triple { subject: ss, predicate: pp, object: obj });
                        }
                    }
                }
            }
            (Some(ss), None, Some(oo)) => {
                if let Some(obj_map) = self.sop.get(&ss) {
                    if let Some(preds) = obj_map.get(&oo) {
                        for &pred in preds {
                            results.push(Triple { subject: ss, predicate: pred, object: oo });
                        }
                    }
                }
            }
            (None, Some(pp), Some(oo)) => {
                for (&sub, obj_map) in &self.sop {
                    if let Some(preds) = obj_map.get(&oo) {
                        if preds.contains(&pp) {
                            results.push(Triple { subject: sub, predicate: pp, object: oo });
                        }
                    }
                }
            }
            (Some(ss), None, None) => {
                if let Some(obj_map) = self.sop.get(&ss) {
                    for (&obj, preds) in obj_map {
                        for &pred in preds {
                            results.push(Triple { subject: ss, predicate: pred, object: obj });
                        }
                    }
                }
            }
            (None, Some(pp), None) => {
                for (&sub, obj_map) in &self.sop {
                    for (&obj, preds) in obj_map {
                        if preds.contains(&pp) {
                            results.push(Triple { subject: sub, predicate: pp, object: obj });
                        }
                    }
                }
            }
            (None, None, Some(oo)) => {
                for (&sub, obj_map) in &self.sop {
                    if let Some(preds) = obj_map.get(&oo) {
                        for &pred in preds {
                            results.push(Triple { subject: sub, predicate: pred, object: oo });
                        }
                    }
                }
            }
            (None, None, None) => {
                for (&sub, obj_map) in &self.sop {
                    for (&obj, preds) in obj_map {
                        for &pred in preds {
                            results.push(Triple { subject: sub, predicate: pred, object: obj });
                        }
                    }
                }
            }
        }
        results
    }

    fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
        let (s, p, o) = pattern;
        let sub = match s { Constant(x) => Some(*x), Variable(_) => None };
        let pre = match p { Constant(x) => Some(*x), Variable(_) => None };
        let obj = match o { Constant(x) => Some(*x), Variable(_) => None };
        self.query(sub, pre, obj)
    }

    fn clear(&mut self) { self.sop.clear(); }

    fn scan_so(&self, s: u32, o: u32) -> Option<&HashSet<u32>> {
        self.sop.get(&s).and_then(|obj_map| obj_map.get(&o))
    }

    fn scan_sp(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_po(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }

    fn optimize(&mut self) {
        use rayon::prelude::*;
        self.sop.par_iter_mut().for_each(|(_, obj_map)| {
            obj_map.par_iter_mut().for_each(|(_, preds)| {
                preds.shrink_to_fit();
            });
            obj_map.shrink_to_fit();
        });
        self.sop.shrink_to_fit();
    }
}

impl SOPSingleIndex {
    pub fn new() -> Self { Self { sop: HashMap::new() } }

    pub fn merge_from(&mut self, other: SOPSingleIndex) {
        for (s, obj_map) in other.sop {
            let entry = self.sop.entry(s).or_insert_with(HashMap::new);
            for (o, preds) in obj_map {
                entry.entry(o).or_insert_with(HashSet::new).extend(preds);
            }
        }
    }

    #[inline]
    fn insert_optimized(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(obj_map) = self.sop.get(&s) {
            if let Some(preds) = obj_map.get(&o) {
                if preds.contains(&p) { return false; }
            }
        }
        self.sop.entry(s).or_insert_with(|| HashMap::with_capacity(16))
            .entry(o).or_insert_with(|| HashSet::with_capacity(8))
            .insert(p);
        true
    }

    fn optimize_post_build(&mut self) {
        use rayon::prelude::*;
        self.sop.par_iter_mut().for_each(|(_, obj_map)| {
            obj_map.shrink_to_fit();
            obj_map.par_iter_mut().for_each(|(_, preds)| { preds.shrink_to_fit(); });
        });
        self.sop.shrink_to_fit();
    }
}
