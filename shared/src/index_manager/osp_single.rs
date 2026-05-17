use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OSPSingleIndex {
    pub osp: HashMap<u32, HashMap<u32, HashSet<u32>>>,
}

impl TripleIndex for OSPSingleIndex {
    fn clone_empty(&self) -> Box<dyn TripleIndex> { Box::new(OSPSingleIndex::new()) }
    fn clone_box(&self) -> Box<dyn TripleIndex> { Box::new(self.clone()) }

    fn triple_count(&self) -> usize {
        self.osp.values().map(|sub_map| sub_map.values().map(|ps| ps.len()).sum::<usize>()).sum()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport {
            sp: false, so: false, po: false,
            ps: false, os: true, op: false
        }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(sub_map) = self.osp.get(&o) {
            if let Some(preds) = sub_map.get(&s) {
                if preds.contains(&p) { return false; }
            }
        }
        self.osp.entry(o).or_default().entry(s).or_default().insert(p);
        true
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(sub_map) = self.osp.get_mut(&o) {
            if let Some(preds) = sub_map.get_mut(&s) {
                if preds.remove(&p) {
                    if preds.is_empty() { sub_map.remove(&s); }
                    if sub_map.is_empty() { self.osp.remove(&o); }
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
        self.osp.reserve(capacity);

        let num_threads = rayon::current_num_threads();
        let chunk_size = (triples.len() / num_threads).max(10_000);

        let partials: Vec<OSPSingleIndex> = triples
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut local = OSPSingleIndex::new();
                local.osp.reserve((chunk.len() / 50).max(1));
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
                if let Some(sub_map) = self.osp.get(&oo) {
                    if let Some(preds) = sub_map.get(&ss) {
                        if preds.contains(&pp) {
                            results.push(Triple { subject: ss, predicate: pp, object: oo });
                        }
                    }
                }
            }
            (Some(ss), Some(pp), None) => {
                for (&obj, sub_map) in &self.osp {
                    if let Some(preds) = sub_map.get(&ss) {
                        if preds.contains(&pp) {
                            results.push(Triple { subject: ss, predicate: pp, object: obj });
                        }
                    }
                }
            }
            (Some(ss), None, Some(oo)) => {
                if let Some(sub_map) = self.osp.get(&oo) {
                    if let Some(preds) = sub_map.get(&ss) {
                        for &pred in preds {
                            results.push(Triple { subject: ss, predicate: pred, object: oo });
                        }
                    }
                }
            }
            (None, Some(pp), Some(oo)) => {
                if let Some(sub_map) = self.osp.get(&oo) {
                    for (&sub, preds) in sub_map {
                        if preds.contains(&pp) {
                            results.push(Triple { subject: sub, predicate: pp, object: oo });
                        }
                    }
                }
            }
            (Some(ss), None, None) => {
                for (&obj, sub_map) in &self.osp {
                    if let Some(preds) = sub_map.get(&ss) {
                        for &pred in preds {
                            results.push(Triple { subject: ss, predicate: pred, object: obj });
                        }
                    }
                }
            }
            (None, Some(pp), None) => {
                for (&obj, sub_map) in &self.osp {
                    for (&sub, preds) in sub_map {
                        if preds.contains(&pp) {
                            results.push(Triple { subject: sub, predicate: pp, object: obj });
                        }
                    }
                }
            }
            (None, None, Some(oo)) => {
                if let Some(sub_map) = self.osp.get(&oo) {
                    for (&sub, preds) in sub_map {
                        for &pred in preds {
                            results.push(Triple { subject: sub, predicate: pred, object: oo });
                        }
                    }
                }
            }
            (None, None, None) => {
                for (&obj, sub_map) in &self.osp {
                    for (&sub, preds) in sub_map {
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
        let sub = match s { Constant(x) => Some(*x), Variable(_) => None, QuotedTriple(_) => None };
        let pre = match p { Constant(x) => Some(*x), Variable(_) => None, QuotedTriple(_) => None };
        let obj = match o { Constant(x) => Some(*x), Variable(_) => None, QuotedTriple(_) => None };
        self.query(sub, pre, obj)
    }

    fn clear(&mut self) { self.osp.clear(); }

    fn scan_os(&self, o: u32, s: u32) -> Option<&HashSet<u32>> {
        self.osp.get(&o).and_then(|sub_map| sub_map.get(&s))
    }

    fn scan_sp(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_so(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_po(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }

    fn optimize(&mut self) {
        use rayon::prelude::*;
        self.osp.par_iter_mut().for_each(|(_, sub_map)| {
            sub_map.par_iter_mut().for_each(|(_, preds)| { preds.shrink_to_fit(); });
            sub_map.shrink_to_fit();
        });
        self.osp.shrink_to_fit();
    }
}

impl OSPSingleIndex {
    pub fn new() -> Self { Self { osp: HashMap::new() } }

    pub fn merge_from(&mut self, other: OSPSingleIndex) {
        for (o, sub_map) in other.osp {
            let entry = self.osp.entry(o).or_insert_with(HashMap::new);
            for (s, preds) in sub_map {
                entry.entry(s).or_insert_with(HashSet::new).extend(preds);
            }
        }
    }

    #[inline]
    fn insert_optimized(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(sub_map) = self.osp.get(&o) {
            if let Some(preds) = sub_map.get(&s) {
                if preds.contains(&p) { return false; }
            }
        }
        self.osp.entry(o).or_insert_with(|| HashMap::with_capacity(16))
            .entry(s).or_insert_with(|| HashSet::with_capacity(8))
            .insert(p);
        true
    }

    fn optimize_post_build(&mut self) {
        use rayon::prelude::*;
        self.osp.par_iter_mut().for_each(|(_, sub_map)| {
            sub_map.shrink_to_fit();
            sub_map.par_iter_mut().for_each(|(_, preds)| { preds.shrink_to_fit(); });
        });
        self.osp.shrink_to_fit();
    }
}
