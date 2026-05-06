use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SPOSingleIndex {
    pub spo: HashMap<u32, HashMap<u32, HashSet<u32>>>,
}

impl TripleIndex for SPOSingleIndex {
    fn clone_empty(&self) -> Box<dyn TripleIndex> {
        Box::new(SPOSingleIndex::new())
    }

    fn clone_box(&self) -> Box<dyn TripleIndex> {
        Box::new(self.clone())
    }

    fn triple_count(&self) -> usize {
        self.spo.values()
            .map(|pred_map| pred_map.values().map(|objs| objs.len()).sum::<usize>())
            .sum()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport {
            sp: true, so: false, po: false,
            ps: false, os: false, op: false
        }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(pred_map) = self.spo.get(&s) {
            if let Some(objects) = pred_map.get(&p) {
                if objects.contains(&o) {
                    return false;
                }
            }
        }
        self.spo.entry(s).or_default().entry(p).or_default().insert(o);
        true
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;

        if let Some(pred_map) = self.spo.get_mut(&s) {
            if let Some(obj_set) = pred_map.get_mut(&p) {
                if obj_set.remove(&o) {
                    // cleanup empty maps
                    if obj_set.is_empty() {
                        pred_map.remove(&p);
                    }
                    if pred_map.is_empty() {
                        self.spo.remove(&s);
                    }
                    return true;
                }
            }
        }
        false
    }

    fn build_from_triples(&mut self, triples: &[Triple]) {
        use rayon::prelude::*;

        self.clear();
        if triples.is_empty() {
            return;
        }

        let capacity = (triples.len() / 100).max(1);
        self.spo.reserve(capacity);

        let num_threads = rayon::current_num_threads();
        let chunk_size = (triples.len() / num_threads).max(10_000);

        let partials: Vec<SPOSingleIndex> = triples
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut local = SPOSingleIndex::new();
                let local_capacity = (chunk.len() / 50).max(1);
                local.spo.reserve(local_capacity);

                for t in chunk {
                    local.insert_optimized(t);
                }
                local
            })
            .collect();

        for part in partials {
            self.merge_from(part);
        }

        self.optimize_post_build();
    }

    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        let mut results = Vec::new();
        match (s, p, o) {
            (Some(ss), Some(pp), Some(oo)) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    if let Some(objs) = pred_map.get(&pp) {
                        if objs.contains(&oo) {
                            results.push(Triple { subject: ss, predicate: pp, object: oo });
                        }
                    }
                }
            }
            (Some(ss), Some(pp), None) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    if let Some(objs) = pred_map.get(&pp) {
                        for &obj in objs {
                            results.push(Triple { subject: ss, predicate: pp, object: obj });
                        }
                    }
                }
            }
            (Some(ss), None, Some(oo)) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    for (&pred, objs) in pred_map {
                        if objs.contains(&oo) {
                            results.push(Triple { subject: ss, predicate: pred, object: oo });
                        }
                    }
                }
            }
            (None, Some(pp), Some(oo)) => {
                for (&sub, pred_map) in &self.spo {
                    if let Some(objs) = pred_map.get(&pp) {
                        if objs.contains(&oo) {
                            results.push(Triple { subject: sub, predicate: pp, object: oo });
                        }
                    }
                }
            }
            (Some(ss), None, None) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    for (&pred, objs) in pred_map {
                        for &obj in objs {
                            results.push(Triple { subject: ss, predicate: pred, object: obj });
                        }
                    }
                }
            }
            (None, Some(pp), None) => {
                for (&sub, pred_map) in &self.spo {
                    if let Some(objs) = pred_map.get(&pp) {
                        for &obj in objs {
                            results.push(Triple { subject: sub, predicate: pp, object: obj });
                        }
                    }
                }
            }
            (None, None, Some(oo)) => {
                for (&sub, pred_map) in &self.spo {
                    for (&pred, objs) in pred_map {
                        if objs.contains(&oo) {
                            results.push(Triple { subject: sub, predicate: pred, object: oo });
                        }
                    }
                }
            }
            (None, None, None) => {
                for (&sub, pred_map) in &self.spo {
                    for (&pred, objs) in pred_map {
                        for &obj in objs {
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

    fn clear(&mut self) {
        self.spo.clear();
    }

    fn scan_sp(&self, s: u32, p: u32) -> Option<&HashSet<u32>> {
        self.spo.get(&s).and_then(|pred_map| pred_map.get(&p))
    }

    fn scan_so(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_po(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }

    fn optimize(&mut self) {
        use rayon::prelude::*;
        self.spo.par_iter_mut().for_each(|(_, pred_map)| {
            pred_map.par_iter_mut().for_each(|(_, obj_set)| {
                obj_set.shrink_to_fit();
            });
            pred_map.shrink_to_fit();
        });
        self.spo.shrink_to_fit();
    }
}

impl SPOSingleIndex {
    pub fn new() -> Self {
        Self { spo: HashMap::new() }
    }

    pub fn merge_from(&mut self, other: SPOSingleIndex) {
        for (s, pred_map) in other.spo {
            let entry = self.spo.entry(s).or_insert_with(HashMap::new);
            for (p, obj_set) in pred_map {
                entry.entry(p).or_insert_with(HashSet::new).extend(obj_set);
            }
        }
    }

    #[inline]
    fn insert_optimized(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(pred_map) = self.spo.get(&s) {
            if let Some(objs) = pred_map.get(&p) {
                if objs.contains(&o) { return false; }
            }
        }
        self.spo.entry(s).or_insert_with(|| HashMap::with_capacity(16))
            .entry(p).or_insert_with(|| HashSet::with_capacity(8))
            .insert(o);
        true
    }

    fn optimize_post_build(&mut self) {
        use rayon::prelude::*;
        self.spo.par_iter_mut().for_each(|(_, pred_map)| {
            pred_map.shrink_to_fit();
            pred_map.par_iter_mut().for_each(|(_, obj_set)| {
                obj_set.shrink_to_fit();
            });
        });
        self.spo.shrink_to_fit();
    }
}
