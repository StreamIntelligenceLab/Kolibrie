use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct POSSingleIndex {
    pub pos: HashMap<u32, HashMap<u32, HashSet<u32>>>,
}

impl TripleIndex for POSSingleIndex {
    fn clone_empty(&self) -> Box<dyn TripleIndex> { Box::new(POSSingleIndex::new()) }
    fn clone_box(&self) -> Box<dyn TripleIndex> { Box::new(self.clone()) }

    fn triple_count(&self) -> usize {
        self.pos.values().map(|obj_map| obj_map.values().map(|subs| subs.len()).sum::<usize>()).sum()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport {
            sp: false, so: false, po: true,
            ps: false, os: false, op: false
        }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(obj_map) = self.pos.get(&p) {
            if let Some(subs) = obj_map.get(&o) {
                if subs.contains(&s) { return false; }
            }
        }
        self.pos.entry(p).or_default().entry(o).or_default().insert(s);
        true
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(obj_map) = self.pos.get_mut(&p) {
            if let Some(subs) = obj_map.get_mut(&o) {
                if subs.remove(&s) {
                    if subs.is_empty() { obj_map.remove(&o); }
                    if obj_map.is_empty() { self.pos.remove(&p); }
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
        self.pos.reserve(capacity);

        let num_threads = rayon::current_num_threads();
        let chunk_size = (triples.len() / num_threads).max(10_000);

        let partials: Vec<POSSingleIndex> = triples
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut local = POSSingleIndex::new();
                local.pos.reserve((chunk.len() / 50).max(1));
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
                if let Some(obj_map) = self.pos.get(&pp) {
                    if let Some(subs) = obj_map.get(&oo) {
                        if subs.contains(&ss) {
                            results.push(Triple { subject: ss, predicate: pp, object: oo });
                        }
                    }
                }
            }
            (Some(ss), Some(pp), None) => {
                if let Some(obj_map) = self.pos.get(&pp) {
                    for (&obj, subs) in obj_map {
                        if subs.contains(&ss) {
                            results.push(Triple { subject: ss, predicate: pp, object: obj });
                        }
                    }
                }
            }
            (Some(ss), None, Some(oo)) => {
                for (&pred, obj_map) in &self.pos {
                    if let Some(subs) = obj_map.get(&oo) {
                        if subs.contains(&ss) {
                            results.push(Triple { subject: ss, predicate: pred, object: oo });
                        }
                    }
                }
            }
            (None, Some(pp), Some(oo)) => {
                if let Some(obj_map) = self.pos.get(&pp) {
                    if let Some(subs) = obj_map.get(&oo) {
                        for &sub in subs {
                            results.push(Triple { subject: sub, predicate: pp, object: oo });
                        }
                    }
                }
            }
            (Some(ss), None, None) => {
                for (&pred, obj_map) in &self.pos {
                    for (&obj, subs) in obj_map {
                        if subs.contains(&ss) {
                            results.push(Triple { subject: ss, predicate: pred, object: obj });
                        }
                    }
                }
            }
            (None, Some(pp), None) => {
                if let Some(obj_map) = self.pos.get(&pp) {
                    for (&obj, subs) in obj_map {
                        for &sub in subs {
                            results.push(Triple { subject: sub, predicate: pp, object: obj });
                        }
                    }
                }
            }
            (None, None, Some(oo)) => {
                for (&pred, obj_map) in &self.pos {
                    if let Some(subs) = obj_map.get(&oo) {
                        for &sub in subs {
                            results.push(Triple { subject: sub, predicate: pred, object: oo });
                        }
                    }
                }
            }
            (None, None, None) => {
                for (&pred, obj_map) in &self.pos {
                    for (&obj, subs) in obj_map {
                        for &sub in subs {
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

    fn clear(&mut self) { self.pos.clear(); }

    fn scan_po(&self, p: u32, o: u32) -> Option<&HashSet<u32>> {
        self.pos.get(&p).and_then(|obj_map| obj_map.get(&o))
    }

    fn scan_sp(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_so(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> { None }

    fn optimize(&mut self) {
        use rayon::prelude::*;
        self.pos.par_iter_mut().for_each(|(_, obj_map)| {
            obj_map.par_iter_mut().for_each(|(_, subs)| { subs.shrink_to_fit(); });
            obj_map.shrink_to_fit();
        });
        self.pos.shrink_to_fit();
    }
}

impl POSSingleIndex {
    pub fn new() -> Self { Self { pos: HashMap::new() } }

    pub fn merge_from(&mut self, other: POSSingleIndex) {
        for (p, obj_map) in other.pos {
            let entry = self.pos.entry(p).or_insert_with(HashMap::new);
            for (o, subs) in obj_map {
                entry.entry(o).or_insert_with(HashSet::new).extend(subs);
            }
        }
    }

    #[inline]
    fn insert_optimized(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        if let Some(obj_map) = self.pos.get(&p) {
            if let Some(subs) = obj_map.get(&o) {
                if subs.contains(&s) { return false; }
            }
        }
        self.pos.entry(p).or_insert_with(|| HashMap::with_capacity(16))
            .entry(o).or_insert_with(|| HashSet::with_capacity(8))
            .insert(s);
        true
    }

    fn optimize_post_build(&mut self) {
        use rayon::prelude::*;
        self.pos.par_iter_mut().for_each(|(_, obj_map)| {
            obj_map.shrink_to_fit();
            obj_map.par_iter_mut().for_each(|(_, subs)| { subs.shrink_to_fit(); });
        });
        self.pos.shrink_to_fit();
    }
}
