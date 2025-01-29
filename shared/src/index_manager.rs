use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::terms::Term::*;
use crate::triple::Triple;

#[derive(Debug, Clone)]
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

    /// Bulk-build the index from a list of triples
    pub fn build_from_triples(&mut self, triples: &[Triple]) {
        self.clear();
        for t in triples {
            self.insert(t);
        }
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

    pub fn optimize(&mut self) {
        for (_s, pmap) in &mut self.spo {
            for (_p, obj_set) in pmap {
                obj_set.shrink_to_fit();
            }
        }
    }
}