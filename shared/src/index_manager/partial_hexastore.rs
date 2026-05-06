use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::triple::Triple;
use crate::index_manager::*;
use crate::index_manager::dynamic_hexastore::{IndexType, CardinalitySnapshot};
use crate::query::PlannedAccessPattern;

#[derive(Debug, Clone)]
pub struct PartialHexastoreIndex {
    pub spo: Option<HashMap<u32, HashMap<u32, HashSet<u32>>>>,
    pub pos: Option<HashMap<u32, HashMap<u32, HashSet<u32>>>>,
    pub osp: Option<HashMap<u32, HashMap<u32, HashSet<u32>>>>,
    pub pso: Option<HashMap<u32, HashMap<u32, HashSet<u32>>>>,
    pub ops: Option<HashMap<u32, HashMap<u32, HashSet<u32>>>>,
    pub sop: Option<HashMap<u32, HashMap<u32, HashSet<u32>>>>,
    
    pub latest_card: CardinalitySnapshot,
}

impl PartialHexastoreIndex {
    pub fn new(patterns: Vec<PlannedAccessPattern>) -> Self {
        let required_indexes = Self::determine_smallest_index_set(&patterns);
        
        let mut created_names = Vec::new();
        if required_indexes.contains(&IndexType::SPO) { created_names.push("SPO"); }
        if required_indexes.contains(&IndexType::POS) { created_names.push("POS"); }
        if required_indexes.contains(&IndexType::OSP) { created_names.push("OSP"); }
        if required_indexes.contains(&IndexType::PSO) { created_names.push("PSO"); }
        if required_indexes.contains(&IndexType::OPS) { created_names.push("OPS"); }
        if required_indexes.contains(&IndexType::SOP) { created_names.push("SOP"); }
        
        println!("PartialHexastoreIndex initialized with indexes: {:?}", created_names);
        
        Self {
            spo: if required_indexes.contains(&IndexType::SPO) { Some(HashMap::new()) } else { None },
            pos: if required_indexes.contains(&IndexType::POS) { Some(HashMap::new()) } else { None },
            osp: if required_indexes.contains(&IndexType::OSP) { Some(HashMap::new()) } else { None },
            pso: if required_indexes.contains(&IndexType::PSO) { Some(HashMap::new()) } else { None },
            ops: if required_indexes.contains(&IndexType::OPS) { Some(HashMap::new()) } else { None },
            sop: if required_indexes.contains(&IndexType::SOP) { Some(HashMap::new()) } else { None },
            latest_card: CardinalitySnapshot::from_stats(0, 1, 1, 1, 1),
        }
    }
    
    pub fn update_cardinalities(&mut self, card: CardinalitySnapshot) {
        self.latest_card = card;
    }

    /// Finds the absolute smallest set of indexes that covers all physical access patterns efficiently.
    fn determine_smallest_index_set(patterns: &[PlannedAccessPattern]) -> HashSet<IndexType> {
        if patterns.is_empty() {
            return HashSet::from([IndexType::SPO]);
        }

        let all_types = [
            IndexType::SPO, IndexType::SOP, IndexType::PSO, 
            IndexType::POS, IndexType::OSP, IndexType::OPS
        ];

        let mut valid_types_per_pattern = Vec::new();
        for planned in patterns {
            let (s, p, o) = &planned.pattern;
            
            // A variable is considered bound if it's a true constant OR if it's pipeline-bound (from previous steps)
            let bound_s = matches!(s, Term::Constant(_)) || planned.bound_subject;
            let bound_p = matches!(p, Term::Constant(_)) || planned.bound_predicate;
            let bound_o = matches!(o, Term::Constant(_)) || planned.bound_object;

            let mut valid = Vec::new();
            match (bound_s, bound_p, bound_o) {
                (true, true, _) => { valid.push(IndexType::SPO); valid.push(IndexType::PSO); }
                (true, _, true) => { valid.push(IndexType::SOP); valid.push(IndexType::OSP); }
                (_, true, true) => { valid.push(IndexType::POS); valid.push(IndexType::OPS); }
                (true, false, false) => { valid.push(IndexType::SPO); valid.push(IndexType::SOP); }
                (false, true, false) => { valid.push(IndexType::PSO); valid.push(IndexType::POS); }
                (false, false, true) => { valid.push(IndexType::OSP); valid.push(IndexType::OPS); }
                (false, false, false) | (true, true, true) => {
                    valid.extend_from_slice(&all_types);
                }
            }
            valid_types_per_pattern.push(valid);
        }

        let mut min_size = usize::MAX;
        let mut best_set = HashSet::new();
        let n = all_types.len();

        for mask in 1..=(1 << n) - 1 {
            let mut candidate_set = HashSet::new();
            for (i, &t) in all_types.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    candidate_set.insert(t);
                }
            }

            let covers_all = valid_types_per_pattern.iter().all(|valid| {
                valid.iter().any(|vt| candidate_set.contains(vt))
            });

            if covers_all && candidate_set.len() < min_size {
                min_size = candidate_set.len();
                best_set = candidate_set.clone();
            }
        }

        if best_set.is_empty() {
            best_set.insert(IndexType::SPO);
        }

        best_set
    }

    /// Selects the best index on-query based on bound variables and root cardinality (lean index tiebreaker).
    fn select_best_index(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> IndexType {
        let mut candidates = Vec::new();
        
        let add_if_available = |candidates: &mut Vec<(IndexType, f64)>, idx: IndexType, available: bool, root_card: f64| {
            if available {
                candidates.push((idx, root_card));
            }
        };

        // Rule 1: Bound variables first.
        match (s.is_some(), p.is_some(), o.is_some()) {
            (true, true, _) => {
                add_if_available(&mut candidates, IndexType::SPO, self.spo.is_some(), self.latest_card.num_subjects);
                add_if_available(&mut candidates, IndexType::PSO, self.pso.is_some(), self.latest_card.num_predicates);
            }
            (true, _, true) => {
                add_if_available(&mut candidates, IndexType::SOP, self.sop.is_some(), self.latest_card.num_subjects);
                add_if_available(&mut candidates, IndexType::OSP, self.osp.is_some(), self.latest_card.num_objects);
            }
            (_, true, true) => {
                add_if_available(&mut candidates, IndexType::POS, self.pos.is_some(), self.latest_card.num_predicates);
                add_if_available(&mut candidates, IndexType::OPS, self.ops.is_some(), self.latest_card.num_objects);
            }
            (true, false, false) => {
                add_if_available(&mut candidates, IndexType::SPO, self.spo.is_some(), self.latest_card.num_subjects);
                add_if_available(&mut candidates, IndexType::SOP, self.sop.is_some(), self.latest_card.num_subjects);
            }
            (false, true, false) => {
                add_if_available(&mut candidates, IndexType::PSO, self.pso.is_some(), self.latest_card.num_predicates);
                add_if_available(&mut candidates, IndexType::POS, self.pos.is_some(), self.latest_card.num_predicates);
            }
            (false, false, true) => {
                add_if_available(&mut candidates, IndexType::OSP, self.osp.is_some(), self.latest_card.num_objects);
                add_if_available(&mut candidates, IndexType::OPS, self.ops.is_some(), self.latest_card.num_objects);
            }
            (false, false, false) | (true, true, true) => {
                add_if_available(&mut candidates, IndexType::SPO, self.spo.is_some(), self.latest_card.num_subjects);
                add_if_available(&mut candidates, IndexType::SOP, self.sop.is_some(), self.latest_card.num_subjects);
                add_if_available(&mut candidates, IndexType::PSO, self.pso.is_some(), self.latest_card.num_predicates);
                add_if_available(&mut candidates, IndexType::POS, self.pos.is_some(), self.latest_card.num_predicates);
                add_if_available(&mut candidates, IndexType::OSP, self.osp.is_some(), self.latest_card.num_objects);
                add_if_available(&mut candidates, IndexType::OPS, self.ops.is_some(), self.latest_card.num_objects);
            }
        }

        // Fallback if none of the optimal indexes for this query shape were instantiated
        if candidates.is_empty() {
            add_if_available(&mut candidates, IndexType::SPO, self.spo.is_some(), self.latest_card.num_subjects);
            add_if_available(&mut candidates, IndexType::SOP, self.sop.is_some(), self.latest_card.num_subjects);
            add_if_available(&mut candidates, IndexType::PSO, self.pso.is_some(), self.latest_card.num_predicates);
            add_if_available(&mut candidates, IndexType::POS, self.pos.is_some(), self.latest_card.num_predicates);
            add_if_available(&mut candidates, IndexType::OSP, self.osp.is_some(), self.latest_card.num_objects);
            add_if_available(&mut candidates, IndexType::OPS, self.ops.is_some(), self.latest_card.num_objects);
        }

        // Rule 2: Tiebreaker - lean index is better (smaller root cardinality)
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates[0].0
    }

    #[inline]
    fn query_index(
        index: &HashMap<u32, HashMap<u32, HashSet<u32>>>, 
        q_root: Option<u32>, q_mid: Option<u32>, q_leaf: Option<u32>,
        build_triple: impl Fn(u32, u32, u32) -> Triple
    ) -> Vec<Triple> {
        let mut results = Vec::new();
        let mut scan_mid = |root_val: u32, mid_map: &HashMap<u32, HashSet<u32>>| {
            if let Some(mv) = q_mid {
                if let Some(leaf_set) = mid_map.get(&mv) {
                    if let Some(lv) = q_leaf {
                        if leaf_set.contains(&lv) { results.push(build_triple(root_val, mv, lv)); }
                    } else {
                        for &lv in leaf_set { results.push(build_triple(root_val, mv, lv)); }
                    }
                }
            } else {
                for (&mv, leaf_set) in mid_map {
                    if let Some(lv) = q_leaf {
                        if leaf_set.contains(&lv) { results.push(build_triple(root_val, mv, lv)); }
                    } else {
                        for &lv in leaf_set { results.push(build_triple(root_val, mv, lv)); }
                    }
                }
            }
        };

        if let Some(rv) = q_root {
            if let Some(mid_map) = index.get(&rv) { scan_mid(rv, mid_map); }
        } else {
            for (&rv, mid_map) in index { scan_mid(rv, mid_map); }
        }
        results
    }
}

impl TripleIndex for PartialHexastoreIndex {
    fn insert(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        let mut inserted = false;
        if let Some(ref mut idx) = self.spo { inserted |= idx.entry(s).or_default().entry(p).or_default().insert(o); }
        if let Some(ref mut idx) = self.pos { inserted |= idx.entry(p).or_default().entry(o).or_default().insert(s); }
        if let Some(ref mut idx) = self.osp { inserted |= idx.entry(o).or_default().entry(s).or_default().insert(p); }
        if let Some(ref mut idx) = self.pso { inserted |= idx.entry(p).or_default().entry(s).or_default().insert(o); }
        if let Some(ref mut idx) = self.ops { inserted |= idx.entry(o).or_default().entry(p).or_default().insert(s); }
        if let Some(ref mut idx) = self.sop { inserted |= idx.entry(s).or_default().entry(o).or_default().insert(p); }
        inserted
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        let Triple { subject: s, predicate: p, object: o } = *triple;
        let mut deleted = false;
        
        let check_and_delete = |idx: &mut Option<HashMap<u32, HashMap<u32, HashSet<u32>>>>, r, m, l| {
            if let Some(map) = idx {
                remove_from_index(map, r, m, l);
                return true; 
            }
            false
        };

        if let Some(ref mut idx) = self.spo { 
            let exists = idx.get(&s).and_then(|pm| pm.get(&p)).map_or(false, |os| os.contains(&o));
            if !exists { return false; }
        }

        deleted |= check_and_delete(&mut self.spo, s, p, o);
        check_and_delete(&mut self.pos, p, o, s);
        check_and_delete(&mut self.osp, o, s, p);
        check_and_delete(&mut self.pso, p, s, o);
        check_and_delete(&mut self.ops, o, p, s);
        check_and_delete(&mut self.sop, s, o, p);
        
        deleted
    }

    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        let best_index = self.select_best_index(s, p, o);
        match best_index {
            IndexType::SPO => Self::query_index(self.spo.as_ref().unwrap(), s, p, o, |s, p, o| Triple { subject: s, predicate: p, object: o }),
            IndexType::SOP => Self::query_index(self.sop.as_ref().unwrap(), s, o, p, |s, o, p| Triple { subject: s, predicate: p, object: o }),
            IndexType::PSO => Self::query_index(self.pso.as_ref().unwrap(), p, s, o, |p, s, o| Triple { subject: s, predicate: p, object: o }),
            IndexType::POS => Self::query_index(self.pos.as_ref().unwrap(), p, o, s, |p, o, s| Triple { subject: s, predicate: p, object: o }),
            IndexType::OSP => Self::query_index(self.osp.as_ref().unwrap(), o, s, p, |o, s, p| Triple { subject: s, predicate: p, object: o }),
            IndexType::OPS => Self::query_index(self.ops.as_ref().unwrap(), o, p, s, |o, p, s| Triple { subject: s, predicate: p, object: o }),
        }
    }

    fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
        let (s, p, o) = pattern;
        let sub = match s { Term::Constant(x) => Some(*x), Term::Variable(_) => None };
        let pre = match p { Term::Constant(x) => Some(*x), Term::Variable(_) => None };
        let obj = match o { Term::Constant(x) => Some(*x), Term::Variable(_) => None };
        self.query(sub, pre, obj)
    }

    fn clear(&mut self) {
        if let Some(idx) = &mut self.spo { idx.clear(); }
        if let Some(idx) = &mut self.pos { idx.clear(); }
        if let Some(idx) = &mut self.osp { idx.clear(); }
        if let Some(idx) = &mut self.pso { idx.clear(); }
        if let Some(idx) = &mut self.ops { idx.clear(); }
        if let Some(idx) = &mut self.sop { idx.clear(); }
    }

    fn clone_empty(&self) -> Box<dyn TripleIndex> {
        Box::new(Self {
            spo: self.spo.as_ref().map(|_| HashMap::new()),
            pos: self.pos.as_ref().map(|_| HashMap::new()),
            osp: self.osp.as_ref().map(|_| HashMap::new()),
            pso: self.pso.as_ref().map(|_| HashMap::new()),
            ops: self.ops.as_ref().map(|_| HashMap::new()),
            sop: self.sop.as_ref().map(|_| HashMap::new()),
            latest_card: self.latest_card.clone(),
        })
    }

    fn clone_box(&self) -> Box<dyn TripleIndex> {
        Box::new(self.clone())
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport {
            sp: self.spo.is_some() || self.pso.is_some(),
            so: self.sop.is_some() || self.osp.is_some(),
            po: self.pos.is_some() || self.ops.is_some(),
            ps: self.pso.is_some() || self.spo.is_some(),
            os: self.osp.is_some() || self.sop.is_some(),
            op: self.ops.is_some() || self.pos.is_some(),
        }
    }

    fn scan_sp(&self, s: u32, p: u32) -> Option<&HashSet<u32>> {
        self.spo.as_ref().and_then(|idx| idx.get(&s).and_then(|m| m.get(&p)))
    }
    fn scan_so(&self, s: u32, o: u32) -> Option<&HashSet<u32>> {
        self.sop.as_ref().and_then(|idx| idx.get(&s).and_then(|m| m.get(&o)))
    }
    fn scan_po(&self, p: u32, o: u32) -> Option<&HashSet<u32>> {
        self.pos.as_ref().and_then(|idx| idx.get(&p).and_then(|m| m.get(&o)))
    }
    fn scan_ps(&self, p: u32, s: u32) -> Option<&HashSet<u32>> {
        self.pso.as_ref().and_then(|idx| idx.get(&p).and_then(|m| m.get(&s)))
    }
    fn scan_os(&self, o: u32, s: u32) -> Option<&HashSet<u32>> {
        self.osp.as_ref().and_then(|idx| idx.get(&o).and_then(|m| m.get(&s)))
    }
    fn scan_op(&self, o: u32, p: u32) -> Option<&HashSet<u32>> {
        self.ops.as_ref().and_then(|idx| idx.get(&o).and_then(|m| m.get(&p)))
    }
}