use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::terms::Term::*;

pub type RuleId = usize;
pub const WILDCARD: u32 = u32::MAX;

#[derive(Debug, Clone)]
pub struct RuleIndex {
    pub spo: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    pub pos: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    pub osp: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    pub pso: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    pub ops: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    pub sop: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
}

impl RuleIndex {
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

    pub fn clear(&mut self) {
        self.spo.clear();
        self.pos.clear();
        self.osp.clear();
        self.pso.clear();
        self.ops.clear();
        self.sop.clear();
    }

    pub fn insert_premise_pattern(&mut self, pattern: &TriplePattern, rule_id: RuleId) {
        let (s_val, p_val, o_val) = triple_pattern_to_keys(pattern);

        // Insert into each of the six permutations
        self.spo.entry(s_val).or_default().entry(p_val).or_default().insert(rule_id);
        self.pos.entry(p_val).or_default().entry(o_val).or_default().insert(rule_id);
        self.osp.entry(o_val).or_default().entry(s_val).or_default().insert(rule_id);
        self.pso.entry(p_val).or_default().entry(s_val).or_default().insert(rule_id);
        self.ops.entry(o_val).or_default().entry(p_val).or_default().insert(rule_id);
        self.sop.entry(s_val).or_default().entry(o_val).or_default().insert(rule_id);
    }

    pub fn query_candidate_rules(
        &self,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> HashSet<RuleId> {
        let mut results = HashSet::new();

        match (s, p, o) {
            // (S, P, O) fully bound
            (Some(ss), Some(pp), Some(oo)) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    if let Some(rule_set) = pred_map.get(&pp) {
                        results.extend(rule_set);
                    }
                }
                //   - s -> sop
                if let Some(obj_map) = self.sop.get(&ss) {
                    if let Some(rule_set) = obj_map.get(&oo) {
                        results.extend(rule_set);
                    }
                }
                //   - p -> pos
                if let Some(obj_map) = self.pos.get(&pp) {
                    if let Some(rule_set) = obj_map.get(&oo) {
                        results.extend(rule_set);
                    }
                }
                //   - o -> osp or ops
                if let Some(subj_map) = self.osp.get(&oo) {
                    if let Some(rule_set) = subj_map.get(&ss) {
                        results.extend(rule_set);
                    }
                }
                if let Some(pred_map) = self.ops.get(&oo) {
                    if let Some(rule_set) = pred_map.get(&pp) {
                        results.extend(rule_set);
                    }
                }
            }

            // (S, P, None)
            (Some(ss), Some(pp), None) => {
                // subject + predicate known, object unknown
                // => use the spo map if it has s->p => union all rule_ids
                if let Some(pred_map) = self.spo.get(&ss) {
                    if let Some(rule_set) = pred_map.get(&pp) {
                        results.extend(rule_set);
                    }
                }

                // plus the "pso" map if it has p->s => union all
                if let Some(subj_map) = self.pso.get(&pp) {
                    if let Some(rule_set) = subj_map.get(&ss) {
                        results.extend(rule_set);
                    }
                }
                // etc.
            }

            // (S, None, O)
            (Some(ss), None, Some(oo)) => {
                // We can use the sop map if it has s->o => union all
                if let Some(obj_map) = self.sop.get(&ss) {
                    if let Some(rule_set) = obj_map.get(&oo) {
                        results.extend(rule_set);
                    }
                }
                // and also the osp map if it has o->s => union
                if let Some(subj_map) = self.osp.get(&oo) {
                    if let Some(rule_set) = subj_map.get(&ss) {
                        results.extend(rule_set);
                    }
                }
            }

            // (None, P, O)
            (None, Some(pp), Some(oo)) => {
                // pos, ops
                if let Some(obj_map) = self.pos.get(&pp) {
                    if let Some(rule_set) = obj_map.get(&oo) {
                        results.extend(rule_set);
                    }
                }
                if let Some(pred_map) = self.ops.get(&oo) {
                    if let Some(rule_set) = pred_map.get(&pp) {
                        results.extend(rule_set);
                    }
                }
            }

            // (S, None, None)
            (Some(ss), None, None) => {
                // union everything from spo[s] and sop[s]
                if let Some(pred_map) = self.spo.get(&ss) {
                    for (_p, rule_set) in pred_map {
                        results.extend(rule_set);
                    }
                }
                if let Some(obj_map) = self.sop.get(&ss) {
                    for (_o, rule_set) in obj_map {
                        results.extend(rule_set);
                    }
                }
            }

            // (None, P, None)
            (None, Some(pp), None) => {
                // union everything from pos[p] and pso[p]
                if let Some(obj_map) = self.pos.get(&pp) {
                    for (_o, rule_set) in obj_map {
                        results.extend(rule_set);
                    }
                }
                if let Some(subj_map) = self.pso.get(&pp) {
                    for (_s, rule_set) in subj_map {
                        results.extend(rule_set);
                    }
                }
            }

            // (None, None, O)
            (None, None, Some(oo)) => {
                // union everything from osp[o] and ops[o]
                if let Some(subj_map) = self.osp.get(&oo) {
                    for (_s, rule_set) in subj_map {
                        results.extend(rule_set);
                    }
                }
                if let Some(pred_map) = self.ops.get(&oo) {
                    for (_p, rule_set) in pred_map {
                        results.extend(rule_set);
                    }
                }
            }

            // (None, None, None): no constraints => return all rule_ids
            (None, None, None) => {
                // union everything from all 6 permutations
                for (_, pred_map) in &self.spo {
                    for (_, rule_set) in pred_map {
                        results.extend(rule_set);
                    }
                }
            }
        }

        results
    }
}

/// Helper: convert a triple pattern to (u32,u32,u32) using WILDCARD for variables
fn triple_pattern_to_keys(pattern: &TriplePattern) -> (u32, u32, u32) {
    let s_val = match &pattern.0 {
        Constant(c) => *c,
        Variable(_) => WILDCARD,
    };
    let p_val = match &pattern.1 {
        Constant(c) => *c,
        Variable(_) => WILDCARD,
    };
    let o_val = match &pattern.2 {
        Constant(c) => *c,
        Variable(_) => WILDCARD,
    };
    (s_val, p_val, o_val)
}