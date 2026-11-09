/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::terms::Term::*;
use crate::terms::*;
use std::collections::{HashMap, HashSet};

/// Index of a rule in the reasoner's rule list
pub type RuleId = usize;
/// Stands in for a variable position when a pattern is indexed
pub const WILDCARD: u32 = u32::MAX;

#[derive(Debug, Clone)]
/// Maps a fact to the rules whose premises could match it
pub struct RuleIndex {
    /// Subject then predicate
    pub spo: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    /// Predicate then object
    pub pos: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    /// Object then subject
    pub osp: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    /// Predicate then subject
    pub pso: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    /// Object then predicate
    pub ops: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
    /// Subject then object
    pub sop: HashMap<u32, HashMap<u32, HashSet<RuleId>>>,
}

impl RuleIndex {
    /// An empty index
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

    /// Drops every indexed premise, keeping the allocation
    pub fn clear(&mut self) {
        self.spo.clear();
        self.pos.clear();
        self.osp.clear();
        self.pso.clear();
        self.ops.clear();
        self.sop.clear();
    }

    /// Files one premise of `rule_id`, so a matching fact will list that rule as a candidate
    pub fn insert_premise_pattern(&mut self, pattern: &TriplePattern, rule_id: RuleId) {
        let (s_val, p_val, o_val) = triple_pattern_to_keys(pattern);

        self.spo
            .entry(s_val)
            .or_default()
            .entry(p_val)
            .or_default()
            .insert(rule_id);
        self.pos
            .entry(p_val)
            .or_default()
            .entry(o_val)
            .or_default()
            .insert(rule_id);
        self.osp
            .entry(o_val)
            .or_default()
            .entry(s_val)
            .or_default()
            .insert(rule_id);
        self.pso
            .entry(p_val)
            .or_default()
            .entry(s_val)
            .or_default()
            .insert(rule_id);
        self.ops
            .entry(o_val)
            .or_default()
            .entry(p_val)
            .or_default()
            .insert(rule_id);
        self.sop
            .entry(s_val)
            .or_default()
            .entry(o_val)
            .or_default()
            .insert(rule_id);
    }

    /// Rules that might fire for a fact with the given bound positions
    pub fn query_candidate_rules(
        &self,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> HashSet<RuleId> {
        let mut results = HashSet::new();

        match (s, p, o) {
            (Some(ss), Some(pp), Some(oo)) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    if let Some(rule_set) = pred_map.get(&pp) {
                        results.extend(rule_set);
                    }
                }
                if let Some(obj_map) = self.sop.get(&ss) {
                    if let Some(rule_set) = obj_map.get(&oo) {
                        results.extend(rule_set);
                    }
                }
                if let Some(obj_map) = self.pos.get(&pp) {
                    if let Some(rule_set) = obj_map.get(&oo) {
                        results.extend(rule_set);
                    }
                }
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

            (Some(ss), Some(pp), None) => {
                if let Some(pred_map) = self.spo.get(&ss) {
                    if let Some(rule_set) = pred_map.get(&pp) {
                        results.extend(rule_set);
                    }
                }

                if let Some(subj_map) = self.pso.get(&pp) {
                    if let Some(rule_set) = subj_map.get(&ss) {
                        results.extend(rule_set);
                    }
                }
            }

            (Some(ss), None, Some(oo)) => {
                if let Some(obj_map) = self.sop.get(&ss) {
                    if let Some(rule_set) = obj_map.get(&oo) {
                        results.extend(rule_set);
                    }
                }
                if let Some(subj_map) = self.osp.get(&oo) {
                    if let Some(rule_set) = subj_map.get(&ss) {
                        results.extend(rule_set);
                    }
                }
            }

            (None, Some(pp), Some(oo)) => {
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

            (Some(ss), None, None) => {
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

            (None, Some(pp), None) => {
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

            (None, None, Some(oo)) => {
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

            (None, None, None) => {
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
        Variable(_) | QuotedTriple(_) => WILDCARD,
    };
    let p_val = match &pattern.1 {
        Constant(c) => *c,
        Variable(_) | QuotedTriple(_) => WILDCARD,
    };
    let o_val = match &pattern.2 {
        Constant(c) => *c,
        Variable(_) | QuotedTriple(_) => WILDCARD,
    };
    (s_val, p_val, o_val)
}
