/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */
pub mod materialisation;
pub mod to_dot;
pub mod backward_chaining;
pub mod rules;
pub mod repairs;
pub mod helpers;

use shared::dictionary::Dictionary;
use shared::index_manager::*;
use shared::rule::Rule;
use shared::rule_index::RuleIndex;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use crate::reasoning::rules::join_rule;

// Single solution mapping: (V -> I U B U L) (one result entry)
type Bindings = HashMap<String, Term>;

// Logic part: Knowledge Graph

#[derive(Debug, Clone)]
// Are there RDF connections here or not?
pub struct Reasoner {
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // List of dynamic rules

    pub index_manager: UnifiedIndex,
    pub rule_index: RuleIndex,
    pub constraints: Vec<Rule>,
}

pub fn convert_string_binding_to_u32(
    binding: &BTreeMap<String, String>,
    dict: &Dictionary
) -> HashMap<String, u32> {
    let mut result = HashMap::new();
    for (var, value) in binding {
        if let Some(&id) = dict.string_to_id.get(value) {
            result.insert(var.clone(), id);
        }
    }
    result
}

impl Reasoner {
    pub fn new() -> Self {
        Self {
            dictionary: Dictionary::new(),
            rules: Vec::new(),
            index_manager: UnifiedIndex::new(),
            rule_index: RuleIndex::new(),
            constraints: Vec::new(),
        }
    }

    /// Add an ABox triple (instance-level information)
    pub fn add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let s = self.dictionary.encode(subject);
        let p = self.dictionary.encode(predicate);
        let o = self.dictionary.encode(object);

        self.index_manager.insert(&Triple {
            subject: s,
            predicate: p,
            object: o,
        });
    }

    /// Query the ABox for instance-level assertions (using TrieIndex now)
    pub fn query_abox(
        &mut self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> Vec<Triple> {
        let s = subject.map(|s| self.dictionary.encode(s));
        let p = predicate.map(|p| self.dictionary.encode(p));
        let o = object.map(|o| self.dictionary.encode(o));

        self.index_manager.query(s, p, o)
    }

    /// Add new method to handle constraints
    pub fn add_constraint(&mut self, constraint: Rule) {
        self.constraints.push(constraint);
    }

    /// New method to check if a set of facts violates constraints
    fn violates_constraints(&self, facts: &HashSet<Triple>) -> bool {
        for constraint in &self.constraints {
            let bindings = join_rule(constraint, facts, facts);
            if !bindings.is_empty() {
                return true;
            }
        }
        false
    }

    /// New method to find repairs
    fn compute_repairs(&self, facts: &HashSet<Triple>) -> Vec<HashSet<Triple>> {
        let mut repairs = Vec::new();
        let mut work_queue = vec![facts.clone()];
        let mut seen = BTreeSet::new(); // Using BTreeSet instead of HashSet

        while let Some(current_set) = work_queue.pop() {
            // Convert current_set to a Vec for consistent ordering when inserting into seen
            let current_vec: Vec<_> = current_set.iter().cloned().collect();

            // Skip if we've seen this combination before
            if !seen.insert(current_vec.clone()) {
                continue;
            }

            if !self.violates_constraints(&current_set) {
                // Found a consistent subset
                let is_maximal = repairs.iter().all(|repair: &HashSet<Triple>| {
                    !repair.is_superset(&current_set) || repair == &current_set
                });

                if is_maximal {
                    repairs.push(current_set);
                }
            } else {
                // Try removing each fact to create new candidate repairs
                for fact in current_set.iter() {
                    let mut new_set = current_set.clone();
                    new_set.remove(fact);

                    // Convert new_set to Vec for checking seen
                    let new_vec: Vec<_> = new_set.iter().cloned().collect();
                    if !seen.contains(&new_vec) {
                        work_queue.push(new_set);
                    }
                }
            }
        }
        repairs
    }
}