/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::rule::Rule;
use shared::triple::Triple;
use crate::knowledge_graph::KnowledgeGraph;
use crate::knowledge_graph::matches_rule_pattern;
use crate::knowledge_graph::construct_triple;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReasoningLevel {
    Base = 0,           // Ground facts
    Deductive = 1,      // Logical deduction rules
    Abductive = 2,      // Hypothesis generation
    MetaReasoning = 3,  // Reasoning about reasoning
}

#[derive(Debug, Clone)]
pub struct HierarchicalRule {
    pub rule: Rule,
    pub level: ReasoningLevel,
    pub priority: u32,
    pub dependencies: Vec<ReasoningLevel>,
}

#[derive(Debug, Clone)]
pub struct ReasoningHierarchy {
    pub levels: BTreeMap<ReasoningLevel, KnowledgeGraph>,
    pub cross_level_rules: Vec<HierarchicalRule>,
    pub propagation_rules: Vec<HierarchicalRule>, // Rules for moving facts between levels
}

impl ReasoningHierarchy {
    pub fn new() -> Self {
        let mut levels = BTreeMap::new();
        levels.insert(ReasoningLevel::Base, KnowledgeGraph::new());
        levels.insert(ReasoningLevel::Deductive, KnowledgeGraph::new());
        levels.insert(ReasoningLevel::Abductive, KnowledgeGraph::new());
        levels.insert(ReasoningLevel::MetaReasoning, KnowledgeGraph::new());
        
        Self {
            levels,
            cross_level_rules: Vec::new(),
            propagation_rules: Vec::new(),
        }
    }

    /// Add a fact at a specific reasoning level
    pub fn add_fact_at_level(&mut self, level: ReasoningLevel, subject: &str, predicate: &str, object: &str) {
        if let Some(kg) = self.levels.get_mut(&level) {
            kg.add_abox_triple(subject, predicate, object);
        }
    }

    /// Add a rule at a specific reasoning level
    pub fn add_rule_at_level(&mut self, level: ReasoningLevel, rule: Rule, priority: u32) {
        let hierarchical_rule = HierarchicalRule {
            rule: rule.clone(),
            level: level.clone(),
            priority,
            dependencies: vec![level.clone()],
        };
        
        if let Some(kg) = self.levels.get_mut(&level) {
            kg.add_rule(rule);
        }
        self.cross_level_rules.push(hierarchical_rule);
    }

    /// Add a cross-level rule 
    /// (e.g., abductive rule that generates hypotheses from deductive conclusions)
    pub fn add_cross_level_rule(&mut self, rule: HierarchicalRule) {
        self.cross_level_rules.push(rule);
    }

    /// Perform hierarchical inference
    /// process each level in order
    pub fn hierarchical_inference(&mut self) -> BTreeMap<ReasoningLevel, Vec<Triple>> {
        let mut all_inferred = BTreeMap::new();
        
        // Process levels in order: Base -> Deductive -> Abductive -> MetaReasoning
        for level in [ReasoningLevel::Base, ReasoningLevel::Deductive, 
                     ReasoningLevel::Abductive, ReasoningLevel::MetaReasoning].iter() {
            
            // Run inference within this level
            if let Some(kg) = self.levels.get_mut(level) {
                let inferred = kg.infer_new_facts_semi_naive();
                all_inferred.insert(level.clone(), inferred);
            }
            
            // Apply cross-level rules that output to this level
            self.apply_cross_level_rules(level.clone());
        }
        
        all_inferred
    }

    /// Apply cross-level rules that target a specific level
    fn apply_cross_level_rules(&mut self, target_level: ReasoningLevel) {
        let applicable_rules: Vec<_> = self.cross_level_rules.iter()
            .filter(|r| r.level == target_level)
            .cloned()
            .collect();
            
        for hierarchical_rule in applicable_rules {
            // Collect facts from dependency levels
            let mut combined_facts = Vec::new();
            
            // Iterate over dependencies to gather facts
            let dep_levels: Vec<_> = hierarchical_rule.dependencies.clone();
            
            for dep_level in dep_levels {
                // Use the index manager directly to avoid mutable borrow issues
                if let Some(kg) = self.levels.get(&dep_level) {
                    // Use the index manager's query method instead of query_abox
                    combined_facts.extend(kg.index_manager.query(None, None, None));
                }
            }
            
            // Apply the rule to the combined facts
            let new_facts = self.apply_rule_to_facts(&hierarchical_rule.rule, &combined_facts);
            
            // Add new facts to target level
            if let Some(target_kg) = self.levels.get_mut(&target_level) {
                for fact in new_facts {
                    target_kg.index_manager.insert(&fact);
                }
            }
        }
    }

    /// Apply a single rule to a set of facts
    fn apply_rule_to_facts(&mut self, rule: &Rule, facts: &[Triple]) -> Vec<Triple> {
        let mut new_facts = Vec::new();
        
        match rule.premise.len() {
            1 => {
                for fact in facts {
                    let mut bindings = HashMap::new();
                    if matches_rule_pattern(&rule.premise[0], fact, &mut bindings) {
                        for conclusion in &rule.conclusion {
                            // Construct new fact based on conclusion and bindings
                            if let Some(base_kg) = self.levels.get_mut(&ReasoningLevel::Base) {
                                let new_fact = construct_triple(conclusion, &bindings, &mut base_kg.dictionary);
                                new_facts.push(new_fact);
                            }
                        }
                    }
                }
            }
            2 => {
                for fact1 in facts {
                    for fact2 in facts {
                        let mut bindings = HashMap::new();
                        if matches_rule_pattern(&rule.premise[0], fact1, &mut bindings) 
                            && matches_rule_pattern(&rule.premise[1], fact2, &mut bindings) {
                            for conclusion in &rule.conclusion {
                                if let Some(base_kg) = self.levels.get_mut(&ReasoningLevel::Base) {
                                    let new_fact = construct_triple(conclusion, &bindings, &mut base_kg.dictionary);
                                    new_facts.push(new_fact);
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        
        new_facts
    }

    /// Query across all levels or a specific level
    pub fn query_hierarchy(&mut self, level: Option<ReasoningLevel>, subject: Option<&str>, 
                          predicate: Option<&str>, object: Option<&str>) -> Vec<(ReasoningLevel, Triple)> {
        let mut results = Vec::new();
        
        let levels_to_search = if let Some(specific_level) = level {
            vec![specific_level]
        } else {
            self.levels.keys().cloned().collect()
        };
        
        for search_level in levels_to_search {
            if let Some(kg) = self.levels.get_mut(&search_level) {
                let level_results = kg.query_abox(subject, predicate, object);
                for triple in level_results {
                    results.push((search_level.clone(), triple));
                }
            }
        }
        
        results
    }

    /// Get certainty/confidence based on reasoning level
    pub fn get_fact_certainty(&mut self, fact: &Triple) -> f64 {
        // Higher levels might have lower certainty
        let levels: Vec<_> = self.levels.keys().cloned().collect();
        
        for level in levels {
            if let Some(kg) = self.levels.get_mut(&level) {
                let facts = kg.query_abox(None, None, None);
                if facts.contains(fact) {
                    return match level {
                        ReasoningLevel::Base => 1.0,           // Ground truth
                        ReasoningLevel::Deductive => 0.9,     // Logical deduction
                        ReasoningLevel::Abductive => 0.6,     // Hypothesis
                        ReasoningLevel::MetaReasoning => 0.4,  // Meta-level reasoning
                    };
                }
            }
        }
        0.0 // Not found
    }
}