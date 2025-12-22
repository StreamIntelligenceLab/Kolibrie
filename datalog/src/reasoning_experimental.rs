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
use crate::reasoning::Reasoner;
use std::collections::{BTreeMap, HashMap, HashSet};
use shared::terms::Term;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReasoningLevel {
    Base = 0,
    Deductive = 1,
    Abductive = 2,
    MetaReasoning = 3,
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
    pub levels: BTreeMap<ReasoningLevel, Reasoner>,
    pub cross_level_rules: Vec<HierarchicalRule>,
    pub propagation_rules: Vec<HierarchicalRule>,
}

impl ReasoningHierarchy {
    pub fn new() -> Self {
        let mut levels = BTreeMap::new();
        levels.insert(ReasoningLevel::Base, Reasoner::new());
        levels.insert(ReasoningLevel::Deductive, Reasoner::new());
        levels.insert(ReasoningLevel::Abductive, Reasoner::new());
        levels.insert(ReasoningLevel::MetaReasoning, Reasoner::new());
        
        Self {
            levels,
            cross_level_rules: Vec::new(),
            propagation_rules: Vec::new(),
        }
    }

    pub fn add_fact_at_level(&mut self, level: ReasoningLevel, subject: &str, predicate: &str, object: &str) {
        if let Some(kg) = self.levels.get_mut(&level) {
            kg.add_abox_triple(subject, predicate, object);
        }
    }

    pub fn add_rule_at_level(&mut self, level: ReasoningLevel, rule: Rule, priority: u32) {
        // Add rule to the specific level's knowledge graph
        if let Some(kg) = self.levels.get_mut(&level) {
            kg.add_rule(rule.clone());
        }
        
        // For cross-level processing, rules should depend on Base level plus their own level
        let mut dependencies = vec![ReasoningLevel::Base];
        if level != ReasoningLevel::Base {
            dependencies.push(level.clone());
        }
        
        let hierarchical_rule = HierarchicalRule {
            rule,
            level: level.clone(),
            priority,
            dependencies,
        };
        self.cross_level_rules.push(hierarchical_rule);
    }

    pub fn add_cross_level_rule(&mut self, rule: HierarchicalRule) {
        self.cross_level_rules.push(rule);
    }

    pub fn hierarchical_inference(&mut self) -> BTreeMap<ReasoningLevel, Vec<Triple>> {
        let mut all_inferred = BTreeMap::new();
        
        // Process levels in dependency order
        for level in [ReasoningLevel::Base, ReasoningLevel::Deductive, 
                     ReasoningLevel::Abductive, ReasoningLevel::MetaReasoning].iter() {
            
            println!("Processing level: {:?}", level);
            
            // First, run standard inference within this level
            if let Some(kg) = self.levels.get_mut(level) {
                let inferred = kg.infer_new_facts_semi_naive();
                println!("  Standard inference produced {} facts", inferred.len());
                all_inferred.insert(level.clone(), inferred);
            }
            
            // Then apply cross-level rules that target this level
            let new_cross_level_facts = self.apply_cross_level_rules(level.clone());
            println!("  Cross-level inference produced {} facts", new_cross_level_facts.len());
            
            // Add cross-level facts to the results
            if let Some(existing) = all_inferred.get_mut(level) {
                existing.extend(new_cross_level_facts);
            } else {
                all_inferred.insert(level.clone(), new_cross_level_facts);
            }
        }
        
        all_inferred
    }

    fn apply_cross_level_rules(&mut self, target_level: ReasoningLevel) -> Vec<Triple> {
        let mut new_facts = Vec::new();
        
        // Get rules that target this level
        let applicable_rules: Vec<_> = self.cross_level_rules.iter()
            .filter(|r| r.level == target_level)
            .cloned()
            .collect();
            
        println!("    Found {} applicable cross-level rules for {:?}", 
                 applicable_rules.len(), target_level);
        
        for hierarchical_rule in applicable_rules {
            println!("    Applying rule with {} dependencies", hierarchical_rule.dependencies.len());
            
            // Collect all facts from dependency levels
            let mut all_available_facts = Vec::new();
            
            for dep_level in &hierarchical_rule.dependencies {
                if let Some(kg) = self.levels.get(dep_level) {
                    let facts = kg.index_manager.query(None, None, None);
                    println!("      Collected {} facts from {:?} level", facts.len(), dep_level);
                    all_available_facts.extend(facts);
                }
            }
            
            // Apply the rule to generate new facts
            let rule_results = self.apply_rule_to_facts(&hierarchical_rule.rule, &all_available_facts);
            println!("      Rule generated {} new facts", rule_results.len());
            
            // Add to target level immediately
            if let Some(target_kg) = self.levels.get_mut(&target_level) {
                for fact in &rule_results {
                    target_kg.index_manager.insert(fact);
                    println!("        Added fact: {:?}", fact);
                }
            }
            
            new_facts.extend(rule_results);
        }
        
        new_facts
    }

    fn apply_rule_to_facts(&mut self, rule: &Rule, facts: &[Triple]) -> Vec<Triple> {
        let mut new_facts = Vec::new();
        let mut seen_facts = HashSet::new();
        
        match rule.premise.len() {
            1 => {
                // Single premise rule
                for fact in facts {
                    let mut bindings = HashMap::new();
                    if self.matches_rule_pattern(&rule.premise[0], fact, &mut bindings) {
                        for conclusion in &rule.conclusion {
                            if let Some(new_fact) = self.construct_triple_from_pattern(conclusion, &bindings) {
                                if seen_facts.insert(new_fact.clone()) {
                                    new_facts.push(new_fact);
                                }
                            }
                        }
                    }
                }
            }
            2 => {
                // Two premise rule - try both orderings
                for (i, fact1) in facts.iter().enumerate() {
                    for (j, fact2) in facts.iter().enumerate() {
                        if i == j { continue; } // Don't match same fact twice
                        
                        let mut bindings = HashMap::new();
                        if self.matches_rule_pattern(&rule.premise[0], fact1, &mut bindings) 
                            && self.matches_rule_pattern(&rule.premise[1], fact2, &mut bindings) {
                            
                            for conclusion in &rule.conclusion {
                                if let Some(new_fact) = self.construct_triple_from_pattern(conclusion, &bindings) {
                                    if seen_facts.insert(new_fact.clone()) {
                                        new_facts.push(new_fact);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                println!("      Unsupported rule premise length: {}", rule.premise.len());
            }
        }
        
        new_facts
    }

    fn matches_rule_pattern(&self, pattern: &(Term, Term, Term), fact: &Triple, bindings: &mut HashMap<String, u32>) -> bool {
        // Match subject
        if !Self::match_term(&pattern.0, fact.subject, bindings) {
            return false;
        }
        
        // Match predicate
        if !Self::match_term(&pattern.1, fact.predicate, bindings) {
            return false;
        }
        
        // Match object
        if !Self::match_term(&pattern.2, fact.object, bindings) {
            return false;
        }
        
        true
    }

    fn match_term(pattern_term: &Term, fact_term: u32, bindings: &mut HashMap<String, u32>) -> bool {
        match pattern_term {
            Term::Variable(var_name) => {
                // Variable can bind to any value
                if let Some(&existing_binding) = bindings.get(var_name) {
                    existing_binding == fact_term
                } else {
                    bindings.insert(var_name.clone(), fact_term);
                    true
                }
            }
            Term::Constant(pattern_id) => {
                // Constant must match exactly
                *pattern_id == fact_term
            }
        }
    }

    fn construct_triple_from_pattern(&self, pattern: &(Term, Term, Term), bindings: &HashMap<String, u32>) -> Option<Triple> {
        let subject = Self::resolve_term(&pattern.0, bindings)?;
        let predicate = Self::resolve_term(&pattern.1, bindings)?;
        let object = Self::resolve_term(&pattern.2, bindings)?;
        
        Some(Triple { subject, predicate, object })
    }

    fn resolve_term(term: &Term, bindings: &HashMap<String, u32>) -> Option<u32> {
        match term {
            Term::Variable(var_name) => {
                bindings.get(var_name).copied()
            }
            Term::Constant(id) => {
                Some(*id)
            }
        }
    }

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

    pub fn get_fact_certainty(&mut self, fact: &Triple) -> f64 {
        let levels: Vec<_> = self.levels.keys().cloned().collect();
        
        for level in levels {
            if let Some(kg) = self.levels.get_mut(&level) {
                let facts = kg.query_abox(None, None, None);
                if facts.contains(fact) {
                    return match level {
                        ReasoningLevel::Base => 1.0,
                        ReasoningLevel::Deductive => 0.9,
                        ReasoningLevel::Abductive => 0.6,
                        ReasoningLevel::MetaReasoning => 0.4,
                    };
                }
            }
        }
        0.0
    }
}
