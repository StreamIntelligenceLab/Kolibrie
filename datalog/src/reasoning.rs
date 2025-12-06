/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::dictionary::Dictionary;
use shared::triple::Triple;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use shared::index_manager::*;
use shared::rule_index::RuleIndex;
use shared::terms::{Term, TriplePattern};
use shared::rule::Rule;
use shared::join_algorithm::perform_hash_join_for_rules;
use rayon::prelude::*;
use std::sync::Arc;
use shared::rule::FilterCondition;

// Logic part: Knowledge Graph

#[derive(Debug, Clone)]
pub struct Reasoner {
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // List of dynamic rules

    pub index_manager: UnifiedIndex,
    pub rule_index: RuleIndex,
    pub constraints: Vec<Rule>,
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

        self.index_manager.insert(&Triple { subject: s, predicate: p, object: o });
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

    /// Add a dynamic rule to the graph
    pub fn add_rule(&mut self, rule: Rule) {
        let rule_id = self.rules.len();
        self.rules.push(rule.clone());
        for prem in &rule.premise {
            self.rule_index.insert_premise_pattern(prem, rule_id);
        }
    }

    /// Convert rule evaluation to use the optimized hash join
    fn evaluate_rule_with_optimized_join(&self, rule: &Rule, all_facts: &Vec<Triple>) -> Vec<HashMap<String, u32>> {
        if rule.premise.is_empty() {
            return Vec::new();
        }

        let mut current_bindings = vec![BTreeMap::new()];
        
        // Process each premise using the optimized join
        for premise in &rule.premise {
            current_bindings = self.join_premise_with_hash_join(premise, all_facts, current_bindings);
            if current_bindings.is_empty() {
                break;
            }
        }

        // Convert results back to HashMap<String, u32>
        current_bindings.into_iter()
            .map(|binding| self.convert_string_binding_to_u32(&binding))
            .collect()
    }

    fn join_premise_with_hash_join(
        &self,
        premise: &TriplePattern,
        all_facts: &Vec<Triple>,
        current_bindings: Vec<BTreeMap<String, String>>,
    ) -> Vec<BTreeMap<String, String>> {
        // Extract variable names and predicate from the premise
        let (subject_var, predicate_str, object_var) = self.extract_join_parameters(premise);
        
        // Use the optimized hash join
        perform_hash_join_for_rules(
            subject_var,
            predicate_str,
            object_var,
            all_facts.clone(),
            &self.dictionary,
            current_bindings,
            None,
        )
    }

    fn extract_join_parameters(&self, premise: &TriplePattern) -> (String, String, String) {
        let (subject_term, predicate_term, object_term) = premise;
        
        let subject_var = match subject_term {
            Term::Variable(v) => v.clone(),
            Term::Constant(c) => {
                // For constants, create a synthetic variable name
                format!("__const_subj_{}", c)
            }
        };
        
        let object_var = match object_term {
            Term::Variable(v) => v.clone(),
            Term::Constant(c) => {
                // For constants, create a synthetic variable name  
                format!("__const_obj_{}", c)
            }
        };
        
        let predicate_str = match predicate_term {
            Term::Constant(c) => {
                self.dictionary.decode(*c).unwrap_or("unknown").to_string()
            }
            Term::Variable(v) => {
                format!("__var_pred_{}", v)
            }
        };
        
        (subject_var, predicate_str, object_var)
    }

    fn convert_string_binding_to_u32(&self, binding: &BTreeMap<String, String>) -> HashMap<String, u32> {
        let mut result = HashMap::new();
        for (var, value) in binding {
            if let Some(&id) = self.dictionary.string_to_id.get(value) {
                result.insert(var.clone(), id);
            }
        }
        result
    }
    
    pub fn infer_new_facts(&mut self) -> Vec<Triple> {
        let mut inferred_facts = Vec::new();
        let mut all_facts = self.index_manager.query(None, None, None);
        let mut known_facts: HashSet<Triple> = all_facts.iter().cloned().collect();

        loop {
            let mut new_facts_this_round = Vec::new();
            let rules = self.rules.clone();

            for rule in &rules {
                let bindings = self.evaluate_rule_with_optimized_join(rule, &all_facts);
                
                for binding in bindings {
                    if evaluate_filters(&binding, &rule.filters, &self.dictionary) {
                        for conclusion in &rule.conclusion {
                            let inferred = construct_triple(conclusion, &binding, &mut self.dictionary);
                            if !known_facts.contains(&inferred) {
                                known_facts.insert(inferred.clone());
                                new_facts_this_round.push(inferred.clone());
                                inferred_facts.push(inferred);
                            }
                        }
                    }
                }
            }

            if new_facts_this_round.is_empty() {
                break;
            }

            for fact in &new_facts_this_round {
                self.index_manager.insert(fact);
            }

            all_facts.extend(new_facts_this_round);
        }

        inferred_facts
    }

    pub fn infer_new_facts_semi_naive(&mut self) -> Vec<Triple> {
        let all_initial = self.index_manager.query(None, None, None);
        let mut all_facts: HashSet<Triple> = all_initial.iter().cloned().collect();
        let mut delta: Vec<Triple> = all_initial;
        let mut inferred_so_far = Vec::new();

        loop {
            let mut new_delta = HashSet::new();

            for rule in &self.rules.clone() {
                let bindings = self.evaluate_rule_with_delta(&rule, &all_facts.iter().cloned().collect(), &delta);
                
                for binding in bindings {
                    if evaluate_filters(&binding, &rule.filters, &self.dictionary) {
                        for conclusion in &rule.conclusion {
                            let inferred = construct_triple(conclusion, &binding, &mut self.dictionary);
                            if !all_facts.contains(&inferred) {
                                new_delta.insert(inferred.clone());
                                self.index_manager.insert(&inferred);
                            }
                        }
                    }
                }
            }

            if new_delta.is_empty() {
                break;
            }

            for fact in &new_delta {
                all_facts.insert(fact.clone());
                inferred_so_far.push(fact.clone());
            }
            
            delta = new_delta.iter().cloned().collect();
        }

        inferred_so_far
    }

    fn evaluate_rule_with_delta(&self, rule: &Rule, all_facts: &Vec<Triple>, delta_facts: &Vec<Triple>) -> Vec<HashMap<String, u32>> {
        let n = rule.premise.len();
        let mut results = Vec::new();

        for i in 0..n {
            let mut current_bindings = vec![BTreeMap::new()];
            
            current_bindings = self.join_premise_with_hash_join(&rule.premise[i], delta_facts, current_bindings);
            
            // Join remaining premises with all facts
            for j in 0..n {
                if j == i {
                    continue;
                }
                current_bindings = self.join_premise_with_hash_join(&rule.premise[j], all_facts, current_bindings);
                if current_bindings.is_empty() {
                    break;
                }
            }
            
            // Convert and add results
            for binding in current_bindings {
                let u32_binding = self.convert_string_binding_to_u32(&binding);
                results.push(u32_binding);
            }
        }

        results
    }

    pub fn infer_new_facts_semi_naive_parallel(&mut self) -> Vec<Triple> {
        // Collect all known facts
        let all_initial = self.index_manager.query(None, None, None);
        let mut all_facts: HashSet<Triple> = all_initial.into_iter().collect();

        // Delta = all the initial facts
        let mut delta = all_facts.clone();

        // Keep track of newly inferred facts so we can return them later
        let mut inferred_so_far = Vec::new();

        // Repeat until no new facts are inferred
        loop {
            // Wrap all_facts in an Arc for shared read-only access in parallel
            let all_facts_arc = Arc::new(all_facts.clone());
            let new_facts: HashSet<Triple> = delta
                .par_iter()
                .fold(
                    || HashSet::new(),
                    |mut local_set, triple1| {
                        // Use only the predicate for candidate rule lookup
                        let candidate_rule_ids = self.rule_index.query_candidate_rules(
                            None,
                            Some(triple1.predicate),
                            None,
                        );
                        for &rule_id in candidate_rule_ids.iter() {
                            let rule = &self.rules[rule_id];
                            match rule.premise.len() {
                                1 => {
                                    // Single-premise rule
                                    let mut variable_bindings = HashMap::new();
                                    if matches_rule_pattern(&rule.premise[0], triple1, &mut variable_bindings) {
                                        // Process each conclusion
                                        for conclusion in &rule.conclusion {
                                            let inferred = construct_triple(conclusion, &variable_bindings, &mut self.dictionary.clone());
                                            if !all_facts_arc.contains(&inferred) {
                                                local_set.insert(inferred);
                                            }
                                        }
                                    }
                                }

                                2 => {
                                    // Two-premise rule
                                    let mut variable_bindings_1 = HashMap::new();
                                    if matches_rule_pattern(&rule.premise[0], triple1, &mut variable_bindings_1) {
                                        // Process join in parallel over all_facts
                                        let local_new: HashSet<Triple> = all_facts_arc
                                            .par_iter()
                                            .flat_map(|triple2| {
                                                let mut variable_bindings_2 = variable_bindings_1.clone();
                                                if matches_rule_pattern(&rule.premise[1], triple2, &mut variable_bindings_2) {
                                                    // Process each conclusion
                                                    rule.conclusion.iter()
                                                        .filter_map(|conclusion| {
                                                            let inferred = construct_triple(conclusion, &variable_bindings_2, &mut self.dictionary.clone());
                                                            if !all_facts_arc.contains(&inferred) {
                                                                Some(inferred)
                                                            } else {
                                                                None
                                                            }
                                                        })
                                                        .collect::<Vec<_>>()
                                                } else {
                                                    Vec::new()
                                                }
                                            })
                                            .collect();
                                        local_set.extend(local_new);
                                    }

                                    // Option 2: Assume triple1 matches the second premise
                                    let mut variable_bindings_1b = HashMap::new();
                                    if matches_rule_pattern(&rule.premise[1], triple1, &mut variable_bindings_1b) {
                                        let local_new: HashSet<Triple> = all_facts_arc
                                            .par_iter()
                                            .flat_map(|triple2| {
                                                let mut variable_bindings_2b = variable_bindings_1b.clone();
                                                if matches_rule_pattern(&rule.premise[0], triple2, &mut variable_bindings_2b) {
                                                    // Process each conclusion
                                                    rule.conclusion.iter()
                                                        .filter_map(|conclusion| {
                                                            let inferred = construct_triple(conclusion, &variable_bindings_2b, &mut self.dictionary.clone());
                                                            if !all_facts_arc.contains(&inferred) {
                                                                Some(inferred)
                                                            } else {
                                                                None
                                                            }
                                                        })
                                                        .collect::<Vec<_>>()
                                                } else {
                                                    Vec::new()
                                                }
                                            })
                                            .collect();
                                        local_set.extend(local_new);
                                    }
                                }

                                _ => {}
                            }
                        }
                        local_set
                    },
                )
                .reduce(
                    || HashSet::new(),
                    |mut acc, local_set| {
                        acc.extend(local_set);
                        acc
                    },
                );

            // If no new facts were found, we've reached a fixpoint
            if new_facts.is_empty() {
                break;
            } else {
                for fact in new_facts.iter() {
                    all_facts.insert(fact.clone());
                    inferred_so_far.push(fact.clone());
                    self.index_manager.insert(fact);
                }
                delta = new_facts;
            }
        }

        inferred_so_far
    }

    pub fn backward_chaining(&self, query: &TriplePattern) -> Vec<HashMap<String, Term>> {
        let bindings = HashMap::new();
        let mut variable_counter = 0;
        self.backward_chaining_helper(query, &bindings, 0, &mut variable_counter)
    }

    fn backward_chaining_helper(
        &self,
        query: &TriplePattern,
        bindings: &HashMap<String, Term>,
        depth: usize,
        variable_counter: &mut usize,
    ) -> Vec<HashMap<String, Term>> {
        const MAX_DEPTH: usize = 10;
        if depth > MAX_DEPTH {
            return Vec::new();
        }

        let substituted = substitute(query, bindings);

        let mut results = Vec::new();
        // Get facts from the index manager
        let all_facts: Vec<Triple> = self.index_manager.query(None, None, None);

        for fact in &all_facts {
            let fact_pattern = triple_to_pattern(fact);
            if let Some(new_bindings) = unify_patterns(&substituted, &fact_pattern, bindings) {
                results.push(new_bindings);
            }
        }

        // match with rules
        for rule in &self.rules {
            let renamed_rule = rename_rule_variables(rule, variable_counter);

            // Try to unify with each conclusion in the rule
            for conclusion in &renamed_rule.conclusion {
                if let Some(rb) = unify_patterns(conclusion, &substituted, bindings) {
                    // We have a match => we need all premises to succeed
                    let mut premise_results = vec![rb.clone()];
                    for prem in &renamed_rule.premise {
                        let mut new_premise_results = Vec::new();
                        for b in &premise_results {
                            let sub_res = self.backward_chaining_helper(prem, b, depth + 1, variable_counter);
                            new_premise_results.extend(sub_res);
                        }
                        premise_results = new_premise_results;
                    }
                    results.extend(premise_results);
                }
            }
        }

        results
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
        let mut seen = BTreeSet::new();  // Using BTreeSet instead of HashSet

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

    /// Modified infer_new_facts_semi_naive to handle inconsistencies
    pub fn infer_new_facts_semi_naive_with_repairs(&mut self) -> Vec<Triple> {
        let all_initial = self.index_manager.query(None, None, None);
        let mut all_facts: HashSet<Triple> = all_initial.into_iter().collect();
        
        // First, check if initial facts are consistent
        if self.violates_constraints(&all_facts) {
            let repairs = self.compute_repairs(&all_facts);
            if let Some(best_repair) = repairs.into_iter().max_by_key(|r| r.len()) {
                // Clear index manager and reinsert repaired facts
                self.index_manager = UnifiedIndex::new();
                for fact in &best_repair {
                    self.index_manager.insert(fact);
                }
                all_facts = best_repair;
            }
        }

        let mut delta = all_facts.clone();
        let mut inferred_so_far = Vec::new();

        loop {
            let mut new_delta = HashSet::new();
            
            // Process each rule using the semi-naive approach
            for rule in &self.rules {
                let bindings = join_rule(rule, &all_facts, &delta);
                for binding in bindings {
                    if evaluate_filters(&binding, &rule.filters, &self.dictionary) {
                        // Process each conclusion
                        for conclusion in &rule.conclusion {
                            let inferred = construct_triple(conclusion, &binding, &mut self.dictionary);
                            
                            // Check if adding this fact would cause inconsistency
                            let mut temp_facts = all_facts.clone();
                            temp_facts.insert(inferred.clone());
                            
                            if !self.violates_constraints(&temp_facts) {
                                if self.index_manager.insert(&inferred) && !all_facts.contains(&inferred) {
                                    new_delta.insert(inferred.clone());
                                    all_facts.insert(inferred.clone());
                                    inferred_so_far.push(inferred);
                                }
                            }
                        }
                    }
                }
            }

            // Terminate when no new facts were inferred
            if new_delta.is_empty() {
                break;
            }
            
            delta = new_delta;
        }

        inferred_so_far
    }

    /// New method: Query with inconsistency-tolerant semantics
    pub fn query_with_repairs(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>> {
        let all_facts: HashSet<Triple> = self.index_manager.query(None, None, None)
            .into_iter()
            .collect();

        // Compute all repairs
        let repairs = self.compute_repairs(&all_facts);
        
        // IAR semantics: only return answers that are present in all repairs
        let mut results = Vec::new();
        if let Some(first_repair) = repairs.first() {
            // Start with results from first repair
            for fact in first_repair {
                let mut vmap = HashMap::new();
                if matches_rule_pattern(query, fact, &mut vmap) {
                    results.push(vmap);
                }
            }

            // Filter out results not present in all repairs
            results.retain(|binding| {
                repairs.iter().skip(1).all(|repair| {
                    repair.iter().any(|fact| {
                        let mut test_map = HashMap::new();
                        matches_rule_pattern(query, fact, &mut test_map) && test_map == *binding
                    })
                })
            });
        }
        
        results
    }            
}

fn unify_patterns(
    pattern1: &TriplePattern,
    pattern2: &TriplePattern,
    bindings: &HashMap<String, Term>,
) -> Option<HashMap<String, Term>> {
    let mut new_bindings = bindings.clone();

    if !unify_terms(&pattern1.0, &pattern2.0, &mut new_bindings) {
        return None;
    }
    if !unify_terms(&pattern1.1, &pattern2.1, &mut new_bindings) {
        return None;
    }
    if !unify_terms(&pattern1.2, &pattern2.2, &mut new_bindings) {
        return None;
    }

    Some(new_bindings)
}

fn unify_terms(term1: &Term, term2: &Term, bindings: &mut HashMap<String, Term>) -> bool {
    let term1 = resolve_term(term1, bindings);
    let term2 = resolve_term(term2, bindings);

    match (&term1, &term2) {
        (Term::Constant(c1), Term::Constant(c2)) => c1 == c2,
        (Term::Variable(v), Term::Constant(c)) | (Term::Constant(c), Term::Variable(v)) => {
            bindings.insert(v.clone(), Term::Constant(*c));
            true
        }
        (Term::Variable(v1), Term::Variable(v2)) => {
            if v1 != v2 {
                bindings.insert(v1.clone(), Term::Variable(v2.clone()));
            }
            true
        }
    }
}

pub fn resolve_term<'a>(term: &'a Term, bindings: &'a HashMap<String, Term>) -> Term {
    match term {
        Term::Variable(v) => {
            if let Some(bound_term) = bindings.get(v) {
                resolve_term(bound_term, bindings)
            } else {
                term.clone()
            }
        }
        _ => term.clone(),
    }
}

fn substitute(pattern: &TriplePattern, bindings: &HashMap<String, Term>) -> TriplePattern {
    let s = substitute_term(&pattern.0, bindings);
    let p = substitute_term(&pattern.1, bindings);
    let o = substitute_term(&pattern.2, bindings);
    (s, p, o)
}

fn substitute_term(term: &Term, bindings: &HashMap<String, Term>) -> Term {
    match term {
        Term::Variable(var_name) => {
            if let Some(bound_term) = bindings.get(var_name) {
                substitute_term(bound_term, bindings)
            } else {
                Term::Variable(var_name.clone())
            }
        }
        Term::Constant(value) => Term::Constant(*value),
    }
}

fn triple_to_pattern(triple: &Triple) -> TriplePattern {
    (
        Term::Constant(triple.subject),
        Term::Constant(triple.predicate),
        Term::Constant(triple.object),
    )
}

fn rename_rule_variables(rule: &Rule, counter: &mut usize) -> Rule {
    let mut var_map = HashMap::new();

    fn rename_term(
        term: &Term,
        var_map: &mut HashMap<String, String>,
        counter: &mut usize,
    ) -> Term {
        match term {
            Term::Variable(v) => {
                if let Some(new_v) = var_map.get(v) {
                    Term::Variable(new_v.clone())
                } else {
                    let new_v = format!("v{}", *counter);
                    *counter += 1;
                    var_map.insert(v.clone(), new_v.clone());
                    Term::Variable(new_v)
                }
            }
            Term::Constant(c) => Term::Constant(*c),
        }
    }

    let mut new_premise = Vec::new();
    for p in &rule.premise {
        let s = rename_term(&p.0, &mut var_map, counter);
        let p_term = rename_term(&p.1, &mut var_map, counter);
        let o = rename_term(&p.2, &mut var_map, counter);
        new_premise.push((s, p_term, o));
    }

    // Rename all conclusions
    let mut new_conclusions = Vec::new();
    for conclusion in &rule.conclusion {
        let conclusion_s = rename_term(&conclusion.0, &mut var_map, counter);
        let conclusion_p = rename_term(&conclusion.1, &mut var_map, counter);
        let conclusion_o = rename_term(&conclusion.2, &mut var_map, counter);
        new_conclusions.push((conclusion_s, conclusion_p, conclusion_o));
    }

    Rule {
        premise: new_premise,
        conclusion: new_conclusions,
        filters: rule.filters.clone(),
    }
}

/// Construct a new Triple from a conclusion pattern and bound variables
pub fn construct_triple(
    conclusion: &TriplePattern, 
    vars: &HashMap<String, u32>, 
    dict: &mut Dictionary
) -> Triple {
    let subject = match &conclusion.0 {
        Term::Variable(v) => {
            vars.get(v).copied().unwrap_or_else(|| {
                eprintln!("Warning: Variable '{}' not found in bindings. Available variables: {:?}", v, vars.keys().collect::<Vec<_>>());
                0
            })
        },
        Term::Constant(c) => *c,
    };
    
    let predicate = match &conclusion.1 {
        Term::Variable(v) => {
            vars.get(v).copied().unwrap_or_else(|| {
                eprintln!("Warning: Variable '{}' not found in bindings. Available variables: {:?}", v, vars.keys().collect::<Vec<_>>());
                0
            })
        },
        Term::Constant(c) => *c,
    };

    let object = match &conclusion.2 {
        Term::Variable(v) => {
            // Check if this variable is bound in the current context
            if let Some(&bound_value) = vars.get(v) {
                bound_value
            } else {
                // If not bound, create a new placeholder in the dictionary
                dict.encode(&format!("ml_output_placeholder_{}", v))
            }
        },
        Term::Constant(c) => *c,
    };

    Triple {
        subject,
        predicate,
        object,
    }
}

pub fn matches_rule_pattern(
    pattern: &TriplePattern,
    fact: &Triple,
    variable_bindings: &mut HashMap<String, u32>,
) -> bool {
    // Create a copy of bindings to test against (rollback on failure)
    let mut temp_bindings = variable_bindings.clone();
    
    // Subject
    let s_ok = match &pattern.0 {
        Term::Variable(v) => {
            if let Some(&bound) = temp_bindings.get(v) {
                bound == fact.subject
            } else {
                temp_bindings.insert(v.clone(), fact.subject);
                true
            }
        }
        Term::Constant(c) => *c == fact.subject,
    };
    if !s_ok {
        return false; // Don't modify original bindings on failure
    }

    // Predicate
    let p_ok = match &pattern.1 {
        Term::Variable(v) => {
            if let Some(&bound) = temp_bindings.get(v) {
                bound == fact.predicate
            } else {
                temp_bindings.insert(v.clone(), fact.predicate);
                true
            }
        }
        Term::Constant(c) => *c == fact.predicate,
    };
    if !p_ok {
        return false; // Don't modify original bindings on failure
    }

    // Object
    let o_ok = match &pattern.2 {
        Term::Variable(v) => {
            if let Some(&bound) = temp_bindings.get(v) {
                bound == fact.object
            } else {
                temp_bindings.insert(v.clone(), fact.object);
                true
            }
        }
        Term::Constant(c) => *c == fact.object,
    };

    // Only if ALL parts match, commit the bindings
    if s_ok && p_ok && o_ok {
        *variable_bindings = temp_bindings;
        true
    } else {
        false
    }
}

fn evaluate_filters(
    bindings: &HashMap<String, u32>, 
    filters: &Vec<FilterCondition>, 
    dict: &Dictionary
) -> bool {
    for filter in filters {
        if let Some(&value_code) = bindings.get(&filter.variable) {
            let value_str = dict.decode(value_code).unwrap_or("");
            // Try to parse both the bound value and the filter's value as numbers.
            let bound_num: f64 = value_str.parse().unwrap_or(0.0);
            let filter_num: f64 = filter.value.parse().unwrap_or(0.0);
            match filter.operator.as_str() {
                ">" if bound_num <= filter_num => return false,
                "<" if bound_num >= filter_num => return false,
                ">=" if bound_num < filter_num => return false,
                "<=" if bound_num > filter_num => return false,
                "=" if (bound_num - filter_num).abs() > std::f64::EPSILON => return false,
                "!=" if (bound_num - filter_num).abs() <= std::f64::EPSILON => return false,
                _ => {}
            }
        }
    }
    true
}

/// Given a rule, a set of all facts, and a set of "changed" facts (delta)
fn join_rule(
    rule: &Rule,
    all_facts: &HashSet<Triple>,
    delta: &HashSet<Triple>,
) -> Vec<HashMap<String, u32>> {
    let n = rule.premise.len();
    let mut results = Vec::new();

    // For each premise position i
    for i in 0..n {
        // For each fact in the delta that might "fire" the rule on this premise
        for fact in delta.iter() {
            let mut binding = HashMap::new();
            // NOTE: For a rule with one premise, use index 0 (not 1)
            if matches_rule_pattern(&rule.premise[i], fact, &mut binding) {
                // Now join with the remaining premises (all j ≠ i)
                let joined = join_remaining(rule, i, all_facts, binding);
                results.extend(joined);
            }
        }
    }
    results
}

/// Given a rule, a set of all facts, and a binding that matches some premise
fn join_remaining(
    rule: &Rule,
    changed_idx: usize,
    all_facts: &HashSet<Triple>,
    binding: HashMap<String, u32>,
) -> Vec<HashMap<String, u32>> {
    let mut results = vec![binding];
    let n = rule.premise.len();

    // For each other premise j (order can be arbitrary)
    for j in 0..n {
        if j == changed_idx {
            continue;
        }
        let mut new_results = Vec::new();
        // For every binding so far
        for partial_binding in results.into_iter() {
            // And for every fact in all_facts
            for fact in all_facts.iter() {
                let mut b = partial_binding.clone();
                if matches_rule_pattern(&rule.premise[j], fact, &mut b) {
                    new_results.push(b);
                }
            }
        }
        results = new_results;
        if results.is_empty() {
            break;
        }
    }
    results
}
