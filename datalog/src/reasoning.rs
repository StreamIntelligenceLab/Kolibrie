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
use shared::rule::FilterCondition;
use shared::terms::Term;
use shared::terms::TriplePattern;
use shared::triple::Triple;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use shared::index_manager::TripleIndex;
use shared::rule_index::RuleIndex;
use shared::rule::Rule;
use shared::provenance::Provenance;
use shared::tag_store::TagStore;
use std::sync::Arc;
use std::sync::RwLock;
use crate::reasoning::rules::join_rule;

// Logic part: Knowledge Graph

#[derive(Debug, Clone)]
// Are there RDF connections here or not?
pub struct Reasoner {
    pub dictionary: Arc<RwLock<Dictionary>>,
    pub rules: Vec<Rule>, // List of dynamic rules
    pub index_manager: Box<dyn TripleIndex>,
    pub rule_index: RuleIndex,
    pub constraints: Vec<Rule>,
    pub probability_seeds: HashMap<Triple, f64>, // Input probabilities for provenance seeding
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
        Self::with_index(Box::new(shared::index_manager::HexastoreIndex::new()))
    }

    pub fn with_index(index: Box<dyn TripleIndex>) -> Self {
        Self {
            dictionary: Arc::new(RwLock::new(Dictionary::new())),
            rules: Vec::new(),
            index_manager: index,
            rule_index: RuleIndex::new(),
            constraints: Vec::new(),
            probability_seeds: HashMap::new(),
        }
    }

    /// Add a triple with an associated probability value.
    /// The triple is added to the index; the probability is stored for provenance seeding.
    pub fn add_tagged_triple(&mut self, subject: &str, predicate: &str, object: &str, probability: f64) {
        let mut dict = self.dictionary.write().unwrap();
        let s = dict.encode(subject);
        let p = dict.encode(predicate);
        let o = dict.encode(object);
        drop(dict);

        let triple = Triple { subject: s, predicate: p, object: o };
        self.index_manager.insert(&triple);
        self.probability_seeds.insert(triple, probability);
    }

    /// Materialize provenance tags as RDF-star triples so they are queryable via SPARQL-star.
    /// Generates triples of the form: << s p o >> <prob:value> "0.7"^^xsd:double
    pub fn materialize_tags_as_rdf_star<P: Provenance>(&mut self, tag_store: &TagStore<P>) {
        let mut dict = self.dictionary.write().unwrap();
        let mut qt_store = shared::quoted_triple_store::QuotedTripleStore::new();
        let rdf_star_triples = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
        drop(dict);

        for triple in &rdf_star_triples {
            self.index_manager.insert(triple);
        }
    }

    /// Add an ABox triple (instance-level information)
    pub fn add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let mut dict = self.dictionary.write().unwrap();
        let s = dict.encode(subject);
        let p = dict.encode(predicate);
        let o = dict.encode(object);
        drop(dict);  // Release lock early

        self.index_manager.insert(&Triple {
            subject: s,
            predicate: p,
            object: o,
        });
    }

    /// Insert an already-ground triple directly into the fact index.
    pub fn insert_ground_triple(&mut self, triple: Triple) {
        self.index_manager.insert(&triple);
    }

    /// Query the ABox for instance-level assertions (using TrieIndex now)
    pub fn query_abox(
        &mut self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> Vec<Triple> {
        let mut dict = self.dictionary.write().unwrap();
        let s = subject.map(|s| dict.encode(s));
        let p = predicate.map(|p| dict.encode(p));
        let o = object.map(|o| dict.encode(o));
        drop(dict);  // Release lock early

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
        (Term::Variable(_), Term::QuotedTriple(_)) => todo!(),
        (Term::Constant(_), Term::QuotedTriple(_)) => todo!(),
        (Term::QuotedTriple(_), Term::Variable(_)) => todo!(),
        (Term::QuotedTriple(_), Term::Constant(_)) => todo!(),
        (Term::QuotedTriple(_), Term::QuotedTriple(_)) => todo!(),
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
        Term::QuotedTriple(_) => todo!(),
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
            Term::QuotedTriple(_) => todo!(),
        }
    }

    let mut new_premise = Vec::new();
    for p in &rule.premise {
        let s = rename_term(&p.0, &mut var_map, counter);
        let p_term = rename_term(&p.1, &mut var_map, counter);
        let o = rename_term(&p.2, &mut var_map, counter);
        new_premise.push((s, p_term, o));
    }

    let mut new_negative_premise = Vec::new();
    for p in &rule.negative_premise {
        let s = rename_term(&p.0, &mut var_map, counter);
        let p_term = rename_term(&p.1, &mut var_map, counter);
        let o = rename_term(&p.2, &mut var_map, counter);
        new_negative_premise.push((s, p_term, o));
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
        negative_premise: new_negative_premise,
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
        Term::QuotedTriple(_) => todo!(),
    };
    
    let predicate = match &conclusion.1 {
        Term::Variable(v) => {
            vars.get(v).copied().unwrap_or_else(|| {
                eprintln!("Warning: Variable '{}' not found in bindings. Available variables: {:?}", v, vars.keys().collect::<Vec<_>>());
                0
            })
        },
        Term::Constant(c) => *c,
        Term::QuotedTriple(_) => todo!(),
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
        Term::QuotedTriple(_) => todo!(),
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
        Term::QuotedTriple(_) => todo!(),
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
        Term::QuotedTriple(_) => todo!(),
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
        Term::QuotedTriple(_) => todo!(),
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
