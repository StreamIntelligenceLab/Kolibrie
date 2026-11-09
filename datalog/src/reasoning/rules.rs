//! Matching rule premises against facts, and joining the bindings that result

use crate::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::join_algorithm::perform_hash_join_for_rules;
pub use shared::pattern::{construct_triple, matches_rule_pattern};
use shared::rule::{FilterCondition, Rule};
use shared::terms::{Bindings, TriplePattern};
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Given a rule, a set of all facts, and a set of "changed" facts (delta)
pub fn join_rule(rule: &Rule, all_facts: &HashSet<Triple>, delta: &HashSet<Triple>) -> Bindings {
    let n = rule.premise.len();
    let mut results = Bindings::empty();

    for i in 0..n {
        for fact in delta.iter() {
            let mut binding = HashMap::new();
            if matches_rule_pattern(&rule.premise[i], fact, &mut binding) {
                let joined = join_remaining(rule, i, all_facts, binding);
                results.extend(joined);
            }
        }
    }
    results
}

/// Given a rule, a set of all facts, and a binding that matches some premise
pub fn join_remaining(
    rule: &Rule,
    changed_idx: usize,
    all_facts: &HashSet<Triple>,
    binding: HashMap<String, u32>,
) -> Bindings {
    let mut results = Bindings::from(vec![binding]);
    let n = rule.premise.len();

    for j in 0..n {
        if j == changed_idx {
            continue;
        }
        let mut new_results = Bindings::empty();
        for partial_binding in results.into_iter() {
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

/// Tests a candidate binding against a rule's filters
pub fn evaluate_filters(
    bindings: &HashMap<String, u32>,
    filters: &Vec<FilterCondition>,
    dict: &Dictionary,
) -> bool {
    for filter in filters {
        if let Some(&lhs_code) = bindings.get(&filter.variable) {
            if let Some(&rhs_code) = bindings.get(&filter.value) {
                match filter.operator.as_str() {
                    "!=" if lhs_code == rhs_code => return false,
                    "=" if lhs_code != rhs_code => return false,
                    _ => {}
                }
            } else {
                let value_str = dict.decode(lhs_code).unwrap_or("");
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
    }
    true
}

/// Extends each binding with the ways one premise can match the facts
pub fn join_premise_with_hash_join(
    premise: &TriplePattern,
    all_facts: &[Triple],
    current_bindings: Vec<BTreeMap<String, String>>,
    dict: &Dictionary,
) -> Vec<BTreeMap<String, String>> {
    perform_hash_join_for_rules(premise, all_facts, &dict, current_bindings)
}

impl Reasoner {
    /// Add a dynamic rule to the graph
    pub fn add_rule(&mut self, rule: Rule) {
        self.try_add_rule(rule).expect("rule safety check failed");
    }

    /// Add a dynamic rule to the graph, returning `Err` if it violates safety
    pub fn try_add_rule(&mut self, rule: Rule) -> Result<(), String> {
        shared::rule::check_rule_safety(&rule)?;
        let rule_id = self.rules.len();
        self.rules.push(rule.clone());
        for prem in &rule.premise {
            self.rule_index.insert_premise_pattern(prem, rule_id);
        }
        Ok(())
    }
}
