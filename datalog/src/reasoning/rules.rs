//! Matching rule premises against facts, and joining the bindings that result.
//!
//! This is the inner loop every forward-chaining strategy shares. Premises are
//! joined one at a time, left to right, through
//! [`shared::join_algorithm::perform_hash_join_for_rules`]; an empty intermediate
//! result ends the rule early.
//!
//! Rule filters are not SPARQL filters: see [`evaluate_filters`] for where the two
//! semantics part company.

use crate::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::join_algorithm::perform_hash_join_for_rules;
// Moved to `shared` in step 6: these are vocabulary operations, and they were the
// only thing `datalogmtl` needed from this crate. Re-exported below so
// `datalog::reasoning::{matches_rule_pattern, construct_triple}` keeps working.
pub use shared::pattern::{construct_triple, matches_rule_pattern};
use shared::rule::{FilterCondition, Rule};
use shared::terms::TriplePattern;
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Given a rule, a set of all facts, and a set of "changed" facts (delta)
pub fn join_rule(
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
pub fn join_remaining(
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

/// Tests a candidate binding against a rule's filters.
///
/// # Not SPARQL FILTER semantics
///
/// Two deliberate differences from the SPARQL evaluation in `kolibrie`, both of
/// which make a filter here more permissive:
///
/// - A filter on a variable the binding does not contain is **skipped**, not
///   failed. In SPARQL an unbound variable makes the filter error and the row is
///   dropped.
/// - A comparison against a non-numeric value parses as `0.0` rather than
///   erroring, so `?x > 5` on the value `"abc"` is false while `?x < 5` is true.
///
/// A rule relying on either behaviour will not translate to a SPARQL FILTER
/// unchanged.
pub fn evaluate_filters(
    bindings: &HashMap<String, u32>,
    filters: &Vec<FilterCondition>,
    dict: &Dictionary,
) -> bool {
    for filter in filters {
        if let Some(&lhs_code) = bindings.get(&filter.variable) {
            // If the filter value is itself a bound variable, compare by dictionary ID
            if let Some(&rhs_code) = bindings.get(&filter.value) {
                match filter.operator.as_str() {
                    "!=" if lhs_code == rhs_code => return false,
                    "=" if lhs_code != rhs_code => return false,
                    _ => {}
                }
            } else {
                // Compare bound variable against a numeric constant
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

/// Extends each binding with the ways one premise can match the facts.
///
/// Returns the empty vector when the premise matches nothing, which the callers
/// treat as the rule having failed and stop early rather than continuing with
/// nothing to join against.
pub fn join_premise_with_hash_join(
    premise: &TriplePattern,
    all_facts: &[Triple],
    current_bindings: Vec<BTreeMap<String, String>>,
    dict: &Dictionary,
) -> Vec<BTreeMap<String, String>> {
    perform_hash_join_for_rules(premise, all_facts, &dict, current_bindings)
}

impl Reasoner {
    /// Add a dynamic rule to the graph.
    ///
    /// Panics if the rule has unsafe negation (a variable in `negative_premise`
    /// that is not bound by `premise`). For a non-panicking version use [`try_add_rule`].
    ///
    /// [`try_add_rule`]: Self::try_add_rule
    pub fn add_rule(&mut self, rule: Rule) {
        self.try_add_rule(rule).expect("rule safety check failed");
    }

    /// Add a dynamic rule to the graph, returning `Err` if it violates safety.
    ///
    /// Safety requirement: every variable in `negative_premise` must appear in `premise`.
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
