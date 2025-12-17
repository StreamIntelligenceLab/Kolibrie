use shared::dictionary::Dictionary;
use shared::join_algorithm::perform_hash_join_for_rules;
use shared::rule::{FilterCondition, Rule};
use shared::terms::{Term, TriplePattern, TriplePatternStrings};
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap, HashSet};
use crate::reasoning::Reasoner;

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

pub fn evaluate_filters(
    bindings: &HashMap<String, u32>,
    filters: &Vec<FilterCondition>,
    dict: &Dictionary,
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

/// Extracts parameters
/// current_bindings: the hashtable that is used
pub fn join_premise_with_hash_join(
    premise: &TriplePattern,
    all_facts: &[Triple],
    current_bindings: Vec<BTreeMap<String, String>>,
    dict: &Dictionary
) -> Vec<BTreeMap<String, String>> {
    // Extract variable names and predicate from the premise
    let join_params = extract_join_parameters(premise, dict);
    perform_hash_join_for_rules(
        join_params,
        all_facts,
        &dict,
        current_bindings,
        None,
    )
}

fn extract_join_parameters(premise: &TriplePattern, dict: &Dictionary) -> TriplePatternStrings {
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
        Term::Constant(c) => dict.decode(*c).unwrap_or("unknown").to_string(),
        Term::Variable(v) => {
            format!("__var_pred_{}", v)
        }
    };

    TriplePatternStrings {
        subject: subject_var,
        predicate: predicate_str,
        object: object_var,
    }
}

impl Reasoner {
    /// Add a dynamic rule to the graph
    pub fn add_rule(&mut self, rule: Rule) {
        let rule_id = self.rules.len();
        self.rules.push(rule.clone());
        for prem in &rule.premise {
            self.rule_index.insert_premise_pattern(prem, rule_id);
        }
    }
}
