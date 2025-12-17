use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap, HashSet};
use crate::reasoning::{convert_string_binding_to_u32, Reasoner};
use crate::reasoning::materialisation::infer_generic::InferenceStrategy;
use crate::reasoning::rules::join_premise_with_hash_join;

pub struct NaiveStrategy;

impl InferenceStrategy for NaiveStrategy {
    /// Convert rule evaluation to use the optimized hash join
    /// Optimised hash-join: return early when build map is empty (see for loop)
    fn evaluate_rule(&mut self, dict: &Dictionary, rule: &Rule, all_facts: &Vec<Triple>) -> Vec<HashMap<String, u32>> {
        if rule.premise.is_empty() {
            return Vec::new();
        }

        // Initialise current bindings to an empty vector
        let mut current_bindings = vec![BTreeMap::new()];

        // Process each premise with the build hash-table using the optimized join
        for premise in &rule.premise {
            current_bindings = join_premise_with_hash_join(premise, all_facts, current_bindings, dict);
            if current_bindings.is_empty() {
                break;
            }
        }

        // Convert results back to HashMap<String, u32>
        current_bindings
            .into_iter()
            .map(|binding| convert_string_binding_to_u32(&binding, dict))
            .collect()
    }
}

impl Reasoner {
    pub fn infer_new_facts_naive(&mut self) -> Vec<Triple> {
        self.infer_with_strategy(NaiveStrategy)
    }

    /// For backward compatibility
    pub fn infer_new_facts(&mut self) -> Vec<Triple> {
        self.infer_new_facts_naive()
    }
}
