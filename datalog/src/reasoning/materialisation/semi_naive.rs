use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap};
use crate::reasoning::{convert_string_binding_to_u32, Reasoner};
use crate::reasoning::materialisation::infer_generic::InferenceStrategy;
use crate::reasoning::rules::join_premise_with_hash_join;

struct SemiNaiveStrategy {
    start_idx_for_delta: usize,
}

impl InferenceStrategy for SemiNaiveStrategy {
    /// Evaluates rule by making use of delta
    fn evaluate_rule(&mut self, dict: &Dictionary, rule: &Rule, all_facts: &Vec<Triple>) -> Vec<HashMap<String, u32>> {
        let end_idx_for_delta = all_facts.len();
        let delta_facts = &all_facts[self.start_idx_for_delta..end_idx_for_delta]; // Take derived facts from last round and use as delta
        self.start_idx_for_delta = end_idx_for_delta; // Update the pointer for the next round

        let nr_premises = rule.premise.len();
        let mut results = Vec::new();

        for i in 0..nr_premises {
            let mut current_bindings = vec![BTreeMap::new()];

            current_bindings = join_premise_with_hash_join(&rule.premise[i], &delta_facts, current_bindings, dict);

            // Join remaining premises with all facts
            for j in 0..nr_premises {
                if j == i {
                    continue;
                }
                current_bindings = join_premise_with_hash_join(&rule.premise[j], &all_facts, current_bindings, dict);
                if current_bindings.is_empty() {
                    break;
                }
            }

            // Convert and add results
            for binding in current_bindings {
                let u32_binding = convert_string_binding_to_u32(&binding, dict);
                results.push(u32_binding);
            }
        }

        results
    }
}

impl Reasoner {
    pub fn infer_new_facts_semi_naive(&mut self) -> Vec<Triple> {
        self.infer_with_strategy(SemiNaiveStrategy { start_idx_for_delta: 0 })
    }
}
