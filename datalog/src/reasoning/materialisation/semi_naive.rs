use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{BTreeMap, HashSet};
use crate::reasoning::{convert_string_binding_to_u32, Reasoner};
use crate::reasoning::materialisation::infer_generic::{SolutionMapping, InferenceStrategy};
use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::rules::{evaluate_filters, join_premise_with_hash_join};

struct SemiNaiveStrategy {
    start_idx_for_delta: usize,
}

impl SemiNaiveStrategy {

    /// Makes use of facts inferred from last round (delta) for better efficiency
    fn find_premise_solutions(&mut self, dict: &Dictionary, rule: &Rule, all_facts: &Vec<Triple>, delta_facts: &[Triple]) -> Vec<SolutionMapping> {

        let nr_premises = rule.premise.len();
        let mut results = Vec::new();

        for i in 0..nr_premises {
            let mut current_bindings = vec![BTreeMap::new()];

            // At least one premise should be satisfied by facts derived from last round (if not, then you simply derive the same things)
            current_bindings = join_premise_with_hash_join(&rule.premise[i], &delta_facts, current_bindings, dict);

            // Join remaining premises with all facts (includes delta facts as well)
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

impl InferenceStrategy for SemiNaiveStrategy {

    fn infer_round(&mut self, dictionary: &mut Dictionary, rules: &Vec<Rule>, all_facts: &Vec<Triple>, known_facts: &HashSet<Triple>) -> HashSet<Triple> {
            // Also HashSet to prevent duplicate triples from being added this round
            let mut inferred_facts_this_round: HashSet<Triple> = HashSet::new();

            let end_idx_for_delta = all_facts.len();
            let delta_facts = &all_facts[self.start_idx_for_delta..end_idx_for_delta]; // Take derived facts from last round and use as delta
            self.start_idx_for_delta = end_idx_for_delta; // Update the pointer for the next round

            // Loop over each rule
            for rule in rules {
                // These are all bindings such that the premise is satisfied for the given rule
                let binding_sets = self.find_premise_solutions(dictionary, rule, all_facts, delta_facts);

                // For each binding that satisfies the premises of the rule, get to the conclusion and apply bindings
                for binding_set in &binding_sets {
                    if evaluate_filters(&binding_set, &rule.filters, dictionary) {
                        // Loop over each conclusion of the rule, since for the current binding,
                        // the conclusions of the rule can be inferred (because premises are met)
                        for conclusion in &rule.conclusion {
                            
                            let inferred_fact =
                                replace_variables_with_bound_values(conclusion, binding_set, dictionary);

                            if !known_facts.contains(&inferred_fact) {
                                inferred_facts_this_round.insert(inferred_fact);
                            }
                        }
                    }
                }
            }

            inferred_facts_this_round
    }
}

impl Reasoner {
    pub fn infer_new_facts_semi_naive(&mut self) -> Vec<Triple> {
        self.infer_with_strategy(SemiNaiveStrategy { start_idx_for_delta: 0 })
    }
}
