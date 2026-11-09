//! Semi-naive forward chaining: each round joins against only what the last one derived

use crate::reasoning::materialisation::infer_generic::{InferenceStrategy, SolutionMapping};
use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::rules::{evaluate_filters, join_premise_with_hash_join};
use crate::reasoning::{convert_string_binding_to_u32, Reasoner};
use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{BTreeMap, HashSet};

struct SemiNaiveStrategy {
    start_idx_for_delta: usize,
}

impl SemiNaiveStrategy {
    /// Makes use of facts inferred from last round (delta) for better efficiency
    fn find_premise_solutions(
        &mut self,
        dict: &Dictionary,
        rule: &Rule,
        all_facts: &Vec<Triple>,
        delta_facts: &[Triple],
    ) -> Vec<SolutionMapping> {
        let nr_premises = rule.premise.len();
        let mut results = Vec::new();

        for i in 0..nr_premises {
            let mut current_bindings = vec![BTreeMap::new()];

            current_bindings =
                join_premise_with_hash_join(&rule.premise[i], &delta_facts, current_bindings, dict);

            for j in 0..nr_premises {
                if j == i {
                    continue;
                }
                current_bindings = join_premise_with_hash_join(
                    &rule.premise[j],
                    &all_facts,
                    current_bindings,
                    dict,
                );
                if current_bindings.is_empty() {
                    break;
                }
            }

            for binding in current_bindings {
                let u32_binding = convert_string_binding_to_u32(&binding, dict);
                results.push(u32_binding);
            }
        }

        results
    }
}

impl InferenceStrategy for SemiNaiveStrategy {
    fn infer_round(
        &mut self,
        dictionary: &mut Dictionary,
        rules: &Vec<Rule>,
        all_facts: &Vec<Triple>,
        known_facts: &HashSet<Triple>,
    ) -> HashSet<Triple> {
        let mut inferred_facts_this_round: HashSet<Triple> = HashSet::new();

        let end_idx_for_delta = all_facts.len();
        let delta_facts = &all_facts[self.start_idx_for_delta..end_idx_for_delta];
        self.start_idx_for_delta = end_idx_for_delta;

        for rule in rules {
            let binding_sets =
                self.find_premise_solutions(dictionary, rule, all_facts, delta_facts);

            for binding_set in &binding_sets {
                if evaluate_filters(&binding_set, &rule.filters, dictionary) {
                    for conclusion in &rule.conclusion {
                        let inferred_fact = replace_variables_with_bound_values(
                            conclusion,
                            binding_set,
                            dictionary,
                        );

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
    /// Derives every entailed fact by semi-naive forward chaining and returns the new ones
    pub fn infer_new_facts_semi_naive(&mut self) -> Vec<Triple> {
        self.infer_with_strategy(SemiNaiveStrategy {
            start_idx_for_delta: 0,
        })
    }

    /// As [`infer_new_facts_semi_naive`](Self::infer_new_facts_semi_naive), reading base facts from
    /// `base`
    pub fn infer_new_facts_semi_naive_over(
        &mut self,
        base: &dyn shared::quad_source::QuadSource,
    ) -> Vec<Triple> {
        self.infer_with_strategy_over(
            Some(base),
            SemiNaiveStrategy {
                start_idx_for_delta: 0,
            },
        )
    }
}
