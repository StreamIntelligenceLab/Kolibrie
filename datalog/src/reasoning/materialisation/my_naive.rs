//! Naive forward chaining: every rule against every fact, every round

use crate::reasoning::materialisation::infer_generic::{InferenceStrategy, SolutionMapping};
use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::rules::{evaluate_filters, join_premise_with_hash_join};
use crate::reasoning::{convert_string_binding_to_u32, Reasoner};
use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{BTreeMap, HashSet};

pub(crate) struct NaiveStrategy;

impl NaiveStrategy {
    /// Convert rule evaluation to use the optimized hash join
    fn find_premise_solutions(
        &mut self,
        dict: &Dictionary,
        rule: &Rule,
        all_facts: &Vec<Triple>,
    ) -> Vec<SolutionMapping> {
        if rule.premise.is_empty() {
            return Vec::new();
        }

        let mut cur_binding_set = vec![BTreeMap::new()];

        for premise in &rule.premise {
            cur_binding_set =
                join_premise_with_hash_join(premise, all_facts, cur_binding_set, dict);
            if cur_binding_set.is_empty() {
                break;
            }
        }

        cur_binding_set
            .into_iter()
            .map(|binding| convert_string_binding_to_u32(&binding, dict))
            .collect()
    }
}

impl InferenceStrategy for NaiveStrategy {
    fn infer_round(
        &mut self,
        dictionary: &mut Dictionary,
        rules: &Vec<Rule>,
        all_facts: &Vec<Triple>,
        known_facts: &HashSet<Triple>,
    ) -> HashSet<Triple> {
        let mut inferred_facts_this_round: HashSet<Triple> = HashSet::new();

        for rule in rules {
            let binding_sets = self.find_premise_solutions(dictionary, rule, all_facts);

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
    /// Derives every entailed fact by naive forward chaining, returning the new ones and leaving
    /// them in [`dataset_index`](Reasoner::dataset_index)
    pub fn infer_new_facts_naive(&mut self) -> Vec<Triple> {
        self.infer_with_strategy(NaiveStrategy)
    }

    /// For backward compatibility
    pub fn infer_new_facts(&mut self) -> Vec<Triple> {
        self.infer_new_facts_naive()
    }
}
