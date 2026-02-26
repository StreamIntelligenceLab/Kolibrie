use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap, HashSet};
use crate::reasoning::{convert_string_binding_to_u32, Reasoner};
use crate::reasoning::materialisation::infer_generic::{SolutionMapping, InferenceStrategy};
use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::rules::{evaluate_filters, join_premise_with_hash_join};

pub struct NaiveStrategy;

impl NaiveStrategy {

    /// Convert rule evaluation to use the optimized hash join
    /// Optimised hash-join: return early when build map is empty (see for loop)
    fn find_premise_solutions(&mut self, dict: &Dictionary, rule: &Rule, all_facts: &Vec<Triple>) -> Vec<SolutionMapping> {
        if rule.premise.is_empty() {
            return Vec::new();
        }

        // Initialise current bindings to an empty vector
        let mut cur_binding_set = vec![BTreeMap::new()];

        // Process each premise with the build hash-table using the optimized join
        for premise in &rule.premise {
            cur_binding_set = join_premise_with_hash_join(premise, all_facts, cur_binding_set, dict);
            if cur_binding_set.is_empty() {
                break;
            }
        }

        // Convert results back to HashMap<String, u32>
        cur_binding_set
            .into_iter()
            .map(|binding| convert_string_binding_to_u32(&binding, dict))
            .collect()
    }
}

impl InferenceStrategy for NaiveStrategy {

    fn infer_round(&mut self, dictionary: &mut Dictionary, rules: &Vec<Rule>, all_facts: &Vec<Triple>, known_facts: &HashSet<Triple>) -> HashSet<Triple> {
        // Also HashSet to prevent duplicate triples from being added this round
        let mut inferred_facts_this_round: HashSet<Triple> = HashSet::new();

        // Loop over each rule
        for rule in rules {
            // These are all bindings such that the premise is satisfied for the given rule
            let binding_sets = self.find_premise_solutions(dictionary, rule, all_facts);

            // For each binding that satisfies the premises of the rule, get to the conclusion and apply bindings
            for binding_set in &binding_sets {
                if evaluate_filters(&binding_set, &rule.filters, &dictionary) {
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
    pub fn infer_new_facts_naive(&mut self) -> Vec<Triple> {
        self.infer_with_strategy(NaiveStrategy)
    }

    /// For backward compatibility
    pub fn infer_new_facts(&mut self) -> Vec<Triple> {
        self.infer_new_facts_naive()
    }
}
