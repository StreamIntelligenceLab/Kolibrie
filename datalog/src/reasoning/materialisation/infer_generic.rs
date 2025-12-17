use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};
use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::Reasoner;
use crate::reasoning::rules::evaluate_filters;

pub trait InferenceStrategy {

    ///
    fn evaluate_rule(&mut self, dict: &Dictionary, rule: &Rule, all_facts: &Vec<Triple>) -> Vec<HashMap<String, u32>>;
}

impl Reasoner {
    fn infer_round<S: InferenceStrategy>(
        &mut self,
        strat: &mut S,
        all_facts: &Vec<Triple>,
        known_facts: &HashSet<Triple>,
    ) -> HashSet<Triple> {
        // Also HashSet to prevent duplicate triples from being added this round
        let mut inferred_facts_this_round: HashSet<Triple> = HashSet::new();

        for rule in &self.rules {
            // These are all bindings such that the premise is satisfied for the given rule
            let bindings = strat.evaluate_rule(&mut self.dictionary, rule, all_facts);

            // For each binding that satisfies the premises of the rule, get to the conclusion and apply bindings
            for binding in &bindings {
                if evaluate_filters(&binding, &rule.filters, &self.dictionary) {
                    // Loop over each conclusion of the rule, since for the current binding,
                    // the conclusions of the rule can be inferred (because premises are met)
                    for conclusion in &rule.conclusion {
                        let inferred_fact =
                            replace_variables_with_bound_values(conclusion, binding, &mut self.dictionary);

                        if (!known_facts.contains(&inferred_fact)) {
                            inferred_facts_this_round.insert(inferred_fact);
                        }
                    }
                }
            }
        }

        inferred_facts_this_round
    }

    pub fn infer_with_strategy<S: InferenceStrategy>(&mut self, mut strat: S) -> Vec<Triple> {
        // In each iteration, facts are added to this list. Use vector to preserve index for initial facts
        let mut all_facts: Vec<Triple> = self.index_manager.query(None, None, None);
        let mut known_facts: HashSet<Triple> = all_facts.iter().cloned().collect();
        let idx_before_inference = all_facts.len(); // Used to keep track of which facts are inferred by the algorithm

        loop {
            let mut inferred_facts_this_round = self.infer_round(&mut strat, &all_facts, &known_facts);

            if (inferred_facts_this_round.is_empty()) {
                break;
            }

            for fact in inferred_facts_this_round.drain() {
                // Insert into known_facts first; if it was not present, also store it.
                if (!known_facts.contains(&fact)) {
                    known_facts.insert(fact.clone()); // Necessary clone apparently
                    self.index_manager.insert(&fact);
                    all_facts.push(fact);
                }
            }
        }

        let idx_after_inference = all_facts.len();

        let inferred_facts: Vec<Triple> = all_facts[idx_before_inference..idx_after_inference]
            .iter()
            .cloned()
            .collect();

        println!("nr inferred facts: {}", inferred_facts.len());

        inferred_facts
    }
}
