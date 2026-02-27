use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};
use crate::reasoning::Reasoner;

pub type SolutionMapping = HashMap<String, u32>;

pub trait InferenceStrategy {

    /// Applies a single round of inference within the materialisation algorithm.
    /// `param` all_facts: all facts currently in the knowledge base (base facts + those derived in previous rounds)
    /// `known_facts`: same facts but in HashSet for quick membership checks
    /// `returns`: facts that were inferred in this round
    fn infer_round(
        &mut self,
        dictionary: &mut Dictionary,
        rules: &Vec<Rule>,
        all_facts: &Vec<Triple>,
        known_facts: &HashSet<Triple>,
    ) -> HashSet<Triple>;
}

impl Reasoner {

    /// Generic function that infers all derivable facts using a given strategy, e.g. SemiNaive, or Naive
    pub fn infer_with_strategy<S: InferenceStrategy>(&mut self, mut strat: S) -> Vec<Triple> {
        // In each iteration, facts are added to this list. Use vector to preserve index for initial facts
        let mut all_facts: Vec<Triple> = self.index_manager.query(None, None, None);
        let mut known_facts: HashSet<Triple> = all_facts.iter().cloned().collect();
        let idx_before_inference = all_facts.len(); // Used to keep track of which facts are inferred by the algorithm

        loop {

            let mut dict = self.dictionary.write().unwrap();
            let mut inferred_facts_this_round = strat.infer_round(&mut dict, &self.rules, &all_facts, &known_facts);

            if inferred_facts_this_round.is_empty() {
                break;
            }

            for fact in inferred_facts_this_round.drain() {
                // Insert into known_facts first; if it was not present, also store it.
                if !known_facts.contains(&fact) {
                    known_facts.insert(fact.clone()); // Necessary clone apparently
                    self.index_manager.insert(&fact);
                    all_facts.push(fact);
                }
            }
        }

        all_facts.split_off(idx_before_inference)
    }
}
