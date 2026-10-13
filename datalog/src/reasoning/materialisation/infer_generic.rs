//! The driver loop shared by the forward-chaining strategies that carry no provenance.
//!
//! A strategy supplies one round ([`InferenceStrategy::infer_round`]) and the driver
//! repeats it until a round derives nothing new, inserting each round's facts into
//! the reasoner as it goes. The strategies themselves are `pub(crate)`; the loop is
//! public because it is also what a new strategy would plug into.

use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};
use crate::reasoning::Reasoner;

/// One way of satisfying a rule's premises: variable name to term id.
pub type SolutionMapping = HashMap<String, u32>;

/// One round of forward chaining, which [`Reasoner::infer_with_strategy`] repeats.
///
/// A strategy decides which facts a round considers; it does not decide when to
/// stop, since the driver stops as soon as a round derives nothing new.
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
        let mut all_facts: Vec<Triple> = self.dataset_index.query(None, None, None);
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
                    self.dataset_index.insert(&fact);
                    all_facts.push(fact);
                }
            }
        }

        all_facts.split_off(idx_before_inference)
    }
}

