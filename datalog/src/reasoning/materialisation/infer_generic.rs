//! The driver loop shared by the forward-chaining strategies that carry no provenance.
//!
//! A strategy supplies one round ([`InferenceStrategy::infer_round`]) and the driver
//! repeats it until a round derives nothing new, inserting each round's facts into
//! the reasoner as it goes. The strategies themselves are `pub(crate)`; the loop is
//! public because it is also what a new strategy would plug into.

use crate::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::quad_source::QuadSource;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};

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
    pub fn infer_with_strategy<S: InferenceStrategy>(&mut self, strat: S) -> Vec<Triple> {
        self.infer_with_strategy_over(None, strat)
    }

    /// As [`infer_with_strategy`](Self::infer_with_strategy), reading the base facts
    /// from `base` as well as from this reasoner's own index.
    ///
    /// Lets a caller reason over a store it does not own without copying it in. The
    /// three call sites that did copy were each building a second full index — three
    /// sorted orders over facts that this function immediately flattens back into a
    /// `Vec` — so the index was constructed to be enumerated once and then dropped.
    ///
    /// Facts already held here are still included, so a reasoner seeded with a few
    /// facts of its own and pointed at a larger base sees the union. Duplicates
    /// between the two are dropped, since `known_facts` dedupes.
    ///
    /// Derived facts land in [`dataset_index`](Reasoner::dataset_index) as before.
    /// Base facts read from `base` do not, which is the saving; a caller that needs
    /// them there should insert them itself.
    pub fn infer_with_strategy_over<S: InferenceStrategy>(
        &mut self,
        base: Option<&dyn QuadSource>,
        mut strat: S,
    ) -> Vec<Triple> {
        // In each iteration, facts are added to this list. Use vector to preserve index for initial facts
        let mut all_facts: Vec<Triple> = self.dataset_index.query(None, None, None);
        if let Some(base) = base {
            all_facts.extend(base.query_default(None, None, None));
        }
        let mut known_facts: HashSet<Triple> = all_facts.iter().cloned().collect();
        let idx_before_inference = all_facts.len(); // Used to keep track of which facts are inferred by the algorithm

        loop {
            let mut inferred_facts_this_round = HashSet::new();
            self.with_terms(&mut |dictionary| {
                inferred_facts_this_round =
                    strat.infer_round(dictionary, &self.rules, &all_facts, &known_facts);
            });

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
