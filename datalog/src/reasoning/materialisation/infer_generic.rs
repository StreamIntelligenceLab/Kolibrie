//! The driver loop shared by the forward-chaining strategies that carry no provenance

use crate::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::quad_source::QuadSource;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};

/// One way of satisfying a rule's premises: variable name to term id
pub type SolutionMapping = HashMap<String, u32>;

/// One round of forward chaining, which [`Reasoner::infer_with_strategy`] repeats
pub trait InferenceStrategy {
    /// Applies a single round of inference within the materialisation algorithm
    fn infer_round(
        &mut self,
        dictionary: &mut Dictionary,
        rules: &Vec<Rule>,
        all_facts: &Vec<Triple>,
        known_facts: &HashSet<Triple>,
    ) -> HashSet<Triple>;
}

impl Reasoner {
    /// Generic function that infers all derivable facts using a given strategy, e.g. SemiNaive, or
    /// Naive
    pub fn infer_with_strategy<S: InferenceStrategy>(&mut self, strat: S) -> Vec<Triple> {
        self.infer_with_strategy_over(None, strat)
    }

    /// As [`infer_with_strategy`](Self::infer_with_strategy), reading the base facts from `base` as
    /// well as from this reasoner's own index
    pub fn infer_with_strategy_over<S: InferenceStrategy>(
        &mut self,
        base: Option<&dyn QuadSource>,
        mut strat: S,
    ) -> Vec<Triple> {
        let mut all_facts: Vec<Triple> = self.dataset_index.query(None, None, None);
        if let Some(base) = base {
            all_facts.extend(base.query_default(None, None, None));
        }
        let mut known_facts: HashSet<Triple> = all_facts.iter().cloned().collect();
        let idx_before_inference = all_facts.len();

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
                if !known_facts.contains(&fact) {
                    known_facts.insert(fact.clone());
                    self.dataset_index.insert(&fact);
                    all_facts.push(fact);
                }
            }
        }

        all_facts.split_off(idx_before_inference)
    }
}
