/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The driver loop for strategies that carry a provenance tag per fact

use crate::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::provenance::Provenance;
use shared::quad_source::QuadSource;
use shared::rule::Rule;
use shared::tag_store::TagStore;
use shared::triple::Triple;
use std::collections::HashSet;

/// Result of a single provenance inference round
#[derive(Debug)]
pub struct ProvenanceInferResult {
    /// Newly derived triples (not previously in known_facts)
    pub new_facts: HashSet<Triple>,
    /// Whether any existing triple's tag was updated this round
    pub tag_changed: bool,
}

/// Strategy trait for provenance-based materialisation
pub trait ProvenanceInferenceStrategy<P: Provenance> {
    /// Runs one round, updating `tag_store` and reporting whether any tag changed
    fn infer_round(
        &mut self,
        dictionary: &mut Dictionary,
        rules: &[Rule],
        all_facts: &[Triple],
        known_facts: &HashSet<Triple>,
        tag_store: &mut TagStore<P>,
    ) -> ProvenanceInferResult;
}

impl Reasoner {
    /// Generic driver for provenance-based materialisation
    pub fn infer_with_provenance_strategy<P, S>(
        &mut self,
        base: Option<&dyn QuadSource>,
        strat: S,
        tag_store: &mut TagStore<P>,
    ) -> Vec<Triple>
    where
        P: Provenance,
        S: ProvenanceInferenceStrategy<P>,
    {
        let rules: Vec<Rule> = self.rules.clone();
        self.infer_with_provenance_strategy_and_rules(base, strat, tag_store, &rules)
    }

    /// Same as `infer_with_provenance_strategy` but with an explicit rule slice
    pub fn infer_with_provenance_strategy_and_rules<P, S>(
        &mut self,
        base: Option<&dyn QuadSource>,
        mut strat: S,
        tag_store: &mut TagStore<P>,
        rules: &[Rule],
    ) -> Vec<Triple>
    where
        P: Provenance,
        S: ProvenanceInferenceStrategy<P>,
    {
        let mut all_facts: Vec<Triple> = self.dataset_index.query(None, None, None);
        if let Some(base) = base {
            all_facts.extend(base.query_default(None, None, None));
        }
        let mut known_facts: HashSet<Triple> = all_facts.iter().cloned().collect();
        let idx_before_inference = all_facts.len();

        loop {
            let mut result = None;
            self.with_terms(&mut |dict| {
                result = Some(strat.infer_round(dict, rules, &all_facts, &known_facts, tag_store));
            });
            let result = result.expect("the visitor always runs");

            let has_new_facts = !result.new_facts.is_empty();

            for fact in result.new_facts {
                if !known_facts.contains(&fact) {
                    known_facts.insert(fact.clone());
                    self.dataset_index.insert(&fact);
                    all_facts.push(fact);
                }
            }

            if !has_new_facts && !result.tag_changed {
                break;
            }
        }

        all_facts.split_off(idx_before_inference)
    }
}
