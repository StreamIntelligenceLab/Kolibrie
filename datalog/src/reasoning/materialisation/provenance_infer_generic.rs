/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::dictionary::Dictionary;
use shared::provenance::Provenance;
use shared::rule::Rule;
use shared::tag_store::TagStore;
use shared::triple::Triple;
use std::collections::HashSet;
use crate::reasoning::Reasoner;

/// Result of a single provenance inference round.
pub struct ProvenanceInferResult {
    /// Newly derived triples (not previously in known_facts).
    pub new_facts: HashSet<Triple>,
    /// Whether any existing triple's tag was updated this round.
    pub tag_changed: bool,
}

/// Strategy trait for provenance-based materialisation.
///
/// Implementors define how a single inference round works (e.g. naive, semi-naive).
/// The driver loop calls `infer_round` repeatedly until fixpoint.
pub trait ProvenanceInferenceStrategy<P: Provenance> {
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
    /// Generic driver for provenance-based materialisation. Loops until fixpoint.
    pub fn infer_with_provenance_strategy<P, S>(
        &mut self,
        strat: S,
        tag_store: &mut TagStore<P>,
    ) -> Vec<Triple>
    where
        P: Provenance,
        S: ProvenanceInferenceStrategy<P>,
    {
        let rules: Vec<Rule> = self.rules.clone();
        self.infer_with_provenance_strategy_and_rules(strat, tag_store, &rules)
    }

    /// Same as `infer_with_provenance_strategy` but with an explicit rule slice.
    /// Used for stratified evaluation where stratum-0 uses only positive rules.
    pub fn infer_with_provenance_strategy_and_rules<P, S>(
        &mut self,
        mut strat: S,
        tag_store: &mut TagStore<P>,
        rules: &[Rule],
    ) -> Vec<Triple>
    where
        P: Provenance,
        S: ProvenanceInferenceStrategy<P>,
    {
        let mut all_facts: Vec<Triple> = self.index_manager.query(None, None, None);
        let mut known_facts: HashSet<Triple> = all_facts.iter().cloned().collect();
        let idx_before_inference = all_facts.len();

        loop {
            let mut dict = self.dictionary.write().unwrap();
            let result = strat.infer_round(
                &mut dict,
                rules,
                &all_facts,
                &known_facts,
                tag_store,
            );
            drop(dict);

            let has_new_facts = !result.new_facts.is_empty();

            for fact in result.new_facts {
                if !known_facts.contains(&fact) {
                    known_facts.insert(fact.clone());
                    self.index_manager.insert(&fact);
                    all_facts.push(fact);
                }
            }

            // Terminate when no new facts AND no tag updates
            if !has_new_facts && !result.tag_changed {
                break;
            }
        }

        all_facts.split_off(idx_before_inference)
    }
}
