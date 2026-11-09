/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Semi-naive materialisation that tags every derived fact with its provenance

use crate::reasoning::convert_string_binding_to_u32;
use crate::reasoning::materialisation::provenance_infer_generic::{
    ProvenanceInferResult, ProvenanceInferenceStrategy,
};
use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::rules::{evaluate_filters, join_premise_with_hash_join};
use crate::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::provenance::Provenance;
use shared::quad_source::QuadSource;
use shared::rule::Rule;
use shared::tag_store::TagStore;
use shared::triple::Triple;
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// Semi-naive materialisation strategy parameterized by a provenance semiring
struct ProvenanceSemiNaiveStrategy {
    start_idx_for_delta: usize,
    delta_improved: Vec<Triple>,
    explicit_initial_delta: Option<Vec<Triple>>,
    first_round: bool,
}

impl ProvenanceSemiNaiveStrategy {
    /// Find all bindings where at least one premise is matched by delta facts
    fn find_premise_solutions_with_triples(
        &mut self,
        dict: &Dictionary,
        rule: &Rule,
        all_facts: &[Triple],
        delta_facts: &[Triple],
    ) -> Vec<(BTreeMap<String, String>, Vec<Triple>)> {
        let nr_premises = rule.premise.len();
        let mut results = Vec::new();
        let mut seen_derivations = BTreeSet::new();

        for i in 0..nr_premises {
            let mut current_bindings = vec![BTreeMap::new()];

            current_bindings =
                join_premise_with_hash_join(&rule.premise[i], delta_facts, current_bindings, dict);

            for j in 0..nr_premises {
                if j == i {
                    continue;
                }
                current_bindings = join_premise_with_hash_join(
                    &rule.premise[j],
                    all_facts,
                    current_bindings,
                    dict,
                );
                if current_bindings.is_empty() {
                    break;
                }
            }

            for binding in current_bindings {
                let u32_binding = convert_string_binding_to_u32(&binding, dict);
                let matched_triples = resolve_premise_triples(&rule.premise, &u32_binding);
                if seen_derivations.insert((binding.clone(), matched_triples.clone())) {
                    results.push((binding, matched_triples));
                }
            }
        }

        results
    }
}

/// Resolve premise patterns to concrete triples using the current binding
fn resolve_premise_triples(
    premises: &[shared::terms::TriplePattern],
    bindings: &std::collections::HashMap<String, u32>,
) -> Vec<Triple> {
    premises
        .iter()
        .filter_map(|pat| {
            let s = resolve_term(&pat.0, bindings)?;
            let p = resolve_term(&pat.1, bindings)?;
            let o = resolve_term(&pat.2, bindings)?;
            Some(Triple {
                subject: s,
                predicate: p,
                object: o,
            })
        })
        .collect()
}

fn resolve_term(
    term: &shared::terms::Term,
    bindings: &std::collections::HashMap<String, u32>,
) -> Option<u32> {
    match term {
        shared::terms::Term::Variable(v) => bindings.get(v).copied(),
        shared::terms::Term::Constant(c) => Some(*c),
        shared::terms::Term::QuotedTriple(_) => None,
    }
}

impl<P: Provenance> ProvenanceInferenceStrategy<P> for ProvenanceSemiNaiveStrategy {
    fn infer_round(
        &mut self,
        dictionary: &mut Dictionary,
        rules: &[Rule],
        all_facts: &[Triple],
        known_facts: &HashSet<Triple>,
        tag_store: &mut TagStore<P>,
    ) -> ProvenanceInferResult {
        let mut new_facts: HashSet<Triple> = HashSet::new();
        let mut tag_changed = false;
        let provenance = tag_store.provenance().clone();
        if provenance.is_exhausted() {
            return ProvenanceInferResult {
                new_facts,
                tag_changed,
            };
        }

        let end_idx = all_facts.len();

        let effective_delta: Vec<Triple> = if self.first_round {
            self.first_round = false;
            self.start_idx_for_delta = end_idx;
            match self.explicit_initial_delta.take() {
                Some(explicit) => explicit,
                None => all_facts[0..end_idx].to_vec(),
            }
        } else {
            let slice = all_facts[self.start_idx_for_delta..end_idx].to_vec();
            self.start_idx_for_delta = end_idx;
            let mut combined = slice;
            combined.extend(self.delta_improved.drain(..));
            combined
        };

        let mut improved_this_round: Vec<Triple> = Vec::new();

        for rule in rules {
            let binding_sets = self.find_premise_solutions_with_triples(
                dictionary,
                rule,
                all_facts,
                &effective_delta,
            );

            for (binding, matched_triples) in &binding_sets {
                if provenance.is_exhausted() {
                    break;
                }
                let u32_binding = convert_string_binding_to_u32(binding, dictionary);

                if !evaluate_filters(&u32_binding, &rule.filters, dictionary) {
                    continue;
                }

                let conclusion_tag = matched_triples
                    .iter()
                    .map(|t| tag_store.get_tag(t))
                    .fold(provenance.one(), |acc, tag| {
                        provenance.conjunction(&acc, &tag)
                    });

                if conclusion_tag == provenance.zero() {
                    continue;
                }

                for conclusion in &rule.conclusion {
                    let inferred_fact =
                        replace_variables_with_bound_values(conclusion, &u32_binding, dictionary);

                    let is_new = !known_facts.contains(&inferred_fact);

                    if is_new && !new_facts.contains(&inferred_fact) {
                        tag_store.set_tag(&inferred_fact, conclusion_tag.clone());
                        new_facts.insert(inferred_fact);
                    } else {
                        if tag_store.update_disjunction(&inferred_fact, &conclusion_tag) {
                            if !is_new {
                                tag_changed = true;
                                improved_this_round.push(inferred_fact);
                            }
                        }
                    }
                }
            }
        }

        self.delta_improved = improved_this_round;

        ProvenanceInferResult {
            new_facts,
            tag_changed,
        }
    }
}

impl Reasoner {
    /// Run provenance-based semi-naive materialisation with single-stratum NAF support
    pub fn infer_new_facts_with_provenance<P: Provenance>(
        &mut self,
        provenance: P,
    ) -> (Vec<Triple>, TagStore<P>) {
        self.infer_new_facts_with_provenance_over(None, provenance)
    }

    /// As [`infer_new_facts_with_provenance`](Self::infer_new_facts_with_provenance), also reading
    /// base facts from `base`
    pub fn infer_new_facts_with_provenance_over<P: Provenance>(
        &mut self,
        base: Option<&dyn QuadSource>,
        provenance: P,
    ) -> (Vec<Triple>, TagStore<P>) {
        let mut tag_store = TagStore::new(provenance.clone());

        let mut seeds: Vec<(&Triple, f64)> = self
            .probability_seeds
            .iter()
            .map(|(t, &p)| (t, p))
            .collect();
        seeds.sort_by_key(|(t, _)| *t);
        for (id, (triple, prob)) in seeds.iter().enumerate() {
            let tag = provenance.tag_from_probability_with_id(*prob, id);
            tag_store.set_tag(triple, tag);
        }
        tag_store.seed_triples = seeds.iter().map(|(t, _)| (*t).clone()).collect();

        let new_facts = semi_naive_with_initial_tags(self, base, provenance, tag_store.clone());
        (new_facts.0, new_facts.1)
    }
}

/// Runs the two strata to a fixpoint, starting from tags the caller has already set
pub fn semi_naive_with_initial_tags<P: Provenance>(
    reasoner: &mut Reasoner,
    base: Option<&dyn QuadSource>,
    provenance: P,
    mut initial_tags: TagStore<P>,
) -> (Vec<Triple>, TagStore<P>) {
    let positive_rules: Vec<Rule> = reasoner
        .rules
        .iter()
        .filter(|r| r.negative_premise.is_empty())
        .cloned()
        .collect();
    let negative_rules: Vec<Rule> = reasoner
        .rules
        .iter()
        .filter(|r| !r.negative_premise.is_empty())
        .cloned()
        .collect();

    let mut new_facts = reasoner.infer_with_provenance_strategy_and_rules(
        base,
        ProvenanceSemiNaiveStrategy {
            start_idx_for_delta: 0,
            delta_improved: Vec::new(),
            explicit_initial_delta: None,
            first_round: true,
        },
        &mut initial_tags,
        &positive_rules,
    );

    if !negative_rules.is_empty() {
        let neg_new = run_negative_stratum_pass(
            reasoner,
            base,
            &negative_rules,
            &mut initial_tags,
            &provenance,
        );
        new_facts.extend(neg_new);
    }

    (new_facts, initial_tags)
}

/// As [`semi_naive_with_initial_tags`], seeded with an explicit delta for the first round
pub fn semi_naive_with_initial_tags_and_delta<P: Provenance>(
    reasoner: &mut Reasoner,
    base: Option<&dyn QuadSource>,
    _provenance: P,
    mut initial_tags: TagStore<P>,
    initial_delta: Vec<Triple>,
) -> (Vec<Triple>, TagStore<P>) {
    let positive_rules: Vec<Rule> = reasoner
        .rules
        .iter()
        .filter(|r| r.negative_premise.is_empty())
        .cloned()
        .collect();

    let new_facts = reasoner.infer_with_provenance_strategy_and_rules(
        base,
        ProvenanceSemiNaiveStrategy {
            start_idx_for_delta: 0,
            delta_improved: Vec::new(),
            explicit_initial_delta: Some(initial_delta),
            first_round: true,
        },
        &mut initial_tags,
        &positive_rules,
    );

    (new_facts, initial_tags)
}

/// Single forward pass for NAF rules (stratum 1) over the stratum-0 closure
fn run_negative_stratum_pass<P: Provenance>(
    reasoner: &mut Reasoner,
    base: Option<&dyn QuadSource>,
    rules: &[Rule],
    tag_store: &mut TagStore<P>,
    provenance: &P,
) -> Vec<Triple> {
    let mut all_facts: Vec<Triple> = reasoner.dataset_index.query(None, None, None);
    if let Some(base) = base {
        all_facts.extend(base.query_default(None, None, None));
    }
    let all_facts_set: HashSet<Triple> = all_facts.iter().cloned().collect();
    let mut new_derived: Vec<Triple> = Vec::new();

    let terms = reasoner.terms();
    shared::term_encoder::TermEncoder::with_terms(&terms, &mut |dict| {
        for rule in rules {
            let mut bindings: Vec<BTreeMap<String, String>> = vec![BTreeMap::new()];
            for prem in &rule.premise {
                bindings = join_premise_with_hash_join(prem, &all_facts, bindings, &dict);
                if bindings.is_empty() {
                    break;
                }
            }

            for binding in &bindings {
                let u32_binding = convert_string_binding_to_u32(binding, &dict);

                if !evaluate_filters(&u32_binding, &rule.filters, &dict) {
                    continue;
                }

                let pos_triples = resolve_premise_triples(&rule.premise, &u32_binding);
                let pos_tag = pos_triples
                    .iter()
                    .map(|t| tag_store.get_tag(t))
                    .fold(provenance.one(), |acc, tag| {
                        provenance.conjunction(&acc, &tag)
                    });

                if pos_tag == provenance.zero() {
                    continue;
                }

                let mut neg_tag = provenance.one();
                for neg_pat in &rule.negative_premise {
                    let s = resolve_term(&neg_pat.0, &u32_binding);
                    let p = resolve_term(&neg_pat.1, &u32_binding);
                    let o = resolve_term(&neg_pat.2, &u32_binding);

                    let contrib = match (s, p, o) {
                        (Some(s), Some(p), Some(o)) => {
                            let neg_triple = Triple {
                                subject: s,
                                predicate: p,
                                object: o,
                            };
                            if all_facts_set.contains(&neg_triple) {
                                provenance.negate(&tag_store.get_tag(&neg_triple))
                            } else {
                                provenance.one()
                            }
                        }
                        _ => provenance.zero(),
                    };

                    neg_tag = provenance.conjunction(&neg_tag, &contrib);
                    if neg_tag == provenance.zero() {
                        break;
                    }
                }

                let conclusion_tag = provenance.conjunction(&pos_tag, &neg_tag);
                if conclusion_tag == provenance.zero() {
                    continue;
                }

                for conclusion in &rule.conclusion {
                    let inferred =
                        replace_variables_with_bound_values(conclusion, &u32_binding, dict);

                    if !all_facts_set.contains(&inferred) && !new_derived.contains(&inferred) {
                        tag_store.set_tag(&inferred, conclusion_tag.clone());
                        reasoner.dataset_index.insert(&inferred);
                        new_derived.push(inferred);
                    } else {
                        tag_store.update_disjunction(&inferred, &conclusion_tag);
                    }
                }
            }
        }
    });
    new_derived
}
