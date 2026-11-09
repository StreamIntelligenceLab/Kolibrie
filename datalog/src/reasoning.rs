/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */
//! The [`Reasoner`]: rules, facts, and the strategies that derive one from the other

pub mod backward_chaining;
pub mod helpers;
pub mod materialisation;
pub mod rules;
pub mod to_dot;

pub use rules::{construct_triple, join_remaining, join_rule, matches_rule_pattern};

use shared::dataset_index::DatasetIndex;
use shared::dictionary::Dictionary;
use shared::provenance::Provenance;
use shared::rule::Rule;
use shared::rule_index::RuleIndex;
use shared::tag_store::TagStore;
use shared::term_encoder::{SharedTerms, TermEncoder, TermView};
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap};

/// A rule set, a fact base, and the dictionary the two are encoded against
#[derive(Debug, Clone)]
pub struct Reasoner {
    terms: SharedTerms,
    /// Rules to evaluate
    pub rules: Vec<Rule>,

    /// The fact base, base and derived facts alike
    pub dataset_index: DatasetIndex,
    /// Predicate-to-rule lookup, so a fact only tries rules that could match it
    pub rule_index: RuleIndex,
    /// Input probabilities for provenance seeding
    pub probability_seeds: HashMap<Triple, f64>,
}

/// Interns a string binding against an existing dictionary, without extending it
pub fn convert_string_binding_to_u32(
    binding: &BTreeMap<String, String>,
    dict: &Dictionary,
) -> HashMap<String, u32> {
    let mut result = HashMap::new();
    for (var, value) in binding {
        if let Some(&id) = dict.string_to_id.get(value) {
            result.insert(var.clone(), id);
        }
    }
    result
}

impl Reasoner {
    /// Adopts `terms` as this reasoner's term space
    pub fn adopt_terms(&mut self, terms: SharedTerms) {
        self.terms = terms;
    }

    /// The term space this reasoner is using, to share with something else
    pub fn terms(&self) -> SharedTerms {
        self.terms.clone()
    }

    /// Runs `visit` against the dictionary, for the duration of the call only
    pub fn with_terms(&self, visit: &mut dyn FnMut(&mut Dictionary)) {
        TermEncoder::with_terms(&self.terms, visit);
    }

    /// Interns `text` in this reasoner's term space and returns its id
    pub fn encode_term(&self, text: &str) -> u32 {
        let mut id = 0;
        self.with_terms(&mut |dictionary| id = dictionary.encode(text));
        id
    }

    /// A read-only copy of the term space, for decoding many ids at once
    pub fn term_view(&self) -> TermView {
        self.terms.view()
    }

    /// Interns a triple's three terms under one lock
    pub fn encode_triple(&self, subject: &str, predicate: &str, object: &str) -> (u32, u32, u32) {
        self.terms.encode_triple(subject, predicate, object)
    }

    /// Interns whichever of a pattern's positions are given
    pub fn encode_pattern(
        &self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> (Option<u32>, Option<u32>, Option<u32>) {
        let mut ids = (None, None, None);
        self.with_terms(&mut |dictionary| {
            ids = (
                subject.map(|s| dictionary.encode(s)),
                predicate.map(|p| dictionary.encode(p)),
                object.map(|o| dictionary.encode(o)),
            );
        });
        ids
    }

    /// The string `id` stands for, if this term space knows it
    pub fn decode_term(&self, id: u32) -> Option<String> {
        let mut text = None;
        self.with_terms(&mut |dictionary| {
            text = dictionary.decode(id).map(str::to_string);
        });
        text
    }

    /// An empty reasoner with a fresh dictionary of its own
    pub fn new() -> Self {
        Self {
            terms: SharedTerms::new(),
            rules: Vec::new(),
            dataset_index: DatasetIndex::new(),
            rule_index: RuleIndex::new(),
            probability_seeds: HashMap::new(),
        }
    }

    /// Add a triple with an associated probability value
    pub fn add_tagged_triple(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
        probability: f64,
    ) {
        let (s, p, o) = self.encode_triple(subject, predicate, object);

        let triple = Triple {
            subject: s,
            predicate: p,
            object: o,
        };
        self.dataset_index.insert(&triple);
        self.probability_seeds.insert(triple, probability);
    }

    /// Materialize provenance tags as RDF-star triples so they are queryable via SPARQL-star
    pub fn materialize_tags_as_rdf_star<P: Provenance>(&mut self, tag_store: &TagStore<P>) {
        let mut qt_store = shared::quoted_triple_store::QuotedTripleStore::new();
        let mut rdf_star_triples = Vec::new();
        self.with_terms(&mut |dictionary| {
            rdf_star_triples = tag_store.encode_as_rdf_star(dictionary, &mut qt_store);
        });

        for triple in &rdf_star_triples {
            self.dataset_index.insert(triple);
        }
    }

    /// Add an ABox triple (instance-level information)
    pub fn add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let (s, p, o) = self.encode_triple(subject, predicate, object);

        self.dataset_index.insert(&Triple {
            subject: s,
            predicate: p,
            object: o,
        });
    }

    /// Insert an already-ground triple directly into the fact index
    pub fn insert_ground_triple(&mut self, triple: Triple) {
        self.dataset_index.insert(&triple);
    }

    /// Query the ABox for instance-level assertions (using TrieIndex now)
    pub fn query_abox(
        &mut self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> Vec<Triple> {
        let (s, p, o) = self.encode_pattern(subject, predicate, object);

        self.dataset_index.query(s, p, o)
    }
}

#[cfg(test)]
mod term_space_tests {
    use super::*;

    #[test]
    fn two_components_adopting_one_space_agree_on_ids() {
        let left = Reasoner::new();
        let mut right = Reasoner::new();
        right.adopt_terms(left.terms());

        let from_left = left.encode_term("urn:shared");
        let from_right = right.encode_term("urn:shared");
        assert_eq!(
            from_left, from_right,
            "one term space mints one id per term"
        );

        let late = right.encode_term("urn:late");
        assert_eq!(left.decode_term(late).as_deref(), Some("urn:late"));
    }

    #[test]
    fn two_dictionaries_from_one_starting_point_mint_colliding_ids() {
        let reasoner = Reasoner::new();
        reasoner.encode_term("urn:already-here");

        let mut copy = Dictionary::new();
        reasoner.with_terms(&mut |shared| copy = shared.clone());

        let in_copy = copy.encode("urn:only-in-the-copy");
        let in_shared = reasoner.encode_term("urn:only-in-the-shared-space");

        assert_eq!(
            in_copy, in_shared,
            "both counters stood in the same place, so they hand out the same number"
        );
        assert_eq!(
            reasoner.decode_term(in_copy).as_deref(),
            Some("urn:only-in-the-shared-space"),
            "the id minted in the copy names a different term in the shared space"
        );
    }

    #[test]
    fn a_view_does_not_see_terms_interned_after_it() {
        let reasoner = Reasoner::new();
        let before = reasoner.encode_term("urn:before");
        let view = reasoner.term_view();
        let after = reasoner.encode_term("urn:after");

        assert_eq!(view.decode(before), Some("urn:before"));
        assert_eq!(
            view.decode(after),
            None,
            "a view cannot know a term interned after it was taken"
        );
    }
}
