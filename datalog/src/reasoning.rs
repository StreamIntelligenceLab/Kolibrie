/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */
//! The [`Reasoner`]: rules, facts, and the strategies that derive one from the other.
//!
//! A [`Reasoner`] owns its rule set and a [`DatasetIndex`] of facts, and shares its
//! [`Dictionary`] with whoever created it — an `Arc<RwLock<_>>`, not a private copy,
//! because the ids in its facts have to mean the same thing to the caller.
//!
//! Forward chaining lives in [`materialisation`] and is chosen by calling the
//! matching method on the reasoner rather than by naming a strategy type: several of
//! those submodules are `pub(crate)`, and their methods are reachable through
//! [`Reasoner`] regardless. [`backward_chaining`] proves a single goal without
//! materialising anything, and [`to_dot`] renders the whole thing for inspection.

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

// Logic part: Knowledge Graph

/// A rule set, a fact base, and the dictionary the two are encoded against.
///
/// The fields are public, which means a caller can insert facts directly into
/// [`dataset_index`](Self::dataset_index) or swap the dictionary for a shared one
/// without going through a method, and the cross-window strategies do exactly that.
/// Nothing here is cached or derived from anything else, so there is no invariant
/// between the fields to break — except that every id in `dataset_index` and
/// `rules` must be meaningful in `dictionary`, which is why replacing the
/// dictionary wholesale is only safe before any facts are inserted.
#[derive(Debug, Clone)]
// Are there RDF connections here or not?
pub struct Reasoner {
    /// Term interning, shared with the caller rather than owned: a fact inserted
    /// here has to decode to the same string on the other side.
    ///
    /// Private, and reached through [`terms`](Self::terms) or
    /// [`with_terms`](Self::with_terms). It was a public `Arc<RwLock<Dictionary>>`,
    /// which is P5's "nothing leaks" breached in the plainest way: a caller had to
    /// name the locking strategy to intern a term, and six places shared a dictionary
    /// by assigning the field rather than by asking.
    terms: SharedTerms,
    /// Rules to evaluate. Order does not affect the fixpoint.
    pub rules: Vec<Rule>, // List of dynamic rules

    /// The fact base, base and derived facts alike. Materialisation inserts into
    /// this as it goes, so after a run it holds both.
    pub dataset_index: DatasetIndex,
    /// Predicate-to-rule lookup, so a fact only tries rules that could match it.
    pub rule_index: RuleIndex,
    /// Input probabilities for provenance seeding. Only the provenance strategies
    /// read this; the plain ones ignore it entirely.
    pub probability_seeds: HashMap<Triple, f64>, // Input probabilities for provenance seeding
}

/// Interns a string binding against an existing dictionary, without extending it.
///
/// A variable whose value is not already in the dictionary is **dropped**, so the
/// result can be smaller than the input. That is deliberate — a value absent from
/// the dictionary cannot appear in any fact, so a binding mentioning it cannot
/// match — but it means the result's length is not a count of the input's.
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
    /// Adopts `terms` as this reasoner's term space.
    ///
    /// The point of taking a [`SharedTerms`] rather than the handle is that the
    /// caller does not have to name `Arc<RwLock<Dictionary>>`, and cannot pass
    /// something single-threaded by mistake: only the shared encoder is `Sync`.
    ///
    /// Sharing rather than copying, so a term interned here afterwards is
    /// immediately visible to everything else holding the same `SharedTerms`. Code
    /// that assigns the handle and then assigns it back to "sync" the result is
    /// doing nothing; see the note this replaced in `kolibrie-http-server`.
    pub fn adopt_terms(&mut self, terms: SharedTerms) {
        self.terms = terms;
    }

    /// The term space this reasoner is using, to share with something else.
    ///
    /// The counterpart to [`adopt_terms`](Self::adopt_terms): two components agree on
    /// a term space by one handing its own over, without either naming how it is
    /// held.
    pub fn terms(&self) -> SharedTerms {
        self.terms.clone()
    }

    /// Runs `visit` against the dictionary, for the duration of the call only.
    ///
    /// The scoped form: the lock is taken and released around `visit`, so a caller
    /// cannot hold it open across unrelated work, which is what reaching into a
    /// public handle allowed.
    pub fn with_terms(&self, visit: &mut dyn FnMut(&mut Dictionary)) {
        TermEncoder::with_terms(&self.terms, visit);
    }

    /// Interns `text` in this reasoner's term space and returns its id.
    ///
    /// The common case by far, and worth a name: most callers took a write lock to
    /// encode one term and dropped it again.
    pub fn encode_term(&self, text: &str) -> u32 {
        let mut id = 0;
        self.with_terms(&mut |dictionary| id = dictionary.encode(text));
        id
    }

    /// A read-only copy of the term space, for decoding many ids at once.
    ///
    /// The scoped encoder will not hand its lock out, so code that decodes every
    /// term it prints either takes the lock per term or copies once. This copies, and
    /// returns a [`TermView`] rather than a `Dictionary` so that interning into the
    /// copy is not expressible -- see that type for why it would be wrong and not
    /// merely useless.
    pub fn term_view(&self) -> TermView {
        self.terms.view()
    }

    /// Interns a triple's three terms under one lock.
    ///
    /// Four callers took a write lock, encoded subject, predicate and object, and
    /// dropped it. One lock acquisition, as before, but the caller no longer says so.
    pub fn encode_triple(&self, subject: &str, predicate: &str, object: &str) -> (u32, u32, u32) {
        self.terms.encode_triple(subject, predicate, object)
    }

    /// Interns whichever of a pattern's positions are given.
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

    /// The string `id` stands for, if this term space knows it.
    pub fn decode_term(&self, id: u32) -> Option<String> {
        let mut text = None;
        self.with_terms(&mut |dictionary| {
            text = dictionary.decode(id).map(str::to_string);
        });
        text
    }

    /// An empty reasoner with a fresh dictionary of its own.
    ///
    /// To share a caller's term space instead, call
    /// [`adopt_terms`](Self::adopt_terms) before inserting anything.
    pub fn new() -> Self {
        Self {
            terms: SharedTerms::new(),
            rules: Vec::new(),
            dataset_index: DatasetIndex::new(),
            rule_index: RuleIndex::new(),
            probability_seeds: HashMap::new(),
        }
    }

    /// Add a triple with an associated probability value.
    /// The triple is added to the index; the probability is stored for provenance seeding.
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

    /// Materialize provenance tags as RDF-star triples so they are queryable via SPARQL-star.
    /// Generates triples of the form: << s p o >> <prob:value> "0.7"^^xsd:double
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
        let (s, p, o) = self.encode_triple(subject, predicate, object); // Release lock early

        self.dataset_index.insert(&Triple {
            subject: s,
            predicate: p,
            object: o,
        });
    }

    /// Insert an already-ground triple directly into the fact index.
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
        let (s, p, o) = self.encode_pattern(subject, predicate, object); // Release lock early

        self.dataset_index.query(s, p, o)
    }
}

#[cfg(test)]
mod term_space_tests {
    use super::*;

    #[test]
    fn two_components_adopting_one_space_agree_on_ids() {
        // The invariant the scoped API exists to provide.
        let left = Reasoner::new();
        let mut right = Reasoner::new();
        right.adopt_terms(left.terms());

        let from_left = left.encode_term("urn:shared");
        let from_right = right.encode_term("urn:shared");
        assert_eq!(
            from_left, from_right,
            "one term space mints one id per term"
        );

        // And a term interned on one side is visible from the other, with no copying
        // back: that is what sharing means, and what the removed sync-back assumed.
        let late = right.encode_term("urn:late");
        assert_eq!(left.decode_term(late).as_deref(), Some("urn:late"));
    }

    #[test]
    fn two_dictionaries_from_one_starting_point_mint_colliding_ids() {
        // Why `term_view` hands out a read-only view instead of a `Dictionary`.
        //
        // This is the underlying fact, shown against `Dictionary` directly, because
        // the hazard it used to allow is no longer expressible through the reasoner:
        // a `TermView` has no `encode`. Were it still a copy, a fact built from an id
        // minted in it would decode, through the shared space, to someone else's
        // term -- corrupting the fact base rather than losing the term.
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
        // The staleness half, which `TermView` does not and cannot prevent. It is
        // sound rather than wrong -- a blank, not someone else's term -- and every
        // caller is safe because the ids it decodes come from facts already stored,
        // so they predate the view.
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
