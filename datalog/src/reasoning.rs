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
//! materialising anything, [`repairs`] answers queries over an inconsistent fact
//! base, and [`to_dot`] renders the whole thing for inspection.

pub mod materialisation;
pub mod to_dot;
pub mod backward_chaining;
pub mod rules;
pub mod repairs;
pub mod helpers;

pub use rules::{matches_rule_pattern, construct_triple, join_rule, join_remaining};

use shared::dictionary::Dictionary;
use shared::dataset_index::DatasetIndex;
use shared::triple::Triple;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use shared::rule_index::RuleIndex;
use shared::rule::Rule;
use shared::provenance::Provenance;
use shared::tag_store::TagStore;
use std::sync::Arc;
use std::sync::RwLock;

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
    pub dictionary: Arc<RwLock<Dictionary>>,
    /// Rules to evaluate. Order does not affect the fixpoint.
    pub rules: Vec<Rule>, // List of dynamic rules

    /// The fact base, base and derived facts alike. Materialisation inserts into
    /// this as it goes, so after a run it holds both.
    pub dataset_index: DatasetIndex,
    /// Predicate-to-rule lookup, so a fact only tries rules that could match it.
    pub rule_index: RuleIndex,
    /// Constraints for the repair-based strategies. These are not evaluated as
    /// rules: a violated constraint means the fact base needs repairing.
    pub constraints: Vec<Rule>,
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
    dict: &Dictionary
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
    /// An empty reasoner with a fresh dictionary of its own.
    ///
    /// To share a caller's dictionary instead, assign to
    /// [`dictionary`](Self::dictionary) before inserting anything.
    pub fn new() -> Self {
        Self {
            dictionary: Arc::new(RwLock::new(Dictionary::new())),
            rules: Vec::new(),
            dataset_index: DatasetIndex::new(),
            rule_index: RuleIndex::new(),
            constraints: Vec::new(),
            probability_seeds: HashMap::new(),
        }
    }

    /// Add a triple with an associated probability value.
    /// The triple is added to the index; the probability is stored for provenance seeding.
    pub fn add_tagged_triple(&mut self, subject: &str, predicate: &str, object: &str, probability: f64) {
        let mut dict = self.dictionary.write().unwrap();
        let s = dict.encode(subject);
        let p = dict.encode(predicate);
        let o = dict.encode(object);
        drop(dict);

        let triple = Triple { subject: s, predicate: p, object: o };
        self.dataset_index.insert(&triple);
        self.probability_seeds.insert(triple, probability);
    }

    /// Materialize provenance tags as RDF-star triples so they are queryable via SPARQL-star.
    /// Generates triples of the form: << s p o >> <prob:value> "0.7"^^xsd:double
    pub fn materialize_tags_as_rdf_star<P: Provenance>(&mut self, tag_store: &TagStore<P>) {
        let mut dict = self.dictionary.write().unwrap();
        let mut qt_store = shared::quoted_triple_store::QuotedTripleStore::new();
        let rdf_star_triples = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
        drop(dict);

        for triple in &rdf_star_triples {
            self.dataset_index.insert(triple);
        }
    }

    /// Add an ABox triple (instance-level information)
    pub fn add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let mut dict = self.dictionary.write().unwrap();
        let s = dict.encode(subject);
        let p = dict.encode(predicate);
        let o = dict.encode(object);
        drop(dict);  // Release lock early

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
        let mut dict = self.dictionary.write().unwrap();
        let s = subject.map(|s| dict.encode(s));
        let p = predicate.map(|p| dict.encode(p));
        let o = object.map(|o| dict.encode(o));
        drop(dict);  // Release lock early

        self.dataset_index.query(s, p, o)
    }

    /// Add new method to handle constraints
    pub fn add_constraint(&mut self, constraint: Rule) {
        self.constraints.push(constraint);
    }

    /// New method to check if a set of facts violates constraints
    fn violates_constraints(&self, facts: &HashSet<Triple>) -> bool {
        for constraint in &self.constraints {
            let bindings = join_rule(constraint, facts, facts);
            if !bindings.is_empty() {
                return true;
            }
        }
        false
    }

    /// New method to find repairs
    fn compute_repairs(&self, facts: &HashSet<Triple>) -> Vec<HashSet<Triple>> {
        let mut repairs = Vec::new();
        let mut work_queue = vec![facts.clone()];
        let mut seen = BTreeSet::new(); // Using BTreeSet instead of HashSet

        while let Some(current_set) = work_queue.pop() {
            // Convert current_set to a Vec for consistent ordering when inserting into seen
            let current_vec: Vec<_> = current_set.iter().cloned().collect();

            // Skip if we've seen this combination before
            if !seen.insert(current_vec.clone()) {
                continue;
            }

            if !self.violates_constraints(&current_set) {
                // Found a consistent subset
                let is_maximal = repairs.iter().all(|repair: &HashSet<Triple>| {
                    !repair.is_superset(&current_set) || repair == &current_set
                });

                if is_maximal {
                    repairs.push(current_set);
                }
            } else {
                // Try removing each fact to create new candidate repairs
                for fact in current_set.iter() {
                    let mut new_set = current_set.clone();
                    new_set.remove(fact);

                    // Convert new_set to Vec for checking seen
                    let new_vec: Vec<_> = new_set.iter().cloned().collect();
                    if !seen.contains(&new_vec) {
                        work_queue.push(new_set);
                    }
                }
            }
        }
        repairs
    }
}

