/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use crate::provenance::Provenance;
use crate::triple::Triple;
use crate::dictionary::Dictionary;
use crate::quoted_triple_store::QuotedTripleStore;

/// Stores provenance tags for triples, parameterized by a [`Provenance`] semiring.
///
/// Absence of a triple in the store means it carries the `one()` tag (certain fact).
#[derive(Debug, Clone)]
pub struct TagStore<P: Provenance> {
    tags: HashMap<Triple, P::Tag>,
    provenance: P,
    /// Seed triples in sort order (index == seed variable ID).
    pub seed_triples: Vec<Triple>,
}

impl<P: Provenance> TagStore<P> {
    pub fn new(provenance: P) -> Self {
        Self {
            tags: HashMap::new(),
            provenance,
            seed_triples: Vec::new(),
        }
    }

    /// Get the provenance reference.
    pub fn provenance(&self) -> &P {
        &self.provenance
    }

    /// Get the tag for a triple. Returns `one()` if not explicitly stored (certain fact).
    pub fn get_tag(&self, triple: &Triple) -> P::Tag {
        self.tags.get(triple).cloned().unwrap_or_else(|| self.provenance.one())
    }

    /// Set the tag for a triple. If the tag equals `one()`, it is removed (implicit certainty).
    pub fn set_tag(&mut self, triple: &Triple, tag: P::Tag) {
        if tag == self.provenance.one() {
            self.tags.remove(triple);
        } else {
            self.tags.insert(triple.clone(), tag);
        }
    }

    /// Update a triple's tag using disjunction (⊕) with a new derivation tag.
    /// Returns `true` if the tag changed.
    pub fn update_disjunction(&mut self, triple: &Triple, new_tag: &P::Tag) -> bool {
        let old_tag = self.get_tag(triple);
        let combined = self.provenance.disjunction(&old_tag, new_tag);
        if self.provenance.is_saturated(&old_tag, &combined) {
            false
        } else {
            self.set_tag(triple, combined);
            true
        }
    }

    /// Check if a triple has an explicit (non-one) tag stored.
    pub fn has_explicit_tag(&self, triple: &Triple) -> bool {
        self.tags.contains_key(triple)
    }

    /// Returns the number of triples with explicit tags.
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Iterate over all (triple, tag) pairs with explicit tags.
    pub fn iter(&self) -> impl Iterator<Item = (&Triple, &P::Tag)> {
        self.tags.iter()
    }

    /// Export tags as RDF-star triples encoding probability values.
    pub fn encode_as_rdf_star(
        &self,
        dict: &mut Dictionary,
        qt_store: &mut QuotedTripleStore,
    ) -> Vec<Triple> {
        let prob_pred_id = dict.encode("http://www.w3.org/ns/prob#value");
        let mut result = Vec::with_capacity(self.tags.len());

        for (triple, tag) in &self.tags {
            let qt_id = qt_store.encode(triple.subject, triple.predicate, triple.object);
            let prob = self.provenance.recover_probability(tag);
            let prob_literal = format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#double>", prob);
            let prob_obj_id = dict.encode(&prob_literal);

            result.push(Triple {
                subject: qt_id,
                predicate: prob_pred_id,
                object: prob_obj_id,
            });
        }

        result
    }
}

use crate::provenance::WmcProvenance;

impl TagStore<WmcProvenance> {
    /// Export tags as RDF-star triples enriched with proof-path explanation.
    ///
    /// Superset of [`encode_as_rdf_star`]; also emits `prob:proofCount`,
    /// `prob:hasProof`, `prob:formula`, `prob:hasSeed`, and `prob:hasNegatedSeed` triples.
    pub fn encode_as_rdf_star_with_explanation(
        &self,
        dict: &mut Dictionary,
        qt_store: &mut QuotedTripleStore,
    ) -> Vec<Triple> {
        let mut result = self.encode_as_rdf_star(dict, qt_store);

        let xsd_int = "http://www.w3.org/2001/XMLSchema#integer";
        let xsd_str = "http://www.w3.org/2001/XMLSchema#string";
        let proof_count_id    = dict.encode("http://www.w3.org/ns/prob#proofCount");
        let has_proof_id      = dict.encode("http://www.w3.org/ns/prob#hasProof");
        let has_seed_id       = dict.encode("http://www.w3.org/ns/prob#hasSeed");
        let has_neg_seed_id   = dict.encode("http://www.w3.org/ns/prob#hasNegatedSeed");
        let formula_id        = dict.encode("http://www.w3.org/ns/prob#formula");

        for (triple, formula) in &self.tags {
            let derived_qt = qt_store.encode(triple.subject, triple.predicate, triple.object);

            // prob:proofCount
            let count_lit = format!("\"{}\"^^<{}>", formula.len(), xsd_int);
            let count_id  = dict.encode(&count_lit);
            result.push(Triple { subject: derived_qt, predicate: proof_count_id, object: count_id });

            // prob:formula — raw debug representation of the DNF
            let raw = format!("{:?}", formula);
            let formula_lit = format!("\"{}\"^^<{}>", raw.replace('"', "'"), xsd_str);
            let formula_lit_id = dict.encode(&formula_lit);
            result.push(Triple { subject: derived_qt, predicate: formula_id, object: formula_lit_id });

            // per-proof path annotations
            for (proof_idx, clause) in formula.iter().enumerate() {
                let idx_lit = format!("\"{}\"^^<{}>", proof_idx, xsd_int);
                let idx_id  = dict.encode(&idx_lit);

                // << d >> prob:hasProof "N"^^xsd:integer
                result.push(Triple { subject: derived_qt, predicate: has_proof_id, object: idx_id });

                // Level-2: << << d >> prob:hasProof "N" >> prob:hasSeed << seed >>
                //          or prob:hasNegatedSeed for polarity=false literals
                let proof_annot_qt = qt_store.encode(derived_qt, has_proof_id, idx_id);
                for &(seed_var_id, polarity) in clause {
                    if let Some(seed_t) = self.seed_triples.get(seed_var_id as usize) {
                        let seed_qt = qt_store.encode(
                            seed_t.subject, seed_t.predicate, seed_t.object,
                        );
                        // Polarity-aware predicate: hasSeed for positive, hasNegatedSeed for negative
                        let pred_id = if polarity { has_seed_id } else { has_neg_seed_id };
                        result.push(Triple {
                            subject: proof_annot_qt,
                            predicate: pred_id,
                            object: seed_qt,
                        });
                    }
                }
            }
        }

        result
    }
}

use crate::sdd::SddProvenance;

impl TagStore<SddProvenance> {
    /// Export tags as RDF-star triples enriched with proof-path explanation via SDD model enumeration.
    pub fn encode_as_rdf_star_with_explanation(
        &self,
        dict: &mut Dictionary,
        qt_store: &mut QuotedTripleStore,
    ) -> Vec<Triple> {
        let mut result = self.encode_as_rdf_star(dict, qt_store);

        let mgr = self.provenance.manager().lock().unwrap();

        let xsd_int = "http://www.w3.org/2001/XMLSchema#integer";
        let xsd_str = "http://www.w3.org/2001/XMLSchema#string";
        let proof_count_id    = dict.encode("http://www.w3.org/ns/prob#proofCount");
        let has_proof_id      = dict.encode("http://www.w3.org/ns/prob#hasProof");
        let has_seed_id       = dict.encode("http://www.w3.org/ns/prob#hasSeed");
        let has_neg_seed_id   = dict.encode("http://www.w3.org/ns/prob#hasNegatedSeed");
        let formula_id        = dict.encode("http://www.w3.org/ns/prob#formula");

        for (triple, sdd_id) in &self.tags {
            let derived_qt = qt_store.encode(triple.subject, triple.predicate, triple.object);

            // Enumerate proof paths from the SDD
            let models = mgr.enumerate_models(*sdd_id);

            // prob:proofCount
            let count_lit = format!("\"{}\"^^<{}>", models.len(), xsd_int);
            let count_id  = dict.encode(&count_lit);
            result.push(Triple { subject: derived_qt, predicate: proof_count_id, object: count_id });

            // prob:formula — debug representation
            let raw = format!("{:?}", sdd_id);
            let formula_lit = format!("\"{}\"^^<{}>", raw.replace('"', "'"), xsd_str);
            let formula_lit_id = dict.encode(&formula_lit);
            result.push(Triple { subject: derived_qt, predicate: formula_id, object: formula_lit_id });

            // per-proof path annotations
            for (proof_idx, clause) in models.iter().enumerate() {
                let idx_lit = format!("\"{}\"^^<{}>", proof_idx, xsd_int);
                let idx_id  = dict.encode(&idx_lit);

                result.push(Triple { subject: derived_qt, predicate: has_proof_id, object: idx_id });

                let proof_annot_qt = qt_store.encode(derived_qt, has_proof_id, idx_id);
                for &(seed_var_id, polarity) in clause {
                    if let Some(seed_t) = self.seed_triples.get(seed_var_id as usize) {
                        let seed_qt = qt_store.encode(
                            seed_t.subject, seed_t.predicate, seed_t.object,
                        );
                        let pred_id = if polarity { has_seed_id } else { has_neg_seed_id };
                        result.push(Triple {
                            subject: proof_annot_qt,
                            predicate: pred_id,
                            object: seed_qt,
                        });
                    }
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provenance::{MinMaxProbability, AddMultProbability, BooleanProvenance, WmcProvenance};

    fn make_triple(s: u32, p: u32, o: u32) -> Triple {
        Triple { subject: s, predicate: p, object: o }
    }

    #[test]
    fn default_tag_is_one() {
        let store = TagStore::new(MinMaxProbability);
        let tag = store.get_tag(&make_triple(1, 2, 3));
        assert!((tag - 1.0).abs() < 1e-9);
    }

    #[test]
    fn set_and_get_tag() {
        let mut store = TagStore::new(MinMaxProbability);
        let t = make_triple(1, 2, 3);
        store.set_tag(&t, 0.7);
        assert!((store.get_tag(&t) - 0.7).abs() < 1e-9);
    }

    #[test]
    fn one_tag_not_stored_explicitly() {
        let mut store = TagStore::new(MinMaxProbability);
        let t = make_triple(1, 2, 3);
        store.set_tag(&t, 1.0);
        assert!(!store.has_explicit_tag(&t));
    }

    #[test]
    fn update_disjunction_minmax() {
        let mut store = TagStore::new(MinMaxProbability);
        let t = make_triple(1, 2, 3);
        store.set_tag(&t, 0.5);

        // max(0.5, 0.8) = 0.8 -> changed
        assert!(store.update_disjunction(&t, &0.8));
        assert!((store.get_tag(&t) - 0.8).abs() < 1e-9);

        // max(0.8, 0.6) = 0.8 -> no change (saturated)
        assert!(!store.update_disjunction(&t, &0.6));
        assert!((store.get_tag(&t) - 0.8).abs() < 1e-9);
    }

    #[test]
    fn update_disjunction_addmult() {
        let mut store = TagStore::new(AddMultProbability);
        let t = make_triple(1, 2, 3);
        store.set_tag(&t, 0.3);

        // noisy-OR: 0.3 + 0.4 - 0.3*0.4 = 0.7 - 0.12 = 0.58 -> changed
        assert!(store.update_disjunction(&t, &0.4));
        assert!((store.get_tag(&t) - 0.58).abs() < 1e-9);
    }

    #[test]
    fn boolean_store_basic() {
        let mut store = TagStore::new(BooleanProvenance);
        let t = make_triple(1, 2, 3);
        store.set_tag(&t, false);

        // false ⊕ true = true -> changed
        assert!(store.update_disjunction(&t, &true));
        assert_eq!(store.get_tag(&t), true);

        // true ⊕ true = true -> no change
        assert!(!store.update_disjunction(&t, &true));
    }

    #[test]
    fn rdf_star_encoding() {
        let mut store = TagStore::new(MinMaxProbability);
        store.set_tag(&make_triple(1, 2, 3), 0.75);

        let mut dict = Dictionary::new();
        let mut qt_store = QuotedTripleStore::new();
        let triples = store.encode_as_rdf_star(&mut dict, &mut qt_store);

        assert_eq!(triples.len(), 1);
    }

    fn make_wmc_store_two_proofs() -> TagStore<WmcProvenance> {
        use crate::provenance::{WmcClause, WmcFormula};
        // formula {{(0,T),(1,T)},{(0,T),(2,T)}} — two proof paths sharing seed 0
        let p = WmcProvenance::new();
        let mut store = TagStore::new(p);
        let clause0: WmcClause = [(0u32, true), (1u32, true)].iter().copied().collect();
        let clause1: WmcClause = [(0u32, true), (2u32, true)].iter().copied().collect();
        let formula: WmcFormula = [clause0, clause1].iter().cloned().collect();
        store.tags.insert(make_triple(10, 20, 30), formula);
        // seed_triples: id0 = (1,2,3), id1 = (4,5,6), id2 = (7,8,9)
        store.seed_triples = vec![make_triple(1, 2, 3), make_triple(4, 5, 6), make_triple(7, 8, 9)];
        store
    }

    #[test]
    fn wmc_explanation_proof_count() {
        let store = make_wmc_store_two_proofs();
        let mut dict = Dictionary::new();
        let mut qt = QuotedTripleStore::new();
        let triples = store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt);

        let proof_count_pred = dict.encode("http://www.w3.org/ns/prob#proofCount");
        let count_triples: Vec<_> = triples.iter().filter(|t| t.predicate == proof_count_pred).collect();
        assert_eq!(count_triples.len(), 1, "one proofCount triple per derived fact");

        // The object should encode "2"^^xsd:integer
        let count_str = dict.decode(count_triples[0].object).unwrap();
        assert!(count_str.contains('2'), "proofCount value should contain 2, got: {}", count_str);
    }

    #[test]
    fn wmc_explanation_has_proof_indices() {
        let store = make_wmc_store_two_proofs();
        let mut dict = Dictionary::new();
        let mut qt = QuotedTripleStore::new();
        let triples = store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt);

        let has_proof_pred = dict.encode("http://www.w3.org/ns/prob#hasProof");
        let hp_triples: Vec<_> = triples.iter().filter(|t| t.predicate == has_proof_pred).collect();
        assert_eq!(hp_triples.len(), 2, "one hasProof triple per proof path (indices 0 and 1)");
    }

    #[test]
    fn wmc_explanation_seed_membership() {
        let store = make_wmc_store_two_proofs();
        let mut dict = Dictionary::new();
        let mut qt = QuotedTripleStore::new();
        let triples = store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt);

        let has_seed_pred = dict.encode("http://www.w3.org/ns/prob#hasSeed");
        let hs_triples: Vec<_> = triples.iter().filter(|t| t.predicate == has_seed_pred).collect();
        // clause {0,1} -> 2 seeds, clause {0,2} -> 2 seeds = 4 total
        assert_eq!(hs_triples.len(), 4, "4 hasSeed triples for formula {{0,1}},{{0,2}}");
    }

    #[test]
    fn wmc_explanation_empty_seeds_no_panic() {
        use crate::provenance::{WmcClause, WmcFormula};
        let p = WmcProvenance::new();
        let mut store = TagStore::new(p);
        let clause: WmcClause = [(0u32, true), (1u32, true)].iter().copied().collect();
        let formula: WmcFormula = std::iter::once(clause).collect();
        store.tags.insert(make_triple(10, 20, 30), formula);
        // seed_triples intentionally empty — should produce no hasSeed triples, no panic
        assert!(store.seed_triples.is_empty());

        let mut dict = Dictionary::new();
        let mut qt = QuotedTripleStore::new();
        let triples = store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt);

        let has_seed_pred = dict.encode("http://www.w3.org/ns/prob#hasSeed");
        let hs_count = triples.iter().filter(|t| t.predicate == has_seed_pred).count();
        assert_eq!(hs_count, 0, "no hasSeed triples when seed_triples is empty");
    }
}
