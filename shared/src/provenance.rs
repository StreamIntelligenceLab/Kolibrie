/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

/// Trait defining a provenance semiring for annotated datalog evaluation.
///
/// Implementors provide a tag space with identity elements, semiring operations
/// (disjunction ⊕, conjunction ⊗), negation, saturation, and probability I/O.
pub trait Provenance: Clone + 'static {
    /// The type of annotations (tags) on derived facts.
    type Tag: Clone + PartialEq + std::fmt::Debug;

    /// Additive identity (false / empty derivation).
    fn zero(&self) -> Self::Tag;

    /// Multiplicative identity (true / certain fact).
    fn one(&self) -> Self::Tag;

    /// Disjunction (⊕): combines tags from alternative derivation paths.
    /// Used when the same fact is derived via multiple rule applications.
    fn disjunction(&self, a: &Self::Tag, b: &Self::Tag) -> Self::Tag;

    /// Conjunction (⊗): combines tags from joined premises.
    /// Used when multiple premise triples contribute to a single derivation.
    fn conjunction(&self, a: &Self::Tag, b: &Self::Tag) -> Self::Tag;

    /// Negation (⊖): used for stratified negation-as-failure.
    fn negate(&self, a: &Self::Tag) -> Self::Tag;

    /// Saturation (⊜): applied at fixpoint to stabilize tags.
    /// For simple provenances this is typically the identity function.
    fn saturate(&self, a: &Self::Tag) -> Self::Tag;

    /// Convert a probability value (from input data) to a tag.
    fn tag_from_probability(&self, prob: f64) -> Self::Tag;

    /// Convert a probability and a unique input-fact index to a tag.
    /// Default ignores `_id` and delegates to `tag_from_probability`.
    /// Override for provenances that track variable identities (e.g. TopKProofs).
    fn tag_from_probability_with_id(&self, prob: f64, _id: usize) -> Self::Tag {
        self.tag_from_probability(prob)
    }

    /// Recover a probability value from a tag (for output / RDF-star encoding).
    fn recover_probability(&self, tag: &Self::Tag) -> f64;

    /// Check whether a tag has converged (old ≈ new after saturation).
    /// Returns `true` if the tag is considered stable.
    fn is_saturated(&self, old: &Self::Tag, new: &Self::Tag) -> bool;
}

/// Epsilon for floating-point convergence comparisons.
const PROB_EPSILON: f64 = 1e-9;

/// Min-max probability provenance.
///
/// - Tag: f64 in [0, 1]; ⊕ = max, ⊗ = min
/// - Possibilistic semantics (fuzzy Datalog).
#[derive(Debug, Clone)]
pub struct MinMaxProbability;

impl Provenance for MinMaxProbability {
    type Tag = f64;

    fn zero(&self) -> f64 { 0.0 }
    fn one(&self) -> f64 { 1.0 }

    fn disjunction(&self, a: &f64, b: &f64) -> f64 {
        a.max(*b)
    }

    fn conjunction(&self, a: &f64, b: &f64) -> f64 {
        a.min(*b)
    }

    fn negate(&self, a: &f64) -> f64 {
        1.0 - a
    }

    fn saturate(&self, a: &f64) -> f64 {
        *a
    }

    fn tag_from_probability(&self, prob: f64) -> f64 {
        prob.clamp(0.0, 1.0)
    }

    fn recover_probability(&self, tag: &f64) -> f64 {
        *tag
    }

    fn is_saturated(&self, old: &f64, new: &f64) -> bool {
        (old - new).abs() < PROB_EPSILON
    }
}

/// Add-multiply probability provenance.
///
/// - Tag: f64 in [0, 1]; ⊕ = noisy-OR, ⊗ = multiplication
/// - Models independent probabilistic events.
#[derive(Debug, Clone)]
pub struct AddMultProbability;

impl Provenance for AddMultProbability {
    type Tag = f64;

    fn zero(&self) -> f64 { 0.0 }
    fn one(&self) -> f64 { 1.0 }

    fn disjunction(&self, a: &f64, b: &f64) -> f64 {
        a + b - a * b
    }

    fn conjunction(&self, a: &f64, b: &f64) -> f64 {
        a * b
    }

    fn negate(&self, a: &f64) -> f64 {
        1.0 - a
    }

    fn saturate(&self, a: &f64) -> f64 {
        *a
    }

    fn tag_from_probability(&self, prob: f64) -> f64 {
        prob.clamp(0.0, 1.0)
    }

    fn recover_probability(&self, tag: &f64) -> f64 {
        *tag
    }

    fn is_saturated(&self, old: &f64, new: &f64) -> bool {
        (old - new).abs() < PROB_EPSILON
    }
}

/// Boolean provenance (classical two-valued logic).
///
/// - Tag: bool; ⊕ = OR, ⊗ = AND
/// - No probability tracking; equivalent to classical datalog.
#[derive(Debug, Clone)]
pub struct BooleanProvenance;

impl Provenance for BooleanProvenance {
    type Tag = bool;

    fn zero(&self) -> bool { false }
    fn one(&self) -> bool { true }

    fn disjunction(&self, a: &bool, b: &bool) -> bool {
        *a || *b
    }

    fn conjunction(&self, a: &bool, b: &bool) -> bool {
        *a && *b
    }

    fn negate(&self, a: &bool) -> bool {
        !a
    }

    fn saturate(&self, a: &bool) -> bool {
        *a
    }

    fn tag_from_probability(&self, prob: f64) -> bool {
        prob > 0.0
    }

    fn recover_probability(&self, tag: &bool) -> f64 {
        if *tag { 1.0 } else { 0.0 }
    }

    fn is_saturated(&self, old: &bool, new: &bool) -> bool {
        old == new
    }
}

/// A proof is a set of input-variable IDs (all must hold simultaneously).
pub type Proof = BTreeSet<u32>;
/// A tag is at most k proofs, ranked by descending probability.
pub type TopKTag = Vec<Proof>;

/// Top-K proof-tracking provenance.
///
/// Retains the k most probable proof paths for each derived fact.
/// Probability is recovered from the retained proofs via weighted model counting.
///
/// Larger k gives more accurate results at higher cost; k ≤ 10 is recommended for
/// most use cases. `k` must be in `[1, 63]`.
#[derive(Debug, Clone)]
pub struct TopKProofs {
    pub k: usize,
    prob_table: Arc<Mutex<Vec<f64>>>,
}

impl TopKProofs {
    pub fn new(k: usize) -> Self {
        assert!(k >= 1 && k <= 63, "k must be in [1, 63]: u64 bitmask limit in recover_probability");
        Self { k, prob_table: Arc::new(Mutex::new(Vec::new())) }
    }
}

fn proof_prob(proof: &Proof, table: &[f64]) -> f64 {
    proof.iter().map(|&v| *table.get(v as usize).unwrap_or(&1.0)).product()
}

impl Provenance for TopKProofs {
    type Tag = TopKTag;

    fn zero(&self) -> TopKTag { vec![] }
    fn one(&self) -> TopKTag { vec![BTreeSet::new()] }

    /// ⊕ = union, dedup, sort by descending probability, truncate to k
    fn disjunction(&self, a: &TopKTag, b: &TopKTag) -> TopKTag {
        let mut merged: Vec<Proof> = a.iter().chain(b.iter()).cloned().collect();
        merged.sort();
        merged.dedup();
        let table = self.prob_table.lock().unwrap();
        merged.sort_by(|p, q|
            proof_prob(q, &table).partial_cmp(&proof_prob(p, &table))
                .unwrap_or(std::cmp::Ordering::Equal));
        drop(table);
        merged.truncate(self.k);
        merged
    }

    /// ⊗ = Cartesian product — merge variable ID sets (BTreeSet handles shared IDs)
    fn conjunction(&self, a: &TopKTag, b: &TopKTag) -> TopKTag {
        if a.is_empty() || b.is_empty() { return vec![]; }
        let mut result: Vec<Proof> = a.iter().flat_map(|pa|
            b.iter().map(move |pb| pa.iter().chain(pb.iter()).copied().collect())
        ).collect();
        result.sort();
        result.dedup();
        let table = self.prob_table.lock().unwrap();
        result.sort_by(|p, q|
            proof_prob(q, &table).partial_cmp(&proof_prob(p, &table))
                .unwrap_or(std::cmp::Ordering::Equal));
        drop(table);
        result.truncate(self.k);
        result
    }

    /// Approximate negation. Returns `one()` for empty tags, `zero()` for certain facts;
    /// otherwise allocates a synthetic seed with probability `1 − p`.
    /// Use `WmcProvenance` for exact correlation-aware negation.
    fn negate(&self, a: &TopKTag) -> TopKTag {
        if a.is_empty() {
            return self.one();
        }
        let p = self.recover_probability(a);
        let complement = (1.0_f64 - p).clamp(0.0, 1.0);
        if complement <= 0.0 {
            return self.zero();
        }
        let mut table = self.prob_table.lock().unwrap();
        let new_id = table.len() as u32;
        table.push(complement);
        drop(table);
        let mut proof = BTreeSet::new();
        proof.insert(new_id);
        vec![proof]
    }

    fn saturate(&self, a: &TopKTag) -> TopKTag { a.clone() }

    fn tag_from_probability(&self, prob: f64) -> TopKTag {
        let mut table = self.prob_table.lock().unwrap();
        let id = table.len() as u32;
        table.push(prob.clamp(0.0, 1.0));
        let mut proof = BTreeSet::new();
        proof.insert(id);
        vec![proof]
    }

    fn tag_from_probability_with_id(&self, prob: f64, id: usize) -> TopKTag {
        let mut table = self.prob_table.lock().unwrap();
        if id >= table.len() { table.resize(id + 1, 0.0); }
        table[id] = prob.clamp(0.0, 1.0);
        drop(table);
        let mut proof = BTreeSet::new();
        proof.insert(id as u32);
        vec![proof]
    }

    /// Computes the probability of a derived fact from its retained proof paths.
    /// Result is an approximation when fewer than all proof paths were retained.
    fn recover_probability(&self, tag: &TopKTag) -> f64 {
        if tag.is_empty() { return 0.0; }
        let table = self.prob_table.lock().unwrap();
        let m = tag.len();
        let mut total = 0.0_f64;
        for mask in 1u64..(1u64 << m) {
            let sign = if mask.count_ones() % 2 == 1 { 1.0 } else { -1.0 };
            let vars: BTreeSet<u32> = (0..m)
                .filter(|&i| mask & (1 << i) != 0)
                .flat_map(|i| tag[i].iter().copied())
                .collect();
            let prod: f64 = vars.iter()
                .map(|&v| *table.get(v as usize).unwrap_or(&1.0))
                .product();
            total += sign * prod;
        }
        total.clamp(0.0, 1.0)
    }

    fn is_saturated(&self, old: &TopKTag, new: &TopKTag) -> bool { old == new }
}

/// A signed literal: `(seed_id, polarity)` where `true` = positive, `false` = negated.
pub type WmcLiteral = (u32, bool);
/// A single conjunction of signed seed literals (one proof path).
pub type WmcClause = BTreeSet<WmcLiteral>;
/// A DNF formula: disjunction of conjunctions — the complete proof formula.
pub type WmcFormula = BTreeSet<WmcClause>;

/// WMC provenance — exact Weighted Model Count via recursive Shannon expansion.
///
/// ⊕ = set-union of DNF clauses (subsumption-pruned)
/// ⊗ = Cartesian-product of DNF clauses (contradictory + subsumed pruned)
/// `negate` = De Morgan complement (signed literals, exact)
/// `recover_probability` = Shannon WMC with memoization
#[derive(Debug, Clone)]
pub struct DnfWmcProvenance {
    prob_table: Arc<Mutex<Vec<f64>>>,
}

impl DnfWmcProvenance {
    pub fn new() -> Self {
        Self { prob_table: Arc::new(Mutex::new(Vec::new())) }
    }
}

impl Default for DnfWmcProvenance {
    fn default() -> Self { Self::new() }
}

/// Backward-compatible alias. Use [`DnfWmcProvenance`] for explicit DNF or
/// [`crate::sdd::SddProvenance`] for the faster SDD-based implementation.
pub type WmcProvenance = DnfWmcProvenance;

/// Remove clauses subsumed by a shorter clause in the same formula.
fn remove_subsumed(formula: WmcFormula) -> WmcFormula {
    let clauses: Vec<WmcClause> = formula.into_iter().collect();
    clauses.iter()
        .filter(|c1| !clauses.iter().any(|c2| c2 != *c1 && c2.is_subset(c1)))
        .cloned()
        .collect()
}

/// Remove contradictory clauses (those containing both `(v, true)` and `(v, false)`).
pub(crate) fn remove_contradictory(formula: WmcFormula) -> WmcFormula {
    formula.into_iter()
        .filter(|c| !c.iter().any(|&(v, pol)| c.contains(&(v, !pol))))
        .collect()
}

fn shannon_wmc(
    formula: &WmcFormula,
    table: &[f64],
    memo: &mut std::collections::HashMap<WmcFormula, f64>,
) -> f64 {
    if formula.is_empty() { return 0.0; }
    if formula.contains(&BTreeSet::new()) { return 1.0; }
    if let Some(&cached) = memo.get(formula) { return cached; }

    let x = formula.iter().flat_map(|c| c.iter()).map(|&(v, _)| v).min().unwrap();
    let px = *table.get(x as usize).unwrap_or(&1.0);

    let phi_true: WmcFormula = formula.iter()
        .filter(|c| !c.contains(&(x, false)))
        .map(|c| c.iter().filter(|&&(v, _)| v != x).copied().collect())
        .collect();
    let phi_false: WmcFormula = formula.iter()
        .filter(|c| !c.contains(&(x, true)))
        .map(|c| c.iter().filter(|&&(v, _)| v != x).copied().collect())
        .collect();

    let result = px * shannon_wmc(&phi_true, table, memo)
               + (1.0 - px) * shannon_wmc(&phi_false, table, memo);
    memo.insert(formula.clone(), result);
    result
}

impl Provenance for DnfWmcProvenance {
    type Tag = WmcFormula;

    fn zero(&self) -> WmcFormula { BTreeSet::new() }
    fn one(&self) -> WmcFormula { std::iter::once(BTreeSet::new()).collect() }

    fn disjunction(&self, a: &WmcFormula, b: &WmcFormula) -> WmcFormula {
        let union: WmcFormula = a.iter().chain(b.iter()).cloned().collect();
        remove_subsumed(union)
    }

    fn conjunction(&self, a: &WmcFormula, b: &WmcFormula) -> WmcFormula {
        if a.is_empty() || b.is_empty() { return self.zero(); }
        let product: WmcFormula = a.iter().flat_map(|ca|
            b.iter().map(move |cb| ca.iter().chain(cb.iter()).copied().collect::<WmcClause>())
        ).collect();
        remove_subsumed(remove_contradictory(product))
    }

    /// Exact negation over the proof formula, preserving correlations between literals.
    fn negate(&self, a: &WmcFormula) -> WmcFormula {
        if a.is_empty() { return self.one(); }
        if a.contains(&BTreeSet::new()) { return self.zero(); }
        let mut result = self.one();
        for clause in a {
            if result.is_empty() { break; }
            let neg_clause: WmcFormula = clause.iter()
                .map(|&(v, pol)| std::iter::once((v, !pol)).collect::<WmcClause>())
                .collect();
            result = self.conjunction(&result, &neg_clause);
        }
        result
    }

    fn saturate(&self, a: &WmcFormula) -> WmcFormula { a.clone() }

    fn tag_from_probability(&self, prob: f64) -> WmcFormula {
        let mut table = self.prob_table.lock().unwrap();
        let id = table.len() as u32;
        table.push(prob.clamp(0.0, 1.0));
        std::iter::once(std::iter::once((id, true)).collect()).collect()
    }

    fn tag_from_probability_with_id(&self, prob: f64, id: usize) -> WmcFormula {
        let mut table = self.prob_table.lock().unwrap();
        if id >= table.len() { table.resize(id + 1, 0.0); }
        table[id] = prob.clamp(0.0, 1.0);
        drop(table);
        std::iter::once(std::iter::once((id as u32, true)).collect()).collect()
    }

    fn recover_probability(&self, tag: &WmcFormula) -> f64 {
        if tag.is_empty() { return 0.0; }
        let table = self.prob_table.lock().unwrap();
        let mut memo = std::collections::HashMap::new();
        shannon_wmc(tag, &table, &mut memo).clamp(0.0, 1.0)
    }

    fn is_saturated(&self, old: &WmcFormula, new: &WmcFormula) -> bool { old == new }
}

/// Expiration-time provenance semiring
#[derive(Debug, Clone)]
pub struct ExpirationProvenance;

impl Provenance for ExpirationProvenance {
    type Tag = u64;

    fn zero(&self) -> u64 { 0 }
    fn one(&self) -> u64 { u64::MAX }

    /// ⊕ = max: multiple derivation paths — keep the longest-lived
    fn disjunction(&self, a: &u64, b: &u64) -> u64 { (*a).max(*b) }

    /// ⊗ = min: joined premises — expiry bounded by the weakest premise
    fn conjunction(&self, a: &u64, b: &u64) -> u64 { (*a).min(*b) }

    fn negate(&self, _a: &u64) -> u64 { 0 }
    fn saturate(&self, a: &u64) -> u64 { *a }
    fn tag_from_probability(&self, _: f64) -> u64 { u64::MAX }
    fn recover_probability(&self, tag: &u64) -> f64 { *tag as f64 }
    fn is_saturated(&self, old: &u64, new: &u64) -> bool { old == new }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minmax_identities() {
        let p = MinMaxProbability;
        let a = 0.7;
        // ⊕ identity: a ⊕ 0 = a
        assert!((p.disjunction(&a, &p.zero()) - a).abs() < PROB_EPSILON);
        // ⊗ identity: a ⊗ 1 = a
        assert!((p.conjunction(&a, &p.one()) - a).abs() < PROB_EPSILON);
    }

    #[test]
    fn minmax_annihilator() {
        let p = MinMaxProbability;
        // a ⊗ 0 = 0
        assert!((p.conjunction(&0.7, &p.zero()) - p.zero()).abs() < PROB_EPSILON);
    }

    #[test]
    fn minmax_commutativity() {
        let p = MinMaxProbability;
        let (a, b) = (0.3, 0.8);
        assert!((p.disjunction(&a, &b) - p.disjunction(&b, &a)).abs() < PROB_EPSILON);
        assert!((p.conjunction(&a, &b) - p.conjunction(&b, &a)).abs() < PROB_EPSILON);
    }

    #[test]
    fn minmax_associativity() {
        let p = MinMaxProbability;
        let (a, b, c) = (0.3, 0.5, 0.8);
        let lhs = p.disjunction(&p.disjunction(&a, &b), &c);
        let rhs = p.disjunction(&a, &p.disjunction(&b, &c));
        assert!((lhs - rhs).abs() < PROB_EPSILON);

        let lhs = p.conjunction(&p.conjunction(&a, &b), &c);
        let rhs = p.conjunction(&a, &p.conjunction(&b, &c));
        assert!((lhs - rhs).abs() < PROB_EPSILON);
    }

    #[test]
    fn addmult_identities() {
        let p = AddMultProbability;
        let a = 0.7;
        assert!((p.disjunction(&a, &p.zero()) - a).abs() < PROB_EPSILON);
        assert!((p.conjunction(&a, &p.one()) - a).abs() < PROB_EPSILON);
    }

    #[test]
    fn addmult_annihilator() {
        let p = AddMultProbability;
        assert!((p.conjunction(&0.7, &p.zero()) - p.zero()).abs() < PROB_EPSILON);
    }

    #[test]
    fn addmult_commutativity() {
        let p = AddMultProbability;
        let (a, b) = (0.3, 0.4);
        assert!((p.disjunction(&a, &b) - p.disjunction(&b, &a)).abs() < PROB_EPSILON);
        assert!((p.conjunction(&a, &b) - p.conjunction(&b, &a)).abs() < PROB_EPSILON);
    }

    #[test]
    fn addmult_conjunction_is_product() {
        let p = AddMultProbability;
        let result = p.conjunction(&0.8, &0.7);
        assert!((result - 0.56).abs() < PROB_EPSILON);
    }

    #[test]
    fn addmult_disjunction_noisy_or() {
        let p = AddMultProbability;
        let result = p.disjunction(&0.7, &0.6);
        // noisy-OR: 0.7 + 0.6 - 0.7*0.6 = 1.3 - 0.42 = 0.88
        assert!((result - 0.88).abs() < PROB_EPSILON);
    }

    #[test]
    fn boolean_identities() {
        let p = BooleanProvenance;
        assert_eq!(p.disjunction(&true, &p.zero()), true);
        assert_eq!(p.disjunction(&false, &p.zero()), false);
        assert_eq!(p.conjunction(&true, &p.one()), true);
        assert_eq!(p.conjunction(&false, &p.one()), false);
    }

    #[test]
    fn boolean_annihilator() {
        let p = BooleanProvenance;
        assert_eq!(p.conjunction(&true, &p.zero()), false);
    }

    #[test]
    fn minmax_roundtrip() {
        let p = MinMaxProbability;
        let tag = p.tag_from_probability(0.75);
        assert!((p.recover_probability(&tag) - 0.75).abs() < PROB_EPSILON);
    }

    #[test]
    fn boolean_from_probability() {
        let p = BooleanProvenance;
        assert_eq!(p.tag_from_probability(0.5), true);
        assert_eq!(p.tag_from_probability(0.0), false);
    }

    #[test]
    fn minmax_saturation_convergence() {
        let p = MinMaxProbability;
        assert!(p.is_saturated(&0.7, &0.7));
        assert!(!p.is_saturated(&0.7, &0.8));
    }

    #[test]
    fn boolean_saturation_convergence() {
        let p = BooleanProvenance;
        assert!(p.is_saturated(&true, &true));
        assert!(!p.is_saturated(&true, &false));
    }

    fn make_proof(ids: &[u32]) -> Proof {
        ids.iter().copied().collect()
    }

    #[test]
    fn topk_zero_and_one() {
        let p = TopKProofs::new(5);
        assert_eq!(p.zero(), vec![]);
        assert_eq!(p.one(), vec![BTreeSet::new()]);
    }

    #[test]
    fn topk_conjunction_identity() {
        let p = TopKProofs::new(5);
        // a ⊗ one = a
        let a = vec![make_proof(&[0])];
        let result = p.conjunction(&a, &p.one());
        assert_eq!(result, a);
        // zero ⊗ a = zero
        let zero_result = p.conjunction(&p.zero(), &a);
        assert_eq!(zero_result, vec![]);
    }

    #[test]
    fn topk_conjunction_shared_variable() {
        let p = TopKProofs::new(5);
        // {0} ⊗ {0, 1} = {{0, 1}} (union, no duplication)
        let a = vec![make_proof(&[0])];
        let b = vec![make_proof(&[0, 1])];
        let result = p.conjunction(&a, &b);
        assert_eq!(result, vec![make_proof(&[0, 1])]);
    }

    #[test]
    fn topk_disjunction_dedup_and_truncate() {
        let p = TopKProofs::new(2);
        // [{0}] ⊕ [{0}, {1}] = [{0}, {1}] (dedup, truncate to 2)
        let a = vec![make_proof(&[0])];
        let b = vec![make_proof(&[0]), make_proof(&[1])];
        let result = p.disjunction(&a, &b);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn topk_wmc_single_proof() {
        let p = TopKProofs::new(5);
        p.tag_from_probability_with_id(0.8, 0);
        let tag = vec![make_proof(&[0])];
        let prob = p.recover_probability(&tag);
        assert!((prob - 0.8).abs() < PROB_EPSILON, "expected 0.8, got {}", prob);
    }

    #[test]
    fn topk_wmc_two_independent() {
        let p = TopKProofs::new(5);
        p.tag_from_probability_with_id(0.8, 0);
        p.tag_from_probability_with_id(0.6, 1);
        // {0} and {1} are independent — noisy-OR: 0.8 + 0.6 - 0.48 = 0.92
        let tag = vec![make_proof(&[0]), make_proof(&[1])];
        let prob = p.recover_probability(&tag);
        assert!((prob - 0.92).abs() < PROB_EPSILON, "expected 0.92, got {}", prob);
    }

    #[test]
    fn topk_wmc_overlap_canonical() {
        // Canonical overlap example: proof1={0,1}, proof2={0,2}
        // P(x)=0.8, P(y)=0.6, P(z)=0.5
        // Exact: P(x∧y) + P(x∧z) - P(x∧y∧z) = 0.48 + 0.40 - 0.24 = 0.64
        // noisy-OR of 0.48 and 0.40 = 0.688 (wrong — overcounts x)
        let p = TopKProofs::new(5);
        p.tag_from_probability_with_id(0.8, 0); // x
        p.tag_from_probability_with_id(0.6, 1); // y
        p.tag_from_probability_with_id(0.5, 2); // z
        let tag = vec![make_proof(&[0, 1]), make_proof(&[0, 2])];
        let wmc = p.recover_probability(&tag);
        assert!((wmc - 0.64).abs() < PROB_EPSILON, "expected 0.64, got {}", wmc);
    }

    #[test]
    fn topk_saturation() {
        let p = TopKProofs::new(5);
        let a = vec![make_proof(&[0])];
        let b = vec![make_proof(&[1])];
        assert!(p.is_saturated(&a, &a));
        assert!(!p.is_saturated(&a, &b));
    }

}
