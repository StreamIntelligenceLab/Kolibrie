/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::sparql_database::SparqlDatabase;
use shared::dataset_index::GraphId;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

/// Non-cryptographic hasher for dictionary ids (sequential, not user-controlled)
#[derive(Default, Clone, Copy)]
pub struct TermHasher(u64);

impl TermHasher {
    /// splitmix64 mixing step
    #[inline]
    fn mix(&mut self, value: u64) {
        let mut z = self
            .0
            .wrapping_add(value)
            .wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        self.0 = z ^ (z >> 31);
    }
}

impl Hasher for TermHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    /// Handles writes other than the integer fast paths, e.g. the `GraphId` discriminant
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut buffer = [0u8; 8];
            buffer[..chunk.len()].copy_from_slice(chunk);
            self.mix(u64::from_le_bytes(buffer));
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.mix(value as u64)
    }
    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.mix(value as u64)
    }
    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.mix(value)
    }
    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.mix(value as u64)
    }
    #[inline]
    fn write_isize(&mut self, value: isize) {
        self.mix(value as u64)
    }
}

pub type TermBuildHasher = BuildHasherDefault<TermHasher>;
/// A map keyed by dictionary id
pub type TermMap<V> = HashMap<u32, V, TermBuildHasher>;
/// A set of dictionary ids
pub type TermSet = HashSet<u32, TermBuildHasher>;
/// A map keyed by graph identity
pub type GraphMap<V> = HashMap<GraphId, V, TermBuildHasher>;

/// Above this many quads the statistics are estimated from a sample rather
const EXACT_SAMPLING_THRESHOLD: u64 = 100_000;

/// Deterministic quad hash used for sampling, stable across processes
fn stable_quad_hash(graph: GraphId, subject: u32, predicate: u32, object: u32) -> u64 {
    // Offset named ids so `Default` and `Named(0)` hash differently
    let graph_key = match graph {
        GraphId::Default => 0u64,
        GraphId::Named(id) => id as u64 + 1,
    };

    // FNV-1a over the four components
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for component in [graph_key, subject as u64, predicate as u64, object as u64] {
        hash ^= component;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }

    // Avalanche finalizer; the sampler uses the low bits
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^= hash >> 33;
    hash
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SamplingPlan {
    step: u64,
}

impl SamplingPlan {
    fn for_total(total: u64) -> Self {
        if total <= EXACT_SAMPLING_THRESHOLD {
            return SamplingPlan { step: 1 };
        }

        SamplingPlan {
            step: total.div_ceil(EXACT_SAMPLING_THRESHOLD),
        }
    }

    fn is_exact(&self) -> bool {
        self.step <= 1
    }

    /// Returns true if the quad is in the sample (hash-based, independent of iteration order)
    fn accepts(&self, graph: GraphId, subject: u32, predicate: u32, object: u32) -> bool {
        self.is_exact() || stable_quad_hash(graph, subject, predicate, object) % self.step == 0
    }
}

/// Scale factor from sampled counts to dataset totals
#[derive(Debug, Clone, Copy)]
struct SampleScale {
    scale: f64,
    exact: bool,
}

impl SampleScale {
    /// Uses the observed sample size rather than the expected one
    fn new(plan: SamplingPlan, total: u64, observed: u64) -> Self {
        if plan.is_exact() || observed == 0 {
            return SampleScale {
                scale: 1.0,
                exact: true,
            };
        }
        SampleScale {
            scale: total as f64 / observed as f64,
            exact: false,
        }
    }

    fn scale_up(&self, observed: u64) -> u64 {
        if self.exact {
            observed
        } else {
            (observed as f64 * self.scale).round() as u64
        }
    }
}

/// Database statistics for cost-based optimization
#[derive(Debug)]
pub struct DatabaseStats {
    pub total_triples: u64,
    pub quoted_triple_count: u64,
    pub named_graph_count: u64,
    pub graph_cardinalities: GraphMap<u64>,
    pub predicate_cardinalities: TermMap<u64>,
    pub subject_cardinalities: TermMap<u64>,
    pub object_cardinalities: TermMap<u64>,
    /// Join-key domain sizes: a join through predicate `p` fans out by roughly `cardinality(p) / distinct(p)` rows per binding
    pub predicate_distinct_subjects: TermMap<u64>,
    pub predicate_distinct_objects: TermMap<u64>,
    /// Dataset-wide domain sizes, used when a pattern leaves its predicate unbound
    pub distinct_subjects: u64,
    pub distinct_objects: u64,
}

impl DatabaseStats {
    /// Creates a new empty DatabaseStats instance
    pub fn new() -> Self {
        Self {
            total_triples: 0,
            quoted_triple_count: 0,
            named_graph_count: 0,
            graph_cardinalities: GraphMap::default(),
            predicate_cardinalities: TermMap::default(),
            subject_cardinalities: TermMap::default(),
            object_cardinalities: TermMap::default(),
            predicate_distinct_subjects: TermMap::default(),
            predicate_distinct_objects: TermMap::default(),
            distinct_subjects: 0,
            distinct_objects: 0,
        }
    }

    /// Gathers statistics from the database using sampling for performance
    pub fn gather_stats_fast(database: &SparqlDatabase) -> Self {
        let index = &database.dataset_index;

        // Pass 1: exact per-graph and total quad counts
        let named_graphs = index.named_graphs();
        let mut graph_cardinalities =
            GraphMap::with_capacity_and_hasher(named_graphs.len() + 1, TermBuildHasher::default());
        let mut total_triples = index.count_graph(GraphId::Default) as u64;
        graph_cardinalities.insert(GraphId::Default, total_triples);
        for graph in &named_graphs {
            let size = index.count_graph(*graph) as u64;
            total_triples += size;
            graph_cardinalities.insert(*graph, size);
        }
        let named_graph_count = named_graphs.len() as u64;

        // Pass 2: term statistics over the sampled quads
        let plan = SamplingPlan::for_total(total_triples);
        let mut sampled: u64 = 0;
        let mut predicate_cardinalities = TermMap::<u64>::default();
        let mut subject_cardinalities = TermMap::<u64>::default();
        let mut object_cardinalities = TermMap::<u64>::default();
        let mut predicate_subjects = TermMap::<TermSet>::default();
        let mut predicate_objects = TermMap::<TermSet>::default();
        let mut all_subjects = TermSet::default();
        let mut all_objects = TermSet::default();

        index.for_each_quad(|graph, subject, predicate, object| {
            if !plan.accepts(graph, subject, predicate, object) {
                return;
            }
            sampled += 1;
            *predicate_cardinalities.entry(predicate).or_insert(0) += 1;
            *subject_cardinalities.entry(subject).or_insert(0) += 1;
            *object_cardinalities.entry(object).or_insert(0) += 1;
            predicate_subjects
                .entry(predicate)
                .or_default()
                .insert(subject);
            predicate_objects
                .entry(predicate)
                .or_default()
                .insert(object);
            all_subjects.insert(subject);
            all_objects.insert(object);
        });

        // Scale sampled counts back up to the whole dataset
        let scale = SampleScale::new(plan, total_triples, sampled);
        predicate_cardinalities
            .values_mut()
            .for_each(|v| *v = scale.scale_up(*v));
        subject_cardinalities
            .values_mut()
            .for_each(|v| *v = scale.scale_up(*v));
        object_cardinalities
            .values_mut()
            .for_each(|v| *v = scale.scale_up(*v));

        // Distinct counts do not grow linearly with the sample, so each is capped at its cardinality
        let scale_distinct =
            |observed: u64, cardinality: u64| scale.scale_up(observed).min(cardinality).max(1);
        let predicate_distinct_subjects = predicate_subjects
            .into_iter()
            .map(|(predicate, subjects)| {
                let cardinality = predicate_cardinalities.get(&predicate).copied().unwrap_or(0);
                (predicate, scale_distinct(subjects.len() as u64, cardinality))
            })
            .collect();
        let predicate_distinct_objects = predicate_objects
            .into_iter()
            .map(|(predicate, objects)| {
                let cardinality = predicate_cardinalities.get(&predicate).copied().unwrap_or(0);
                (predicate, scale_distinct(objects.len() as u64, cardinality))
            })
            .collect();
        let distinct_subjects = scale_distinct(all_subjects.len() as u64, total_triples);
        let distinct_objects = scale_distinct(all_objects.len() as u64, total_triples);

        let quoted_triple_count = database.quoted_triple_store.read().unwrap().len() as u64;

        Self {
            total_triples,
            quoted_triple_count,
            named_graph_count,
            graph_cardinalities,
            predicate_cardinalities,
            subject_cardinalities,
            object_cardinalities,
            predicate_distinct_subjects,
            predicate_distinct_objects,
            distinct_subjects,
            distinct_objects,
        }
    }

    /// Distinct subjects reached through a predicate
    pub fn get_predicate_distinct_subjects(&self, predicate: u32) -> u64 {
        self.predicate_distinct_subjects
            .get(&predicate)
            .copied()
            .unwrap_or(0)
    }

    /// Distinct objects reached through a predicate
    pub fn get_predicate_distinct_objects(&self, predicate: u32) -> u64 {
        self.predicate_distinct_objects
            .get(&predicate)
            .copied()
            .unwrap_or(0)
    }

    /// Number of distinct predicates observed
    pub fn distinct_predicates(&self) -> u64 {
        self.predicate_cardinalities.len() as u64
    }

    /// Gets the cardinality for a predicate
    pub fn get_predicate_cardinality(&self, predicate: u32) -> u64 {
        self.predicate_cardinalities
            .get(&predicate)
            .copied()
            .unwrap_or(0)
    }

    /// Gets the cardinality for a subject
    pub fn get_subject_cardinality(&self, subject: u32) -> u64 {
        self.subject_cardinalities
            .get(&subject)
            .copied()
            .unwrap_or(0)
    }

    /// Gets the cardinality for an object
    pub fn get_object_cardinality(&self, object: u32) -> u64 {
        self.object_cardinalities.get(&object).copied().unwrap_or(0)
    }

    pub fn get_graph_cardinality(&self, graph: GraphId) -> u64 {
        self.graph_cardinalities.get(&graph).copied().unwrap_or(0)
    }

    /// Updates statistics with new data
    pub fn update_stats(&mut self, subject: u32, predicate: u32, object: u32) {
        self.total_triples += 1;
        *self.predicate_cardinalities.entry(predicate).or_insert(0) += 1;
        *self.subject_cardinalities.entry(subject).or_insert(0) += 1;
        *self.object_cardinalities.entry(object).or_insert(0) += 1;
    }

    /// Removes statistics for deleted data
    pub fn remove_stats(&mut self, subject: u32, predicate: u32, object: u32) {
        if self.total_triples > 0 {
            self.total_triples -= 1;
        }

        if let Some(count) = self.predicate_cardinalities.get_mut(&predicate) {
            if *count > 0 {
                *count -= 1;
            }
        }

        if let Some(count) = self.subject_cardinalities.get_mut(&subject) {
            if *count > 0 {
                *count -= 1;
            }
        }

        if let Some(count) = self.object_cardinalities.get_mut(&object) {
            if *count > 0 {
                *count -= 1;
            }
        }
    }
}

impl Default for DatabaseStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Number of quads accepted by the plan
    fn sample_size(plan: SamplingPlan, quads: &[(GraphId, u32, u32, u32)]) -> u64 {
        quads
            .iter()
            .filter(|(g, s, p, o)| plan.accepts(*g, *s, *p, *o))
            .count() as u64
    }

    fn synthetic_quads(count: u32) -> Vec<(GraphId, u32, u32, u32)> {
        (0..count)
            .map(|i| (GraphId::Default, i, i % 7, i * 3))
            .collect()
    }

    #[test]
    fn datasets_at_or_below_the_threshold_are_counted_exactly() {
        for total in [0, 1, 99_999, EXACT_SAMPLING_THRESHOLD] {
            let plan = SamplingPlan::for_total(total);
            assert_eq!(plan.step, 1, "total {}", total);
            assert!(plan.is_exact(), "total {}", total);
            assert!(plan.accepts(GraphId::Default, 7, 8, 9), "total {}", total);
            let scale = SampleScale::new(plan, total, total);
            assert_eq!(scale.scale_up(42), 42, "total {}", total);
        }
    }

    #[test]
    fn just_past_the_threshold_the_step_is_two_not_one() {
        // An integer `total / budget` truncated to 1 here, sampling the first
        let plan = SamplingPlan::for_total(EXACT_SAMPLING_THRESHOLD + 1);
        assert_eq!(plan.step, 2);
        assert!(!plan.is_exact());
        let scale = SampleScale::new(plan, EXACT_SAMPLING_THRESHOLD + 1, 50_000);
        assert!(scale.scale > 1.0);
    }

    #[test]
    fn the_whole_old_bias_window_now_samples_with_a_step() {
        for total in [100_001, 150_000, 199_999] {
            let plan = SamplingPlan::for_total(total);
            assert_eq!(plan.step, 2, "total {}", total);
        }
    }

    #[test]
    fn the_step_grows_with_the_dataset() {
        assert_eq!(SamplingPlan::for_total(200_000).step, 2);
        assert_eq!(SamplingPlan::for_total(200_001).step, 3);
        assert_eq!(SamplingPlan::for_total(1_000_000).step, 10);
    }

    /// The point of the scale factor: counting a sample and scaling it back up
    #[test]
    fn scaling_a_sample_back_up_recovers_the_dataset_size() {
        for total in [100_001u64, 150_000, 200_000, 999_999, 10_000_000] {
            let plan = SamplingPlan::for_total(total);
            let sampled = total.div_ceil(plan.step);
            let scale = SampleScale::new(plan, total, sampled);
            let recovered = scale.scale_up(sampled);
            let error = recovered.abs_diff(total);
            assert!(
                error <= 1,
                "total {} recovered as {} (step {})",
                total,
                recovered,
                plan.step
            );
        }
    }

    /// Scaling uses the observed sample size
    #[test]
    fn scaling_uses_the_sample_that_was_actually_drawn() {
        let plan = SamplingPlan::for_total(1_000_000);
        let scale = SampleScale::new(plan, 1_000_000, 90_000);
        assert_eq!(scale.scale_up(90_000), 1_000_000);
    }

    #[test]
    fn an_empty_sample_never_divides_by_zero() {
        let plan = SamplingPlan::for_total(1_000_000);
        let scale = SampleScale::new(plan, 1_000_000, 0);
        assert_eq!(scale.scale_up(0), 0);
        assert_eq!(scale.scale_up(5), 5);
    }

    #[test]
    fn the_plan_is_reproducible() {
        assert_eq!(
            SamplingPlan::for_total(1_234_567),
            SamplingPlan::for_total(1_234_567)
        );
    }

    #[test]
    fn the_quad_hash_is_fixed_not_per_process() {
        assert_eq!(
            stable_quad_hash(GraphId::Default, 1, 2, 3),
            stable_quad_hash(GraphId::Default, 1, 2, 3)
        );
        assert_ne!(
            stable_quad_hash(GraphId::Default, 1, 2, 3),
            stable_quad_hash(GraphId::Named(0), 1, 2, 3),
            "the default graph must not collide with Named(0)"
        );
        assert_ne!(
            stable_quad_hash(GraphId::Default, 1, 2, 3),
            stable_quad_hash(GraphId::Default, 3, 2, 1),
            "component order must matter"
        );
    }

    #[test]
    fn the_sample_does_not_depend_on_visit_order() {
        let plan = SamplingPlan::for_total(400_000);
        let quads = synthetic_quads(4_000);
        let forward = sample_size(plan, &quads);

        let mut shuffled = quads.clone();
        shuffled.reverse();
        assert_eq!(forward, sample_size(plan, &shuffled));

        // Simulates a different iteration order
        let mut rotated = quads.clone();
        rotated.rotate_left(1_337);
        assert_eq!(forward, sample_size(plan, &rotated));
    }

    #[test]
    fn the_sample_is_roughly_one_in_step_quads() {
        for (total, count) in [(400_000u64, 4_000u32), (1_000_000, 10_000)] {
            let plan = SamplingPlan::for_total(total);
            let quads = synthetic_quads(count);
            let drawn = sample_size(plan, &quads);
            let expected = count as f64 / plan.step as f64;
            let error = (drawn as f64 - expected).abs() / expected;
            assert!(
                error < 0.25,
                "step {} drew {} of {}, expected about {:.0}",
                plan.step,
                drawn,
                count,
                expected
            );
        }
    }
}
