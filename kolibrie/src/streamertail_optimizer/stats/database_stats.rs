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
use rayon::prelude::*;
use shared::dataset_index::GraphId;
use std::collections::{HashMap, HashSet};

/// Above this many quads the statistics are estimated from a sample rather
const EXACT_SAMPLING_THRESHOLD: u64 = 100_000;

/// How [`DatabaseStats::gather_stats_fast`] reduces a dataset before counting
#[derive(Debug, Clone, Copy, PartialEq)]
struct SamplingPlan {
    step: usize,
    scale: f64,
}

impl SamplingPlan {
    fn for_total(total: u64) -> Self {
        if total <= EXACT_SAMPLING_THRESHOLD {
            return SamplingPlan {
                step: 1,
                scale: 1.0,
            };
        }

        let step = total.div_ceil(EXACT_SAMPLING_THRESHOLD);
        // `step_by` yields this many elements out of `total`
        let sampled = total.div_ceil(step);
        SamplingPlan {
            step: step as usize,
            scale: total as f64 / sampled as f64,
        }
    }

    /// Scales one observed count back up to the whole dataset
    fn scale_up(&self, observed: u64) -> u64 {
        if self.step == 1 {
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
    pub graph_cardinalities: HashMap<GraphId, u64>,
    pub predicate_cardinalities: HashMap<u32, u64>,
    pub subject_cardinalities: HashMap<u32, u64>,
    pub object_cardinalities: HashMap<u32, u64>,
    /// Join-key domain sizes: a join through predicate `p` fans out by roughly `cardinality(p) / distinct(p)` rows per binding
    pub predicate_distinct_subjects: HashMap<u32, u64>,
    pub predicate_distinct_objects: HashMap<u32, u64>,
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
            graph_cardinalities: HashMap::new(),
            predicate_cardinalities: HashMap::new(),
            subject_cardinalities: HashMap::new(),
            object_cardinalities: HashMap::new(),
            predicate_distinct_subjects: HashMap::new(),
            predicate_distinct_objects: HashMap::new(),
            distinct_subjects: 0,
            distinct_objects: 0,
        }
    }

    /// Gathers statistics from the database using sampling for performance
    pub fn gather_stats_fast(database: &SparqlDatabase) -> Self {
        // Term and predicate distributions must cover the complete RDF
        let quads = database.dataset_index.all_quads();
        let total_triples = quads.len() as u64;

        // Sample large datasets instead of counting every quad
        let plan = SamplingPlan::for_total(total_triples);
        let sampled_quads: Vec<_> = quads.iter().step_by(plan.step).collect();

        // Use parallel processing for stats gathering
        let stats_data: Vec<_> = sampled_quads
            .par_iter()
            .map(|triple| {
                let subject = triple.subject;
                let predicate = triple.predicate;
                let object = triple.object;
                (subject, predicate, object)
            })
            .collect();

        // Build cardinality maps
        let mut predicate_cardinalities: HashMap<u32, u64> = HashMap::new();
        let mut subject_cardinalities: HashMap<u32, u64> = HashMap::new();
        let mut object_cardinalities: HashMap<u32, u64> = HashMap::new();
        let mut predicate_subjects: HashMap<u32, HashSet<u32>> = HashMap::new();
        let mut predicate_objects: HashMap<u32, HashSet<u32>> = HashMap::new();
        let mut all_subjects: HashSet<u32> = HashSet::new();
        let mut all_objects: HashSet<u32> = HashSet::new();

        for (subject, predicate, object) in stats_data {
            *predicate_cardinalities.entry(predicate).or_insert(0) += 1;
            *subject_cardinalities.entry(subject).or_insert(0) += 1;
            *object_cardinalities.entry(object).or_insert(0) += 1;
            predicate_subjects.entry(predicate).or_default().insert(subject);
            predicate_objects.entry(predicate).or_default().insert(object);
            all_subjects.insert(subject);
            all_objects.insert(object);
        }

        // Scale sampled counts back up to the whole dataset
        predicate_cardinalities
            .values_mut()
            .for_each(|v| *v = plan.scale_up(*v));
        subject_cardinalities
            .values_mut()
            .for_each(|v| *v = plan.scale_up(*v));
        object_cardinalities
            .values_mut()
            .for_each(|v| *v = plan.scale_up(*v));

        // Distinct counts do not grow linearly with the sample, so each is capped at its cardinality
        let scale_distinct =
            |observed: u64, cardinality: u64| plan.scale_up(observed).min(cardinality).max(1);
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
        let mut graph_cardinalities = HashMap::new();
        graph_cardinalities.insert(
            GraphId::Default,
            database.dataset_index.len_graph(GraphId::Default) as u64,
        );
        for graph in database.dataset_index.named_graphs() {
            graph_cardinalities.insert(graph, database.dataset_index.len_graph(graph) as u64);
        }
        let named_graph_count = database.dataset_index.named_graphs().len() as u64;

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

    #[test]
    fn datasets_at_or_below_the_threshold_are_counted_exactly() {
        for total in [0, 1, 99_999, EXACT_SAMPLING_THRESHOLD] {
            let plan = SamplingPlan::for_total(total);
            assert_eq!(plan.step, 1, "total {}", total);
            assert_eq!(plan.scale, 1.0, "total {}", total);
            assert_eq!(plan.scale_up(42), 42, "total {}", total);
        }
    }

    #[test]
    fn just_past_the_threshold_the_step_is_two_not_one() {
        // An integer `total / budget` truncated to 1 here, sampling the first
        let plan = SamplingPlan::for_total(EXACT_SAMPLING_THRESHOLD + 1);
        assert_eq!(plan.step, 2);
        assert!(plan.scale > 1.0);
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
        for total in [100_001, 150_000, 200_000, 999_999, 10_000_000] {
            let plan = SamplingPlan::for_total(total);
            let sampled = total.div_ceil(plan.step as u64);
            let recovered = plan.scale_up(sampled);
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

    #[test]
    fn the_plan_is_reproducible() {
        assert_eq!(
            SamplingPlan::for_total(1_234_567),
            SamplingPlan::for_total(1_234_567)
        );
    }
}
