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
use std::collections::HashMap;
use std::sync::RwLock;

/// Database statistics for cost-based optimization
#[derive(Debug)]
pub struct DatabaseStats {
    pub total_triples: u64,
    pub predicate_cardinalities: HashMap<u32, u64>,
    pub subject_cardinalities: HashMap<u32, u64>,
    pub object_cardinalities: HashMap<u32, u64>,
    pub join_selectivity_cache: RwLock<HashMap<u32, f64>>,
    pub predicate_histogram: HashMap<u32, Vec<(u32, u64)>>, // For better selectivity estimation
}

impl DatabaseStats {
    /// Creates a new empty DatabaseStats instance
    pub fn new() -> Self {
        Self {
            total_triples: 0,
            predicate_cardinalities: HashMap::new(),
            subject_cardinalities: HashMap::new(),
            object_cardinalities: HashMap::new(),
            join_selectivity_cache: RwLock::new(HashMap::new()),
            predicate_histogram: HashMap::new(),
        }
    }

    /// Gathers statistics from the database using sampling for performance
    pub fn gather_stats_fast(database: &SparqlDatabase) -> Self {
        let total_triples = database.triples.len() as u64;

        // Convert BTreeSet to Vec for sampling
        let triples_vec: Vec<_> = database.triples.iter().collect();

        // Use sampling for large datasets instead of full scan
        let sample_size = (total_triples as usize).min(100_000);
        let step = if total_triples > sample_size as u64 {
            total_triples as usize / sample_size
        } else {
            1
        };

        // Sample the data by stepping through the vector
        let sampled_triples: Vec<_> = triples_vec.iter().step_by(step).take(sample_size).collect();

        // Use parallel processing for stats gathering
        let stats_data: Vec<_> = sampled_triples
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

        for (subject, predicate, object) in stats_data {
            *predicate_cardinalities.entry(predicate).or_insert(0) += 1;
            *subject_cardinalities.entry(subject).or_insert(0) += 1;
            *object_cardinalities.entry(object).or_insert(0) += 1;
        }

        // Scale up sampled statistics
        let scale_factor = if step > 1 { step as u64 } else { 1 };
        predicate_cardinalities
            .values_mut()
            .for_each(|v| *v *= scale_factor);
        subject_cardinalities
            .values_mut()
            .for_each(|v| *v *= scale_factor);
        object_cardinalities
            .values_mut()
            .for_each(|v| *v *= scale_factor);

        Self {
            total_triples,
            predicate_cardinalities,
            subject_cardinalities,
            object_cardinalities,
            join_selectivity_cache: RwLock::new(HashMap::new()),
            predicate_histogram: HashMap::new(),
        }
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

    /// Gets or computes join selectivity
    pub fn get_join_selectivity(&self, predicate: u32) -> f64 {
        // First, try to read from cache (shared read lock)
        {
            let cache = self.join_selectivity_cache.read().unwrap();
            if let Some(&selectivity) = cache.get(&predicate) {
                return selectivity;
            }
        }  // Read lock released here
        
        // Compute selectivity
        let cardinality = self.get_predicate_cardinality(predicate);
        let selectivity = if self.total_triples > 0 {
            (cardinality as f64) / (self.total_triples as f64)
        } else {
            0.1
        };

        // Cache the result
        {
            let mut cache = self.join_selectivity_cache.write().unwrap();
            cache.insert(predicate, selectivity);
        }
        
        selectivity
    }

    /// Updates statistics with new data
    pub fn update_stats(&mut self, subject: u32, predicate: u32, object: u32) {
        self.total_triples += 1;
        *self.predicate_cardinalities.entry(predicate).or_insert(0) += 1;
        *self.subject_cardinalities.entry(subject).or_insert(0) += 1;
        *self.object_cardinalities.entry(object).or_insert(0) += 1;

        // Clear cache as statistics have changed
        self.join_selectivity_cache.write().unwrap().clear();
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

        // Clear cache as statistics have changed
        self.join_selectivity_cache.write().unwrap().clear();
    }
}

impl Default for DatabaseStats {
    fn default() -> Self {
        Self::new()
    }
}
