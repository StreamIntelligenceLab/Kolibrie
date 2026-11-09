/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Statistics the planner costs with, and the policy for when they are rebuilt

use super::KolibrieDB;
use crate::stats::DatabaseStats;
use shared::dataset_index::DatasetIndex;
use std::sync::Arc;

/// Staleness policy for planning statistics
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PlanningStatsPolicy {
    /// Rebuild on every mutation
    AlwaysFresh,
    /// Reuse a snapshot until the store has changed enough to matter
    Bounded {
        /// Mutations to tolerate before rebuilding, however small the store
        minimum_batch: u64,
        /// Fraction of the store that must change to force a rebuild, so the tolerance grows with
        /// size instead of being a fixed count
        refresh_fraction: f64,
    },
}

impl PlanningStatsPolicy {
    /// Refresh after 100 mutations or half the store
    pub const DEFAULT_BOUNDED: Self = PlanningStatsPolicy::Bounded {
        minimum_batch: 100,
        refresh_fraction: 0.5,
    };
}

impl Default for PlanningStatsPolicy {
    fn default() -> Self {
        Self::DEFAULT_BOUNDED
    }
}

/// A planning statistics snapshot
#[derive(Debug, Clone)]
pub struct PlanningStatsSnapshot {
    /// The cardinalities themselves, shared so reusing a snapshot is free
    pub stats: Arc<DatabaseStats>,
    /// `DatasetIndex::generation` at build time
    pub generation: u64,
    /// `total_triples` at build time
    pub quads_at_build: u64,
    /// Statistics epoch at build time
    pub epoch: u64,
    /// `DatasetIndex::resets` at build time
    pub resets: u64,
}

impl KolibrieDB {
    /// Returns exact statistics, rebuilding them if the dataset generation changed
    pub fn get_or_build_stats(&mut self) -> Arc<DatabaseStats> {
        let generation = self.dataset_index.generation();
        if let Some(stats) = &self.cached_stats {
            if self.exact_stats_generation == generation {
                return stats.clone();
            }
        }

        let stats = self.rebuild_stats();
        self.cached_stats = Some(stats.clone());
        self.exact_stats_generation = generation;
        stats
    }

    /// Returns statistics for plan selection; under `Bounded` they may be stale within the
    /// threshold
    pub fn get_or_build_planning_stats(&mut self) -> Arc<DatabaseStats> {
        if self.planning_stats_policy == PlanningStatsPolicy::AlwaysFresh {
            return self.get_or_build_stats();
        }

        let generation = self.dataset_index.generation();
        if let Some(snapshot) = &self.planning_stats {
            if self.planning_snapshot_is_usable(snapshot, generation) {
                return snapshot.stats.clone();
            }
        }

        let stats = self.rebuild_stats();
        self.planning_stats = Some(PlanningStatsSnapshot {
            stats: stats.clone(),
            generation,
            quads_at_build: stats.total_triples,
            epoch: self.stats_epoch,
            resets: self.dataset_index.resets(),
        });
        stats
    }

    /// Returns true if the cached planning snapshot is still within the policy threshold
    fn planning_snapshot_is_usable(
        &self,
        snapshot: &PlanningStatsSnapshot,
        generation: u64,
    ) -> bool {
        if snapshot.epoch != self.stats_epoch || snapshot.resets != self.dataset_index.resets() {
            return false;
        }

        let mutations = generation.wrapping_sub(snapshot.generation);
        if mutations == 0 {
            return true;
        }

        if snapshot.quads_at_build == 0 {
            return false;
        }

        let PlanningStatsPolicy::Bounded {
            minimum_batch,
            refresh_fraction,
        } = self.planning_stats_policy
        else {
            return false;
        };

        let proportional = (refresh_fraction * snapshot.quads_at_build.max(1) as f64).ceil() as u64;
        mutations < minimum_batch.max(proportional)
    }

    /// Gathers fresh statistics over this store's dataset, without caching them
    pub fn gather_stats(&self) -> DatabaseStats {
        DatabaseStats::gather_stats_fast(
            &self.dataset_index,
            self.quoted_triple_store.read().unwrap().len() as u64,
        )
    }

    /// Builds statistics and records rebuild count and duration
    fn rebuild_stats(&mut self) -> Arc<DatabaseStats> {
        let started = std::time::Instant::now();
        let stats = Arc::new(self.gather_stats());
        self.stats_rebuild_count += 1;
        self.stats_rebuild_nanos = self
            .stats_rebuild_nanos
            .saturating_add(started.elapsed().as_nanos() as u64);
        stats
    }

    /// Invalidates all cached statistics
    pub fn invalidate_stats_cache(&mut self) {
        self.cached_stats = None;
        self.planning_stats = None;
        self.stats_epoch = self.stats_epoch.wrapping_add(1);
    }

    /// Replaces the dataset index and invalidates cached statistics
    pub fn replace_dataset_index(&mut self, dataset_index: DatasetIndex) {
        self.dataset_index = dataset_index;
        self.invalidate_stats_cache();
    }

    /// Number of statistics rebuilds and their total duration
    pub fn stats_rebuild_metrics(&self) -> (u64, std::time::Duration) {
        (
            self.stats_rebuild_count,
            std::time::Duration::from_nanos(self.stats_rebuild_nanos),
        )
    }

    /// When planning statistics are rebuilt rather than reused
    pub fn planning_stats_policy(&self) -> PlanningStatsPolicy {
        self.planning_stats_policy
    }

    /// Sets when planning statistics are rebuilt
    pub fn set_planning_stats_policy(&mut self, policy: PlanningStatsPolicy) {
        self.planning_stats_policy = policy;
    }

    /// How many times planning statistics have been rebuilt
    pub fn stats_rebuild_count(&self) -> u64 {
        self.stats_rebuild_count
    }
}
