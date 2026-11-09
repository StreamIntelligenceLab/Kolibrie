/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Per-tick counters, for seeing where the tick evaluator's time goes

#[derive(Debug, Default, Clone)]
/// What one tick cost, and what it did
pub struct TickMetrics {
    /// The time point this tick evaluated
    pub timestamp: u64,
    /// Rounds the bottom-up fixpoint took
    pub fixpoint_iterations: usize,
    /// Rule evaluations, counted once per rule per round rather than per rule
    pub rules_fired: usize,
    /// Facts derived at this tick, not counting those ingested
    pub new_triples: usize,
    /// Distinct timestamps the store holds after eviction
    pub snapshot_count: usize,
    /// Facts the store holds in total, across every timestamp
    pub total_triples_in_store: usize,
    /// Wall-clock time for the whole tick, in microseconds
    pub eval_time_us: u64,
    /// `Diamond` evaluations
    pub diamond_evals: usize,
    /// `Box` evaluations
    pub box_evals: usize,
    /// `Since` evaluations
    pub since_evals: usize,
    /// Total points scanned by those `Since` evaluations, summed
    pub since_scan_depth: usize,
}

impl TickMetrics {
    /// Zeroed counters for a tick at `timestamp`
    pub fn new(timestamp: u64) -> Self {
        Self {
            timestamp,
            ..Default::default()
        }
    }

    /// Column names for [`to_csv_row`](Self::to_csv_row), without a trailing newline
    pub fn csv_header() -> &'static str {
        "timestamp,fixpoint_iterations,rules_fired,new_triples,snapshot_count,\
         total_triples_in_store,eval_time_us,diamond_evals,box_evals,\
         since_evals,since_scan_depth"
    }

    /// One row in the order [`csv_header`](Self::csv_header) declares, without a trailing newline
    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{}",
            self.timestamp,
            self.fixpoint_iterations,
            self.rules_fired,
            self.new_triples,
            self.snapshot_count,
            self.total_triples_in_store,
            self.eval_time_us,
            self.diamond_evals,
            self.box_evals,
            self.since_evals,
            self.since_scan_depth
        )
    }
}

/// Writes a header and one row per tick to `path`, truncating any existing file
pub fn write_metrics_csv(path: &str, metrics: &[TickMetrics]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(f, "{}", TickMetrics::csv_header())?;
    for m in metrics {
        writeln!(f, "{}", m.to_csv_row())?;
    }
    Ok(())
}
