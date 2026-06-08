/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Debug, Default, Clone)]
pub struct TickMetrics {
    pub timestamp:              u64,
    pub fixpoint_iterations:    usize,
    pub rules_fired:            usize,
    pub new_triples:            usize,
    pub snapshot_count:         usize,
    pub total_triples_in_store: usize,
    pub eval_time_us:           u64,
    pub diamond_evals:          usize,
    pub box_evals:              usize,
    pub since_evals:            usize,
    pub since_scan_depth:       usize,
}

impl TickMetrics {
    pub fn new(timestamp: u64) -> Self { Self { timestamp, ..Default::default() } }

    pub fn csv_header() -> &'static str {
        "timestamp,fixpoint_iterations,rules_fired,new_triples,snapshot_count,\
         total_triples_in_store,eval_time_us,diamond_evals,box_evals,\
         since_evals,since_scan_depth"
    }

    pub fn to_csv_row(&self) -> String {
        format!("{},{},{},{},{},{},{},{},{},{},{}",
            self.timestamp, self.fixpoint_iterations, self.rules_fired,
            self.new_triples, self.snapshot_count, self.total_triples_in_store,
            self.eval_time_us, self.diamond_evals, self.box_evals,
            self.since_evals, self.since_scan_depth)
    }
}

pub fn write_metrics_csv(path: &str, metrics: &[TickMetrics]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(f, "{}", TickMetrics::csv_header())?;
    for m in metrics { writeln!(f, "{}", m.to_csv_row())?; }
    Ok(())
}
