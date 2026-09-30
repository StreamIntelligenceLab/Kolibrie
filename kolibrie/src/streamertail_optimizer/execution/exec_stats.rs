/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Execution work counters behind the `exec-stats` feature.
//!
//! Counters are process-global because scans run across rayon threads. Totals
//! are reproducible for a given plan and dataset, but concurrent queries share
//! them, so a reader must [`reset`] first and run one query at a time.

#[cfg(feature = "exec-stats")]
mod counters {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static SCAN_PROBES: AtomicU64 = AtomicU64::new(0);
    pub static QUADS_EXAMINED: AtomicU64 = AtomicU64::new(0);
    pub static ROWS_EMITTED: AtomicU64 = AtomicU64::new(0);

    /// Work performed by one execution.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Snapshot {
        pub scan_probes: u64,
        pub quads_examined: u64,
        pub rows_emitted: u64,
    }

    pub fn reset() {
        SCAN_PROBES.store(0, Ordering::Relaxed);
        QUADS_EXAMINED.store(0, Ordering::Relaxed);
        ROWS_EMITTED.store(0, Ordering::Relaxed);
    }

    pub fn snapshot() -> Snapshot {
        Snapshot {
            scan_probes: SCAN_PROBES.load(Ordering::Relaxed),
            quads_examined: QUADS_EXAMINED.load(Ordering::Relaxed),
            rows_emitted: ROWS_EMITTED.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "exec-stats")]
pub use counters::{reset, snapshot, Snapshot};

macro_rules! exec_count {
    ($counter:ident) => {
        #[cfg(feature = "exec-stats")]
        {
            crate::streamertail_optimizer::execution::exec_stats::$counter
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        }
    };
}

#[cfg(feature = "exec-stats")]
pub(crate) use counters::{QUADS_EXAMINED, ROWS_EMITTED, SCAN_PROBES};

pub(crate) use exec_count;
