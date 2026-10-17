/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Statistics about the stored data that the cost model reads.
//!
//! Cardinalities per predicate and graph, distinct-subject counts, and the totals
//! used to estimate selectivity. Rebuilding them is not free, which is why the store
//! caches them and refreshes according to its
//! [`PlanningStatsPolicy`](crate::sparql_database::PlanningStatsPolicy).

pub mod database_stats;

pub use database_stats::{DatabaseStats, GraphMap, TermBuildHasher, TermHasher, TermMap, TermSet};
