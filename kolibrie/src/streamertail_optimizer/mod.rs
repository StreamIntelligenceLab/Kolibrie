/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod cost;
pub mod execution;
pub mod operators;
pub mod optimizer;
pub mod stats;
pub mod types;
pub mod utils;

// Re-export main components for convenience
pub use cost::{CostConstants, CostEstimator};
pub use execution::ExecutionEngine;
pub use operators::{LogicalOperator, PhysicalOperator};
pub use optimizer::Streamertail;
pub use stats::DatabaseStats;
pub use types::{Condition, IdResult};
pub use utils::{
    build_logical_plan, estimate_operator_selectivity,
    extract_pattern, pattern_contains_variable, build_logical_plan_from_subquery,
};
