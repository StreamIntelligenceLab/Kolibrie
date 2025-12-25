/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Volcano-style query optimizer with cost-based optimization
//!
//! This module implements a Volcano-style query optimizer that uses dynamic programming
//! with memoization to find optimal query execution plans. The optimizer includes:
//!
//! - Cost-based optimization with cardinality estimation
//! - Multiple join algorithms (hash join, nested loop, parallel join)
//! - Index-based scan optimizations
//! - Filter pushdown and other transformations
//! - ID-based execution for performance
//!
//! ## Architecture
//!
//! The optimizer is structured into several focused modules:
//!
//! - `operators`: Logical and physical operator definitions
//! - `cost`: Cost estimation and cardinality estimation
//! - `execution`: Physical operator execution engine
//! - `stats`: Database statistics gathering and management
//! - `types`: Common types like Condition and IdResult
//! - `utils`: Utility functions for plan building and optimization
//!
//! ## Usage
//!
//! ```rust
//! use volcano_optimizer::VolcanoOptimizer;
//! use volcano_optimizer::operators::LogicalOperator;
//!
//! let mut optimizer = VolcanoOptimizer::new(&database);
//! let logical_plan = LogicalOperator::scan(pattern);
//! let physical_plan = optimizer.find_best_plan(&logical_plan);
//! let results = optimizer.execute_plan(&physical_plan, &mut database);
//! ```

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
pub use optimizer::VolcanoOptimizer;
pub use stats::DatabaseStats;
pub use types::{Condition, IdResult};
pub use utils::{
    build_logical_plan, estimate_operator_selectivity,
    extract_pattern, pattern_contains_variable, build_logical_plan_from_subquery,
};
