/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Cost-based query optimisation, Volcano-style.
//!
//! A query has many possible execution strategies with wildly different costs. This
//! module picks one by enumerating alternatives and costing each against statistics
//! about the data, rather than following the order the query was written in.
//!
//! ```text
//! LogicalOperator ──optimizer──▶ PhysicalOperator ──ExecutionEngine──▶ solutions
//!   (what to compute)   │          (how to compute it)
//!                       └── CostEstimator over DatabaseStats
//! ```
//!
//! | module | role |
//! |---|---|
//! | [`operators`] | the two plan representations: logical *what*, physical *how* |
//! | [`cost`] | cardinality and cost estimates that rank the alternatives |
//! | [`crate::stats`] | the data statistics those estimates read, a layer below this one |
//! | [`optimizer`] | [`Streamertail`], which does the enumeration and selection |
//! | [`execution`] | runs a chosen physical plan |
//! | [`types`] | filter conditions and subquery descriptions shared by both plans |
//! | [`utils`] | building a logical plan from a parsed query |
//!
//! # The logical/physical split
//!
//! One logical operator usually has several physical realisations, and choosing
//! between them is the whole point. A [`LogicalOperator::Join`] can become a hash
//! join, a nested-loop join, a star join, or a bind join that pushes the left side's
//! bindings into the right side's index probes — and which is fastest depends on
//! cardinalities the query text cannot express.
//!
//! # Estimates are estimates
//!
//! The cost model reads [`DatabaseStats`], which are cached and refreshed according
//! to the store's
//! [`PlanningStatsPolicy`](crate::sparql_database::PlanningStatsPolicy). Under the
//! default bounded policy they may lag the data, so a plan can be costed against
//! slightly stale cardinalities. That trades planning accuracy for not rebuilding
//! statistics on every write.

pub mod cost;
pub mod execution;
pub mod operators;
pub mod optimizer;
pub mod types;
pub mod utils;

// Re-export main components for convenience
pub use crate::stats::{DatabaseStats, GraphMap, TermMap, TermSet};
pub use cost::{CostConstants, CostEstimator};
pub use execution::{DatasetView, ExecutionContext, ExecutionEngine};
pub use operators::{LogicalOperator, PhysicalOperator};
pub use optimizer::Streamertail;
pub use types::{
    Condition, ConditionArithmetic, ConditionExpression, IdResult, SubqueryProjection, SubquerySpec,
};
pub use utils::{
    build_logical_plan, build_logical_plan_from_group, build_logical_plan_from_subquery,
    compile_graph_term, compile_term, compile_triple, extract_pattern, pattern_contains_variable,
};
// Re-exported so existing paths keep resolving, though the function is deprecated
#[allow(deprecated)]
pub use utils::estimate_operator_selectivity;
