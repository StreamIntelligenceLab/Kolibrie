/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The Kolibrie engine: an RDF store, a SPARQL evaluator, and the streaming,
//! probabilistic and neural extensions built on them.
//!
//! # Where to start
//!
//! [`prelude`] exports the three things almost every caller needs — the store, the
//! parser entry point, and the query executor:
//!
//! ```ignore
//! use kolibrie::prelude::*;
//!
//! let mut db = SparqlDatabase::new();
//! db.parse_ntriples_and_add(data);
//! let rows = execute(query, &mut db).expect("query should run").into_rows();
//! ```
//!
//! # How a query gets answered
//!
//! ```text
//! text ──parser──▶ CombinedQuery ──▶ logical plan ──optimizer──▶ physical plan ──▶ rows
//!                  (lexical)          (resolved)                  (costed)
//! ```
//!
//! [`parser`] produces the lexical syntax tree defined in `shared::query`; nothing is
//! prefix-expanded or interned at that point. Binding it to a
//! [`SparqlDatabase`](sparql_database::SparqlDatabase)'s dictionary turns terms into
//! integer ids, and [`streamertail_optimizer`] then costs alternative physical plans
//! before [`execute_query`] runs one.
//!
//! # Subsystems
//!
//! | module | role |
//! |---|---|
//! | [`sparql_database`] | the store: encoded triples, dictionary, indexes, statistics |
//! | [`parser`] | SPARQL, RSP-QL and the rule/ML extensions |
//! | [`execute_query`] | evaluation, including SIMD and Rayon variants |
//! | [`streamertail_optimizer`] | cost-based logical-to-physical planning |
//! | [`rsp`], [`rsp_engine`] | RDF stream processing: windows, joins, output streams |
//! | [`program`] | probabilistic programs over rules, with provenance |
//! | [`ml_policy`], [`neural_relations`] | model declarations and the authority to run them |
//! | [`query_builder`] | a programmatic alternative to writing SPARQL text |
//!
//! Reasoning itself lives outside this crate: `datalog` for the Datalog subset and
//! `datalogmtl` for the temporal one. Both are siblings over the `shared`
//! vocabulary rather than layers on this engine.

// Modules with no consumer outside this crate are `pub(crate)`. Keeping them `pub`
// put them on the documentation ledger and let future callers couple to paths we do
// not intend to support. Promote one back the moment something outside needs it.
mod aggregate;
pub(crate) mod error_handler;
pub mod ml_policy;
pub mod ml_runtime;
mod ml_syntax;
#[cfg(test)]
extern crate self as kolibrie;
/// Running an `ML.PREDICT` clause through the Python backend.
#[cfg(feature = "ml")]
pub mod execute_ml;
/// Training a model declared by a `TRAIN` clause.
#[cfg(feature = "ml")]
pub mod execute_ml_train;
/// Query evaluation, including the SIMD and Rayon-parallel variants.
pub mod execute_query;
pub(crate) mod ml_feature_loader;
#[cfg(feature = "ml")]
pub(crate) mod ml_predict_candle;
#[cfg(feature = "ml")]
pub(crate) mod ml_predict_runtime;
pub mod neural_registry;
/// Predicates whose facts come from a model rather than from data.
#[cfg(feature = "ml")]
pub mod neural_relations;
/// Stubs standing in for [`neural_relations`] when the `ml` feature is off, so the
/// rest of the crate compiles without PyO3 and a caller gets a clear
/// `ML_FEATURE_DISABLED` error rather than a missing symbol.
#[cfg(not(feature = "ml"))]
#[path = "neural_relations_disabled.rs"]
pub mod neural_relations;
pub mod parser;
pub mod prelude;
pub mod program;
/// Building a query programmatically instead of writing SPARQL text.
pub mod query_builder;
pub mod query_error;
pub mod rsp;
/// The RDF stream processing engine: named streams, windows, and their joins.
pub mod rsp_engine;
pub mod rule_execution;
/// The RDF store and everything hung off it.
pub mod sparql_database;
pub mod sparql_protocol;
pub mod stats;
pub mod streamertail_optimizer;
pub(crate) mod term_order;
/// Small helpers: user-defined function wrappers, timestamps, a demo server.
pub mod utils;

/// Which [`MlRuntime`](ml_runtime::MlRuntime) the query layer calls through.
///
/// The crate root is the one place that may name both the abstraction and the
/// implementation, which is what keeps `execute_query` and `rule_execution` from
/// depending on the ML layer. At the point the workspace is split into crates this
/// selection moves to the facade crate and the object is threaded through the store
/// instead; until then, a `static` chosen by the feature flag does the same job
/// without a registration step, so no caller has to install anything.
pub(crate) mod ml_runtime_impl {
    use crate::ml_runtime::MlRuntime;

    // No `cfg` here: `neural_relations` is already either the real module or the
    // `#[path]`-substituted disabled one, so naming its `NeuralRuntime` picks the
    // right implementation for the build on its own.
    static RUNTIME: crate::neural_relations::NeuralRuntime = crate::neural_relations::NeuralRuntime;

    /// The runtime for this build.
    pub(crate) fn runtime() -> &'static dyn MlRuntime {
        &RUNTIME
    }
}

pub(crate) use ml_runtime_impl::runtime as ml_runtime_for_build;
