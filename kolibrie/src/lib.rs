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
//! | [`ml`] | model declarations, the authority to run them, and the runtime |
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
#[cfg(test)]
extern crate self as kolibrie;
/// Query evaluation, including the SIMD and Rayon-parallel variants.
pub mod execute_query;
// Documented by its own `//!` header. A `///` summary here would merge with that
// and resolve its links in this scope rather than in `ml`, which is the trap
// `scripts/verify.sh` warns about and which this walked into first time.
pub mod ml;
// The ML *contract*, deliberately outside [`ml`]: the engine depends on these three
// and they reach back no further than the store, while everything inside `ml`
// depends on the engine. Folding all ten into one module made a leaf and a consumer
// the same node and closed a cycle across seven modules, which is how the direction
// of this dependency came to be measured rather than assumed.
pub mod ml_policy;
pub(crate) mod ml_feature_loader;
pub mod neural_registry;
/// What a failed parse says, without naming the parser library.
pub mod parse_error;
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

/// Which [`MlRuntime`](ml::runtime::MlRuntime) the query layer calls through.
///
/// The crate root is the one place that may name both the abstraction and the
/// implementation, which is what keeps `execute_query` and `rule_execution` from
/// depending on the ML layer. At the point the workspace is split into crates this
/// selection moves to the facade crate and the object is threaded through the store
/// instead; until then, a `static` chosen by the feature flag does the same job
/// without a registration step, so no caller has to install anything.
pub(crate) mod ml_runtime_impl {
    use crate::ml::runtime::MlRuntime;

    // No `cfg` here: `ml::neural_relations` is already either the real module or the
    // `#[path]`-substituted disabled one, so naming its `NeuralRuntime` picks the
    // right implementation for the build on its own.
    static RUNTIME: crate::ml::neural_relations::NeuralRuntime = crate::ml::neural_relations::NeuralRuntime;

    /// The runtime for this build.
    pub(crate) fn runtime() -> &'static dyn MlRuntime {
        &RUNTIME
    }
}

pub(crate) use ml_runtime_impl::runtime as ml_runtime_for_build;
