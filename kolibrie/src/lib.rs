/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The Kolibrie engine: an RDF store, a SPARQL evaluator, and the streaming, probabilistic and
//! neural extensions built on them

mod aggregate;
pub mod database;
pub(crate) mod error_handler;
#[cfg(test)]
extern crate self as kolibrie;
/// Query evaluation, including the SIMD and Rayon-parallel variants
pub mod execute_query;
pub mod ml;
pub(crate) mod ml_feature_loader;
pub mod ml_policy;
pub mod neural_registry;
pub mod parse_error;
pub mod parser;
pub mod prelude;
pub mod program;
/// Building a query programmatically instead of writing SPARQL text
pub mod query_builder;
pub mod query_error;
pub mod rsp;
/// The RDF stream processing engine: named streams, windows, and their joins
pub mod rsp_engine;
pub mod rule_execution;
pub mod sparql_protocol;
pub mod stats;
pub mod streamertail_optimizer;
pub(crate) mod term_order;
/// Small helpers: user-defined function wrappers, timestamps, a demo server
pub mod utils;

/// Which [`MlRuntime`] the query layer calls through
pub(crate) mod ml_runtime_impl {
    use crate::ml::runtime::MlRuntime;

    static RUNTIME: crate::ml::neural_relations::NeuralRuntime =
        crate::ml::neural_relations::NeuralRuntime;

    /// The runtime for this build
    pub(crate) fn runtime() -> &'static dyn MlRuntime {
        &RUNTIME
    }
}

pub(crate) use ml_runtime_impl::runtime as ml_runtime_for_build;
