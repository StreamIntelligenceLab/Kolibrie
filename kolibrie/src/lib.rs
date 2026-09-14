/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod error_handler;
mod aggregate;
pub mod ml_policy;
mod ml_syntax;
#[cfg(test)]
extern crate self as kolibrie;
#[cfg(feature = "ml")]
pub mod execute_ml;
#[cfg(feature = "ml")]
pub mod execute_ml_train;
pub mod execute_query;
pub mod ml_feature_loader;
#[cfg(feature = "ml")]
pub mod ml_predict_candle;
#[cfg(feature = "ml")]
pub mod ml_predict_runtime;
#[cfg(feature = "ml")]
pub mod neural_relations;
#[cfg(not(feature = "ml"))]
#[path = "neural_relations_disabled.rs"]
pub mod neural_relations;
pub mod parser;
pub mod query_builder;
pub mod rsp_engine;
pub mod sparql_database;
pub mod term_order;
pub mod utils;
pub mod streamertail_optimizer;
pub mod rsp;
pub mod query_engine;
