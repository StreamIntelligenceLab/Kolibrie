/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Machine learning: declaring models, authorising them, and running them

pub mod runtime;
pub(crate) mod syntax;

/// Running an `ML.PREDICT` clause through the Python backend
#[cfg(feature = "ml")]
pub mod execute;
/// Training a model declared by a `TRAIN` clause
#[cfg(feature = "ml")]
pub mod execute_train;
#[cfg(feature = "ml")]
pub(crate) mod predict_candle;
#[cfg(feature = "ml")]
pub(crate) mod predict_runtime;

/// Predicates whose facts come from a model rather than from data
#[cfg(feature = "ml")]
pub mod neural_relations;
/// Stubs for [`neural_relations`] when the `ml` feature is off, returning `ML_FEATURE_DISABLED`
#[cfg(not(feature = "ml"))]
#[path = "neural_relations_disabled.rs"]
pub mod neural_relations;
