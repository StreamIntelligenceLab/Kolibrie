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

#[cfg(feature = "ml")]
pub mod execute;
#[cfg(feature = "ml")]
pub mod execute_train;
#[cfg(feature = "ml")]
pub(crate) mod predict_candle;
#[cfg(feature = "ml")]
pub(crate) mod predict_runtime;

#[cfg(feature = "ml")]
pub mod neural_relations;
#[cfg(not(feature = "ml"))]
#[path = "neural_relations_disabled.rs"]
pub mod neural_relations;
