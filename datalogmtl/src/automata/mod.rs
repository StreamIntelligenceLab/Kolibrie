/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Interval-native ("automata") evaluation strategy for DatalogMTL

pub mod eval;
pub mod interval;
pub mod omega;
pub mod relation;
pub mod transducer;

pub use eval::materialize;
pub use interval::TInterval;
pub use omega::{entails, materialize_omega, PeriodicModel};
pub use relation::Database;
