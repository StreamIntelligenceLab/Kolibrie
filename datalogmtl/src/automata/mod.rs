/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Interval-native ("automata") evaluation strategy for DatalogMTL.
//!
//! Temporal operators are interval-arithmetic **transducers** (the batch I/O of
//! each operator's automaton) over rational/real-line intervals ported from
//! MeTeoR, and a **semi-naive-ready** interval fixpoint materializes the program.
//! Cost scales with the number of interval endpoints, independent of the time
//! horizon and interval widths — and it matches MeTeoR's semantics exactly
//! (dissolving the ℤ-vs-ℝ artifact of the tick engine).

pub mod interval;
pub mod transducer;
pub mod relation;
pub mod eval;
pub mod omega;

pub use eval::materialize;
pub use interval::TInterval;
pub use omega::{entails, materialize_omega, PeriodicModel};
pub use relation::Database;
