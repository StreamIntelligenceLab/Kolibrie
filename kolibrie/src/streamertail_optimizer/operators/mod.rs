/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The two plan representations.
//!
//! [`LogicalOperator`] says *what* to compute and [`PhysicalOperator`] says *how*.
//! Optimisation is the translation between them, and it is only interesting because
//! the mapping is one-to-many.

pub mod logical;
pub mod physical;

pub use logical::LogicalOperator;
pub use physical::PhysicalOperator;
