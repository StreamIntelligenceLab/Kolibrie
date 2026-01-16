/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::streamertail_optimizer::PhysicalOperator;

use std::any::Any;

/// Helper trait to allow downcasting mutable trait objects to their concrete types.
/// Implementations should return a mutable Any reference to enable `downcast_mut`.
pub trait AsAnyMut {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait R2ROperator<I, R, O>: Send + AsAnyMut {
    fn load_triples(&mut self, data: &str, syntax: String) -> Result<(), String>;
    fn load_rules(&mut self, data: &str) -> Result<(), &'static str>;
    fn add(&mut self, data: I);
    fn remove(&mut self, data: &I);
    fn materialize(&mut self) -> Vec<I>;
    fn execute_query(&mut self, op: &PhysicalOperator) -> Vec<O>;

    fn parse_data(&mut self, data: &str) -> Vec<I>;
}