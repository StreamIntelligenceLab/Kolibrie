/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! R2R: evaluating a query over a window's content.
//!
//! The relation-to-relation stage. An implementation holds whatever state a query
//! needs between firings — loaded rules, materialised facts — and is handed each
//! window's content as it arrives.

use crate::streamertail_optimizer::PhysicalOperator;

use std::any::Any;

/// Helper trait to allow downcasting mutable trait objects to their concrete types.
/// Implementations should return a mutable Any reference to enable `downcast_mut`.
pub trait AsAnyMut {
    /// A mutable `Any` view of `self`, so a caller holding the trait object can
    /// recover the concrete type with `downcast_mut`.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Evaluates a query over a window's content.
///
/// Requires `Send` because a multi-threaded engine moves each window's operator to
/// its own thread, and [`AsAnyMut`] because a caller needing implementation-specific
/// state has to downcast the trait object.
///
/// `execute_query` is deliberately loose about its output type: downcast the result
/// to whatever the implementation produces, typically with
/// `(res as &dyn Any).downcast_ref::<Vec<(String, String)>>()`.
pub trait R2ROperator<I, R, O>: Send + AsAnyMut {
    /// Loads background data in the named syntax.
    fn load_triples(&mut self, data: &str, syntax: String) -> Result<(), String>;
    /// Loads reasoning rules applied to each window's content.
    fn load_rules(&mut self, data: &str) -> Result<(), &'static str>;
    /// Adds one item to the operator's working set.
    fn add(&mut self, data: I);
    /// Removes one item, for when a window slides past it.
    fn remove(&mut self, data: &I);
    /// Derives everything the rules entail from the current working set.
    fn materialize(&mut self) -> Vec<I>;
    /// Evaluates a physical plan, returning the solutions.
    fn execute_query(&mut self, op: &PhysicalOperator) -> Vec<O>;

    /// Parses incoming stream text into items this operator accepts.
    fn parse_data(&mut self, data: &str) -> Vec<I>;
}
