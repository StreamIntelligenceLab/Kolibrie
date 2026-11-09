/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`QuadSource`]: somewhere quads can be read from

use crate::dataset_index::GraphId;
use crate::triple::Triple;

/// Somewhere quads can be read from
pub trait QuadSource {
    /// Triples of the default graph matching a pattern, `None` meaning any
    fn query_default(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple>;

    /// Calls `visit` once per quad, named graphs included
    fn for_each_quad(&self, visit: &mut dyn FnMut(GraphId, u32, u32, u32));

    /// The named graphs present, excluding the default graph
    fn named_graphs(&self) -> Vec<GraphId>;

    /// How many quads one graph holds
    fn count_graph(&self, graph: GraphId) -> usize;
}
