/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`QuadSource`]: somewhere quads can be read from.
//!
//! [`DatasetIndex`](crate::dataset_index::DatasetIndex) is one implementation and was
//! the only one. The point of the trait is that it need not be: a window's content is
//! a short-lived array that gains nothing from three sorted indexes, a dataset on disk
//! should not be loaded to be scanned, and a different indexing strategy should be
//! substitutable without touching the reasoners.
//!
//! # Deliberately small
//!
//! Four methods, against `DatasetIndex`'s 33. The surface is what consumers were
//! measured to need, not what the index happens to offer: `datalog` and `datalogmtl`
//! between them call two of those 33, so a trait mirroring the index would be
//! unimplementable for an array and pointless for a reasoner.
//!
//! Writing is not here. A read-only source — a file, a window that is filled and then
//! frozen — implements this and nothing else.
//!
//! # Why a visitor rather than an iterator
//!
//! [`for_each_quad`](QuadSource::for_each_quad) takes `&mut dyn FnMut` rather than
//! returning `Box<dyn Iterator>`, because `benches/micro_scan.rs` measured the
//! difference and it is not small: a boxed iterator costs 22% on a scan returning 8
//! quads and 25% on one returning a few thousand, disappearing only at around 40,000.
//! The cost is per call, so it falls hardest on exactly the short-lived small sources
//! this trait exists to support. One boxed closure per scan is cheap; one allocation
//! per row is not.
//!
//! The same benchmark found dynamic dispatch free — `&dyn QuadSource` measured within
//! noise of direct field access at every selectivity — so consumers take `&dyn` and
//! stay monomorphisation-free.
//!
//! # Two limits worth knowing before implementing it
//!
//! **Term ids are dictionary-relative.** The `u32`s here mean nothing except against
//! the [`Dictionary`](crate::dictionary::Dictionary) that minted them. A source
//! holding its own term space has to intern through that dictionary, which is the gap
//! a `TermEncoder` abstraction would close; until then, an external source is
//! practical when its terms are interned once, on open, into the dictionary the
//! consumer uses.
//!
//! **Nothing here is fallible.** No method returns `Result`, matching `DatasetIndex`,
//! where reading cannot fail. A source that can fail mid-scan — a file, a socket —
//! has nowhere to report it, and adding `Result` later would touch every consumer.
//! That is a decision to take before the first fallible implementation, not after.

use crate::dataset_index::GraphId;
use crate::triple::Triple;

/// Somewhere quads can be read from.
///
/// Object-safe: no generic methods and no `impl Trait` in argument position, so
/// `&dyn QuadSource` works and heterogeneous sources can sit behind one type.
pub trait QuadSource {
    /// Triples of the default graph matching a pattern, `None` meaning any.
    ///
    /// Returns owned triples rather than borrowing, so a source may compute them.
    fn query_default(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple>;

    /// Calls `visit` once per quad, named graphs included.
    ///
    /// The order is whatever the source finds cheapest and must not be relied on.
    fn for_each_quad(&self, visit: &mut dyn FnMut(GraphId, u32, u32, u32));

    /// The named graphs present, excluding the default graph.
    ///
    /// Empty for a source with no notion of graphs, which is a correct answer rather
    /// than a missing feature: its quads are all in the default graph.
    fn named_graphs(&self) -> Vec<GraphId>;

    /// How many quads one graph holds.
    ///
    /// The cost model reads this, so a source for which it is expensive should expect
    /// to be planned against conservatively rather than return a guess.
    fn count_graph(&self, graph: GraphId) -> usize;
}
