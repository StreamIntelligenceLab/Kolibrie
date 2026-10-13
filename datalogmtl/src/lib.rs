/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! DatalogMTL over RDF: Datalog whose rule bodies can say *when* a pattern held.
//!
//! `Box[0,10]` for throughout the last 10 time units, `Diamond[0,10]` for
//! somewhere in them, `Since` for continuously since a trigger. [`syntax`] defines
//! the operators, [`parser`] reads them from text, and [`validate`] rejects the
//! programs that would derive nothing.
//!
//! # Two evaluation strategies
//!
//! |  | [`evaluator`] (tick) | [`automata`] (interval-native) |
//! |---|---|---|
//! | time | integer points, one tick at a time | real-line intervals |
//! | cost grows with | window width | number of interval endpoints |
//! | streaming | yes | no, needs the whole timeline |
//! | future operators | no | yes |
//! | unbounded time | no | yes, via [`automata::omega`] |
//!
//! The two do not agree in general, and neither is wrong: the tick engine decides
//! `Box` by checking every integer point in its window, so it answers over ℤ,
//! while the interval engine answers over ℝ and matches the MeTeoR reference
//! reasoner exactly. Choose the tick engine for a stream and the interval engine
//! for a fixed corpus.
//!
//! This crate does not depend on `datalog`, deliberately — the two reasoners are
//! siblings over [`shared`]'s vocabulary rather than one layered on the other.

pub mod syntax;
pub mod validate;
pub mod metrics;
pub mod store;
pub mod evaluator;
pub mod stream;
pub mod parser;
pub mod rdf_parser;
pub mod meteor_fmt;
pub mod automata;

// The test modules share fixtures via `pub`, which `unreachable_pub` flags because
// `tests` itself is private. That is the lint working correctly on code where it has
// nothing useful to say.
#[cfg(test)]
#[allow(unreachable_pub)]
mod tests;
