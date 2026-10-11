/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! RDF stream processing: the three-stage RSP-QL pipeline.
//!
//! A continuous query cannot evaluate over an unbounded stream, so RSP-QL cuts the
//! stream into finite relations, evaluates over those, and turns the answers back
//! into a stream:
//!
//! ```text
//! stream ──S2R──▶ relation ──R2R──▶ relation ──R2S──▶ stream
//!         (window)          (query)          (which solutions
//!                                             to publish)
//! ```
//!
//! | stage | module | job |
//! |---|---|---|
//! | S2R | [`s2r`] | windows over a stream; [`s2r::CSPARQLWindow`] is the implementation |
//! | R2R | [`r2r`], [`simple_r2r`] | evaluate the query over a window's content |
//! | R2S | [`r2s`] | choose which solutions reach the output stream |
//!
//! [`window_runner`] drives one window's S2R→R2R cycle; [`builder`] assembles an
//! engine from a parsed query. [`RSPEngine`](crate::rsp_engine::RSPEngine) owns the
//! whole thing.
//!
//! # Window bounds are half-open
//!
//! A `RANGE n` window holds `n` timestamps, not `n + 1`. This is easy to get wrong
//! in both directions and has already invalidated test expectations once.
//!
//! # More than one window changes the timing
//!
//! With a single window a result is ready as soon as that window fires. With
//! several, the engine has to decide how long to wait for the others before
//! emitting — see [`SyncPolicy`](shared::query::SyncPolicy). That choice trades
//! latency against whether one result may mix data from different cycles, so it is a
//! correctness question, not a tuning knob.

pub mod builder;
pub mod r2r;
pub mod r2s;
pub mod s2r;
pub mod simple_r2r;
pub mod window_runner;