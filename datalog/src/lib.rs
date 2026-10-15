/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! A Datalog reasoner over dictionary-encoded RDF triples.
//!
//! Rules are evaluated to a fixpoint over a [`shared::dataset_index::DatasetIndex`],
//! with terms interned as `u32` ids by a [`shared::dictionary::Dictionary`] that the
//! reasoner shares with its caller rather than owning.
//!
//! Three things separate it from a textbook Datalog engine:
//!
//! - **Provenance.** A materialisation can carry a semiring tag per derived fact,
//!   which is what lets a probability or an expiry time be computed for a
//!   conclusion instead of only a truth value. See
//!   [`reasoning::materialisation::provenance_semi_naive`].
//! - **Streaming.** [`cross_window_sds`] represents an RSP-QL streaming dataset, so
//!   a rule can span several sliding windows; facts carry expiry times and leave the
//!   materialisation as their window moves past them.
//! - **Inconsistency.** [`reasoning::repairs`] answers queries under
//!   inconsistency-tolerant semantics instead of refusing to reason.

pub mod cross_window_sds;
pub mod parser_n3_logic;
pub mod reasoning;
