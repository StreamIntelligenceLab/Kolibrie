/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Machine learning: declaring models, authorising them, and running them.
//!
//! Seven modules that were seven files in the crate root, named `ml_runtime`,
//! `execute_ml` and so on — a prefix doing by hand what a module does by language.
//! They are the same modules; `ml_runtime` is [`runtime`], `execute_ml` is `execute`.
//!
//! # Two stayed outside, and that is the design
//!
//! `ml_policy`, `ml_feature_loader` and `neural_registry` are still in the crate
//! root, because they are the ML *contract* rather than the ML implementation: the
//! engine depends on them, and they reach back no further than the store. Everything in here is the other
//! direction. As `lib.rs` puts it, the crate root is the one place that may name both
//! the abstraction and the implementation, and that is what keeps `execute_query` and
//! `rule_execution` from depending on the ML layer.
//!
//! Folding all ten into one module erased that distinction and closed a cycle:
//! `scripts/module_graph.py` counts a directory as one node, so a leaf the engine
//! depends on and a consumer that depends on the engine became the same node, and
//! seven modules collapsed into one strongly-connected component.
//!
//! # What the `ml` feature changes
//!
//! Most of this is present either way. Deciding *whether* a model may run is a
//! policy question the engine answers with the feature off, which is what lets a
//! build without PyO3 refuse a query cleanly instead of failing to link:
//!
//! | always | behind `ml` |
//! |---|---|
//! | [`runtime`], [`syntax`] | `execute`, `execute_train`, `predict_candle`, `predict_runtime`, `neural_relations` |
//!
//! The right-hand column is not linked, and cannot be: `cargo doc` runs with default
//! features, so those modules do not exist in the build that renders this.
//!
//! `neural_relations` is the exception that is always *named*: with the feature off
//! the name resolves to a stub module whose calls return `ML_FEATURE_DISABLED`, so
//! the rest of the crate neither grows a `cfg` nor loses a symbol. Gathering the
//! gates here is the point of the module — they were eight separate `#[cfg]`
//! attributes scattered through `lib.rs`, where it took reading all of them to see
//! which half of this was conditional.
//!
//! # The crate of the same name
//!
//! `kolibrie` depends on a crate called `ml`, which is where `execute` and
//! `neural_relations` get `MLHandler` and `MlpNeuralPredicate`. A `use ml::…`
//! inside this crate is now ambiguous between that crate and this module, so the
//! crate is named `::ml::…` with a leading `::`. Anything new reaching for it has to
//! do the same.

pub mod runtime;
pub(crate) mod syntax;

/// Running an `ML.PREDICT` clause through the Python backend.
#[cfg(feature = "ml")]
pub mod execute;
/// Training a model declared by a `TRAIN` clause.
#[cfg(feature = "ml")]
pub mod execute_train;
#[cfg(feature = "ml")]
pub(crate) mod predict_candle;
#[cfg(feature = "ml")]
pub(crate) mod predict_runtime;

/// Predicates whose facts come from a model rather than from data.
#[cfg(feature = "ml")]
pub mod neural_relations;
/// Stubs standing in for [`neural_relations`] when the `ml` feature is off, so the
/// rest of the crate compiles without PyO3 and a caller gets a clear
/// `ML_FEATURE_DISABLED` error rather than a missing symbol.
#[cfg(not(feature = "ml"))]
#[path = "neural_relations_disabled.rs"]
pub mod neural_relations;
