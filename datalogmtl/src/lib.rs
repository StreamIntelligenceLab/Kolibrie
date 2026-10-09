/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

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
