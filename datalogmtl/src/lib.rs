/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! DatalogMTL over RDF: Datalog whose rule bodies can say *when* a pattern held

pub mod automata;
pub mod evaluator;
pub mod meteor_fmt;
pub mod metrics;
pub mod parser;
pub mod rdf_parser;
pub mod store;
pub mod stream;
pub mod syntax;
pub mod validate;

#[cfg(test)]
#[allow(unreachable_pub)]
mod tests;
