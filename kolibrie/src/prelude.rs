/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The common path: load a store, parse a query, run it.
//!
//! ```ignore
//! use kolibrie::prelude::*;
//! ```
//!
//! Membership is deliberately narrow. Before this module existed, 55 files reached
//! for `use kolibrie::<module>::*`, which made every module's entire contents part
//! of the de-facto API and meant any later module move touched all 55. Replacing
//! that with a glob over *everything* would have changed nothing.
//!
//! So the bar is breadth of use, measured rather than guessed: these three symbols
//! are needed across ~20 directories each. Everything else clusters into one or two
//! directories — the volcano optimizer types, the probabilistic-program API, the
//! datalog rule helpers — and those stay explicit imports, because a file that uses
//! the optimizer should say so.
//!
//! The test for adding something here is whether unrelated callers all need it, not
//! whether more than one does. All six `program` symbols are used by exactly two
//! files, and they are the *same* two files; that is a feature area, not a prelude.

/// The RDF store. Holds encoded triples, the term dictionary, and the indexes.
pub use crate::sparql_database::SparqlDatabase;

/// Parses a combined SPARQL/RSP-QL query, including rule and prefix sections.
pub use crate::parser::parse_combined_query;

/// Runs a request and returns its rows, or says why it could not.
///
/// The entry point to reach for.
pub use crate::execute_query::execute;

/// Why a request could not be answered.
pub use crate::execute_query::QueryError;

// `execute_query_rayon_parallel2_volcano` was here until `execute` replaced it. It is
// deprecated rather than deleted, but it is no longer in the prelude: a glob import
// should not be how anyone reaches a deprecated entry point, and re-exporting one
// here would put a deprecation warning on every file that globs this module.
