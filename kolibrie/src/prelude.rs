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

/// Executes a query against a store.
//
// The name describes the implementation it happened to land on rather than what it
// does. It is re-exported verbatim on purpose: renaming it is the facade step's job
// (architecture doc §4, "one entry point").
//
// To be precise about what this buys, since it is easy to overstate: the prelude
// removes dependence on module *paths*, not on names. 47 files now import via the
// prelude, so moving this function to another crate edits this line rather than 47
// files — which is what step 6 and the crate splits do. Renaming the function itself
// still touches all 61 call sites that spell it out in their bodies, and will need a
// deprecated alias to stay incremental.
pub use crate::execute_query::execute_query_rayon_parallel2_volcano;
