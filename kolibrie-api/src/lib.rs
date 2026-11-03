/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The supported surface of Kolibrie: one crate for a consumer to depend on.
//!
//! # Why this exists
//!
//! The workspace is six crates, and using it meant depending on several and knowing
//! which held what. Measured across the consumers in this repository -- the CLI, the
//! HTTP server, the Python bindings and the examples -- they reach into five:
//! `kolibrie` 121 times, `shared` 41, `datalog` 15, `ml` 6 and `datalogmtl` 6. A
//! reader had to know that a query runs through `kolibrie` but a [`Term`] lives in
//! `shared`, which is a fact about how this repository is organised and not about
//! RDF.
//!
//! So this crate re-exports what those consumers actually use, and nothing else. It
//! holds no logic: every item here is defined in one of the crates below.
//!
//! ```
//! use kolibrie_api::prelude::*;
//!
//! let mut db = SparqlDatabase::new();
//! db.add_triple_parts("urn:alice", "urn:knows", "urn:bob");
//! let answers = execute("SELECT ?who WHERE { <urn:alice> <urn:knows> ?who }", &mut db)
//!     .expect("the query is valid");
//! assert_eq!(answers.len(), 1);
//! ```
//!
//! Anything the prelude leaves out is still reachable, because the crates it fronts
//! are re-exported. Nothing is walled off, and a consumer never needs a second
//! dependency entry to get at a specialised type:
//!
//! ```
//! use kolibrie_api::prelude::*;
//! // Not in the prelude, and still no second dependency entry.
//! use kolibrie_api::shared::provenance::Provenance;
//!
//! fn over_any_provenance<P: Provenance>() {}
//! over_any_provenance::<kolibrie_api::shared::provenance::BooleanProvenance>();
//!
//! let mut db = SparqlDatabase::new();
//! db.add_triple_parts("urn:a", "urn:p", "urn:b");
//! let answers = execute("SELECT ?o WHERE { <urn:a> <urn:p> ?o }", &mut db).unwrap();
//! assert_eq!(answers.len(), 1);
//! ```
//!
//! # What it deliberately leaves out
//!
//! The engine's internals: the optimizer's operators, the execution engine, the
//! parser's combinators, the window and stream machinery. Those are reachable by
//! depending on [`kolibrie`] directly, which is the right thing to do for work on the
//! engine and the wrong thing for using it. Keeping them out is what lets them change
//! without breaking a consumer.
//!
//! # The crates underneath
//!
//! | crate | what it holds |
//! |---|---|
//! | [`kolibrie`] | the store, the parser, the query engine, RSP |
//! | [`shared`] | the vocabulary: terms, triples, rules, dictionaries |
//! | [`datalog`] | the Datalog reasoner |
//! | [`datalogmtl`] | metric temporal Datalog |

/// Everything a consumer normally needs, in one import.
///
/// Re-exported rather than defined, so there is one definition of each item and this
/// crate cannot drift from the crates it fronts.
pub mod prelude {
    // Running a request, and what comes back.
    pub use kolibrie::execute_query::{
        execute, execute_sparql_query, execute_sparql_update, Solution, Solutions, UpdateSummary,
    };
    pub use kolibrie::parse_error::{Expected, ParseError, ParseResult};
    pub use kolibrie::query_error::QueryError;
    pub use kolibrie::sparql_database::SparqlDatabase;

    // Building a request without writing SPARQL text.
    pub use kolibrie::query_builder::QueryBuilder;

    // The vocabulary. A reader should not have to know these live in another crate.
    pub use shared::dictionary::Dictionary;
    pub use shared::rule::Rule;
    pub use shared::terms::{Binding, Bindings, Term, TriplePattern};
    pub use shared::triple::Triple;

    // Reasoning.
    pub use datalog::reasoning::Reasoner;
}

pub use prelude::*;

/// The engine crate, for work that needs more than the [`prelude`].
pub use kolibrie;

/// The shared vocabulary crate.
pub use shared;

/// The Datalog reasoner.
pub use datalog;

/// Metric temporal Datalog.
pub use datalogmtl;
