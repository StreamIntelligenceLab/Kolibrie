/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The supported surface of Kolibrie: one crate for a consumer to depend on
//!
//! ```
//! use kolibrie_api::prelude::*;
//!
//! let mut db = KolibrieDB::new();
//! db.add_triple_parts("urn:alice", "urn:knows", "urn:bob");
//! let answers = execute("SELECT ?who WHERE { <urn:alice> <urn:knows> ?who }", &mut db)
//!     .expect("the query is valid");
//! assert_eq!(answers.len(), 1);
//! ```
//!
//! ```
//! use kolibrie_api::prelude::*;
//! // Not in the prelude, and still no second dependency entry
//! use kolibrie_api::shared::provenance::Provenance;
//!
//! fn over_any_provenance<P: Provenance>() {}
//! over_any_provenance::<kolibrie_api::shared::provenance::BooleanProvenance>();
//!
//! let mut db = KolibrieDB::new();
//! db.add_triple_parts("urn:a", "urn:p", "urn:b");
//! let answers = execute("SELECT ?o WHERE { <urn:a> <urn:p> ?o }", &mut db).unwrap();
//! assert_eq!(answers.len(), 1);
//! ```

/// Everything a consumer normally needs, in one import
pub mod prelude {
    pub use kolibrie::database::KolibrieDB;
    pub use kolibrie::execute_query::{
        execute, execute_sparql_query, execute_sparql_update, Solution, Solutions, UpdateSummary,
    };
    pub use kolibrie::parse_error::{Expected, ParseError, ParseResult};
    pub use kolibrie::query_error::QueryError;

    pub use kolibrie::query_builder::QueryBuilder;

    pub use shared::dictionary::Dictionary;
    pub use shared::rule::Rule;
    pub use shared::terms::{Binding, Bindings, Term, TriplePattern};
    pub use shared::triple::Triple;

    pub use datalog::reasoning::Reasoner;
}

pub use prelude::*;

/// The engine crate, for work that needs more than the [`prelude`]
pub use kolibrie;

/// The shared vocabulary crate
pub use shared;

/// The Datalog reasoner
pub use datalog;

/// Metric temporal Datalog
pub use datalogmtl;
