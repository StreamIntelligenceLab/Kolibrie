/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The common path: load a store, parse a query, run it

/// The RDF store
pub use crate::database::KolibrieDB;

/// Parses a combined SPARQL/RSP-QL query, including rule and prefix sections
pub use crate::parser::parse_combined_query;

/// Runs a request and returns its rows, or says why it could not
pub use crate::execute_query::execute;

/// Why a request could not be answered
pub use crate::execute_query::QueryError;

/// Why N3 text could not be read or reasoned over, as `KolibrieDB::materialize_n3` reports it
pub use n3::N3Error;
pub use crate::query_builder::Queryable;
