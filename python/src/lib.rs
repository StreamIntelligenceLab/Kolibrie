/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Python bindings for Kolibrie, exported as the `kolibrie` extension module.
//!
//! Two surfaces, registered by the submodules here: the SPARQL query builder and
//! the Datalog knowledge graph. The interesting documentation for this crate is
//! the Python API rather than this Rust shim, which is why the `[lib]` sets
//! `doc = false`.
//!
//! The library must be *named* `kolibrie` for Python to import it under that
//! name, which collides with the `kolibrie` crate's own rustdoc output and is the
//! other reason the docs are turned off here.

mod py_knowledge_graph;
mod py_query_builder;

use pyo3::prelude::*;

#[pymodule]
fn kolibrie(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register the Datalog/KG types
    py_knowledge_graph::register(m)?;
    // Register the SPARQL‐QueryBuilder types
    py_query_builder::register(m)?;
    Ok(())
}
