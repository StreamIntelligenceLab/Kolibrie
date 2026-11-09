/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Python bindings for Kolibrie, exported as the `kolibrie` extension module

mod py_knowledge_graph;
mod py_query_builder;
mod py_rdf_term;
mod py_rsp;

use pyo3::prelude::*;

#[pymodule]
fn kolibrie(m: &Bound<'_, PyModule>) -> PyResult<()> {
    py_knowledge_graph::register(m)?;
    py_query_builder::register(m)?;
    py_rdf_term::register(m)?;
    py_rsp::register(m)?;
    Ok(())
}
