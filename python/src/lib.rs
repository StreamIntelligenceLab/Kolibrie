/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod py_query_builder;
mod py_knowledge_graph;

use pyo3::prelude::*;

#[pymodule]
fn py_kolibrie(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register the Datalog/KG types
    py_knowledge_graph::register(m)?;
    // Register the SPARQL‐QueryBuilder types
    py_query_builder::register(m)?;
    Ok(())
}