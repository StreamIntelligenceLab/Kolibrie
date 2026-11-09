/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{self, Debug};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
/// A user-defined function stored so the database can be cloned
pub struct ClonableFn(Arc<dyn Fn(Vec<&str>) -> String + Send + Sync>);

impl ClonableFn {
    /// Wraps `f` for storage in a database
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(Vec<&str>) -> String + Send + Sync + 'static,
    {
        ClonableFn(Arc::new(f))
    }

    /// Invokes the function with lexical arguments, as they appear in the query
    pub fn call(&self, args: Vec<&str>) -> String {
        (self.0)(args)
    }
}

impl Debug for ClonableFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClonableFn(<function>)")
    }
}

/// Seconds since the Unix epoch
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

/// How many threads are worth starting for a parallel parse
pub fn get_num_cpus() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}
