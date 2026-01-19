/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */
use serde::{Serialize, Deserialize};

#[derive(PartialEq, Debug, Clone, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Triple {
    pub subject: u32,
    pub predicate: u32,
    pub object: u32,
}

#[derive(PartialEq, Debug, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TimestampedTriple {
    pub triple: Triple,
    pub timestamp: u64,
}
