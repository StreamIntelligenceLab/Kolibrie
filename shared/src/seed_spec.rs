/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::triple::Triple;

#[derive(Debug, Clone)]
pub enum SeedSpec {
    Independent {
        triple: Triple,
        prob: f64,
        seed_id: u32,
    },
    ExclusiveGroup {
        group_id: u32,
        choices: Vec<ExclusiveChoice>,
    },
}

#[derive(Debug, Clone)]
pub struct ExclusiveChoice {
    pub triple: Triple,
    pub prob: f64,
    pub choice_id: u32,
}
