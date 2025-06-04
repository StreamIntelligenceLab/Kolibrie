/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::utils::current_timestamp;

#[derive(PartialEq, Debug, Clone)]
pub struct SlidingWindow {
    pub width: u64,
    pub slide: u64,
    pub last_evaluated: u64,
}

impl SlidingWindow {
    pub fn new(width: u64, slide: u64) -> Self {
        Self {
            width,
            slide,
            last_evaluated: current_timestamp(),
        }
    }
}
