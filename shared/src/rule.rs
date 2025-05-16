/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::terms::TriplePattern;

#[derive(Debug, Clone)]
pub struct FilterCondition {
    pub variable: String,
    pub operator: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Rule {
    pub premise: Vec<TriplePattern>,
    pub filters: Vec<FilterCondition>,
    pub conclusion: Vec<TriplePattern>,
}