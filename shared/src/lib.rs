/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::AtomicBool;

pub static GPU_MODE_ENABLED: AtomicBool = AtomicBool::new(false);
pub mod dictionary;
pub mod triple;
pub mod index_manager;
pub mod terms;
pub mod rule_index;
pub mod rule;
pub mod query;
pub mod join_algorithm;
