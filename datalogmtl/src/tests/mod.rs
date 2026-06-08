/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, RwLock};
use shared::dictionary::Dictionary;
use shared::triple::Triple;
use shared::terms::Term;

pub fn make_dict() -> Arc<RwLock<Dictionary>> {
    Arc::new(RwLock::new(Dictionary::new()))
}

pub fn enc(dict: &Arc<RwLock<Dictionary>>, s: &str) -> u32 {
    dict.write().unwrap().encode(s)
}

pub fn triple(s: u32, p: u32, o: u32) -> Triple {
    Triple { subject: s, predicate: p, object: o }
}

pub fn c(id: u32) -> Term { Term::Constant(id) }
pub fn v(name: &str) -> Term { Term::Variable(name.to_string()) }

pub mod base;
pub mod diamond;
pub mod box_;
pub mod since;
pub mod recursive;
pub mod integration;
pub mod stream;
