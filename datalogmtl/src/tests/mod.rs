/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::dictionary::Dictionary;
use shared::terms::Term;
use shared::triple::Triple;
use std::sync::{Arc, RwLock};

pub fn make_dict() -> Arc<RwLock<Dictionary>> {
    Arc::new(RwLock::new(Dictionary::new()))
}

pub fn enc(dict: &Arc<RwLock<Dictionary>>, s: &str) -> u32 {
    dict.write().unwrap().encode(s)
}

pub fn triple(s: u32, p: u32, o: u32) -> Triple {
    Triple {
        subject: s,
        predicate: p,
        object: o,
    }
}

pub fn c(id: u32) -> Term {
    Term::Constant(id)
}
pub fn v(name: &str) -> Term {
    Term::Variable(name.to_string())
}

pub mod automata;
pub mod base;
pub mod box_;
pub mod conj;
pub mod diamond;
pub mod integration;
pub mod parser;
pub mod rdf_parser;
pub mod recursive;
pub mod since;
pub mod stream;
