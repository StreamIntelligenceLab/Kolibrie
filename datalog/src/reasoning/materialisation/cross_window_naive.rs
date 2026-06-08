/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::cross_window_sds::{Sds, translate_sds_to_datalog, translate_datalog_back};
use crate::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Compute the SDS+ (materialized SDS) from scratch at `current_time`
pub fn naive_sds_plus(
    rules: &[Rule],
    sds: &Sds,
    dict: &Arc<RwLock<Dictionary>>,
    current_time: u64,
) -> HashMap<String, Vec<Triple>> {
    let annotated = translate_sds_to_datalog(sds, dict, current_time);

    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(dict);

    for (triple, _expiry) in &annotated {
        reasoner.index_manager.insert(triple);
    }

    for rule in rules {
        reasoner.add_rule(rule.clone());
    }

    reasoner.infer_new_facts_semi_naive();

    let all_facts = reasoner.index_manager.query(None, None, None);
    translate_datalog_back(&all_facts, dict, sds)
}
