/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Recomputing a materialised streaming dataset from scratch at each time point

use crate::cross_window_sds::{translate_datalog_back, translate_sds_to_datalog, Sds};
use crate::reasoning::Reasoner;
use shared::rule::Rule;
use shared::term_encoder::SharedTerms;
use shared::triple::Triple;
use std::collections::HashMap;

/// Compute the SDS+ (materialized SDS) from scratch at `current_time`
pub fn naive_sds_plus(
    rules: &[Rule],
    sds: &Sds,
    dict: &SharedTerms,
    current_time: u64,
) -> HashMap<String, Vec<Triple>> {
    let annotated = translate_sds_to_datalog(sds, dict, current_time);

    let mut reasoner = Reasoner::new();
    reasoner.adopt_terms(dict.clone());

    for (triple, _expiry) in &annotated {
        reasoner.dataset_index.insert(triple);
    }

    for rule in rules {
        reasoner.add_rule(rule.clone());
    }

    reasoner.infer_new_facts_semi_naive();

    let all_facts = reasoner.dataset_index.query(None, None, None);
    translate_datalog_back(&all_facts, dict, sds)
}
