/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::cross_window_sds::{Sds, all_component_iris, strip_window_prefix, translate_sds_to_datalog};
use crate::reasoning::Reasoner;
use crate::reasoning::materialisation::provenance_semi_naive::semi_naive_with_initial_tags_and_delta;
use shared::dictionary::Dictionary;
use shared::provenance::ExpirationProvenance;
use shared::rule::Rule;
use shared::tag_store::TagStore;
use shared::triple::Triple;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Internal incremental state: maps component IRI → (annotated-triple → expiry)
pub type SdsWithExpiry = HashMap<String, HashMap<Triple, u64>>;

/// Incrementally compute the SDS+ at `current_time`
pub fn incremental_sds_plus(
    rules: &[Rule],
    sds_current: &Sds,
    sds_plus_old: &SdsWithExpiry,
    dict: &Arc<RwLock<Dictionary>>,
    current_time: u64,
) -> SdsWithExpiry {
    let d_base = translate_sds_to_datalog(sds_current, dict, current_time);

    let d_old: Vec<(Triple, u64)> = sds_plus_old
        .values()
        .flat_map(|m| m.iter())
        .filter(|(_, &expiry)| expiry > current_time)
        .map(|(t, &e)| (t.clone(), e))
        .collect();

    let mut d_old_map: HashMap<Triple, u64> = HashMap::new();
    for (t, e) in &d_old {
        d_old_map
            .entry(t.clone())
            .and_modify(|old| *old = (*old).max(*e))
            .or_insert(*e);
    }

    let d_new: Vec<(Triple, u64)> = d_base
        .into_iter()
        .filter(|(t, expiry_new)| {
            d_old_map
                .get(t)
                .map_or(true, |&expiry_old| expiry_old < *expiry_new)
        })
        .collect();

    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(dict);

    for (t, _) in &d_old {
        reasoner.index_manager.insert(t);
    }
    for (t, _) in &d_new {
        reasoner.index_manager.insert(t);
    }

    // Seed TagStore with expiry values from D_old ∪ D_new.
    let mut initial_tags = TagStore::new(ExpirationProvenance);
    for (t, e) in d_old.iter().chain(d_new.iter()) {
        // set_tag skips u64::MAX (= one()), so static facts are implicitly ∞.
        if *e < u64::MAX {
            initial_tags.set_tag(t, *e);
        }
    }

    for rule in rules {
        reasoner.add_rule(rule.clone());
    }

    let initial_delta: Vec<Triple> = d_new.iter().map(|(t, _)| t.clone()).collect();
    let (_new_triples, tag_store) = semi_naive_with_initial_tags_and_delta(
        &mut reasoner,
        ExpirationProvenance,
        initial_tags,
        initial_delta,
    );

    let component_iris = all_component_iris(sds_current);
    let all_facts = reasoner.index_manager.query(None, None, None);
    let mut result: SdsWithExpiry = HashMap::new();

    for triple in all_facts {
        let pred_str = match dict.read().unwrap().decode(triple.predicate) {
            Some(s) => s.to_string(),
            None => continue,
        };

        if let Some((comp_iri, _local)) = strip_window_prefix(&pred_str, &component_iris) {
            let expiry = tag_store.get_tag(&triple);
            result
                .entry(comp_iri.to_string())
                .or_default()
                .insert(triple, expiry);
        }
    }

    result
}
