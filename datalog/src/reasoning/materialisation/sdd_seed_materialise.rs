/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::sdd::{BoolOp, SddProvenance, VarKind};
use shared::seed_spec::SeedSpec;
use shared::tag_store::TagStore;
use shared::triple::Triple;

use crate::reasoning::materialisation::provenance_semi_naive::semi_naive_with_initial_tags;
use crate::reasoning::Reasoner;

fn zero_triple() -> Triple {
    Triple {
        subject: 0,
        predicate: 0,
        object: 0,
    }
}

pub fn infer_new_facts_with_sdd_seed_specs(
    reasoner: &mut Reasoner,
    seeds: Vec<SeedSpec>,
) -> (Vec<Triple>, TagStore<SddProvenance>) {
    let provenance = SddProvenance::new();
    let mut initial_tags = TagStore::new(provenance.clone());

    {
        let mut mgr = provenance.manager().lock().unwrap();
        for seed in seeds {
            match seed {
                SeedSpec::Independent { triple, prob, seed_id } => {
                    mgr.ensure_variable(seed_id, prob);
                    let tag = mgr.literal(seed_id, true);
                    initial_tags.set_tag(&triple, tag);
                    if seed_id as usize >= initial_tags.seed_triples.len() {
                        initial_tags.seed_triples.resize(seed_id as usize + 1, zero_triple());
                    }
                    initial_tags.seed_triples[seed_id as usize] = triple.clone();
                    reasoner.insert_ground_triple(triple);
                }
                SeedSpec::ExclusiveGroup { group_id, choices } => {
                    let vars: Vec<u32> = choices.iter().map(|choice| choice.choice_id).collect();
                    for choice in &choices {
                        mgr.ensure_variable_weights(
                            choice.choice_id,
                            choice.prob,
                            1.0,
                            VarKind::ExclusiveGroup(group_id),
                        );
                    }
                    let eo = mgr.exactly_one(&vars);
                    for choice in choices {
                        let lit = mgr.literal(choice.choice_id, true);
                        let tag = mgr.apply(lit, eo, BoolOp::And);
                        initial_tags.set_tag(&choice.triple, tag);
                        if choice.choice_id as usize >= initial_tags.seed_triples.len() {
                            initial_tags.seed_triples.resize(choice.choice_id as usize + 1, zero_triple());
                        }
                        initial_tags.seed_triples[choice.choice_id as usize] = choice.triple.clone();
                        reasoner.insert_ground_triple(choice.triple);
                    }
                }
            }
        }
    }

    semi_naive_with_initial_tags(reasoner, provenance, initial_tags)
}
