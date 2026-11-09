//! Semi-naive forward chaining with the delta scanned in parallel

use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::rules::matches_rule_pattern;
use crate::reasoning::Reasoner;
use rayon::prelude::*;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl Reasoner {
    /// Semi-naive forward chaining with each round's delta scanned across rayon's thread pool
    pub fn infer_new_facts_semi_naive_parallel(&mut self) -> Vec<Triple> {
        let all_initial = self.dataset_index.query(None, None, None);
        let mut all_facts: HashSet<Triple> = all_initial.into_iter().collect();

        let mut delta = all_facts.clone();

        let mut inferred_so_far = Vec::new();

        let mut dict = shared::dictionary::Dictionary::new();
        self.with_terms(&mut |shared| dict = shared.clone());

        loop {
            let all_facts_arc = Arc::new(all_facts.clone());
            let new_facts: HashSet<Triple> = delta
                .par_iter()
                .fold(
                    || HashSet::new(),
                    |mut local_set, triple1| {
                        let candidate_rule_ids = self.rule_index.query_candidate_rules(
                            None,
                            Some(triple1.predicate),
                            None,
                        );
                        for &rule_id in candidate_rule_ids.iter() {
                            let rule = &self.rules[rule_id];
                            match rule.premise.len() {
                                1 => {
                                    let mut variable_bindings = HashMap::new();
                                    if matches_rule_pattern(
                                        &rule.premise[0],
                                        triple1,
                                        &mut variable_bindings,
                                    ) {
                                        for conclusion in &rule.conclusion {
                                            let inferred = replace_variables_with_bound_values(
                                                conclusion,
                                                &variable_bindings,
                                                &mut dict.clone(),
                                            );
                                            if !all_facts_arc.contains(&inferred) {
                                                local_set.insert(inferred);
                                            }
                                        }
                                    }
                                }

                                2 => {
                                    let mut variable_bindings_1 = HashMap::new();
                                    if matches_rule_pattern(
                                        &rule.premise[0],
                                        triple1,
                                        &mut variable_bindings_1,
                                    ) {
                                        let local_new: HashSet<Triple> = all_facts_arc
                                            .par_iter()
                                            .flat_map(|triple2| {
                                                let mut variable_bindings_2 =
                                                    variable_bindings_1.clone();
                                                if matches_rule_pattern(
                                                    &rule.premise[1],
                                                    triple2,
                                                    &mut variable_bindings_2,
                                                ) {
                                                    rule.conclusion
                                                        .iter()
                                                        .filter_map(|conclusion| {
                                                            let inferred =
                                                                replace_variables_with_bound_values(
                                                                    conclusion,
                                                                    &variable_bindings_2,
                                                                    &mut dict.clone(),
                                                                );
                                                            if !all_facts_arc.contains(&inferred) {
                                                                Some(inferred)
                                                            } else {
                                                                None
                                                            }
                                                        })
                                                        .collect::<Vec<_>>()
                                                } else {
                                                    Vec::new()
                                                }
                                            })
                                            .collect();
                                        local_set.extend(local_new);
                                    }

                                    let mut variable_bindings_1b = HashMap::new();
                                    if matches_rule_pattern(
                                        &rule.premise[1],
                                        triple1,
                                        &mut variable_bindings_1b,
                                    ) {
                                        let local_new: HashSet<Triple> = all_facts_arc
                                            .par_iter()
                                            .flat_map(|triple2| {
                                                let mut variable_bindings_2b =
                                                    variable_bindings_1b.clone();
                                                if matches_rule_pattern(
                                                    &rule.premise[0],
                                                    triple2,
                                                    &mut variable_bindings_2b,
                                                ) {
                                                    rule.conclusion
                                                        .iter()
                                                        .filter_map(|conclusion| {
                                                            let inferred =
                                                                replace_variables_with_bound_values(
                                                                    conclusion,
                                                                    &variable_bindings_2b,
                                                                    &mut dict.clone(),
                                                                );
                                                            if !all_facts_arc.contains(&inferred) {
                                                                Some(inferred)
                                                            } else {
                                                                None
                                                            }
                                                        })
                                                        .collect::<Vec<_>>()
                                                } else {
                                                    Vec::new()
                                                }
                                            })
                                            .collect();
                                        local_set.extend(local_new);
                                    }
                                }

                                _ => {}
                            }
                        }
                        local_set
                    },
                )
                .reduce(
                    || HashSet::new(),
                    |mut acc, local_set| {
                        acc.extend(local_set);
                        acc
                    },
                );

            if new_facts.is_empty() {
                break;
            } else {
                for fact in new_facts.iter() {
                    all_facts.insert(fact.clone());
                    inferred_so_far.push(fact.clone());
                    self.dataset_index.insert(fact);
                }
                delta = new_facts;
            }
        }

        inferred_so_far
    }
}
