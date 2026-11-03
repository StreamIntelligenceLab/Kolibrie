//! Semi-naive forward chaining with the delta scanned in parallel.
//!
//! Unfinished, and currently has no callers; retained because the approach is worth
//! keeping rather than because it is ready. It takes the dictionary write lock for
//! the whole fixpoint, which serialises anything else holding the same dictionary,
//! so the parallelism it adds inside the loop is paid for outside it.

use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::rules::matches_rule_pattern;
use crate::reasoning::Reasoner;
use rayon::prelude::*;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl Reasoner {
    /// Semi-naive forward chaining with each round's delta scanned across rayon's
    /// thread pool.
    ///
    /// Unfinished and currently unused. It holds the dictionary write lock for the
    /// whole fixpoint, so no other holder of that dictionary makes progress while
    /// it runs.
    pub fn infer_new_facts_semi_naive_parallel(&mut self) -> Vec<Triple> {
        // Collect all known facts
        let all_initial = self.dataset_index.query(None, None, None);
        let mut all_facts: HashSet<Triple> = all_initial.into_iter().collect();

        // Delta = all the initial facts
        let mut delta = all_facts.clone();

        // Keep track of newly inferred facts so we can return them later
        let mut inferred_so_far = Vec::new();

        // This strategy is unsound, and the term handling is why. It has no callers.
        //
        // A `&mut Dictionary` cannot cross rayon's workers, so the loop below clones
        // this copy per inference and interns into the clone. Those ids are minted
        // against a private counter that starts wherever the shared space happened to
        // be, so:
        //
        // - the shared space can later assign the same number to a different term,
        //   and then a fact stored here decodes to the wrong string;
        // - two workers in one run mint the same id for different terms, so the
        //   results are not even internally consistent.
        //
        // And the facts are not discarded: the loop inserts them into
        // `self.dataset_index` and returns them. So this fabricates term ids and
        // stores them as if they were real, rather than merely losing them, which is
        // what an earlier version of this comment said.
        //
        // Fixing it means keeping the parallel section free of interning -- deriving
        // conclusions as unresolved terms and interning them sequentially afterwards
        // -- which is a rewrite of the strategy rather than a change to this line.
        let mut dict = shared::dictionary::Dictionary::new();
        self.with_terms(&mut |shared| dict = shared.clone());

        // Repeat until no new facts are inferred
        loop {
            // Wrap all_facts in an Arc for shared read-only access in parallel
            let all_facts_arc = Arc::new(all_facts.clone());
            let new_facts: HashSet<Triple> = delta
                .par_iter()
                .fold(
                    || HashSet::new(),
                    |mut local_set, triple1| {
                        // Use only the predicate for candidate rule lookup
                        let candidate_rule_ids = self.rule_index.query_candidate_rules(
                            None,
                            Some(triple1.predicate),
                            None,
                        );
                        for &rule_id in candidate_rule_ids.iter() {
                            let rule = &self.rules[rule_id];
                            match rule.premise.len() {
                                1 => {
                                    // Single-premise rule
                                    let mut variable_bindings = HashMap::new();
                                    if matches_rule_pattern(
                                        &rule.premise[0],
                                        triple1,
                                        &mut variable_bindings,
                                    ) {
                                        // Process each conclusion
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
                                    // Two-premise rule
                                    let mut variable_bindings_1 = HashMap::new();
                                    if matches_rule_pattern(
                                        &rule.premise[0],
                                        triple1,
                                        &mut variable_bindings_1,
                                    ) {
                                        // Process join in parallel over all_facts
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
                                                    // Process each conclusion
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

                                    // Option 2: Assume triple1 matches the second premise
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
                                                    // Process each conclusion
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

            // If no new facts were found, we've reached a fixpoint
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
