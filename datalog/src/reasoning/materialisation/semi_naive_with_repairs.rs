use std::collections::HashSet;
use shared::index_manager::UnifiedIndex;
use shared::triple::Triple;
use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::Reasoner;
use crate::reasoning::rules::{evaluate_filters, join_rule};

impl Reasoner {

    /// Modified infer_new_facts_semi_naive to handle inconsistencies
    pub fn infer_new_facts_semi_naive_with_repairs(&mut self) -> Vec<Triple> {
        let all_initial = self.index_manager.query(None, None, None);
        let mut all_facts: HashSet<Triple> = all_initial.into_iter().collect();

        // First, check if initial facts are consistent
        if self.violates_constraints(&all_facts) {
            let repairs = self.compute_repairs(&all_facts);
            if let Some(best_repair) = repairs.into_iter().max_by_key(|r| r.len()) {
                // Clear index manager and reinsert repaired facts
                self.index_manager = UnifiedIndex::new();
                for fact in &best_repair {
                    self.index_manager.insert(fact);
                }
                all_facts = best_repair;
            }
        }

        let mut delta = all_facts.clone();
        let mut inferred_so_far = Vec::new();

        let mut dict = self.dictionary.write().unwrap();

        loop {
            let mut new_delta = HashSet::new();

            // Process each rule using the semi-naive approach
            for rule in &self.rules {
                let bindings = join_rule(rule, &all_facts, &delta);
                for binding in bindings {
                    if evaluate_filters(&binding, &rule.filters, &dict) {
                        // Process each conclusion
                        for conclusion in &rule.conclusion {
                            let inferred =
                                replace_variables_with_bound_values(conclusion, &binding, &mut dict);

                            // Check if adding this fact would cause inconsistency
                            let mut temp_facts = all_facts.clone();
                            temp_facts.insert(inferred.clone());

                            if !self.violates_constraints(&temp_facts) {
                                if self.index_manager.insert(&inferred)
                                    && !all_facts.contains(&inferred)
                                {
                                    new_delta.insert(inferred.clone());
                                    all_facts.insert(inferred.clone());
                                    inferred_so_far.push(inferred);
                                }
                            }
                        }
                    }
                }
            }

            // Terminate when no new facts were inferred
            if new_delta.is_empty() {
                break;
            }

            delta = new_delta;
        }

        inferred_so_far
    }

}