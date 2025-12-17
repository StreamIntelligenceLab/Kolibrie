use std::collections::{HashMap, HashSet};
use shared::terms::TriplePattern;
use shared::triple::Triple;
use crate::reasoning::Reasoner;
use crate::reasoning::rules::matches_rule_pattern;

impl Reasoner {

    /// New method: Query with inconsistency-tolerant semantics
    pub fn query_with_repairs(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>> {
        let all_facts: HashSet<Triple> = self
            .index_manager
            .query(None, None, None)
            .into_iter()
            .collect();

        // Compute all repairs
        let repairs = self.compute_repairs(&all_facts);

        // IAR semantics: only return answers that are present in all repairs
        let mut results = Vec::new();
        if let Some(first_repair) = repairs.first() {
            // Start with results from first repair
            for fact in first_repair {
                let mut vmap = HashMap::new();
                if matches_rule_pattern(query, fact, &mut vmap) {
                    results.push(vmap);
                }
            }

            // Filter out results not present in all repairs
            results.retain(|binding| {
                repairs.iter().skip(1).all(|repair| {
                    repair.iter().any(|fact| {
                        let mut test_map = HashMap::new();
                        matches_rule_pattern(query, fact, &mut test_map) && test_map == *binding
                    })
                })
            });
        }

        results
    }

}