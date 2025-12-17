use std::collections::{BTreeSet, HashMap};
use shared::dictionary::Dictionary;
use shared::index_manager::UnifiedIndex;
use shared::rule::Rule;
use shared::terms::TriplePattern;
use crate::reasoning::materialisation::replace_variables_with_bound_values;
use crate::reasoning::Reasoner;
use crate::reasoning::rules::matches_rule_pattern;

#[derive(Debug)]
pub struct DatalogEngine {
    pub facts: UnifiedIndex,
    pub rules: Vec<Rule>,
    pub dictionary: Dictionary,
}

impl Reasoner {

    /// A convenience method to run the Datalog engine on the current KG
    pub fn datalog_query_kg(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>> {
        let mut engine = DatalogEngine::new_from_kg(self);
        engine.run_datalog();
        engine.datalog_query(query)
    }

    pub fn datalog_inferred_query(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>> {
        let mut engine = DatalogEngine::new_from_kg(self);
        let original = engine
            .facts
            .query(None, None, None)
            .into_iter()
            .collect::<BTreeSet<_>>();
        engine.run_datalog();
        let all_after = engine
            .facts
            .query(None, None, None)
            .into_iter()
            .collect::<BTreeSet<_>>();

        // newly inferred = all_after - original
        let inferred = all_after.difference(&original).cloned().collect::<Vec<_>>();

        // unify with the query
        let mut results = Vec::new();
        for fact in inferred {
            let mut vb = HashMap::new();
            if matches_rule_pattern(query, &fact, &mut vb) {
                results.push(vb);
            }
        }
        results
    }
}
// ! This isn't important
impl DatalogEngine {
    /// Construct a new DatalogEngine from an existing KnowledgeGraph
    pub fn new_from_kg(kg: &Reasoner) -> Self {
        Self {
            facts: kg.index_manager.clone(),
            rules: kg.rules.clone(),
            dictionary: kg.dictionary.clone(),
        }
    }

    /// Perform naive bottom-up evaluation until no new facts are derived
    pub fn run_datalog(&mut self) {
        let mut changed = true;
        while changed {
            changed = false;
            // Snapshot current facts
            let current_facts = self.facts.query(None, None, None);

            // For each rule, see if we can derive new facts
            for rule in &self.rules {
                match rule.premise.len() {
                    1 => {
                        let p = &rule.premise[0];
                        for fact in &current_facts {
                            let mut vmap = HashMap::new();
                            if matches_rule_pattern(p, fact, &mut vmap) {
                                // Process each conclusion
                                for conclusion in &rule.conclusion {
                                    let inferred =
                                        replace_variables_with_bound_values(conclusion, &vmap, &mut self.dictionary);
                                    // Insert if new
                                    if self.facts.insert(&inferred) {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                    2 => {
                        let p1 = &rule.premise[0];
                        let p2 = &rule.premise[1];
                        // Nested loop over all facts
                        for f1 in &current_facts {
                            for f2 in &current_facts {
                                // e.g. transitivity requirement
                                if f1.object != f2.subject {
                                    continue;
                                }
                                let mut vmap = HashMap::new();
                                if matches_rule_pattern(p1, f1, &mut vmap)
                                    && matches_rule_pattern(p2, f2, &mut vmap)
                                {
                                    // Process each conclusion
                                    for conclusion in &rule.conclusion {
                                        let inferred = replace_variables_with_bound_values(
                                            conclusion,
                                            &vmap,
                                            &mut self.dictionary,
                                        );
                                        if self.facts.insert(&inferred) {
                                            changed = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Returns variable bindings for matches
    pub fn datalog_query(&self, pattern: &TriplePattern) -> Vec<HashMap<String, u32>> {
        let mut results = Vec::new();
        let all_facts = self.facts.query(None, None, None);
        for fact in &all_facts {
            let mut vmap = HashMap::new();
            if matches_rule_pattern(pattern, fact, &mut vmap) {
                results.push(vmap);
            }
        }
        results
    }
}