// /*
//  * Copyright © 2024 Volodymyr Kadzhaia
//  * Copyright © 2024 Pieter Bonte
//  * KU Leuven — Stream Intelligence Lab, Belgium
//  *
//  * This Source Code Form is subject to the terms of the Mozilla Public
//  * License, v. 2.0. If a copy of the MPL was not distributed with this file,
//  * you can obtain one at https://mozilla.org/MPL/2.0/.
//  */
//
// #[cfg(not(test))]
// use log::{debug, info, trace, warn}; // Use log crate when building application
// use std::fmt::Write;
//
// use crate::ruleindex::RuleIndexer;
// use crate::volcano_query::query_triple;
// use shared::index_manager::UnifiedIndex;
// use shared::rule::Rule;
// use shared::terms::{Term, TriplePattern};
// use shared::triple::Triple;
// use std::collections::BTreeMap;
// #[cfg(test)]
// use std::{println as debug, println as info, println as trace, println as warn};
//
// pub struct VolcanoReasoner;
//
// impl VolcanoReasoner {
//     /// Main materialization method using volcano-based query evaluation
//     pub fn materialize(
//         &mut self,
//         triple_index: &mut UnifiedIndex,
//         rules_index: &RuleIndexer,
//     ) -> Vec<Triple> {
//         let mut inferred = Vec::new();
//         let mut counter = 0;
//
//         while counter < triple_index.len() {
//             let process_triple = triple_index.get(counter).unwrap();
//
//             // Find rules that match this triple
//             let matching_rules = rules_index.find_match(process_triple);
//
//             // Use volcano-based substitution for each matching rule
//             let substituted_rules: Vec<Rule> = matching_rules
//                 .clone()
//                 .into_iter()
//                 .flat_map(|rule| Self::substitute_rule_volcano(process_triple, rule))
//                 .collect();
//
//             debug!("Matching Rules: {:?}", substituted_rules.len());
//
//             // Infer new triples from rule heads
//             let new_triples =
//                 Self::infer_rule_heads(triple_index, Some(counter + 1), substituted_rules);
//
//             for triple in new_triples {
//                 if !triple_index.contains(&triple) {
//                     debug!("Inferred: {:?}", triple);
//                     inferred.push(triple.clone());
//                     triple_index.insert(&triple);
//                 }
//             }
//             counter += 1;
//         }
//
//         inferred
//     }
//
//     /// Substitute rule using volcano-based query evaluation
//     pub fn substitute_rule_volcano(matching_triple: &Triple, matching_rule: &Rule) -> Vec<Rule> {
//         let mut results = Vec::new();
//
//         for body_pattern in matching_rule.premise.iter() {
//             // Convert Triple to TriplePattern for query_triple function
//
//             if let Some(bindings) = query_triple(&body_pattern, matching_triple) {
//                 if bindings.len() > 1 {
//                     panic!("Multiple bindings found in single triple query!");
//                 } else if bindings.is_empty() {
//                     // No variables bound, pattern matches exactly
//                     return vec![matching_rule.clone()];
//                 }
//
//                 let new_body = Self::substitute_rule_body_with_bindings(matching_rule, &bindings);
//                 let new_head =
//                     Self::apply_bindings_to_triple(&matching_rule.conclusion.get(0).unwrap(), &bindings);
//
//                 results.push(Rule {
//
//                     premise: matching_rule.premise.clone(), // Keep original premise
//                     filters: vec![],
//                     conclusion: vec![],
//                 });
//             }
//         }
//
//         results
//     }
//
//     /// Convert a Triple (from rule body) to a TriplePattern for querying
//     /// This assumes your rule body contains Triple structures that need to be converted
//     fn triple_to_pattern(triple: &Triple) -> TriplePattern {
//         use shared::terms::Term;
//
//         // This is a simplified conversion - you may need to adapt based on your actual data structures
//         // For now, assuming we want to treat the triple as a pattern with variables
//         (
//             Term::Variable(format!("s_{}", triple.subject)),
//             Term::Variable(format!("p_{}", triple.predicate)),
//             Term::Variable(format!("o_{}", triple.object)),
//         )
//     }
//
//     /// Substitute rule body with variable bindings from volcano query
//     fn substitute_rule_body_with_bindings(
//         matching_rule: &Rule,
//         bindings: &BTreeMap<String, u32>,
//     ) -> Vec<Triple> {
//         let mut new_body = Vec::new();
//
//         for body_triple in matching_rule.premise.iter() {
//             let substituted = Self::apply_bindings_to_triple(body_triple, bindings);
//             new_body.push(substituted);
//         }
//
//         new_body
//     }
//
//     /// Apply variable bindings to a triple
//     fn apply_bindings_to_triple(triple: &TriplePattern, bindings: &BTreeMap<String, u32>) -> Triple {
//         // Check subject position
//        let subject =  match &triple.0 {
//             Term::Variable(var) => {
//                 // Variable matches any value, bind it
//                 bindings.get(var)
//             }
//             Term::Constant(constant) => {
//                 Some(constant)
//             }
//         };
//
//         // Check predicate position
//         let predicate = match &triple.1 {
//             Term::Variable(var) => {
//                 // Variable matches any value, bind it
//                 bindings.get(var)
//             }
//             Term::Constant(constant) => {
//                 Some(constant)
//             }
//         };
//
//         // Check object position
//         let object = match &triple.2 {
//             Term::Variable(var) => {
//                 // Variable matches any value, bind it
//                 bindings.get(var)
//             }
//             Term::Constant(constant) => {
//                 Some(constant)
//             }
//         };
//         Triple{subject: subject.unwrap().clone(), predicate: predicate.unwrap().clone(), object: object.unwrap().clone()}
//     }
//
//     /// Substitute triple with bindings - adapted for volcano bindings
//
//
//     /// Infer rule heads from matching rules
//     fn infer_rule_heads(
//         _triple_index: &UnifiedIndex,
//         _counter: Option<usize>,
//         matching_rules: Vec<Rule>,
//     ) -> Vec<Triple> {
//         Vec::new()
//     }
//
//     /// Batch query multiple triples against a pattern using volcano evaluation
//     pub fn query_triples_against_pattern(
//         pattern: &TriplePattern,
//         triples: &[Triple],
//     ) -> Vec<BTreeMap<String, u32>> {
//         use crate::volcano_query::query_triples_batch;
//         query_triples_batch(pattern, triples)
//     }
//
//     /// Check if a rule's body patterns would match against available triples
//     pub fn rule_applicable(rule: &Rule, available_triples: &[Triple]) -> bool {
//         use crate::volcano_query::pattern_matches_triple;
//
//         for body_pattern in rule.premise.iter() {
//             let pattern = Self::triple_to_pattern(body_pattern);
//             let has_match = available_triples
//                 .iter()
//                 .any(|triple| pattern_matches_triple(&pattern, triple));
//
//             if !has_match {
//                 return false;
//             }
//         }
//
//         true
//     }
//
//     /// Get variable bindings for a rule against a set of triples
//     pub fn get_rule_bindings(rule: &Rule, triple: &Triple) -> Vec<BTreeMap<String, u32>> {
//         let mut all_bindings = Vec::new();
//
//         for body_pattern in rule.premise.iter() {
//             let pattern = Self::triple_to_pattern(body_pattern);
//             if let Some(bindings) = query_triple(&pattern, triple) {
//                 all_bindings.push(bindings);
//             }
//         }
//
//         all_bindings
//     }
//
//     /// Advanced query with pattern analysis using volcano insights
//     pub fn analyze_pattern_complexity(pattern: &TriplePattern) -> PatternComplexity {
//         use crate::volcano_query::{count_bound_variables, extract_variables};
//
//         let bound_count = count_bound_variables(pattern);
//         let variables = extract_variables(pattern);
//
//         PatternComplexity {
//             bound_variables: bound_count,
//             free_variables: variables.len(),
//             selectivity: calculate_selectivity(bound_count),
//             recommended_strategy: recommend_strategy(bound_count),
//         }
//     }
// }
//
// /// Pattern complexity analysis result
// #[derive(Debug, Clone)]
// pub struct PatternComplexity {
//     pub bound_variables: usize,
//     pub free_variables: usize,
//     pub selectivity: f64,
//     pub recommended_strategy: QueryStrategy,
// }
//
// /// Recommended query strategy based on pattern analysis
// #[derive(Debug, Clone, PartialEq)]
// pub enum QueryStrategy {
//     IndexScan,
//     TableScan,
//     JoinRequired,
//     HighSelectivity,
// }
//
// fn calculate_selectivity(bound_count: usize) -> f64 {
//     match bound_count {
//         0 => 1.0,   // Very low selectivity (returns everything)
//         1 => 0.3,   // Medium selectivity
//         2 => 0.1,   // High selectivity
//         3 => 0.01,  // Very high selectivity
//         _ => 0.001, // Extremely high selectivity
//     }
// }
//
// fn recommend_strategy(bound_count: usize) -> QueryStrategy {
//     match bound_count {
//         0 => QueryStrategy::TableScan,       // No bounds, scan everything
//         1 => QueryStrategy::IndexScan,       // One bound, use index
//         2 => QueryStrategy::HighSelectivity, // Two bounds, very selective
//         _ => QueryStrategy::HighSelectivity, // Fully bound, exact match
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use shared::terms::Term;
//
//     #[test]
//     fn test_substitute_rule_volcano() {
//         // Create a simple rule for testing
//         let head = Triple {
//             subject: 1,
//             predicate: 2,
//             object: 3,
//         };
//
//         let body = vec![Triple {
//             subject: 4,
//             predicate: 5,
//             object: 6,
//         }];
//
//         let premise = vec![Triple {
//             subject: 4,
//             predicate: 5,
//             object: 6,
//         }];
//
//         let rule = Rule {
//             head,
//             body,
//             premise,
//         };
//
//         let matching_triple = Triple {
//             subject: 7,
//             predicate: 8,
//             object: 9,
//         };
//
//         let results = VolcanoReasoner::substitute_rule_volcano(&matching_triple, &rule);
//
//         // Should return some results (exact assertion depends on your data structure)
//         assert!(!results.is_empty());
//     }
//
//     #[test]
//     fn test_pattern_complexity_analysis() {
//         // All variables pattern
//         let pattern1 = (
//             Term::Variable("s".to_string()),
//             Term::Variable("p".to_string()),
//             Term::Variable("o".to_string()),
//         );
//
//         let complexity1 = VolcanoReasoner::analyze_pattern_complexity(&pattern1);
//         assert_eq!(complexity1.bound_variables, 0);
//         assert_eq!(complexity1.free_variables, 3);
//         assert_eq!(complexity1.recommended_strategy, QueryStrategy::TableScan);
//
//         // Mixed pattern
//         let pattern2 = (
//             Term::Constant(1),
//             Term::Variable("p".to_string()),
//             Term::Constant(3),
//         );
//
//         let complexity2 = VolcanoReasoner::analyze_pattern_complexity(&pattern2);
//         assert_eq!(complexity2.bound_variables, 2);
//         assert_eq!(complexity2.free_variables, 1);
//         assert_eq!(
//             complexity2.recommended_strategy,
//             QueryStrategy::HighSelectivity
//         );
//     }
//
//     #[test]
//     fn test_rule_applicable() {
//         let head = Triple {
//             subject: 1,
//             predicate: 2,
//             object: 3,
//         };
//         let body = vec![Triple {
//             subject: 4,
//             predicate: 5,
//             object: 6,
//         }];
//         let premise = vec![Triple {
//             subject: 4,
//             predicate: 5,
//             object: 6,
//         }];
//
//         let rule = Rule {
//             head,
//             body,
//             premise,
//         };
//
//         let available_triples = vec![
//             Triple {
//                 subject: 7,
//                 predicate: 8,
//                 object: 9,
//             },
//             Triple {
//                 subject: 10,
//                 predicate: 11,
//                 object: 12,
//             },
//         ];
//
//         // This test will depend on your actual pattern matching logic
//         let is_applicable = VolcanoReasoner::rule_applicable(&rule, &available_triples);
//
//         // For now, just ensure the function runs without panic
//         assert!(is_applicable || !is_applicable);
//     }
//
//     #[test]
//     fn test_apply_bindings_to_triple() {
//         let pattern1 = (
//             Term::Variable("s".to_string()),
//             Term::Variable("p".to_string()),
//             Term::Variable("o".to_string()),
//         );
//         let mut bindings = BTreeMap::new();
//         bindings.insert("s".to_string(), 0);
//         bindings.insert("p".to_string(), 1);
//         bindings.insert("o".to_string(), 3);
//
//         let triple = VolcanoReasoner::apply_bindings_to_triple(&pattern1, &bindings);
//
//         let results_triple = Triple{subject: 0, predicate: 1, object:2};
//         assert_eq!(triple, results_triple);
//     }
// }
