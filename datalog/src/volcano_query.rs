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
// use shared::terms::{Term, TriplePattern};
// use shared::triple::Triple;
// use std::collections::BTreeMap;
//
// /// Query a single triple against a triple pattern using volcano optimizer logic
// /// Returns Some(bindings) if the triple matches the pattern, None otherwise
// ///
// /// This function is extracted from the volcano optimizer's table scan logic
// /// and optimized for single triple matching in semi-naive datalog evaluation.
// pub fn query_triple(pattern: &TriplePattern, triple: &Triple) -> Option<BTreeMap<String, u32>> {
//     let mut bindings = BTreeMap::new();
//
//     // Check subject position
//     match &pattern.0 {
//         Term::Variable(var) => {
//             // Variable matches any value, bind it
//             bindings.insert(var.clone(), triple.subject);
//         }
//         Term::Constant(constant) => {
//             // Constant must match exactly
//             if triple.subject != *constant {
//                 return None;
//             }
//         }
//     }
//
//     // Check predicate position
//     match &pattern.1 {
//         Term::Variable(var) => {
//             // Variable matches any value, bind it
//             bindings.insert(var.clone(), triple.predicate);
//         }
//         Term::Constant(constant) => {
//             // Constant must match exactly
//             if triple.predicate != *constant {
//                 return None;
//             }
//         }
//     }
//
//     // Check object position
//     match &pattern.2 {
//         Term::Variable(var) => {
//             // Variable matches any value, bind it
//             bindings.insert(var.clone(), triple.object);
//         }
//         Term::Constant(constant) => {
//             // Constant must match exactly
//             if triple.object != *constant {
//                 return None;
//             }
//         }
//     }
//
//     // All positions matched, return the bindings
//     Some(bindings)
// }
//
// /// Extended version that can handle multiple triples against a single pattern
// /// Useful for batch processing in semi-naive evaluation
// pub fn query_triples_batch(
//     pattern: &TriplePattern,
//     triples: &[Triple],
// ) -> Vec<BTreeMap<String, u32>> {
//     triples
//         .iter()
//         .filter_map(|triple| query_triple(pattern, triple))
//         .collect()
// }
//
// /// Helper function to check if a pattern would match without creating bindings
// /// Useful for quick filtering in rule matching
// pub fn pattern_matches_triple(pattern: &TriplePattern, triple: &Triple) -> bool {
//     // Check subject
//     if let Term::Constant(constant) = &pattern.0 {
//         if triple.subject != *constant {
//             return false;
//         }
//     }
//
//     // Check predicate
//     if let Term::Constant(constant) = &pattern.1 {
//         if triple.predicate != *constant {
//             return false;
//         }
//     }
//
//     // Check object
//     if let Term::Constant(constant) = &pattern.2 {
//         if triple.object != *constant {
//             return false;
//         }
//     }
//
//     true
// }
//
// /// Utility function to count bound variables in a pattern
// /// Useful for query planning decisions
// pub fn count_bound_variables(pattern: &TriplePattern) -> usize {
//     let mut count = 0;
//     if matches!(pattern.0, Term::Constant(_)) {
//         count += 1;
//     }
//     if matches!(pattern.1, Term::Constant(_)) {
//         count += 1;
//     }
//     if matches!(pattern.2, Term::Constant(_)) {
//         count += 1;
//     }
//     count
// }
//
// /// Get all variable names from a pattern
// /// Useful for projection operations
// pub fn extract_variables(pattern: &TriplePattern) -> Vec<String> {
//     let mut vars = Vec::new();
//
//     if let Term::Variable(var) = &pattern.0 {
//         vars.push(var.clone());
//     }
//
//     if let Term::Variable(var) = &pattern.1 {
//         vars.push(var.clone());
//     }
//
//     if let Term::Variable(var) = &pattern.2 {
//         vars.push(var.clone());
//     }
//
//     vars
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_query_triple_all_variables() {
//         // Pattern: (?s, ?p, ?o)
//         let pattern = (
//             Term::Variable("s".to_string()),
//             Term::Variable("p".to_string()),
//             Term::Variable("o".to_string()),
//         );
//
//         let triple = Triple {
//             subject: 1,
//             predicate: 2,
//             object: 3,
//         };
//
//         let result = query_triple(&pattern, &triple);
//         assert!(result.is_some());
//
//         let bindings = result.unwrap();
//         assert_eq!(bindings.get("s"), Some(&1));
//         assert_eq!(bindings.get("p"), Some(&2));
//         assert_eq!(bindings.get("o"), Some(&3));
//     }
//
//     #[test]
//     fn test_query_triple_mixed_pattern() {
//         // Pattern: (1, ?p, 3)
//         let pattern = (
//             Term::Constant(1),
//             Term::Variable("p".to_string()),
//             Term::Constant(3),
//         );
//
//         let matching_triple = Triple {
//             subject: 1,
//             predicate: 2,
//             object: 3,
//         };
//
//         let non_matching_triple = Triple {
//             subject: 1,
//             predicate: 2,
//             object: 4, // Different object
//         };
//
//         // Should match
//         let result = query_triple(&pattern, &matching_triple);
//         assert!(result.is_some());
//         let bindings = result.unwrap();
//         assert_eq!(bindings.get("p"), Some(&2));
//         assert_eq!(bindings.len(), 1); // Only one variable bound
//
//         // Should not match
//         let result = query_triple(&pattern, &non_matching_triple);
//         assert!(result.is_none());
//     }
//
//     #[test]
//     fn test_query_triple_all_constants() {
//         // Pattern: (1, 2, 3)
//         let pattern = (Term::Constant(1), Term::Constant(2), Term::Constant(3));
//
//         let matching_triple = Triple {
//             subject: 1,
//             predicate: 2,
//             object: 3,
//         };
//
//         let non_matching_triple = Triple {
//             subject: 1,
//             predicate: 2,
//             object: 4,
//         };
//
//         // Should match with empty bindings
//         let result = query_triple(&pattern, &matching_triple);
//         assert!(result.is_some());
//         let bindings = result.unwrap();
//         assert!(bindings.is_empty());
//
//         // Should not match
//         let result = query_triple(&pattern, &non_matching_triple);
//         assert!(result.is_none());
//     }
//
//     #[test]
//     fn test_pattern_matches_triple() {
//         let pattern = (
//             Term::Constant(1),
//             Term::Variable("p".to_string()),
//             Term::Constant(3),
//         );
//
//         let matching_triple = Triple {
//             subject: 1,
//             predicate: 2,
//             object: 3,
//         };
//
//         let non_matching_triple = Triple {
//             subject: 2, // Different subject
//             predicate: 2,
//             object: 3,
//         };
//
//         assert!(pattern_matches_triple(&pattern, &matching_triple));
//         assert!(!pattern_matches_triple(&pattern, &non_matching_triple));
//     }
//
//     #[test]
//     fn test_query_triples_batch() {
//         let pattern = (
//             Term::Variable("s".to_string()),
//             Term::Constant(2),
//             Term::Variable("o".to_string()),
//         );
//
//         let triples = vec![
//             Triple {
//                 subject: 1,
//                 predicate: 2,
//                 object: 3,
//             }, // Matches
//             Triple {
//                 subject: 1,
//                 predicate: 3,
//                 object: 3,
//             }, // Doesn't match (wrong predicate)
//             Triple {
//                 subject: 4,
//                 predicate: 2,
//                 object: 5,
//             }, // Matches
//         ];
//
//         let results = query_triples_batch(&pattern, &triples);
//         assert_eq!(results.len(), 2);
//
//         // Check first result
//         assert_eq!(results[0].get("s"), Some(&1));
//         assert_eq!(results[0].get("o"), Some(&3));
//
//         // Check second result
//         assert_eq!(results[1].get("s"), Some(&4));
//         assert_eq!(results[1].get("o"), Some(&5));
//     }
//
//     #[test]
//     fn test_extract_variables() {
//         let pattern = (
//             Term::Variable("s".to_string()),
//             Term::Constant(2),
//             Term::Variable("o".to_string()),
//         );
//
//         let vars = extract_variables(&pattern);
//         assert_eq!(vars, vec!["s", "o"]);
//     }
//
//     #[test]
//     fn test_count_bound_variables() {
//         let pattern1 = (
//             Term::Variable("s".to_string()),
//             Term::Variable("p".to_string()),
//             Term::Variable("o".to_string()),
//         );
//         assert_eq!(count_bound_variables(&pattern1), 0);
//
//         let pattern2 = (
//             Term::Constant(1),
//             Term::Variable("p".to_string()),
//             Term::Constant(3),
//         );
//         assert_eq!(count_bound_variables(&pattern2), 2);
//
//         let pattern3 = (Term::Constant(1), Term::Constant(2), Term::Constant(3));
//         assert_eq!(count_bound_variables(&pattern3), 3);
//     }
// }
