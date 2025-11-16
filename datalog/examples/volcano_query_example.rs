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
// //! Example demonstrating how to use the volcano-based query_triple function
// //! in semi-naive datalog evaluation
//
// use datalog::volcano_query::{
//     analyze_pattern_complexity, count_bound_variables, extract_variables, pattern_matches_triple,
//     query_triple, query_triples_batch,
// };
// use datalog::volcano_reasoner::{QueryStrategy, VolcanoReasoner};
// use shared::rule::Rule;
// use shared::terms::{Term, TriplePattern};
// use shared::triple::Triple;
// use std::collections::BTreeMap;
//
fn main() {
//     println!("=== Volcano Query Example for Semi-Naive Datalog ===\n");
//
//     // Example 1: Basic single triple matching
//     example_basic_matching();
//
//     // Example 2: Pattern analysis and optimization
//     example_pattern_analysis();
//
//     // Example 3: Batch processing for efficiency
//     example_batch_processing();
//
//     // Example 4: Integration with semi-naive evaluation
//     example_semi_naive_integration();
//
//     // Example 5: Rule substitution with volcano queries
//     example_rule_substitution();
}
//
// fn example_basic_matching() {
//     println!("--- Example 1: Basic Triple Matching ---");
//
//     // Create some test triples
//     let triple1 = Triple {
//         subject: 1,
//         predicate: 2,
//         object: 3,
//     };
//     let triple2 = Triple {
//         subject: 4,
//         predicate: 2,
//         object: 5,
//     };
//     let triple3 = Triple {
//         subject: 1,
//         predicate: 6,
//         object: 7,
//     };
//
//     // Test different pattern types
//
//     // Pattern 1: All variables (?s, ?p, ?o)
//     let pattern_all_vars = (
//         Term::Variable("s".to_string()),
//         Term::Variable("p".to_string()),
//         Term::Variable("o".to_string()),
//     );
//
//     println!("Pattern: (?s, ?p, ?o) vs Triple: (1, 2, 3)");
//     if let Some(bindings) = query_triple(&pattern_all_vars, &triple1) {
//         println!("  Bindings: {:?}", bindings);
//     }
//
//     // Pattern 2: Mixed (?s, 2, ?o)
//     let pattern_mixed = (
//         Term::Variable("s".to_string()),
//         Term::Constant(2),
//         Term::Variable("o".to_string()),
//     );
//
//     println!("\nPattern: (?s, 2, ?o) vs Triple: (1, 2, 3)");
//     if let Some(bindings) = query_triple(&pattern_mixed, &triple1) {
//         println!("  Bindings: {:?}", bindings);
//     } else {
//         println!("  No match");
//     }
//
//     println!("Pattern: (?s, 2, ?o) vs Triple: (1, 6, 7)");
//     if let Some(bindings) = query_triple(&pattern_mixed, &triple3) {
//         println!("  Bindings: {:?}", bindings);
//     } else {
//         println!("  No match (predicate doesn't match)");
//     }
//
//     // Pattern 3: All constants (1, 2, 3)
//     let pattern_all_constants = (Term::Constant(1), Term::Constant(2), Term::Constant(3));
//
//     println!("\nPattern: (1, 2, 3) vs Triple: (1, 2, 3)");
//     if let Some(bindings) = query_triple(&pattern_all_constants, &triple1) {
//         println!("  Exact match! Bindings: {:?}", bindings);
//     } else {
//         println!("  No match");
//     }
//
//     println!("\nPattern: (1, 2, 3) vs Triple: (4, 2, 5)");
//     if let Some(bindings) = query_triple(&pattern_all_constants, &triple2) {
//         println!("  Bindings: {:?}", bindings);
//     } else {
//         println!("  No match (subject and object don't match)");
//     }
//
//     println!();
// }
//
// fn example_pattern_analysis() {
//     println!("--- Example 2: Pattern Analysis for Query Optimization ---");
//
//     let patterns = vec![
//         // Pattern 1: No constants
//         (
//             Term::Variable("s".to_string()),
//             Term::Variable("p".to_string()),
//             Term::Variable("o".to_string()),
//         ),
//         // Pattern 2: One constant
//         (
//             Term::Constant(1),
//             Term::Variable("p".to_string()),
//             Term::Variable("o".to_string()),
//         ),
//         // Pattern 3: Two constants
//         (
//             Term::Constant(1),
//             Term::Variable("p".to_string()),
//             Term::Constant(3),
//         ),
//         // Pattern 4: All constants
//         (Term::Constant(1), Term::Constant(2), Term::Constant(3)),
//     ];
//
//     for (i, pattern) in patterns.iter().enumerate() {
//         println!("Pattern {}: {:?}", i + 1, pattern);
//
//         let bound_count = count_bound_variables(pattern);
//         let variables = extract_variables(pattern);
//         let complexity = VolcanoReasoner::analyze_pattern_complexity(pattern);
//
//         println!("  Bound variables: {}", bound_count);
//         println!("  Free variables: {:?}", variables);
//         println!("  Selectivity: {:.3}", complexity.selectivity);
//         println!(
//             "  Recommended strategy: {:?}",
//             complexity.recommended_strategy
//         );
//         println!();
//     }
// }
//
// fn example_batch_processing() {
//     println!("--- Example 3: Batch Processing for Efficiency ---");
//
//     // Create a batch of triples
//     let triples = vec![
//         Triple {
//             subject: 1,
//             predicate: 2,
//             object: 3,
//         },
//         Triple {
//             subject: 4,
//             predicate: 2,
//             object: 5,
//         },
//         Triple {
//             subject: 6,
//             predicate: 7,
//             object: 8,
//         },
//         Triple {
//             subject: 1,
//             predicate: 9,
//             object: 10,
//         },
//         Triple {
//             subject: 11,
//             predicate: 2,
//             object: 12,
//         },
//     ];
//
//     // Pattern that matches predicate = 2
//     let pattern = (
//         Term::Variable("s".to_string()),
//         Term::Constant(2),
//         Term::Variable("o".to_string()),
//     );
//
//     println!(
//         "Batch querying {} triples with pattern (?s, 2, ?o)",
//         triples.len()
//     );
//
//     // Process batch efficiently
//     let results = query_triples_batch(&pattern, &triples);
//
//     println!("Found {} matches:", results.len());
//     for (i, bindings) in results.iter().enumerate() {
//         println!(
//             "  Match {}: s={}, o={}",
//             i + 1,
//             bindings.get("s").unwrap(),
//             bindings.get("o").unwrap()
//         );
//     }
//
//     // Quick filtering without creating bindings
//     println!("\nQuick pattern matching (no bindings):");
//     for (i, triple) in triples.iter().enumerate() {
//         if pattern_matches_triple(&pattern, triple) {
//             println!("  Triple {} matches: {:?}", i + 1, triple);
//         }
//     }
//
//     println!();
// }
//
// fn example_semi_naive_integration() {
//     println!("--- Example 4: Semi-Naive Evaluation Integration ---");
//
//     // Simulate semi-naive evaluation scenario
//     // We have a new triple and want to check which rules it might trigger
//
//     let new_triple = Triple {
//         subject: 1,
//         predicate: 2,
//         object: 3,
//     };
//
//     // Define some rule patterns that might match
//     let rule_patterns = vec![
//         // Rule 1: If (?x, 2, ?y) then infer something
//         (
//             Term::Variable("x".to_string()),
//             Term::Constant(2),
//             Term::Variable("y".to_string()),
//         ),
//         // Rule 2: If (1, ?p, ?o) then infer something
//         (
//             Term::Constant(1),
//             Term::Variable("p".to_string()),
//             Term::Variable("o".to_string()),
//         ),
//         // Rule 3: If (?s, ?p, 5) then infer something (won't match)
//         (
//             Term::Variable("s".to_string()),
//             Term::Variable("p".to_string()),
//             Term::Constant(5),
//         ),
//     ];
//
//     println!("New triple added: {:?}", new_triple);
//     println!("Checking which rule patterns match:");
//
//     for (i, pattern) in rule_patterns.iter().enumerate() {
//         println!("  Rule pattern {}: {:?}", i + 1, pattern);
//
//         if let Some(bindings) = query_triple(pattern, &new_triple) {
//             println!("    ✓ MATCHES with bindings: {:?}", bindings);
//
//             // This is where you would:
//             // 1. Substitute the bindings into the rule body
//             // 2. Evaluate the modified rule body against the database
//             // 3. Generate new facts if the body is satisfied
//         } else {
//             println!("    ✗ Does not match");
//         }
//     }
//
//     println!();
// }
//
// fn example_rule_substitution() {
//     println!("--- Example 5: Rule Substitution with Volcano Queries ---");
//
//     // Example rule: parent(?x, ?y) ∧ parent(?y, ?z) → grandparent(?x, ?z)
//     // In our encoding, let's say:
//     // - predicate 10 = "parent"
//     // - predicate 11 = "grandparent"
//
//     // Rule body patterns
//     let pattern1 = (
//         Term::Variable("x".to_string()),
//         Term::Constant(10), // parent
//         Term::Variable("y".to_string()),
//     );
//
//     let pattern2 = (
//         Term::Variable("y".to_string()), // Note: shared variable y
//         Term::Constant(10),              // parent
//         Term::Variable("z".to_string()),
//     );
//
//     // A new fact: parent(alice, bob) encoded as Triple(1, 10, 2)
//     let new_fact = Triple {
//         subject: 1,    // alice
//         predicate: 10, // parent
//         object: 2,     // bob
//     };
//
//     println!("New fact: parent(alice, bob) → {:?}", new_fact);
//     println!("Rule body pattern 1: parent(?x, ?y) → {:?}", pattern1);
//
//     if let Some(bindings) = query_triple(&pattern1, &new_fact) {
//         println!("Pattern 1 matches with bindings: {:?}", bindings);
//         println!(
//             "This means: x = alice({}), y = bob({})",
//             bindings.get("x").unwrap(),
//             bindings.get("y").unwrap()
//         );
//
//         // Now we need to find facts matching pattern2 with y bound to bob(2)
//         let substituted_pattern2 = (
//             Term::Constant(2),  // y is now bound to bob
//             Term::Constant(10), // parent
//             Term::Variable("z".to_string()),
//         );
//
//         println!("Looking for: parent(bob, ?z) → {:?}", substituted_pattern2);
//
//         // If we had parent(bob, charlie) as Triple(2, 10, 3)
//         let hypothetical_fact = Triple {
//             subject: 2,    // bob
//             predicate: 10, // parent
//             object: 3,     // charlie
//         };
//
//         if let Some(bindings2) = query_triple(&substituted_pattern2, &hypothetical_fact) {
//             println!(
//                 "Found matching fact: parent(bob, charlie) → {:?}",
//                 hypothetical_fact
//             );
//             println!(
//                 "Final bindings: z = charlie({})",
//                 bindings2.get("z").unwrap()
//             );
//
//             println!(
//                 "Would infer: grandparent(alice, charlie) → Triple({}, 11, {})",
//                 bindings.get("x").unwrap(),
//                 bindings2.get("z").unwrap()
//             );
//         } else {
//             println!("No matching parent(?y=bob, ?z) fact found");
//         }
//     }
//
//     println!();
// }
//
// /// Helper function to demonstrate pattern complexity analysis
// fn analyze_pattern_complexity(pattern: &TriplePattern) -> String {
//     let bound_count = count_bound_variables(pattern);
//     let vars = extract_variables(pattern);
//
//     match bound_count {
//         0 => format!(
//             "Very low selectivity - scans all triples. Variables: {:?}",
//             vars
//         ),
//         1 => format!(
//             "Medium selectivity - uses index scan. Variables: {:?}",
//             vars
//         ),
//         2 => format!("High selectivity - very efficient. Variables: {:?}", vars),
//         3 => format!("Exact match - constant time lookup. No variables."),
//         _ => "Unknown pattern".to_string(),
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_example_runs() {
//         // Just ensure examples run without panicking
//         main();
//     }
//
//     #[test]
//     fn test_query_triple_functionality() {
//         let pattern = (
//             Term::Variable("s".to_string()),
//             Term::Constant(2),
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
//         assert_eq!(bindings.get("o"), Some(&3));
//         assert_eq!(bindings.len(), 2);
//     }
//
//     #[test]
//     fn test_batch_processing() {
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
//             },
//             Triple {
//                 subject: 4,
//                 predicate: 5,
//                 object: 6,
//             }, // Won't match
//             Triple {
//                 subject: 7,
//                 predicate: 2,
//                 object: 8,
//             },
//         ];
//
//         let results = query_triples_batch(&pattern, &triples);
//         assert_eq!(results.len(), 2);
//
//         assert_eq!(results[0].get("s"), Some(&1));
//         assert_eq!(results[0].get("o"), Some(&3));
//
//         assert_eq!(results[1].get("s"), Some(&7));
//         assert_eq!(results[1].get("o"), Some(&8));
//     }
// }
