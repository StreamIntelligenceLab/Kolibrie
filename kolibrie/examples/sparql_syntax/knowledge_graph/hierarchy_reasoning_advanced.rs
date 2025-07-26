/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalog::reasoning::ReasoningLevel;
use datalog::reasoning::HierarchicalRule;
use datalog::reasoning::ReasoningHierarchy;
use shared::terms::Term;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::HashSet;
use std::time::Instant;

fn main() {
    println!("=== Hierarchical Reasoning System Demo ===\n");
    
    let mut hierarchy = ReasoningHierarchy::new();
    
    // Add base facts
    println!("1. Adding Base Level Facts:");
    hierarchy.add_fact_at_level(ReasoningLevel::Base, "socrates", "is_a", "human");
    hierarchy.add_fact_at_level(ReasoningLevel::Base, "plato", "is_a", "human");
    hierarchy.add_fact_at_level(ReasoningLevel::Base, "aristotle", "is_a", "human");
    hierarchy.add_fact_at_level(ReasoningLevel::Base, "fluffy", "is_a", "cat");
    hierarchy.add_fact_at_level(ReasoningLevel::Base, "socrates", "teaches", "plato");
    hierarchy.add_fact_at_level(ReasoningLevel::Base, "plato", "teaches", "aristotle");
    
    display_level_facts(&mut hierarchy, ReasoningLevel::Base, "Base Facts");
    
    // Add deductive rules
    println!("\n2. Adding Deductive Level Rules:");
    let mortality_rule = create_mortality_rule(&mut hierarchy);
    hierarchy.add_rule_at_level(ReasoningLevel::Deductive, mortality_rule, 1);
    println!("   - Added rule: human(X) -> mortal(X)");
    
    let transitivity_rule = create_teaching_transitivity_rule(&mut hierarchy);
    hierarchy.add_rule_at_level(ReasoningLevel::Deductive, transitivity_rule, 2);
    println!("   - Added rule: teaches(X,Y) ∧ teaches(Y,Z) -> influences(X,Z)");
    
    let wisdom_rule = create_wisdom_rule(&mut hierarchy);
    hierarchy.add_rule_at_level(ReasoningLevel::Deductive, wisdom_rule, 3);
    println!("   - Added rule: teaches(X,Y) -> wise(X)");
    
    // Add cross-level rules
    println!("\n3. Adding Abductive Level Rules (Cross-level):");
    let soul_hypothesis_rule = create_soul_hypothesis_rule(&mut hierarchy);
    hierarchy.add_cross_level_rule(soul_hypothesis_rule);
    println!("   - Added hypothesis: wise(X) ∧ mortal(X) -> might_have_soul(X)");
    
    let memory_hypothesis_rule = create_memory_hypothesis_rule(&mut hierarchy);
    hierarchy.add_cross_level_rule(memory_hypothesis_rule);
    println!("   - Added hypothesis: influences(X,Y) -> might_be_remembered(X)");
    
    println!("\n4. Adding Meta-Reasoning Rules:");
    let significance_meta_rule = create_significance_meta_rule(&mut hierarchy);
    hierarchy.add_cross_level_rule(significance_meta_rule);
    println!("   - Added meta-rule: might_have_soul(X) ∧ might_be_remembered(X) -> significant_figure(X)");
    
    // Perform inference
    println!("\n5. Performing Hierarchical Inference:");
    let start = Instant::now();
    let _ = hierarchy.hierarchical_inference();
    let duration = start.elapsed();
    println!("   Inference completed in: {:?}\n", duration);
    
    // Display results
    println!("6. Results by Reasoning Level:\n");
    display_level_facts(&mut hierarchy, ReasoningLevel::Base, "Base Level (Ground Facts)");
    display_level_facts(&mut hierarchy, ReasoningLevel::Deductive, "Deductive Level (Logical Inferences)");
    display_level_facts(&mut hierarchy, ReasoningLevel::Abductive, "Abductive Level (Hypotheses)");
    display_level_facts(&mut hierarchy, ReasoningLevel::MetaReasoning, "Meta-Reasoning Level (Higher-order Reasoning)");
    
    // Queries
    println!("\n7. Example Queries:\n");
    
    println!("Who is mortal?");
    let mortal_results = hierarchy.query_hierarchy(None, None, Some("mortal"), None);
    for (level, triple) in mortal_results {
        display_triple_with_level(&mut hierarchy, &triple, &level);
    }
    
    println!("\nWho are significant figures?");
    let significant_results = hierarchy.query_hierarchy(None, None, Some("significant_figure"), None);
    for (level, triple) in significant_results {
        display_triple_with_level(&mut hierarchy, &triple, &level);
    }
    
    // Certainty analysis
    println!("\n8. Certainty Analysis:");
    let all_results = hierarchy.query_hierarchy(None, None, None, None);
    let mut shown_levels = HashSet::new();
    
    for (level, triple) in all_results {
        if !shown_levels.contains(&level) {
            let certainty = hierarchy.get_fact_certainty(&triple);
            let subject = get_decoded_term(&mut hierarchy, triple.subject, ReasoningLevel::Base);
            let predicate = get_decoded_term(&mut hierarchy, triple.predicate, ReasoningLevel::Base);
            let object = get_decoded_term(&mut hierarchy, triple.object, ReasoningLevel::Base);
            
            println!("   {:?} level: '{}' (certainty: {:.1})", 
                     level, format!("{} {} {}", subject, predicate, object), certainty);
            shown_levels.insert(level);
        }
    }
    
    println!("\n=== Hierarchical Reasoning Demo Complete ===");
}

// Helper functions (unchanged from original but included for completeness)
fn create_mortality_rule(hierarchy: &mut ReasoningHierarchy) -> Rule {
    let base_kg = hierarchy.levels.get_mut(&ReasoningLevel::Base).unwrap();
    
    Rule {
        premise: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(base_kg.dictionary.encode("is_a")),
            Term::Constant(base_kg.dictionary.encode("human")),
        )],
        conclusion: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(base_kg.dictionary.encode("is_a")),
            Term::Constant(base_kg.dictionary.encode("mortal")),
        )],
        filters: vec![],
    }
}

fn create_teaching_transitivity_rule(hierarchy: &mut ReasoningHierarchy) -> Rule {
    let base_kg = hierarchy.levels.get_mut(&ReasoningLevel::Base).unwrap();
    
    Rule {
        premise: vec![
            (
                Term::Variable("x".to_string()),
                Term::Constant(base_kg.dictionary.encode("teaches")),
                Term::Variable("y".to_string()),
            ),
            (
                Term::Variable("y".to_string()),
                Term::Constant(base_kg.dictionary.encode("teaches")),
                Term::Variable("z".to_string()),
            ),
        ],
        conclusion: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(base_kg.dictionary.encode("influences")),
            Term::Variable("z".to_string()),
        )],
        filters: vec![],
    }
}

fn create_wisdom_rule(hierarchy: &mut ReasoningHierarchy) -> Rule {
    let base_kg = hierarchy.levels.get_mut(&ReasoningLevel::Base).unwrap();
    
    Rule {
        premise: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(base_kg.dictionary.encode("teaches")),
            Term::Variable("y".to_string()),
        )],
        conclusion: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(base_kg.dictionary.encode("is_a")),
            Term::Constant(base_kg.dictionary.encode("wise")),
        )],
        filters: vec![],
    }
}

fn create_soul_hypothesis_rule(hierarchy: &mut ReasoningHierarchy) -> HierarchicalRule {
    let base_kg = hierarchy.levels.get_mut(&ReasoningLevel::Base).unwrap();
    
    HierarchicalRule {
        rule: Rule {
            premise: vec![
                (
                    Term::Variable("x".to_string()),
                    Term::Constant(base_kg.dictionary.encode("is_a")),
                    Term::Constant(base_kg.dictionary.encode("wise")),
                ),
                (
                    Term::Variable("x".to_string()),
                    Term::Constant(base_kg.dictionary.encode("is_a")),
                    Term::Constant(base_kg.dictionary.encode("mortal")),
                ),
            ],
            conclusion: vec![(
                Term::Variable("x".to_string()),
                Term::Constant(base_kg.dictionary.encode("might_have")),
                Term::Constant(base_kg.dictionary.encode("soul")),
            )],
            filters: vec![],
        },
        level: ReasoningLevel::Abductive,
        priority: 1,
        dependencies: vec![ReasoningLevel::Base, ReasoningLevel::Deductive],
    }
}

fn create_memory_hypothesis_rule(hierarchy: &mut ReasoningHierarchy) -> HierarchicalRule {
    let base_kg = hierarchy.levels.get_mut(&ReasoningLevel::Base).unwrap();
    
    HierarchicalRule {
        rule: Rule {
            premise: vec![(
                Term::Variable("x".to_string()),
                Term::Constant(base_kg.dictionary.encode("influences")),
                Term::Variable("y".to_string()),
            )],
            conclusion: vec![(
                Term::Variable("x".to_string()),
                Term::Constant(base_kg.dictionary.encode("might_be")),
                Term::Constant(base_kg.dictionary.encode("remembered")),
            )],
            filters: vec![],
        },
        level: ReasoningLevel::Abductive,
        priority: 2,
        dependencies: vec![ReasoningLevel::Base, ReasoningLevel::Deductive],
    }
}

fn create_significance_meta_rule(hierarchy: &mut ReasoningHierarchy) -> HierarchicalRule {
    let base_kg = hierarchy.levels.get_mut(&ReasoningLevel::Base).unwrap();
    
    HierarchicalRule {
        rule: Rule {
            premise: vec![
                (
                    Term::Variable("x".to_string()),
                    Term::Constant(base_kg.dictionary.encode("might_have")),
                    Term::Constant(base_kg.dictionary.encode("soul")),
                ),
                (
                    Term::Variable("x".to_string()),
                    Term::Constant(base_kg.dictionary.encode("might_be")),
                    Term::Constant(base_kg.dictionary.encode("remembered")),
                ),
            ],
            conclusion: vec![(
                Term::Variable("x".to_string()),
                Term::Constant(base_kg.dictionary.encode("is_a")),
                Term::Constant(base_kg.dictionary.encode("significant_figure")),
            )],
            filters: vec![],
        },
        level: ReasoningLevel::MetaReasoning,
        priority: 1,
        dependencies: vec![ReasoningLevel::Base, ReasoningLevel::Deductive, ReasoningLevel::Abductive],
    }
}

fn display_level_facts(hierarchy: &mut ReasoningHierarchy, level: ReasoningLevel, title: &str) {
    println!("{}:", title);
    let results = hierarchy.query_hierarchy(Some(level.clone()), None, None, None);
    
    if results.is_empty() {
        println!("   (no facts at this level)");
    } else {
        for (_level, triple) in results {
            let subject = get_decoded_term(hierarchy, triple.subject, ReasoningLevel::Base);
            let predicate = get_decoded_term(hierarchy, triple.predicate, ReasoningLevel::Base);
            let object = get_decoded_term(hierarchy, triple.object, ReasoningLevel::Base);
            println!("   {} {} {}", subject, predicate, object);
        }
    }
}

fn display_triple_with_level(hierarchy: &mut ReasoningHierarchy, triple: &Triple, level: &ReasoningLevel) {
    let subject = get_decoded_term(hierarchy, triple.subject, ReasoningLevel::Base);
    let predicate = get_decoded_term(hierarchy, triple.predicate, ReasoningLevel::Base);
    let object = get_decoded_term(hierarchy, triple.object, ReasoningLevel::Base);
    let certainty = hierarchy.get_fact_certainty(triple);
    
    println!("   {} {} {} (level: {:?}, certainty: {:.1})", 
             subject, predicate, object, level, certainty);
}

fn get_decoded_term(hierarchy: &mut ReasoningHierarchy, term_id: u32, level: ReasoningLevel) -> String {
    if let Some(kg) = hierarchy.levels.get(&level) {
        kg.dictionary.decode(term_id).unwrap_or("unknown").to_string()
    } else {
        "unknown".to_string()
    }
}