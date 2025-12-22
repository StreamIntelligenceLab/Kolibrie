/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalog::reasoning_experimental::ReasoningLevel;
use datalog::reasoning_experimental::HierarchicalRule;
use datalog::reasoning_experimental::ReasoningHierarchy;
use shared::terms::Term;
use shared::rule::Rule;

pub fn setup_example_hierarchy() -> ReasoningHierarchy {
    let mut hierarchy = ReasoningHierarchy::new();
    
    // Base level: Ground facts
    hierarchy.add_fact_at_level(ReasoningLevel::Base, "socrates", "is_a", "man");
    hierarchy.add_fact_at_level(ReasoningLevel::Base, "plato", "is_a", "man");
    
    // Deductive level: Logical rules
    let mortality_rule = Rule {
        premise: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(0), // "is_a"
            Term::Constant(1), // "man"
        )],
        conclusion: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(2), // "is"
            Term::Constant(3), // "mortal"
        )],
        filters: vec![],
    };
    hierarchy.add_rule_at_level(ReasoningLevel::Deductive, mortality_rule, 1);
    
    // Cross-level rule: Generate hypotheses based on patterns
    let hypothesis_rule = HierarchicalRule {
        rule: Rule {
            premise: vec![(
                Term::Variable("x".to_string()),
                Term::Constant(2), // "is"
                Term::Constant(3), // "mortal"
            )],
            conclusion: vec![(
                Term::Variable("x".to_string()),
                Term::Constant(4), // "might_have"
                Term::Constant(5), // "soul"
            )],
            filters: vec![],
        },
        level: ReasoningLevel::Abductive,
        priority: 1,
        dependencies: vec![ReasoningLevel::Deductive],
    };
    hierarchy.add_cross_level_rule(hypothesis_rule);
    
    hierarchy
}

fn main() {
    let hierarchy = setup_example_hierarchy();
    println!("{:#?}", hierarchy);
}
