/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::terms::Term;
use shared::rule::Rule;
use datalog::reasoning::Reasoner;

fn example_with_contradictions() -> Reasoner {
    let mut kg = Reasoner::new();

    // Add some basic facts that will create a contradiction
    kg.add_abox_triple("john", "isA", "professor");
    kg.add_abox_triple("john", "isA", "student");
    kg.add_abox_triple("john", "teaches", "math101");
    kg.add_abox_triple("john", "enrolledIn", "physics101");

    // Add a constraint: No one can be both a professor and a student
    let constraint = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(kg.dictionary.encode("isA")),
                Term::Constant(kg.dictionary.encode("professor"))
            ),
            (
                Term::Variable("X".to_string()),
                Term::Constant(kg.dictionary.encode("isA")),
                Term::Constant(kg.dictionary.encode("student"))
            )
        ],
        // Using a conclusions vector with a single element for the constraint
        conclusion: vec![
            (
                Term::Constant(0),
                Term::Constant(0),
                Term::Constant(0)
            )
        ],
        filters: vec![],
    };
    kg.add_constraint(constraint);

    // Add inference rules
    let professor_rule = Rule {
        premise: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(kg.dictionary.encode("teaches")),
            Term::Variable("Y".to_string())
        )],
        // Using a conclusions vector with a single element for the professor rule
        conclusion: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(kg.dictionary.encode("isA")),
                Term::Constant(kg.dictionary.encode("professor"))
            )
        ],
        filters: vec![],
    };
    kg.add_rule(professor_rule);

    let student_rule = Rule {
        premise: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(kg.dictionary.encode("enrolledIn")),
            Term::Variable("Y".to_string())
        )],
        // Using a conclusions vector with a single element for the student rule
        conclusion: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(kg.dictionary.encode("isA")),
                Term::Constant(kg.dictionary.encode("student"))
            )
        ],
        filters: vec![],
    };
    kg.add_rule(student_rule);

    kg
}

fn main() {
    let mut kg = example_with_contradictions();

    println!("Initial facts:");
    print_all_facts(&kg);

    // Run inference with repair handling and store the inferred facts
    let inferred_facts = kg.infer_new_facts_semi_naive_with_repairs();

    println!("\nAfter inference with repairs:");
    print_all_facts(&kg);

    // Print newly inferred facts
    println!("\nNewly inferred facts:");
    for fact in inferred_facts {
        println!("{} {} {}", 
            kg.dictionary.decode(fact.subject).unwrap_or("unknown"),
            kg.dictionary.decode(fact.predicate).unwrap_or("unknown"),
            kg.dictionary.decode(fact.object).unwrap_or("unknown")
        );
    }

    // Query for John's status
    let query = (
        Term::Constant(kg.dictionary.encode("john")),
        Term::Constant(kg.dictionary.encode("isA")),
        Term::Variable("Role".to_string())
    );

    let results = kg.query_with_repairs(&query);
    println!("\nQuery results for John's roles:");
    for binding in results {
        if let Some(&role_id) = binding.get("Role") {
            println!("Role: {}", kg.dictionary.decode(role_id).unwrap_or("unknown"));
        }
    }
}

fn print_all_facts(kg: &Reasoner) {
    let facts = kg.index_manager.query(None, None, None);
    for fact in facts {
        println!("{} {} {}", 
            kg.dictionary.decode(fact.subject).unwrap_or("unknown"),
            kg.dictionary.decode(fact.predicate).unwrap_or("unknown"),
            kg.dictionary.decode(fact.object).unwrap_or("unknown")
        );
    }
}
