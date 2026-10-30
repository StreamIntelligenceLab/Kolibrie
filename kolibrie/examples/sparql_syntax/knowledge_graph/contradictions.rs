/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalog::reasoning::Reasoner;
use shared::rule::Rule;
use shared::terms::Term;

fn example_with_contradictions() -> Reasoner {
    let mut kg = Reasoner::new();

    // Add some basic facts that will create a contradiction
    kg.add_abox_triple("john", "isA", "professor");
    kg.add_abox_triple("john", "isA", "student");
    kg.add_abox_triple("john", "teaches", "math101");
    kg.add_abox_triple("john", "enrolledIn", "physics101");

    // Add a constraint: No one can be both a professor and a student
    let isa_id = kg.encode_term("isA");
    let professor_id = kg.encode_term("professor");
    let student_id = kg.encode_term("student");

    let constraint = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(isa_id),
                Term::Constant(professor_id),
            ),
            (
                Term::Variable("X".to_string()),
                Term::Constant(isa_id),
                Term::Constant(student_id),
            ),
        ],
        negative_premise: vec![],
        conclusion: vec![(Term::Constant(0), Term::Constant(0), Term::Constant(0))],
        filters: vec![],
    };
    kg.add_constraint(constraint);

    // Add inference rules
    let teaches_id = kg.encode_term("teaches");
    let enrolled_id = kg.encode_term("enrolledIn");

    let professor_rule = Rule {
        premise: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(teaches_id),
            Term::Variable("Y".to_string()),
        )],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(isa_id),
            Term::Constant(professor_id),
        )],
        filters: vec![],
    };
    kg.add_rule(professor_rule);

    let student_rule = Rule {
        premise: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(enrolled_id),
            Term::Variable("Y".to_string()),
        )],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(isa_id),
            Term::Constant(student_id),
        )],
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
        println!(
            "{} {} {}",
            kg.decode_term(fact.subject).unwrap_or_else(|| "unknown".to_string()),
            kg.decode_term(fact.predicate).unwrap_or_else(|| "unknown".to_string()),
            kg.decode_term(fact.object).unwrap_or_else(|| "unknown".to_string())
        );
    }

    // Query for John's status
    let john_id = kg.encode_term("john");
    let isa_id = kg.encode_term("isA");

    let query = (
        Term::Constant(john_id),
        Term::Constant(isa_id),
        Term::Variable("Role".to_string()),
    );

    let results = kg.query_with_repairs(&query);
    println!("\nQuery results for John's roles:");
    for binding in results {
        if let Some(&role_id) = binding.get("Role") {
            println!("Role: {}", kg.decode_term(role_id).unwrap_or_else(|| "unknown".to_string()));
        }
    }
}

fn print_all_facts(kg: &Reasoner) {
    let facts = kg.dataset_index.query_default(None, None, None);
    for fact in facts {
        println!(
            "{} {} {}",
            kg.decode_term(fact.subject).unwrap_or_else(|| "unknown".to_string()),
            kg.decode_term(fact.predicate).unwrap_or_else(|| "unknown".to_string()),
            kg.decode_term(fact.object).unwrap_or_else(|| "unknown".to_string())
        );
    }
}
