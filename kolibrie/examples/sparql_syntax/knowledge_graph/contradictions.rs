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
    let mut dict = kg.dictionary.write().unwrap();
    let isa_id = dict.encode("isA");
    let professor_id = dict.encode("professor");
    let student_id = dict.encode("student");
    drop(dict);
    
    let constraint = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(isa_id),
                Term::Constant(professor_id)
            ),
            (
                Term::Variable("X".to_string()),
                Term::Constant(isa_id),
                Term::Constant(student_id)
            )
        ],
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
    let mut dict = kg.dictionary.write().unwrap();
    let teaches_id = dict.encode("teaches");
    let enrolled_id = dict.encode("enrolledIn");
    drop(dict);
    
    let professor_rule = Rule {
        premise: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(teaches_id),
            Term::Variable("Y".to_string())
        )],
        conclusion: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(isa_id),
                Term::Constant(professor_id)
            )
        ],
        filters: vec![],
    };
    kg.add_rule(professor_rule);

    let student_rule = Rule {
        premise: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(enrolled_id),
            Term::Variable("Y".to_string())
        )],
        conclusion: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(isa_id),
                Term::Constant(student_id)
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
        let dict = kg.dictionary.read().unwrap();
        println!("{} {} {}", 
            dict.decode(fact.subject).unwrap_or("unknown"),
            dict.decode(fact.predicate).unwrap_or("unknown"),
            dict.decode(fact.object).unwrap_or("unknown")
        );
    }

    // Query for John's status
    let mut dict = kg.dictionary.write().unwrap();
    let john_id = dict.encode("john");
    let isa_id = dict.encode("isA");
    drop(dict);
    
    let query = (
        Term::Constant(john_id),
        Term::Constant(isa_id),
        Term::Variable("Role".to_string())
    );

    let results = kg.query_with_repairs(&query);
    println!("\nQuery results for John's roles:");
    for binding in results {
        if let Some(&role_id) = binding.get("Role") {
            let dict = kg.dictionary.read().unwrap();
            println!("Role: {}", dict.decode(role_id).unwrap_or("unknown"));
        }
    }
}

fn print_all_facts(kg: &Reasoner) {
    let facts = kg.index_manager.query(None, None, None);
    let dict = kg.dictionary.read().unwrap();
    for fact in facts {
        println!("{} {} {}", 
            dict.decode(fact.subject).unwrap_or("unknown"),
            dict.decode(fact.predicate).unwrap_or("unknown"),
            dict.decode(fact.object).unwrap_or("unknown")
        );
    }
}
