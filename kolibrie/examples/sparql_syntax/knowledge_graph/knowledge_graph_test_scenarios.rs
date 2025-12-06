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
use shared::terms::Term;
use shared::terms::TriplePattern;
use shared::rule::Rule;
use std::time::Instant;

// Test scenario 1: 100 facts, 2 rules
fn test1() {
    let mut kg = Reasoner::new();

    // Generate 100 facts programmatically (ABox)
    for i in 0..5 {
        let subject = format!("person{}", i);
        let object = format!("person{}", (i + 1) % 5); // Wraps around to connect the last person with the first
        kg.add_abox_triple(&subject, "likes", &object);
    }

    // Add transitivity rule: likes(X, Y) & likes(Y, Z) => likes(X, Z)
    kg.add_rule(Rule {
        premise: vec![
            (
                Term::Variable("x".to_string()),
                Term::Constant(kg.dictionary.clone().encode("likes")),
                Term::Variable("y".to_string()),
            ),
            (
                Term::Variable("y".to_string()),
                Term::Constant(kg.dictionary.clone().encode("likes")),
                Term::Variable("z".to_string()),
            ),
        ],
        conclusion: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(kg.dictionary.clone().encode("likes")),
            Term::Variable("z".to_string()),
        )],
        filters: vec![],
    });

    // Add symmetry rule: likes(X, Y) => likes(Y, X)
    kg.add_rule(Rule {
        premise: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(kg.dictionary.clone().encode("likes")),
            Term::Variable("y".to_string()),
        )],
        conclusion: vec![(
            Term::Variable("y".to_string()),
            Term::Constant(kg.dictionary.clone().encode("likes")),
            Term::Variable("x".to_string()),
        )],
        filters: vec![],
    });

    // Run the optimized inference
    let start = Instant::now();
    let inferred_facts = kg.infer_new_facts_semi_naive();
    let duration = start.elapsed();
    println!("Inference took: {:?}", duration);

    let results = kg.query_abox(
        Some("person0"),                // subject
        Some("likes"),                  // predicate
        None,                           // object -> wildcard
    );

    println!("person0 likes these people:");
    for triple in results {
        let s_str = kg.dictionary.decode(triple.subject).unwrap();
        let p_str = kg.dictionary.decode(triple.predicate).unwrap();
        let o_str = kg.dictionary.decode(triple.object).unwrap();
        println!("  {} {} {}", s_str, p_str, o_str);
    }

    // Display the newly inferred facts
    println!("Inferred facts:");
    for fact in inferred_facts {
        println!(
            "({:?}, {:?}, {:?})",
            kg.dictionary.decode(fact.subject),
            kg.dictionary.decode(fact.predicate),
            kg.dictionary.decode(fact.object),
        );
    }
}

// Test scenario 2: One piece of data, 100 rules
fn test2() {
    let mut kg = Reasoner::new();
    kg.add_abox_triple("myInstance", "type", "Class0");

    for i in 0..5 {
        let premise_pattern: TriplePattern = (
            Term::Variable("x".to_string()),
            Term::Constant(kg.dictionary.encode("type")),
            Term::Constant(kg.dictionary.encode(&format!("Class{}", i))),
        );
        let conclusion_pattern: Vec<TriplePattern> = vec![
            (
                Term::Variable("x".to_string()),
                Term::Constant(kg.dictionary.encode("type")),
                Term::Constant(kg.dictionary.encode(&format!("Class{}", i + 1))),
            ),
        ];
        let rule = Rule {
            premise: vec![premise_pattern],
            conclusion: conclusion_pattern,
            filters: vec![],
        };
        kg.add_rule(rule);
    }
    let start = Instant::now();
    let inferred_facts = kg.infer_new_facts_semi_naive();
    let duration = start.elapsed();
    println!("Inference took: {:?}", duration);
    let results = kg.query_abox(
        Some("myInstance"),    // subject
        Some("type"),          // predicate
        None,                  // object -> wildcard
    );

    println!("myInstance has these types after inference:");
    for triple in &results {
        let s_str = kg.dictionary.decode(triple.subject).unwrap();
        let p_str = kg.dictionary.decode(triple.predicate).unwrap();
        let o_str = kg.dictionary.decode(triple.object).unwrap();
        println!("  {} {} {}", s_str, p_str, o_str);
    }

    println!("Inferred facts:");
    for fact in inferred_facts {
        println!(
            "({:?}, {:?}, {:?})",
            kg.dictionary.decode(fact.subject),
            kg.dictionary.decode(fact.predicate),
            kg.dictionary.decode(fact.object),
        );
    }
}

fn transitivity_benchmark() {
    println!("facts_count,inference_time_s,inferred_facts_count");

    for facts_count in 1..=50 {
        let mut kg = Reasoner::new();

        // Add 'facts_count' facts in a chain: person0 likes person1, person1 likes person2, ...
        for i in 0..facts_count {
            let subject = format!("person{}", i);
            let object = format!("person{}", i + 1);
            kg.add_abox_triple(&subject, "likes", &object);
        }

        // Only transitivity rule
        kg.add_rule(Rule {
            premise: vec![
                (
                    Term::Variable("x".to_string()),
                    Term::Constant(kg.dictionary.clone().encode("likes")),
                    Term::Variable("y".to_string()),
                ),
                (
                    Term::Variable("y".to_string()),
                    Term::Constant(kg.dictionary.clone().encode("likes")),
                    Term::Variable("z".to_string()),
                ),
            ],
            conclusion: vec![(
                Term::Variable("x".to_string()),
                Term::Constant(kg.dictionary.clone().encode("likes")),
                Term::Variable("z".to_string()),
            )],
            filters: vec![],
        });

        let start = Instant::now();
        let inferred_facts = kg.infer_new_facts_semi_naive();
        let duration = start.elapsed();

        // Print time in seconds, e.g. 0.00123
        println!(
            "{},{:.6},{:?}",
            facts_count,
            duration.as_secs_f64(),
            inferred_facts.len()
        );
    }
}

fn main() {
    println!("Test scenario 1:");
    test1();
    println!("---------------------------------");
    println!("Test scenario 2:");
    test2();
    println!("---------------------------------");
    println!("Test benchmark:");
    transitivity_benchmark();
}
