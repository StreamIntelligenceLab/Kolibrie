/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalog::parser_n3_logic::parse_n3_rule;
use datalog::reasoning::backward_chaining::resolve_term;
use datalog::reasoning::*;
use shared::rule::Rule;
use shared::terms::Term;

fn knowledge_graph() {
    let mut graph = Reasoner::new();

    // Add ABox triples (instance-level)
    graph.add_abox_triple("Alice", "hasParent", "Bob");
    graph.add_abox_triple("Bob", "hasParent", "Charlie");

    // Define a dynamic rule: If X hasParent Y and Y hasParent Z, then X hasGrandparent Z
    let has_parent_id = graph.encode_term("hasParent");
    let has_grandparent_id = graph.encode_term("hasGrandparent");

    let grandparent_rule = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(has_parent_id),
                Term::Variable("Y".to_string()),
            ),
            (
                Term::Variable("Y".to_string()),
                Term::Constant(has_parent_id),
                Term::Variable("Z".to_string()),
            ),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(has_grandparent_id),
            Term::Variable("Z".to_string()),
        )],
        filters: vec![],
    };

    // Add the rule to the knowledge graph
    graph.add_rule(grandparent_rule);

    // Infer new facts
    let inferred_facts = graph.infer_new_facts();

    // Print inferred facts
    for triple in inferred_facts {
        println!(
            "{} -- {} -- {}",
            graph.decode_term(triple.subject).unwrap(),
            graph.decode_term(triple.predicate).unwrap(),
            graph.decode_term(triple.object).unwrap()
        );
    }
}

fn backward_chaining() {
    let mut kg = Reasoner::new();

    // ABox (facts)
    kg.add_abox_triple("Alice", "parent", "Bob");
    kg.add_abox_triple("Bob", "parent", "Charlie");

    // Encode terms
    let parent = kg.encode_term("parent");
    let ancestor = kg.encode_term("ancestor");
    let charlie = kg.encode_term("Charlie");

    // Rules
    let rule1 = Rule {
        // ancestor(X, Y) :- parent(X, Y)
        premise: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(parent),
            Term::Variable("Y".to_string()),
        )],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(ancestor),
            Term::Variable("Y".to_string()),
        )],
        filters: vec![],
    };

    let rule2 = Rule {
        // ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(parent),
                Term::Variable("Y".to_string()),
            ),
            (
                Term::Variable("Y".to_string()),
                Term::Constant(ancestor),
                Term::Variable("Z".to_string()),
            ),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(ancestor),
            Term::Variable("Z".to_string()),
        )],
        filters: vec![],
    };

    kg.add_rule(rule1);
    kg.add_rule(rule2);

    // Query: Who are the ancestors of Charlie?
    let query = (
        Term::Variable("A".to_string()),
        Term::Constant(ancestor),
        Term::Constant(charlie),
    );

    let results = kg.backward_chaining(&query);

    // Decode and print results
    for res in results {
        if let Some(ancestor_term) = res.get("A") {
            if let Term::Constant(ancestor_id) = resolve_term(ancestor_term, &res) {
                if let Some(ancestor_name) = kg.decode_term(ancestor_id) {
                    println!("Ancestor: {}", ancestor_name);
                }
            }
        }
    }
}

fn test() {
    let input = "@prefix test: <http://www.test.be/test#>.
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
{ ?s rdf:type test:SubClass. } => { ?s rdf:type test:SuperType. }";

    let mut graph = Reasoner::new();

    graph.add_abox_triple(
        "http://example2.com/a",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://www.test.be/test#SubClass",
    );

    match parse_n3_rule(input, &mut graph) {
        Ok((_, (prefixes, rule))) => {
            println!("Parsed Prefixes:");
            for (prefix, uri) in prefixes {
                println!("{}: <{}>", prefix, uri);
            }

            println!("\nParsed Rule:");
            println!("{:?}", rule);

            // Add parsed rule to KnowledgeGraph
            graph.add_rule(rule);

            let old_facts = graph.dataset_index.query(None, None, None);

            let inferred_facts = graph.infer_new_facts();

            println!("\nOriginal and Inferred Facts:");
            for triple in old_facts.iter().chain(inferred_facts.iter()) {
                let s = graph.decode_term(triple.subject).unwrap();
                let p = graph.decode_term(triple.predicate).unwrap();
                let o = graph.decode_term(triple.object).unwrap();
                println!("<{}> -- <{}> -- <{}> .", s, p, o);
            }
        }
        Err(error) => eprintln!("Failed to parse rule: {:?}", error),
    }
}

fn test2() {
    let mut kg = Reasoner::new();

    let n3_rule = r#"@prefix ex: <http://example.org/family#>.
{ ?x ex:hasParent ?y. ?y ex:hasSibling ?z. } => { ?x ex:hasUncleOrAunt ?z. }."#;

    kg.add_abox_triple("John", "ex:hasParent", "Mary");
    kg.add_abox_triple("Mary", "ex:hasSibling", "Robert");

    match parse_n3_rule(&n3_rule, &mut kg) {
        Ok((_, (prefixes, rule))) => {
            println!("Parsed Prefixes:");
            for (prefix, uri) in prefixes {
                println!("{}: <{}>", prefix, uri);
            }

            println!("\nParsed Rule:");
            println!("{:?}", rule);

            // Add parsed rule to KnowledgeGraph
            kg.add_rule(rule);

            let old_facts = kg.dataset_index.query(None, None, None);

            let inferred_facts = kg.infer_new_facts();

            println!("\nOriginal and Inferred Facts:");
            for triple in old_facts.iter().chain(inferred_facts.iter()) {
                let s = kg.decode_term(triple.subject).unwrap();
                let p = kg.decode_term(triple.predicate).unwrap();
                let o = kg.decode_term(triple.object).unwrap();
                println!("<{}> <{}> <{}>.", s, p, o);
            }
        }
        Err(error) => eprintln!("Failed to parse rule: {:?}", error),
    }
}

fn inconsistency() {
    let mut kg = Reasoner::new();

    // Add some facts
    kg.add_abox_triple("john", "isA", "professor");
    kg.add_abox_triple("john", "isA", "student"); // This could be inconsistent

    // Encode terms
    let is_a = kg.encode_term("isA");
    let professor = kg.encode_term("professor");
    let student = kg.encode_term("student");

    // Add a constraint: someone cannot be both a professor and a student
    let constraint = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(is_a),
                Term::Constant(professor),
            ),
            (
                Term::Variable("X".to_string()),
                Term::Constant(is_a),
                Term::Constant(student),
            ),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Constant(0), // Dummy values for constraint
            Term::Constant(0),
            Term::Constant(0),
        )],
        filters: vec![],
    };
    kg.add_constraint(constraint);

    // Query with inconsistency tolerance
    let query = (
        Term::Variable("X".to_string()),
        Term::Constant(is_a),
        Term::Variable("Y".to_string()),
    );

    let results = kg.query_with_repairs(&query);
    // This will return only consistent results
    println!("{:?}", results);
}

fn main() {
    knowledge_graph();
    println!("=======================================");
    backward_chaining();
    println!("=======================================");
    test();
    println!("=======================================");
    test2();
    println!("=======================================");
    inconsistency();
}
