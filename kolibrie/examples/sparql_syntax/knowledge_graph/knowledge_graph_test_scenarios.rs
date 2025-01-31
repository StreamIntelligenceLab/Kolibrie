use datalog::knowledge_graph::KnowledgeGraph;
use shared::terms::Term;
use shared::terms::TriplePattern;
use shared::rule::Rule;
use std::time::Instant;

// Test scenario 1: 100 facts, 2 rules
fn test1() {
    let mut kg = KnowledgeGraph::new();

    // Generate 100 facts programmatically (ABox)
    for i in 0..100 {
        let subject = format!("person{}", i);
        let object = format!("person{}", (i + 1) % 100); // Wraps around to connect the last person with the first
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
        conclusion: (
            Term::Variable("x".to_string()),
            Term::Constant(kg.dictionary.clone().encode("likes")),
            Term::Variable("z".to_string()),
        ),
    });

    // Add symmetry rule: likes(X, Y) => likes(Y, X)
    kg.add_rule(Rule {
        premise: vec![(
            Term::Variable("x".to_string()),
            Term::Constant(kg.dictionary.clone().encode("likes")),
            Term::Variable("y".to_string()),
        )],
        conclusion: (
            Term::Variable("y".to_string()),
            Term::Constant(kg.dictionary.clone().encode("likes")),
            Term::Variable("x".to_string()),
        ),
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
    let mut kg = KnowledgeGraph::new();
    kg.add_abox_triple("myInstance", "type", "Class0");

    for i in 0..100 {
        let premise_pattern: TriplePattern = (
            Term::Variable("x".to_string()),
            Term::Constant(kg.dictionary.encode("type")),
            Term::Constant(kg.dictionary.encode(&format!("Class{}", i))),
        );
        let conclusion_pattern: TriplePattern = (
            Term::Variable("x".to_string()),
            Term::Constant(kg.dictionary.encode("type")),
            Term::Constant(kg.dictionary.encode(&format!("Class{}", i + 1))),
        );
        let rule = Rule {
            premise: vec![premise_pattern],
            conclusion: conclusion_pattern,
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

fn main() {
    println!("Test scenario 1:");
    test1();
    println!("---------------------------------");
    println!("Test scenario 2:");
    test2();
}