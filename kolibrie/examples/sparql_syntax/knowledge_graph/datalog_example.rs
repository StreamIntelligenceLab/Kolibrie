use datalog::knowledge_graph::KnowledgeGraph;
use datalog::knowledge_graph::Term;
use datalog::knowledge_graph::Rule;
use std::time::Instant;

fn main() {
    let mut kg = KnowledgeGraph::new();

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
    let _inferred_facts = kg.infer_new_facts_optimized();
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
    // println!("Inferred facts:");
    // for fact in inferred_facts {
    //     println!(
    //         "({:?}, {:?}, {:?})",
    //         kg.dictionary.decode(fact.subject),
    //         kg.dictionary.decode(fact.predicate),
    //         kg.dictionary.decode(fact.object),
    //     );
    // }
}
