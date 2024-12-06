use std::collections::BTreeSet;
use kolibrie::dictionary::Dictionary;
use kolibrie::knowledge_graph::*;
use kolibrie::triple::Triple;
use kolibrie::parser_n3_logic::parse_n3_rule;

fn knowledge_graph() {
    let mut graph = KnowledgeGraph::new();

    // Add ABox triples (instance-level)
    graph.add_abox_triple("Alice", "hasParent", "Bob");
    graph.add_abox_triple("Bob", "hasParent", "Charlie");

    // Define a dynamic rule: If X hasParent Y and Y hasParent Z, then X hasGrandparent Z
    let grandparent_rule = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(graph.dictionary.encode("hasParent")),
                Term::Variable("Y".to_string()),
            ),
            (
                Term::Variable("Y".to_string()),
                Term::Constant(graph.dictionary.encode("hasParent")),
                Term::Variable("Z".to_string()),
            ),
        ],
        conclusion: (
            Term::Variable("X".to_string()),
            Term::Constant(graph.dictionary.encode("hasGrandparent")),
            Term::Variable("Z".to_string()),
        ),
    };

    // Add the rule to the knowledge graph
    graph.add_rule(grandparent_rule);

    // Infer new facts
    let inferred_facts = graph.infer_new_facts();

    // Print inferred facts
    for triple in inferred_facts {
        println!(
            "{} -- {} -- {}",
            graph.dictionary.decode(triple.subject).unwrap(),
            graph.dictionary.decode(triple.predicate).unwrap(),
            graph.dictionary.decode(triple.object).unwrap()
        );
    }
}

fn backward_chaining() {
    let mut dict = Dictionary::new();

    let parent = dict.encode("parent");
    let ancestor = dict.encode("ancestor");
    let alice = dict.encode("Alice");
    let bob = dict.encode("Bob");
    let charlie = dict.encode("Charlie");

    // ABox (facts)
    let mut abox = BTreeSet::new();
    abox.insert(Triple {
        subject: alice,
        predicate: parent,
        object: bob,
    });
    abox.insert(Triple {
        subject: bob,
        predicate: parent,
        object: charlie,
    });

    // TBox (schema) - can be empty if not used
    let tbox = BTreeSet::new();

    // Rules
    let rules = vec![
        // Rule 1: ancestor(X, Y) :- parent(X, Y)
        Rule {
            premise: vec![(
                Term::Variable("X".to_string()),
                Term::Constant(parent),
                Term::Variable("Y".to_string()),
            )],
            conclusion: (
                Term::Variable("X".to_string()),
                Term::Constant(ancestor),
                Term::Variable("Y".to_string()),
            ),
        },
        // Rule 2: ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z)
        Rule {
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
            conclusion: (
                Term::Variable("X".to_string()),
                Term::Constant(ancestor),
                Term::Variable("Z".to_string()),
            ),
        },
    ];

    let kg = KnowledgeGraph {
        abox,
        tbox,
        dictionary: dict.clone(),
        rules,
    };

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
                if let Some(ancestor_name) = dict.decode(ancestor_id) {
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

    let mut graph = KnowledgeGraph::new();

    // Add ABox triple for the instance-level information
    graph.add_abox_triple(
        "http://example2.com/a",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://www.test.be/test#SubClass",
    );

    match parse_n3_rule(input,&mut graph) {
        Ok((_, (prefixes, rule))) => {
            println!("Parsed Prefixes:");
            for (prefix, uri) in prefixes {
                println!("{}: <{}>", prefix, uri);
            }

            println!("\nParsed Rule:");
            println!("{:?}", rule);

            // Add parsed rule to KnowledgeGraph
            graph.add_rule(rule);

            let inferred_facts = graph.infer_new_facts();

            println!("Original and Inferred Facts:");
            for triple in graph.abox.iter().chain(inferred_facts.iter()) {
                println!(
                    "<{}> -- <{}> -- <{}> .",
                    graph.dictionary.decode(triple.subject).unwrap(),
                    graph.dictionary.decode(triple.predicate).unwrap(),
                    graph.dictionary.decode(triple.object).unwrap()
                );
            }
        }
        Err(error) => eprintln!("Failed to parse rule: {:?}", error),
    }
}

fn main() {
    knowledge_graph();
    println!("=======================================");
    backward_chaining();
    println!("=======================================");
    test();
}

