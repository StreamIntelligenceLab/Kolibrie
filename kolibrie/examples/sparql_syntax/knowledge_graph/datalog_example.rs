use datalog::knowledge_graph::KnowledgeGraph;
use shared::terms::Term;
use shared::rule::Rule;

fn main() {
    let mut kg = KnowledgeGraph::new();

    kg.add_abox_triple("Alice", "parent", "Bob");
    kg.add_abox_triple("Bob", "parent", "Charlie");
    kg.add_abox_triple("Charlie", "parent", "David");

    let rule1 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), 
                          Term::Constant(kg.dictionary.encode("parent")), 
                          Term::Variable("Y".to_string())),
        ],
        conclusion: vec![(Term::Variable("X".to_string()), 
                                  Term::Constant(kg.dictionary.encode("ancestor")), 
                                  Term::Variable("Y".to_string())),],
        filters: vec![],
    };
    
    let rule2 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), 
                          Term::Constant(kg.dictionary.encode("parent")), 
                          Term::Variable("Y".to_string())),
            (Term::Variable("Y".to_string()), 
                          Term::Constant(kg.dictionary.encode("ancestor")), 
                          Term::Variable("Z".to_string())),
        ],
        conclusion: vec![(Term::Variable("X".to_string()), 
                                  Term::Constant(kg.dictionary.encode("ancestor")), 
                                  Term::Variable("Z".to_string())),],
        filters: vec![],
    };
    
    kg.add_rule(rule1);
    kg.add_rule(rule2);

    let inferred_facts = kg.infer_new_facts_semi_naive();
    for fact in inferred_facts {
        println!("{:?}", kg.dictionary.decode_triple(&fact));
    }
    let query = (
        Term::Variable("X".to_string()), 
        Term::Constant(kg.dictionary.encode("ancestor")), 
        Term::Constant(kg.dictionary.encode("David")),
    );
    
    let results = kg.datalog_query_kg(&query);
    
    for result in results {
        println!("Ancestor: {:?}", kg.dictionary.decode(*result.get("X").unwrap()));
    }
    
}
