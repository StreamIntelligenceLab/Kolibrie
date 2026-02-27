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
use shared::rule::Rule;

fn main() {
    let mut kg = Reasoner::new();

    kg.add_abox_triple("Alice", "parent", "Bob");
    kg.add_abox_triple("Bob", "parent", "Charlie");
    kg.add_abox_triple("Charlie", "parent", "David");

    // Encode predicates once with proper locking
    let parent_id = kg.dictionary.write().unwrap().encode("parent");
    let ancestor_id = kg.dictionary.write().unwrap().encode("ancestor");

    let rule1 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), 
                          Term::Constant(parent_id), 
                          Term::Variable("Y".to_string())),
        ],
        conclusion: vec![(Term::Variable("X".to_string()), 
                                  Term::Constant(ancestor_id), 
                                  Term::Variable("Y".to_string())),],
        filters: vec![],
    };
    
    let rule2 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), 
                          Term::Constant(parent_id), 
                          Term::Variable("Y".to_string())),
            (Term::Variable("Y".to_string()), 
                          Term::Constant(ancestor_id), 
                          Term::Variable("Z".to_string())),
        ],
        conclusion: vec![(Term::Variable("X".to_string()), 
                                  Term::Constant(ancestor_id), 
                                  Term::Variable("Z".to_string())),],
        filters: vec![],
    };
    
    kg.add_rule(rule1);
    kg.add_rule(rule2);

    let inferred_facts = kg.infer_new_facts_semi_naive();
    for fact in inferred_facts {
        let dict = kg.dictionary.read().unwrap();
        if let (Some(s), Some(p), Some(o)) = (
            dict.decode(fact.subject),
            dict.decode(fact.predicate),
            dict.decode(fact.object)
        ) {
            println!("Inferred: {} {} {}", s, p, o);
        }
    }

    let results = kg.query_abox(
        None,              // subject: any (this is our variable X)
        Some("ancestor"),  // predicate: "ancestor"
        Some("David")      // object: "David"
    );
    
    for triple in results {
        let dict = kg.dictionary.read().unwrap();
        if let Some(ancestor) = dict.decode(triple.subject) {
            println!("Ancestor: {}", ancestor);
        }
    }
}
