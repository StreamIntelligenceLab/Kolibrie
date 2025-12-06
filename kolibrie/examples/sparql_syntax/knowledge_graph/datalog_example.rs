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
            (Term::Variable("X". to_string()), 
                          Term::Constant(kg. dictionary.encode("parent")), 
                          Term::Variable("Y".to_string())),
            (Term::Variable("Y".to_string()), 
                          Term::Constant(kg.dictionary.encode("ancestor")), 
                          Term::Variable("Z". to_string())),
        ],
        conclusion: vec![(Term::Variable("X".to_string()), 
                                  Term::Constant(kg.dictionary. encode("ancestor")), 
                                  Term::Variable("Z". to_string())),],
        filters: vec![],
    };
    
    kg.add_rule(rule1);
    kg.add_rule(rule2);

    let inferred_facts = kg.infer_new_facts_semi_naive();
    for fact in inferred_facts {
        println!("{:?}", kg.dictionary.decode_triple(&fact));
    }

    let results = kg.query_abox(
        None,              // subject: any (this is our variable X)
        Some("ancestor"),  // predicate: "ancestor"
        Some("David")      // object: "David"
    );
    
    for triple in results {
        println!("Ancestor: {:?}", kg. dictionary.decode(triple.subject));
    }
}
