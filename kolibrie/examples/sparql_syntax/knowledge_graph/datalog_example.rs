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

fn main() {
    let mut kg = Reasoner::new();

    kg.add_abox_triple("Alice", "parent", "Bob");
    kg.add_abox_triple("Bob", "parent", "Charlie");
    kg.add_abox_triple("Charlie", "parent", "David");

    let parent_id = kg.encode_term("parent");
    let ancestor_id = kg.encode_term("ancestor");

    let rule1 = Rule {
        premise: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(parent_id),
            Term::Variable("Y".to_string()),
        )],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(ancestor_id),
            Term::Variable("Y".to_string()),
        )],
        filters: vec![],
    };

    let rule2 = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(parent_id),
                Term::Variable("Y".to_string()),
            ),
            (
                Term::Variable("Y".to_string()),
                Term::Constant(ancestor_id),
                Term::Variable("Z".to_string()),
            ),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(ancestor_id),
            Term::Variable("Z".to_string()),
        )],
        filters: vec![],
    };

    kg.add_rule(rule1);
    kg.add_rule(rule2);

    let inferred_facts = kg.infer_new_facts_semi_naive();
    for fact in inferred_facts {
        if let (Some(s), Some(p), Some(o)) = (
            kg.decode_term(fact.subject),
            kg.decode_term(fact.predicate),
            kg.decode_term(fact.object),
        ) {
            println!("Inferred: {} {} {}", s, p, o);
        }
    }

    let results = kg.query_abox(None, Some("ancestor"), Some("David"));

    for triple in results {
        if let Some(ancestor) = kg.decode_term(triple.subject) {
            println!("Ancestor: {}", ancestor);
        }
    }
}
