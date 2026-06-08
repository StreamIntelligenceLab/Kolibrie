/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalog::reasoning::Reasoner;
use shared::provenance::Provenance;
use shared::terms::Term;
use shared::rule::Rule;
use shared::provenance::AddMultProbability;

fn main() {
    println!("=== Social Trust Network: Combined Rule Inference Example ===\n");

    let mut kg = Reasoner::new();

    println!("[Stage 1] Loading base facts...");

    // Certain `knows` facts (8 triples)
    kg.add_abox_triple("Alice", "knows", "Bob");
    kg.add_abox_triple("Alice", "knows", "Charlie");
    kg.add_abox_triple("Bob",   "knows", "Diana");
    kg.add_abox_triple("Bob",   "knows", "Eve");
    kg.add_abox_triple("Charlie", "knows", "Frank");
    kg.add_abox_triple("Diana", "knows", "Eve");
    kg.add_abox_triple("Eve",   "knows", "Frank");
    kg.add_abox_triple("Frank", "knows", "Alice");

    // Probabilistic `trusts` facts (7 triples) — seeded for provenance inference
    kg.add_tagged_triple("Alice",   "trusts", "Bob",   0.90);
    kg.add_tagged_triple("Alice",   "trusts", "Charlie", 0.70);
    kg.add_tagged_triple("Bob",     "trusts", "Diana", 0.80);
    kg.add_tagged_triple("Bob",     "trusts", "Eve",   0.60);
    kg.add_tagged_triple("Charlie", "trusts", "Frank", 0.75);
    kg.add_tagged_triple("Diana",   "trusts", "Eve",   0.85);
    kg.add_tagged_triple("Eve",     "trusts", "Frank", 0.65);

    let initial_size = kg.index_manager.query(None, None, None).len();
    println!("  Certain knowledge (knows): 8 facts");
    println!("  Uncertain knowledge (trusts): 7 facts");
    println!("  Initial database size: {} triples", initial_size);

    let knows_id         = kg.dictionary.write().unwrap().encode("knows");
    let connected_id     = kg.dictionary.write().unwrap().encode("connected");
    let trusts_id        = kg.dictionary.write().unwrap().encode("trusts");
    let indirect_id      = kg.dictionary.write().unwrap().encode("indirectTrust");
    let strong_bond_id   = kg.dictionary.write().unwrap().encode("strongBond");
    let trust_comm_id    = kg.dictionary.write().unwrap().encode("trustCommunity");

    // Rule 1: knows(X,Y) ∧ knows(Y,Z) -> connected(X,Z)  (two-hop)
    let rule1 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), Term::Constant(knows_id),     Term::Variable("Y".to_string())),
            (Term::Variable("Y".to_string()), Term::Constant(knows_id),     Term::Variable("Z".to_string())),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()), Term::Constant(connected_id), Term::Variable("Z".to_string()),
        )],
        filters: vec![],
    };

    // Rule 2: connected(X,Y) ∧ connected(Y,Z) -> connected(X,Z)  (transitive closure)
    let rule2 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), Term::Constant(connected_id), Term::Variable("Y".to_string())),
            (Term::Variable("Y".to_string()), Term::Constant(connected_id), Term::Variable("Z".to_string())),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()), Term::Constant(connected_id), Term::Variable("Z".to_string()),
        )],
        filters: vec![],
    };

    // Rule 3: strongBond(X,Y) ∧ strongBond(Y,Z) -> trustCommunity(X,Z)  (uses provenance output)
    let rule3 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), Term::Constant(strong_bond_id), Term::Variable("Y".to_string())),
            (Term::Variable("Y".to_string()), Term::Constant(strong_bond_id), Term::Variable("Z".to_string())),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()), Term::Constant(trust_comm_id), Term::Variable("Z".to_string()),
        )],
        filters: vec![],
    };

    kg.add_rule(rule1);
    kg.add_rule(rule2);
    // Rule 3 is added later — it uses strongBond from the provenance round

    // Rule 4: trusts(X,Y) ∧ trusts(Y,Z) -> indirectTrust(X,Z)
    //   Uses AddMultProbability provenance (⊗ = multiply for conjunction)
    let rule4 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), Term::Constant(trusts_id),   Term::Variable("Y".to_string())),
            (Term::Variable("Y".to_string()), Term::Constant(trusts_id),   Term::Variable("Z".to_string())),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()), Term::Constant(indirect_id), Term::Variable("Z".to_string()),
        )],
        filters: vec![],
    };

    // Rule 5: connected(X,Z) ∧ trusts(X,Z) -> strongBond(X,Z)
    //   Uses classically-inferred `connected` from Round 1
    let rule5 = Rule {
        premise: vec![
            (Term::Variable("X".to_string()), Term::Constant(connected_id),   Term::Variable("Z".to_string())),
            (Term::Variable("X".to_string()), Term::Constant(trusts_id),      Term::Variable("Z".to_string())),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()), Term::Constant(strong_bond_id), Term::Variable("Z".to_string()),
        )],
        filters: vec![],
    };

    println!("\n[Stage 2] Classical Inference (RULE) - Round 1");
    println!("  Classical rules: connected(X,Z) :- knows(X,Y), knows(Y,Z)");
    println!("                   connected(X,Z) :- connected(X,Y), connected(Y,Z)");

    let facts1 = kg.infer_new_facts_semi_naive();
    let after_classical1 = kg.index_manager.query(None, None, None).len();

    let classical1_count = facts1.len();
    println!("  Inferred {} new facts using classical rules", classical1_count);
    println!("  Database size: {} triples", after_classical1);

    println!("  Inferred connected facts (sample):");
    {
        let dict = kg.dictionary.read().unwrap();
        let mut shown = 0;
        for t in &facts1 {
            if shown >= 5 {
                break;
            }
            if t.predicate == connected_id {
                println!("    {} connected {}",
                    dict.decode(t.subject).unwrap_or("?"),
                    dict.decode(t.object).unwrap_or("?"));
                shown += 1;
            }
        }
    }

    println!("\n[Stage 3] Provenance Inference (AddMultProbability semiring)");
    println!("  (Uses classically-inferred connected facts from Stage 2!)");
    println!("  Provenance rules (⊗ = multiply, ⊕ = clamped-add):");
    println!("    indirectTrust(X,Z) :- trusts(X,Y), trusts(Y,Z)");
    println!("    strongBond(X,Z)    :- connected(X,Z), trusts(X,Z)");

    // Add provenance rules (plain Rules — provenance is program-level, not per-rule)
    kg.add_rule(rule4);
    kg.add_rule(rule5);

    let (facts2, tag_store) = kg.infer_new_facts_with_provenance(AddMultProbability);
    let after_prov = kg.index_manager.query(None, None, None).len();

    println!("  Inferred {} new provenance-tagged facts", facts2.len());
    println!("  Database size: {} triples", after_prov);

    println!("  indirectTrust facts:");
    {
        let dict = kg.dictionary.read().unwrap();
        for (triple, tag) in tag_store.iter() {
            if triple.predicate == indirect_id {
                let prob = AddMultProbability.recover_probability(tag);
                println!("    {} indirectTrust {} (prob={:.2})",
                    dict.decode(triple.subject).unwrap_or("?"),
                    dict.decode(triple.object).unwrap_or("?"),
                    prob);
            }
        }
    }

    println!("  strongBond facts:");
    {
        let dict = kg.dictionary.read().unwrap();
        for (triple, tag) in tag_store.iter() {
            if triple.predicate == strong_bond_id {
                let prob = AddMultProbability.recover_probability(tag);
                println!("    {} strongBond {} (prob={:.2})",
                    dict.decode(triple.subject).unwrap_or("?"),
                    dict.decode(triple.object).unwrap_or("?"),
                    prob);
            }
        }
    }

    println!("\n[Stage 4] Classical Inference (RULE) - Round 2");
    println!("  (Uses provenance-inferred strongBond facts from Stage 3!)");
    println!("  Classical rule: trustCommunity(X,Z) :- strongBond(X,Y), strongBond(Y,Z)");

    // Add rule3 now so it participates in the second classical round
    kg.add_rule(rule3);

    let facts3 = kg.infer_new_facts_semi_naive();
    let after_classical2 = kg.index_manager.query(None, None, None).len();

    println!("  Inferred {} new facts", facts3.len());
    println!("  Database size: {} triples", after_classical2);

    println!("  trustCommunity facts:");
    {
        let dict = kg.dictionary.read().unwrap();
        for t in &facts3 {
            if t.predicate == trust_comm_id {
                println!("    {} trustCommunity {}",
                    dict.decode(t.subject).unwrap_or("?"),
                    dict.decode(t.object).unwrap_or("?"));
            }
        }
    }

    println!("\n=== Final Statistics ===");
    let after_classical1_delta = after_classical1 as isize - initial_size as isize;
    let after_prov_delta       = after_prov as isize - after_classical1 as isize;
    let after_classical2_delta = after_classical2 as isize - after_prov as isize;
    let total_inferred         = after_classical2 as isize - initial_size as isize;
    let growth_pct             = (total_inferred as f64 / initial_size as f64) * 100.0;

    println!("  Initial facts: {}", initial_size);
    println!("  After classical round 1: +{} ({} total)", after_classical1_delta, after_classical1);
    println!("  After provenance round: +{} ({} total)", after_prov_delta, after_prov);
    println!("  After classical round 2: +{} ({} total)", after_classical2_delta, after_classical2);
    println!("  Total inferred: {} new facts ({:.0}% database growth)", total_inferred, growth_pct);
}
