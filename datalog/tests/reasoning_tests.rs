use datalog::reasoning::Reasoner;
use shared::rule::{FilterCondition, Rule};
use shared::terms::Term;
use shared::provenance::{AddMultProbability, MinMaxProbability, BooleanProvenance, Provenance};
use shared::provenance::{TopKProofs, WmcProvenance};
use std::collections::HashMap;

fn enc(r: &Reasoner, s: &str) -> u32 {
    r.dictionary.write().unwrap().encode(s)
}

fn rule(premises: Vec<(Term, Term, Term)>, conclusions: Vec<(Term, Term, Term)>) -> Rule {
    Rule {
        premise: premises,
        negative_premise: vec![],
        conclusion: conclusions,
        filters: vec![],
    }
}

fn naf_rule(
    premises: Vec<(Term, Term, Term)>,
    neg_premises: Vec<(Term, Term, Term)>,
    conclusions: Vec<(Term, Term, Term)>,
) -> Rule {
    Rule {
        premise: premises,
        negative_premise: neg_premises,
        conclusion: conclusions,
        filters: vec![],
    }
}

fn inferred(r: &mut Reasoner, s: &str, p: &str, o: &str) -> bool {
    !r.query_abox(Some(s), Some(p), Some(o)).is_empty()
}

fn bc_has(results: &[HashMap<String, Term>], var: &str, val: u32) -> bool {
    results.iter().any(|b| b.get(var) == Some(&Term::Constant(val)))
}

// Forward chaining

#[test]
fn fc_1hop_base() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "ancestor", "B"));
}

#[test]
fn fc_2hop_transitive() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("B", "parent", "C");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    // base: parent -> ancestor
    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));
    // transitive: ancestor + ancestor -> ancestor
    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(ancestor), Term::Variable("Z".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Z".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "ancestor", "B"));
    assert!(inferred(&mut r, "B", "ancestor", "C"));
    assert!(inferred(&mut r, "A", "ancestor", "C"));
}

#[test]
fn fc_3hop_transitive() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("B", "parent", "C");
    r.add_abox_triple("C", "parent", "D");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));
    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(ancestor), Term::Variable("Z".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Z".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "ancestor", "B"));
    assert!(inferred(&mut r, "A", "ancestor", "C"));
    assert!(inferred(&mut r, "A", "ancestor", "D"));
    assert!(inferred(&mut r, "B", "ancestor", "D"));
}

#[test]
fn fc_join_sibling() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "P");
    r.add_abox_triple("B", "parent", "P");

    let parent = enc(&r, "parent");
    let sibling = enc(&r, "sibling");

    r.add_rule(Rule {
        premise: vec![
            (Term::Variable("X".into()), Term::Constant(parent), Term::Variable("P2".into())),
            (Term::Variable("Y".into()), Term::Constant(parent), Term::Variable("P2".into())),
        ],
        negative_premise: vec![],
        conclusion: vec![
            (Term::Variable("X".into()), Term::Constant(sibling), Term::Variable("Y".into())),
        ],
        filters: vec![
            FilterCondition { variable: "X".into(), operator: "!=".into(), value: "Y".into() },
        ],
    });

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "sibling", "B"));
    assert!(inferred(&mut r, "B", "sibling", "A"));

    assert!(!inferred(&mut r, "A", "sibling", "A"));
}

#[test]
fn fc_multi_rule_cascade() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "worksFor", "Corp");

    let works_for = enc(&r, "worksFor");
    let employed = enc(&r, "employed");
    let affiliated = enc(&r, "affiliated");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(works_for), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(employed), Term::Variable("Y".into()))],
    ));
    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(employed), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(affiliated), Term::Variable("Y".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "employed", "Corp"));
    assert!(inferred(&mut r, "A", "affiliated", "Corp"));
}

#[test]
fn fc_three_premise_rule() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "R", "B");
    r.add_abox_triple("B", "S", "C");
    r.add_abox_triple("C", "T", "D");

    let r_pred = enc(&r, "R");
    let s_pred = enc(&r, "S");
    let t_pred = enc(&r, "T");
    let connected = enc(&r, "connected");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(r_pred), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(s_pred), Term::Variable("Z".into())),
            (Term::Variable("Z".into()), Term::Constant(t_pred), Term::Variable("W".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(connected), Term::Variable("W".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "connected", "D"));
}

#[test]
fn fc_no_spurious() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("C", "unrelated", "D");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "ancestor", "B"));
    assert!(!inferred(&mut r, "C", "ancestor", "D"));
}

#[test]
fn fc_sibling_three_children() {
    // A, B, C all share parent P -> 6 directional pairs, no self-sibling
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "P");
    r.add_abox_triple("B", "parent", "P");
    r.add_abox_triple("C", "parent", "P");

    let parent = enc(&r, "parent");
    let sibling = enc(&r, "sibling");

    r.add_rule(Rule {
        premise: vec![
            (Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Z".into())),
            (Term::Variable("Y".into()), Term::Constant(parent), Term::Variable("Z".into())),
        ],
        negative_premise: vec![],
        conclusion: vec![
            (Term::Variable("X".into()), Term::Constant(sibling), Term::Variable("Y".into())),
        ],
        filters: vec![
            FilterCondition { variable: "X".into(), operator: "!=".into(), value: "Y".into() },
        ],
    });

    r.infer_new_facts_semi_naive();

    for (s, o) in [("A","B"),("A","C"),("B","A"),("B","C"),("C","A"),("C","B")] {
        assert!(inferred(&mut r, s, "sibling", o), "{s} should be sibling of {o}");
    }
    for x in ["A","B","C"] {
        assert!(!inferred(&mut r, x, "sibling", x), "{x} should not be sibling of itself");
    }
}

#[test]
fn fc_multi_conclusion() {
    // One rule head produces two distinct conclusions
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "marriedTo", "B");

    let married = enc(&r, "marriedTo");
    let spouse = enc(&r, "spouse");
    let partner = enc(&r, "partner");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(married), Term::Variable("Y".into()))],
        vec![
            (Term::Variable("X".into()), Term::Constant(spouse), Term::Variable("Y".into())),
            (Term::Variable("X".into()), Term::Constant(partner), Term::Variable("Y".into())),
        ],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "spouse", "B"));
    assert!(inferred(&mut r, "A", "partner", "B"));
}

#[test]
fn fc_diamond_ancestor() {
    // Diamond: A->B->D and A->C->D; A should be ancestor of D via both paths
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("A", "parent", "C");
    r.add_abox_triple("B", "parent", "D");
    r.add_abox_triple("C", "parent", "D");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));
    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(ancestor), Term::Variable("Z".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Z".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "ancestor", "D"), "A should be ancestor of D via B and C");
    assert!(inferred(&mut r, "B", "ancestor", "D"));
    assert!(inferred(&mut r, "C", "ancestor", "D"));
    assert!(!inferred(&mut r, "A", "ancestor", "A"), "A should not be its own ancestor");
    assert!(!inferred(&mut r, "D", "ancestor", "A"), "D should not be ancestor of A");
}

#[test]
fn fc_disconnected_graphs() {
    // Two separate parent chains must not cross-contaminate
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("X", "parent", "Y");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("P".into()), Term::Constant(parent), Term::Variable("Q".into()))],
        vec![(Term::Variable("P".into()), Term::Constant(ancestor), Term::Variable("Q".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "ancestor", "B"));
    assert!(inferred(&mut r, "X", "ancestor", "Y"));
    assert!(!inferred(&mut r, "A", "ancestor", "Y"), "A should not be ancestor of Y (different graph)");
    assert!(!inferred(&mut r, "X", "ancestor", "B"), "X should not be ancestor of B (different graph)");
}

#[test]
fn fc_no_matching_facts() {
    // Rules that match no facts should produce no inferred triples
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "likes", "B");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));

    let new_facts = r.infer_new_facts_semi_naive();

    assert!(new_facts.is_empty(), "No facts should be inferred when no premise matches");
}

#[test]
fn fc_idempotent() {
    // Running inference a second time must not add duplicate triples
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));

    r.infer_new_facts_semi_naive();
    let second_round = r.infer_new_facts_semi_naive();

    assert!(second_round.is_empty(), "Second inference pass should derive nothing new");
    let results = r.query_abox(Some("A"), Some("ancestor"), Some("B"));
    assert_eq!(results.len(), 1, "Exactly one ancestor triple should exist, not duplicates");
}

#[test]
fn fc_uncle_derived() {
    // Two-stage: sibling derived first, then uncle derived from sibling + parent
    // Setup: A and B share parent P; C's parent is A -> B is uncle of C
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "P");
    r.add_abox_triple("B", "parent", "P");
    r.add_abox_triple("C", "parent", "A");

    let parent = enc(&r, "parent");
    let sibling = enc(&r, "sibling");
    let uncle = enc(&r, "uncle");

    // sibling(X, Y) :- parent(X, Z), parent(Y, Z), X != Y
    r.add_rule(Rule {
        premise: vec![
            (Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Z".into())),
            (Term::Variable("Y".into()), Term::Constant(parent), Term::Variable("Z".into())),
        ],
        negative_premise: vec![],
        conclusion: vec![
            (Term::Variable("X".into()), Term::Constant(sibling), Term::Variable("Y".into())),
        ],
        filters: vec![
            FilterCondition { variable: "X".into(), operator: "!=".into(), value: "Y".into() },
        ],
    });

    // uncle(U, N) :- sibling(U, Par), parent(N, Par)
    r.add_rule(rule(
        vec![
            (Term::Variable("U".into()), Term::Constant(sibling), Term::Variable("Par".into())),
            (Term::Variable("N".into()), Term::Constant(parent), Term::Variable("Par".into())),
        ],
        vec![(Term::Variable("U".into()), Term::Constant(uncle), Term::Variable("N".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "sibling", "B"), "A should be sibling of B");
    assert!(inferred(&mut r, "B", "sibling", "A"), "B should be sibling of A");
    assert!(inferred(&mut r, "B", "uncle", "C"), "B should be uncle of C");
    // A is the parent of C, not uncle
    assert!(!inferred(&mut r, "A", "uncle", "C"), "A (parent) should not also be uncle of C");
}

// Backward chaining

#[test]
fn bc_direct_fact() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "likes", "B");

    let likes = enc(&r, "likes");
    let a = enc(&r, "A");
    let b = enc(&r, "B");

    let query = (Term::Variable("X".into()), Term::Constant(likes), Term::Variable("Y".into()));
    let results = r.backward_chaining(&query);

    assert!(bc_has(&results, "X", a));
    assert!(bc_has(&results, "Y", b));
}

#[test]
fn bc_1hop_rule() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");
    let a = enc(&r, "A");
    let b = enc(&r, "B");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));

    let query = (Term::Constant(a), Term::Constant(ancestor), Term::Variable("Y".into()));
    let results = r.backward_chaining(&query);

    assert!(bc_has(&results, "Y", b));
}

#[test]
fn bc_2hop_transitive() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("B", "parent", "C");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");
    let a = enc(&r, "A");
    let b = enc(&r, "B");
    let c = enc(&r, "C");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));
    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(ancestor), Term::Variable("Z".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Z".into()))],
    ));

    let query = (Term::Constant(a), Term::Constant(ancestor), Term::Variable("Y".into()));
    let results = r.backward_chaining(&query);

    assert!(bc_has(&results, "Y", b), "Expected A ancestor B");
    assert!(bc_has(&results, "Y", c), "Expected A ancestor C");
}

#[test]
fn bc_3hop_transitive() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("B", "parent", "C");
    r.add_abox_triple("C", "parent", "D");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");
    let a = enc(&r, "A");
    let b = enc(&r, "B");
    let c = enc(&r, "C");
    let d = enc(&r, "D");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));
    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(ancestor), Term::Variable("Z".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Z".into()))],
    ));

    let query = (Term::Constant(a), Term::Constant(ancestor), Term::Variable("Y".into()));
    let results = r.backward_chaining(&query);

    assert!(bc_has(&results, "Y", b), "Expected A ancestor B");
    assert!(bc_has(&results, "Y", c), "Expected A ancestor C");
    assert!(bc_has(&results, "Y", d), "Expected A ancestor D");
}

#[test]
fn bc_specific_target() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("B", "parent", "C");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");
    let a = enc(&r, "A");
    let c = enc(&r, "C");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));
    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(ancestor), Term::Variable("Z".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Z".into()))],
    ));

    let query = (Term::Constant(a), Term::Constant(ancestor), Term::Constant(c));
    let results = r.backward_chaining(&query);

    assert!(!results.is_empty(), "Expected A ancestor C to be derivable");
}

#[test]
fn bc_no_result() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");
    let a = enc(&r, "A");
    let d = enc(&r, "D");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));

    let query = (Term::Constant(a), Term::Constant(ancestor), Term::Constant(d));
    let results = r.backward_chaining(&query);

    assert!(results.is_empty(), "Expected no result for A ancestor D");
}

#[test]
fn bc_multi_rule_chain() {
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "worksFor", "Corp");

    let works_for = enc(&r, "worksFor");
    let employed = enc(&r, "employed");
    let affiliated = enc(&r, "affiliated");
    let a = enc(&r, "A");
    let corp = enc(&r, "Corp");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(works_for), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(employed), Term::Variable("Y".into()))],
    ));
    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(employed), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(affiliated), Term::Variable("Y".into()))],
    ));

    let query = (Term::Constant(a), Term::Constant(affiliated), Term::Variable("Y".into()));
    let results = r.backward_chaining(&query);

    assert!(bc_has(&results, "Y", corp), "Expected A affiliated Corp");
}

#[test]
fn bc_sibling_join() {
    // BC should find a sibling via a 2-premise join rule
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "P");
    r.add_abox_triple("B", "parent", "P");

    let parent = enc(&r, "parent");
    let sibling = enc(&r, "sibling");
    let b = enc(&r, "B");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Z".into())),
            (Term::Variable("Y".into()), Term::Constant(parent), Term::Variable("Z".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(sibling), Term::Variable("Y".into()))],
    ));

    let a_id = enc(&r, "A");
    let query = (Term::Constant(a_id), Term::Constant(sibling), Term::Variable("Y".into()));
    let results = r.backward_chaining(&query);

    assert!(bc_has(&results, "Y", b), "BC should find A sibling B via join");
}

#[test]
fn bc_full_scan() {
    // Fully variable query should return bindings for all matching base facts
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("C", "parent", "D");

    let parent = enc(&r, "parent");
    let a = enc(&r, "A");
    let b = enc(&r, "B");
    let c = enc(&r, "C");
    let d = enc(&r, "D");

    let query = (
        Term::Variable("S".into()),
        Term::Constant(parent),
        Term::Variable("O".into()),
    );
    let results = r.backward_chaining(&query);

    assert!(bc_has(&results, "S", a), "Should bind S to A");
    assert!(bc_has(&results, "O", b), "Should bind O to B");
    assert!(bc_has(&results, "S", c), "Should bind S to C");
    assert!(bc_has(&results, "O", d), "Should bind O to D");
}

#[test]
fn bc_no_spurious_negative() {
    // BC must not return results for a predicate with no facts or applicable rules
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");

    let unknown = enc(&r, "unknown");

    let query = (
        Term::Variable("X".into()),
        Term::Constant(unknown),
        Term::Variable("Y".into()),
    );
    let results = r.backward_chaining(&query);

    assert!(results.is_empty(), "BC should return nothing for unknown predicate");
}

#[test]
fn prov_transitive_addmult_combination() {
    // A related B (0.8), B related C (0.7)
    // Rule: ?X related ?Y, ?Y related ?Z => ?X related ?Z
    // AddMultProbability: ⊗ = multiply, so 0.8 * 0.7 = 0.56
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "related", "B", 0.8);
    r.add_tagged_triple("B", "related", "C", 0.7);

    let related = enc(&r, "related");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
    ));

    let (inferred, tag_store) = r.infer_new_facts_with_provenance(AddMultProbability);

    let a = enc(&r, "A");
    let c = enc(&r, "C");
    let a_related_c = inferred.iter().any(|t| t.subject == a && t.predicate == related && t.object == c);
    assert!(a_related_c, "Should infer A related C");

    let triple = shared::triple::Triple { subject: a, predicate: related, object: c };
    let prob = AddMultProbability.recover_probability(&tag_store.get_tag(&triple));
    assert!((prob - 0.56).abs() < 1e-6, "Probability should be ~0.56, got {}", prob);
}

#[test]
fn prov_addmult_multiple_paths() {
    // A related B (0.6), A related C (0.9), B related D (0.8), C related D (0.5)
    // Rule: ?X related ?Y, ?Y related ?Z => ?X related ?Z
    // Path 1: A->B->D = 0.6*0.8 = 0.48
    // Path 2: A->C->D = 0.9*0.5 = 0.45
    // AddMultProbability ⊕ = noisy-OR: 0.48 + 0.45 - 0.48*0.45 ≈ 0.714
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "related", "B", 0.6);
    r.add_tagged_triple("A", "related", "C", 0.9);
    r.add_tagged_triple("B", "related", "D", 0.8);
    r.add_tagged_triple("C", "related", "D", 0.5);

    let related = enc(&r, "related");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
    ));

    let (_inferred, tag_store) = r.infer_new_facts_with_provenance(AddMultProbability);

    let a = enc(&r, "A");
    let d = enc(&r, "D");
    let triple = shared::triple::Triple { subject: a, predicate: related, object: d };
    let prob = AddMultProbability.recover_probability(&tag_store.get_tag(&triple));
    // noisy-OR: 0.48 + 0.45 - 0.48*0.45 = 0.93 - 0.216 = 0.714
    assert!((prob - 0.714).abs() < 1e-6, "AddMult disjunction should be ~0.714, got {}", prob);
}

#[test]
fn prov_minmax_conjunction() {
    // A knows B (0.9), B trusts C (0.6)
    // MinMaxProbability: ⊗ = min, so min(0.9, 0.6) = 0.6
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "knows", "B", 0.9);
    r.add_tagged_triple("B", "trusts", "C", 0.6);

    let knows = enc(&r, "knows");
    let trusts = enc(&r, "trusts");
    let recommends = enc(&r, "recommends");

    r.add_rule(Rule {
        premise: vec![
            (Term::Variable("X".into()), Term::Constant(knows), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(trusts), Term::Variable("Z".into())),
        ],
        negative_premise: vec![],
        filters: vec![],
        conclusion: vec![
            (Term::Variable("X".into()), Term::Constant(recommends), Term::Variable("Z".into())),
        ],
    });

    let (_inferred, tag_store) = r.infer_new_facts_with_provenance(MinMaxProbability);

    let a = enc(&r, "A");
    let c = enc(&r, "C");
    let triple = shared::triple::Triple { subject: a, predicate: recommends, object: c };
    let prob = MinMaxProbability.recover_probability(&tag_store.get_tag(&triple));
    assert!((prob - 0.6).abs() < 1e-6, "MinMax conjunction should be 0.6, got {}", prob);
}

#[test]
fn prov_minmax_multiple_paths() {
    // A related B (0.6), A related C (0.9), B related D (0.8), C related D (0.5)
    // MinMaxProbability: ⊗ = min, ⊕ = max
    // Path 1: A->B->D = min(0.6, 0.8) = 0.6
    // Path 2: A->C->D = min(0.9, 0.5) = 0.5
    // Disjunction: max(0.6, 0.5) = 0.6
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "related", "B", 0.6);
    r.add_tagged_triple("A", "related", "C", 0.9);
    r.add_tagged_triple("B", "related", "D", 0.8);
    r.add_tagged_triple("C", "related", "D", 0.5);

    let related = enc(&r, "related");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
    ));

    let (_inferred, tag_store) = r.infer_new_facts_with_provenance(MinMaxProbability);

    let a = enc(&r, "A");
    let d = enc(&r, "D");
    let triple = shared::triple::Triple { subject: a, predicate: related, object: d };
    let prob = MinMaxProbability.recover_probability(&tag_store.get_tag(&triple));
    assert!((prob - 0.6).abs() < 1e-6, "MinMax disjunction should be 0.6, got {}", prob);
}

#[test]
fn prov_boolean_matches_classical() {
    // BooleanProvenance should produce the same facts as classical semi-naive
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");
    r.add_abox_triple("B", "parent", "C");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));
    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(parent), Term::Variable("Z".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Z".into())),
        ],
    ));

    let (inferred, tag_store) = r.infer_new_facts_with_provenance(BooleanProvenance);

    // Should infer A ancestor B, B ancestor C, A ancestor C
    let a = enc(&r, "A");
    let b = enc(&r, "B");
    let c = enc(&r, "C");
    assert!(inferred.iter().any(|t| t.subject == a && t.predicate == ancestor && t.object == b));
    assert!(inferred.iter().any(|t| t.subject == b && t.predicate == ancestor && t.object == c));
    assert!(inferred.iter().any(|t| t.subject == a && t.predicate == ancestor && t.object == c));

    // All tags should be true (one())
    for t in &inferred {
        assert_eq!(tag_store.get_tag(t), true);
    }
}

#[test]
fn prov_classical_rules_still_work() {
    // Regression test: classical (non-provenance) rules should still work
    let mut r = Reasoner::new();
    r.add_abox_triple("A", "parent", "B");

    let parent = enc(&r, "parent");
    let ancestor = enc(&r, "ancestor");

    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));

    r.infer_new_facts_semi_naive();
    assert!(inferred(&mut r, "A", "ancestor", "B"));
}

#[test]
fn prov_zero_tag_pruning() {
    // If a fact has probability 0.0, its tag is zero() and derivations through it
    // should be pruned (zero is annihilator for ⊗)
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "related", "B", 0.0); // zero tag
    r.add_tagged_triple("B", "related", "C", 0.9);

    let related = enc(&r, "related");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
    ));

    let (inferred, _tag_store) = r.infer_new_facts_with_provenance(AddMultProbability);

    // A related C should NOT be inferred (zero ⊗ anything = zero, pruned)
    let a = enc(&r, "A");
    let c = enc(&r, "C");
    let a_related_c = inferred.iter().any(|t| t.subject == a && t.predicate == related && t.object == c);
    assert!(!a_related_c, "Should NOT infer A related C through zero-tagged premise");
}

#[test]
fn topk_wmc_overlap_vs_noisy_or() {
    // Canonical overlap test: directly build the tag and verify WMC.
    // proof1 = {0, 1}: uses x (0.8) and y (0.6) -> score 0.48
    // proof2 = {0, 2}: uses x (0.8) and z (0.5) -> score 0.40 (shares x!)
    //
    // Noisy-OR of 0.48 and 0.40 = 0.688 (overcounts x)
    // Exact WMC via inclusion-exclusion:
    //   P(x∧y) + P(x∧z) - P(x∧y∧z) = 0.48 + 0.40 - 0.24 = 0.64
    let p = TopKProofs::new(5);
    p.tag_from_probability_with_id(0.8, 0); // x
    p.tag_from_probability_with_id(0.6, 1); // y
    p.tag_from_probability_with_id(0.5, 2); // z

    let mut proof1: std::collections::BTreeSet<u32> = Default::default();
    proof1.insert(0); proof1.insert(1);
    let mut proof2: std::collections::BTreeSet<u32> = Default::default();
    proof2.insert(0); proof2.insert(2);

    let tag = vec![proof1, proof2];
    let wmc = p.recover_probability(&tag);
    assert!(
        (wmc - 0.64).abs() < 1e-9,
        "WMC with overlap should be 0.64 (not noisy-OR 0.688), got {}",
        wmc
    );
}

#[test]
fn topk_transitive_chain() {
    // A related B (0.9), B related C (0.8)
    // Rule: ?X related ?Y, ?Y related ?Z => ?X related ?Z
    // TopKProofs ⊗ = Cartesian product of proof sets
    // tag(A->B) = [{0}], tag(B->C) = [{1}]
    // conjunction: [{0}] ⊗ [{1}] = [{0,1}]
    // WMC of [{0,1}] = P(A->B) * P(B->C) = 0.9 * 0.8 = 0.72
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "related", "B", 0.9);
    r.add_tagged_triple("B", "related", "C", 0.8);

    let related = enc(&r, "related");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Y".into())),
            (Term::Variable("Y".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(related), Term::Variable("Z".into())),
        ],
    ));

    let (inferred, tag_store) = r.infer_new_facts_with_provenance(TopKProofs::new(3));

    let a = enc(&r, "A");
    let c = enc(&r, "C");
    let a_related_c = inferred.iter().any(|t| t.subject == a && t.predicate == related && t.object == c);
    assert!(a_related_c, "Should infer A related C");

    let triple = shared::triple::Triple { subject: a, predicate: related, object: c };
    let prob = tag_store.provenance().recover_probability(&tag_store.get_tag(&triple));
    // Both seeds are distinct variables, so WMC = 0.9 * 0.8 = 0.72
    assert!((prob - 0.72).abs() < 1e-6, "TopK chain probability should be 0.72, got {}", prob);
}

#[test]
fn topk_overlap_two_paths_share_base_fact() {
    // TopKProofs as the overlap-aware replacement for noisy-OR.
    //
    // Two rule firings produce the same conclusion triple but share a base fact (seed x).
    // Noisy-OR (AddMultProbability) would treat the paths as independent and overcount x,
    // giving 0.688. TopKProofs tracks proof sets and applies exact WMC via
    // inclusion-exclusion, giving the correct 0.64.
    //
    // Facts:
    //   A is_active yes  (seed x, prob 0.8)  ← appears in both proofs
    //   A knows B        (seed y, prob 0.6)
    //   A knows C        (seed z, prob 0.5)
    //
    // Rule: ?X is_active ?_, ?X knows ?Y => ?X is_socially_active yes
    //
    // Firing 1: X=A, Y=B -> proof {x, y} = seeds {0, 1}, score 0.8 * 0.6 = 0.48
    // Firing 2: X=A, Y=C -> proof {x, z} = seeds {0, 2}, score 0.8 * 0.5 = 0.40
    //
    // Exact WMC: P(x∧y) + P(x∧z) − P(x∧y∧z) = 0.48 + 0.40 − 0.24 = 0.64
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "is_active", "yes", 0.8);
    r.add_tagged_triple("A", "knows", "B", 0.6);
    r.add_tagged_triple("A", "knows", "C", 0.5);

    let is_active = enc(&r, "is_active");
    let knows = enc(&r, "knows");
    let is_socially_active = enc(&r, "is_socially_active");
    let yes = enc(&r, "yes");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(is_active), Term::Variable("S".into())),
            (Term::Variable("X".into()), Term::Constant(knows), Term::Variable("Y".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(is_socially_active), Term::Constant(yes)),
        ],
    ));

    let (inferred, tag_store) = r.infer_new_facts_with_provenance(TopKProofs::new(5));

    let a = enc(&r, "A");
    assert!(
        inferred.iter().any(|t| t.subject == a && t.predicate == is_socially_active && t.object == yes),
        "Should infer A is_socially_active yes"
    );

    let triple = shared::triple::Triple { subject: a, predicate: is_socially_active, object: yes };
    let prob = tag_store.provenance().recover_probability(&tag_store.get_tag(&triple));

    assert!(
        (prob - 0.64).abs() < 1e-6,
        "TopKProofs exact WMC should be 0.64 (noisy-OR would give 0.688), got {}",
        prob
    );
}

#[test]
fn wmc_overlap_exact_inference() {
    // Same overlap scenario as topk_overlap_two_paths_share_base_fact, but using
    // WmcProvenance (exact symbolic, no truncation).
    //
    // Two proof paths to "A is_socially_active yes" share seed x (is_active):
    //   Path 1: {x, y} = seeds {0, 1} -> P(x)*P(y) = 0.8*0.6 = 0.48
    //   Path 2: {x, z} = seeds {0, 2} -> P(x)*P(z) = 0.8*0.5 = 0.40
    //
    // Shannon expansion (exact WMC):
    //   Condition on x: P(x)*WMC({y}∨{z}) + P(¬x)*WMC(∅)
    //   = 0.8*(0.6+0.5-0.30) + 0 = 0.8*0.80 = 0.64
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "is_active", "yes", 0.8);
    r.add_tagged_triple("A", "knows", "B", 0.6);
    r.add_tagged_triple("A", "knows", "C", 0.5);

    let is_active = enc(&r, "is_active");
    let knows = enc(&r, "knows");
    let is_socially_active = enc(&r, "is_socially_active");
    let yes = enc(&r, "yes");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(is_active), Term::Variable("S".into())),
            (Term::Variable("X".into()), Term::Constant(knows), Term::Variable("Y".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(is_socially_active), Term::Constant(yes)),
        ],
    ));

    let (inferred, tag_store) = r.infer_new_facts_with_provenance(WmcProvenance::new());

    let a = enc(&r, "A");
    assert!(
        inferred.iter().any(|t| t.subject == a && t.predicate == is_socially_active && t.object == yes),
        "Should infer A is_socially_active yes"
    );

    let triple = shared::triple::Triple { subject: a, predicate: is_socially_active, object: yes };
    let prob = tag_store.provenance().recover_probability(&tag_store.get_tag(&triple));

    assert!(
        (prob - 0.64).abs() < 1e-6,
        "WmcProvenance Shannon WMC should be 0.64, got {}",
        prob
    );
}

#[test]
fn wmc_no_truncation_three_paths() {
    // WmcProvenance keeps all proof paths; TopKProofs(k=2) would truncate the lowest.
    //
    // Three proof paths to "A is_socially_active yes", all sharing seed x (is_active):
    //   Path 1: {x, y} = seeds {0,1}, P = 0.9*0.8 = 0.72
    //   Path 2: {x, z} = seeds {0,2}, P = 0.9*0.7 = 0.63
    //   Path 3: {x, w} = seeds {0,3}, P = 0.9*0.6 = 0.54  ← dropped by TopKProofs(k=2)
    //
    // Exact WMC (all 3 paths via Shannon / inclusion-exclusion):
    //   Condition on x: P(x)*WMC({y}∨{z}∨{w}) + P(¬x)*0
    //   WMC({y}∨{z}∨{w}) = 1-(1-0.8)(1-0.7)(1-0.6) = 1-0.2*0.3*0.4 = 1-0.024 = 0.976
    //   WMC = 0.9 * 0.976 = 0.8784
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "is_active", "yes", 0.9);
    r.add_tagged_triple("A", "knows", "B", 0.8);
    r.add_tagged_triple("A", "knows", "C", 0.7);
    r.add_tagged_triple("A", "knows", "D", 0.6);

    let is_active = enc(&r, "is_active");
    let knows = enc(&r, "knows");
    let is_socially_active = enc(&r, "is_socially_active");
    let yes = enc(&r, "yes");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(is_active), Term::Variable("S".into())),
            (Term::Variable("X".into()), Term::Constant(knows), Term::Variable("Y".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(is_socially_active), Term::Constant(yes)),
        ],
    ));

    let (inferred, tag_store) = r.infer_new_facts_with_provenance(WmcProvenance::new());

    let a = enc(&r, "A");
    assert!(
        inferred.iter().any(|t| t.subject == a && t.predicate == is_socially_active && t.object == yes),
        "Should infer A is_socially_active yes"
    );

    let triple = shared::triple::Triple { subject: a, predicate: is_socially_active, object: yes };
    let prob = tag_store.provenance().recover_probability(&tag_store.get_tag(&triple));

    assert!(
        (prob - 0.8784).abs() < 1e-6,
        "WmcProvenance should give exact WMC 0.8784 for 3 overlapping paths, got {}",
        prob
    );
}

#[test]
fn wmc_explanation_encoding() {
    // Same 3-seed overlap setup as wmc_overlap_exact_inference:
    //   seed0 = A is_active yes  (P=0.8)
    //   seed1 = A knows B        (P=0.6)
    //   seed2 = A knows C        (P=0.5)
    // Rule fires for Y=B -> proof {0,1} and Y=C -> proof {0,2},
    // both deriving the same conclusion: A is_socially_active yes.
    let mut r = Reasoner::new();
    r.add_tagged_triple("A", "is_active", "yes", 0.8);
    r.add_tagged_triple("A", "knows", "B", 0.6);
    r.add_tagged_triple("A", "knows", "C", 0.5);

    let is_active = enc(&r, "is_active");
    let knows = enc(&r, "knows");
    let is_socially_active = enc(&r, "is_socially_active");
    let yes = enc(&r, "yes");

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(is_active), Term::Variable("S".into())),
            (Term::Variable("X".into()), Term::Constant(knows), Term::Variable("Y".into())),
        ],
        vec![
            (Term::Variable("X".into()), Term::Constant(is_socially_active), Term::Constant(yes)),
        ],
    ));

    let (inferred, tag_store) = r.infer_new_facts_with_provenance(WmcProvenance::new());

    // seed_triples should record the 3 seeds in sorted order
    assert_eq!(tag_store.seed_triples.len(), 3, "three probability seeds");

    let a = enc(&r, "A");
    assert!(
        inferred.iter().any(|t| t.subject == a && t.predicate == is_socially_active && t.object == yes),
        "Should infer A is_socially_active yes"
    );

    // formula for the derived triple should have 2 proof paths
    let derived = shared::triple::Triple { subject: a, predicate: is_socially_active, object: yes };
    let formula = tag_store.get_tag(&derived);
    assert_eq!(formula.len(), 2, "two proof paths: {{0,1}} and {{0,2}}");

    // encode_as_rdf_star_with_explanation should produce the explanation triples
    let mut dict = shared::dictionary::Dictionary::new();
    let mut qt = shared::quoted_triple_store::QuotedTripleStore::new();
    let triples = tag_store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt);

    let proof_count_pred = dict.encode("http://www.w3.org/ns/prob#proofCount");
    let has_proof_pred   = dict.encode("http://www.w3.org/ns/prob#hasProof");
    let has_seed_pred    = dict.encode("http://www.w3.org/ns/prob#hasSeed");

    // The tag store holds all 4 tagged triples (3 seeds + 1 derived fact),
    // so annotation counts span all of them:
    //
    //   Triple                      | formula         | proofCount | hasProof | hasSeed
    //   A is_active yes             | {{0}}           |     1      |    1     |    1
    //   A knows B                   | {{1}}           |     1      |    1     |    1
    //   A knows C                   | {{2}}           |     1      |    1     |    1
    //   A is_socially_active yes    | {{0,1},{0,2}}   |     2      |    2     |    4
    //   ────────────────────────────────────────────────────────────────────────────
    //   Total                                         |     4      |    5     |    7

    let proof_count_triples: Vec<_> = triples.iter()
        .filter(|t| t.predicate == proof_count_pred)
        .collect();
    assert_eq!(proof_count_triples.len(), 4, "one proofCount per tagged triple (3 seeds + 1 derived)");

    let has_proof_triples: Vec<_> = triples.iter()
        .filter(|t| t.predicate == has_proof_pred)
        .collect();
    // seeds have 1 proof each (3×1=3), derived has 2 proof paths -> total 5
    assert_eq!(has_proof_triples.len(), 5, "hasProof: 3 seed paths + 2 derived paths");

    let has_seed_triples: Vec<_> = triples.iter()
        .filter(|t| t.predicate == has_seed_pred)
        .collect();
    // seeds: 1+1+1=3; derived: proof {0,1}->2, proof {0,2}->2 = 4 -> total 7
    assert_eq!(has_seed_triples.len(), 7, "hasSeed: 3 from seed paths + 4 from derived paths");
}

// ─── Phase 1 NAF tests ────────────────────────────────────────────────────────

/// BooleanProvenance: exact NAF — blocked fact IS present -> NOT blocked = false -> effective not derived.
#[test]
fn boolean_naf_blocked_present() {
    let mut r = Reasoner::new();
    r.add_abox_triple("P", "active", "yes");
    r.add_abox_triple("P", "blocked", "yes");

    let active    = enc(&r, "active");
    let blocked   = enc(&r, "blocked");
    let effective = enc(&r, "effective");
    let yes       = enc(&r, "yes");

    // RULE: active(?x, yes), NOT blocked(?x, yes) -> effective(?x, yes)
    r.add_rule(naf_rule(
        vec![(Term::Variable("x".into()), Term::Constant(active),    Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(blocked),   Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(effective), Term::Constant(yes))],
    ));

    let (new_facts, _tag_store) = r.infer_new_facts_with_provenance(BooleanProvenance);

    // blocked IS present -> NOT true = false -> effective must NOT be derived
    assert!(!inferred(&mut r, "P", "effective", "yes"),
        "effective must not be derived when blocked is present");

    let p_id  = enc(&r, "P");
    let eff_t = shared::triple::Triple { subject: p_id, predicate: effective, object: yes };
    assert!(!new_facts.iter().any(|t| t == &eff_t),
        "effective must not appear in new_facts when blocked is present");
}

/// BooleanProvenance: exact NAF — blocked fact is ABSENT -> NOT blocked = true -> effective derived.
#[test]
fn boolean_naf_absent_negated() {
    let mut r = Reasoner::new();
    r.add_abox_triple("P", "active", "yes");
    // "blocked" triple intentionally NOT added

    let active    = enc(&r, "active");
    let blocked   = enc(&r, "blocked");
    let effective = enc(&r, "effective");
    let yes       = enc(&r, "yes");

    r.add_rule(naf_rule(
        vec![(Term::Variable("x".into()), Term::Constant(active),    Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(blocked),   Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(effective), Term::Constant(yes))],
    ));

    let (new_facts, tag_store) = r.infer_new_facts_with_provenance(BooleanProvenance);

    assert!(inferred(&mut r, "P", "effective", "yes"),
        "effective must be derived when blocked is absent");

    let p_id  = enc(&r, "P");
    let eff_t = shared::triple::Triple { subject: p_id, predicate: effective, object: yes };
    assert!(tag_store.get_tag(&eff_t),
        "BooleanProvenance tag for effective must be true");

    assert!(new_facts.iter().any(|t| t == &eff_t),
        "effective must appear in new_facts");
}

/// AddMultProbability (approximate): active(0.8) AND NOT blocked(0.3) -> P ≈ 0.8 * 0.7 = 0.56.
///
/// This is approximate — exact only if seeds are independent. Labelled as such.
#[test]
fn addmult_naf_approximate() {
    let mut r = Reasoner::new();
    r.add_tagged_triple("P", "active",  "yes", 0.8);
    r.add_tagged_triple("P", "blocked", "yes", 0.3);

    let active    = enc(&r, "active");
    let blocked   = enc(&r, "blocked");
    let effective = enc(&r, "effective");
    let yes       = enc(&r, "yes");

    r.add_rule(naf_rule(
        vec![(Term::Variable("x".into()), Term::Constant(active),    Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(blocked),   Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(effective), Term::Constant(yes))],
    ));

    let (_, tag_store) = r.infer_new_facts_with_provenance(AddMultProbability);

    let p_id  = enc(&r, "P");
    let eff_t = shared::triple::Triple { subject: p_id, predicate: effective, object: yes };
    let prob  = tag_store.get_tag(&eff_t);

    // P(effective) ≈ P(active) * (1 - P(blocked)) = 0.8 * 0.7 = 0.56
    // (approximate: exact only for independent seeds)
    assert!((prob - 0.56).abs() < 1e-9,
        "AddMultProbability NAF: expected ~0.56, got {}", prob);
}

/// AddMultProbability: blocked absent -> NOT blocked = one() -> P(effective) = P(active) = 0.8.
#[test]
fn addmult_naf_absent_negated() {
    let mut r = Reasoner::new();
    r.add_tagged_triple("P", "active", "yes", 0.8);
    // blocked NOT added

    let active    = enc(&r, "active");
    let blocked   = enc(&r, "blocked");
    let effective = enc(&r, "effective");
    let yes       = enc(&r, "yes");

    r.add_rule(naf_rule(
        vec![(Term::Variable("x".into()), Term::Constant(active),    Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(blocked),   Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(effective), Term::Constant(yes))],
    ));

    let (_, tag_store) = r.infer_new_facts_with_provenance(AddMultProbability);

    let p_id  = enc(&r, "P");
    let eff_t = shared::triple::Triple { subject: p_id, predicate: effective, object: yes };
    let prob  = tag_store.get_tag(&eff_t);

    // blocked absent -> NOT blocked = one() -> P = 0.8 * 1.0 = 0.8
    assert!((prob - 0.8).abs() < 1e-9,
        "AddMultProbability NAF (absent): expected 0.8, got {}", prob);
}

/// try_add_rule rejects rules where a negated variable is unbound by positive premises.
#[test]
fn unsafe_negation_rejected() {
    let mut r = Reasoner::new();

    let active    = enc(&r, "active");
    let blocked   = enc(&r, "blocked");
    let effective = enc(&r, "effective");
    let yes       = enc(&r, "yes");

    // UNSAFE: ?y appears in NOT-body but is NOT bound by premise (?x is bound, ?y is not)
    let bad_rule = Rule {
        premise: vec![
            (Term::Variable("x".into()), Term::Constant(active), Term::Constant(yes)),
        ],
        negative_premise: vec![
            // ?y is unbound — not in any positive premise
            (Term::Variable("y".into()), Term::Constant(blocked), Term::Constant(yes)),
        ],
        conclusion: vec![
            (Term::Variable("x".into()), Term::Constant(effective), Term::Constant(yes)),
        ],
        filters: vec![],
    };

    let result = r.try_add_rule(bad_rule);
    assert!(result.is_err(), "unsafe negation must be rejected by try_add_rule");
    assert!(result.unwrap_err().contains("unsafe negation"),
        "error message must mention unsafe negation");
}

// ─── Phase 2 WMC NAF integration tests ──────────────────────────────────────

/// WmcProvenance: independent seeds, active(P=0.8) AND NOT blocked(P=0.3).
///
/// Exact result: P(active=T AND blocked=F) = 0.8 * 0.7 = 0.56
/// (exact because seeds are independent — no shared variable)
#[test]
fn wmc_naf_independent_seeds_integration() {
    let mut r = Reasoner::new();
    r.add_tagged_triple("P", "active",  "yes", 0.8);
    r.add_tagged_triple("P", "blocked", "yes", 0.3);

    let active    = enc(&r, "active");
    let blocked   = enc(&r, "blocked");
    let effective = enc(&r, "effective");
    let yes       = enc(&r, "yes");

    r.add_rule(naf_rule(
        vec![(Term::Variable("x".into()), Term::Constant(active),    Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(blocked),   Term::Constant(yes))],
        vec![(Term::Variable("x".into()), Term::Constant(effective), Term::Constant(yes))],
    ));

    let (_, tag_store) = r.infer_new_facts_with_provenance(WmcProvenance::new());

    let p_id  = enc(&r, "P");
    let eff_t = shared::triple::Triple { subject: p_id, predicate: effective, object: yes };
    let prob  = tag_store.provenance().recover_probability(&tag_store.get_tag(&eff_t));

    assert!((prob - 0.56).abs() < 1e-9,
        "WMC NAF independent seeds: expected 0.56, got {}", prob);
}

/// WmcProvenance: same triple used for both active and blocked (shared seed).
///
/// active tag  = {{(0,T)}}, blocked tag = {{(0,T)}} (same seed 0)
/// NOT blocked = {{(0,F)}}
/// conjunction = {{(0,T),(0,F)}} = contradictory -> zero -> P = 0.0
#[test]
fn wmc_naf_shared_seed_integration() {
    let mut r = Reasoner::new();
    // Add the SAME triple for both active and blocked at the same probability.
    // Since it's the same triple, both get the same seed ID in the tag store.
    r.add_tagged_triple("P", "status", "active",  0.8);
    // Use a different triple to represent blocked so we can control the seed.
    // To force shared-seed: use the same triple for both predicates is not possible
    // in the normal API. So we test the algebraic property directly via provenance:
    let p = WmcProvenance::new();
    let active_tag  = p.tag_from_probability_with_id(0.8, 0);
    let blocked_tag = p.tag_from_probability_with_id(0.8, 0); // same ID = shared seed

    let not_blocked   = p.negate(&blocked_tag);
    let effective_tag = p.conjunction(&active_tag, &not_blocked);
    let prob = p.recover_probability(&effective_tag);

    assert!((prob - 0.0).abs() < 1e-9,
        "WMC NAF shared seed: expected 0.0 (p ∧ ¬p = ⊥), got {}", prob);
}
