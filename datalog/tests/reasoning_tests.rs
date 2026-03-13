use datalog::reasoning::Reasoner;
use shared::rule::Rule;
use shared::terms::Term;
use std::collections::HashMap;

fn enc(r: &Reasoner, s: &str) -> u32 {
    r.dictionary.write().unwrap().encode(s)
}

fn rule(premises: Vec<(Term, Term, Term)>, conclusions: Vec<(Term, Term, Term)>) -> Rule {
    Rule {
        premise: premises,
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

// ─── Forward chaining ────────────────────────────────────────────────────────

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

    // base: parent → ancestor
    r.add_rule(rule(
        vec![(Term::Variable("X".into()), Term::Constant(parent), Term::Variable("Y".into()))],
        vec![(Term::Variable("X".into()), Term::Constant(ancestor), Term::Variable("Y".into()))],
    ));
    // transitive: ancestor + ancestor → ancestor
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

    r.add_rule(rule(
        vec![
            (Term::Variable("X".into()), Term::Constant(parent), Term::Variable("P2".into())),
            (Term::Variable("Y".into()), Term::Constant(parent), Term::Variable("P2".into())),
        ],
        vec![(Term::Variable("X".into()), Term::Constant(sibling), Term::Variable("Y".into()))],
    ));

    r.infer_new_facts_semi_naive();

    assert!(inferred(&mut r, "A", "sibling", "B"));
    assert!(inferred(&mut r, "B", "sibling", "A"));
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

// ─── Backward chaining ───────────────────────────────────────────────────────

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
