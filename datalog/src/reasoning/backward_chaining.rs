use std::collections::HashMap;
use shared::rule::Rule;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use crate::reasoning::Reasoner;

fn unify_patterns(
    pattern1: &TriplePattern,
    pattern2: &TriplePattern,
    bindings: &HashMap<String, Term>,
) -> Option<HashMap<String, Term>> {
    let mut new_bindings = bindings.clone();

    if !unify_terms(&pattern1.0, &pattern2.0, &mut new_bindings) {
        return None;
    }
    if !unify_terms(&pattern1.1, &pattern2.1, &mut new_bindings) {
        return None;
    }
    if !unify_terms(&pattern1.2, &pattern2.2, &mut new_bindings) {
        return None;
    }

    Some(new_bindings)
}

fn unify_terms(term1: &Term, term2: &Term, bindings: &mut HashMap<String, Term>) -> bool {
    let term1 = resolve_term(term1, bindings);
    let term2 = resolve_term(term2, bindings);

    match (&term1, &term2) {
        (Term::Constant(c1), Term::Constant(c2)) => c1 == c2,
        (Term::Variable(v), Term::Constant(c)) | (Term::Constant(c), Term::Variable(v)) => {
            bindings.insert(v.clone(), Term::Constant(*c));
            true
        }
        (Term::Variable(v1), Term::Variable(v2)) => {
            if v1 != v2 {
                bindings.insert(v1.clone(), Term::Variable(v2.clone()));
            }
            true
        }
        // QuotedTriple terms unify if their components unify
        (Term::QuotedTriple(qt1), Term::QuotedTriple(qt2)) => {
            unify_terms(&qt1.0, &qt2.0, bindings)
                && unify_terms(&qt1.1, &qt2.1, bindings)
                && unify_terms(&qt1.2, &qt2.2, bindings)
        }
        (Term::Variable(v), Term::QuotedTriple(qt)) | (Term::QuotedTriple(qt), Term::Variable(v)) => {
            bindings.insert(v.clone(), Term::QuotedTriple(qt.clone()));
            true
        }
        _ => false,
    }
}

pub fn resolve_term<'a>(term: &'a Term, bindings: &'a HashMap<String, Term>) -> Term {
    match term {
        Term::Variable(v) => {
            if let Some(bound_term) = bindings.get(v) {
                resolve_term(bound_term, bindings)
            } else {
                term.clone()
            }
        }
        _ => term.clone(),
    }
}

fn substitute(pattern: &TriplePattern, bindings: &HashMap<String, Term>) -> TriplePattern {
    let s = substitute_term(&pattern.0, bindings);
    let p = substitute_term(&pattern.1, bindings);
    let o = substitute_term(&pattern.2, bindings);
    (s, p, o)
}

fn substitute_term(term: &Term, bindings: &HashMap<String, Term>) -> Term {
    match term {
        Term::Variable(var_name) => {
            if let Some(bound_term) = bindings.get(var_name) {
                substitute_term(bound_term, bindings)
            } else {
                Term::Variable(var_name.clone())
            }
        }
        Term::Constant(value) => Term::Constant(*value),
        Term::QuotedTriple(qt) => Term::QuotedTriple(Box::new((
            substitute_term(&qt.0, bindings),
            substitute_term(&qt.1, bindings),
            substitute_term(&qt.2, bindings),
        ))),
    }
}

fn rename_rule_variables(rule: &Rule, counter: &mut usize) -> Rule {
    let mut var_map = HashMap::new();

    fn rename_term(
        term: &Term,
        var_map: &mut HashMap<String, String>,
        counter: &mut usize,
    ) -> Term {
        match term {
            Term::Variable(v) => {
                // If a variable mapping already exists (e.g. ?person -> v0) then just clone that name
                if let Some(new_v) = var_map.get(v) {
                    Term::Variable(new_v.clone())
                } else {
                    let new_v = format!("v{}", *counter);
                    *counter += 1;
                    var_map.insert(v.clone(), new_v.clone());
                    Term::Variable(new_v)
                }
            }
            Term::Constant(c) => Term::Constant(*c),
            Term::QuotedTriple(qt) => Term::QuotedTriple(Box::new((
                rename_term(&qt.0, var_map, counter),
                rename_term(&qt.1, var_map, counter),
                rename_term(&qt.2, var_map, counter),
            ))),
        }
    }

    let mut new_premise = Vec::new();
    for p in &rule.premise {
        let s = rename_term(&p.0, &mut var_map, counter);
        let p_term = rename_term(&p.1, &mut var_map, counter);
        let o = rename_term(&p.2, &mut var_map, counter);
        new_premise.push((s, p_term, o));
    }

    let mut new_conclusions = Vec::new();
    for conclusion in &rule.conclusion {
        let conclusion_s = rename_term(&conclusion.0, &mut var_map, counter);
        let conclusion_p = rename_term(&conclusion.1, &mut var_map, counter);
        let conclusion_o = rename_term(&conclusion.2, &mut var_map, counter);
        new_conclusions.push((conclusion_s, conclusion_p, conclusion_o));
    }

    Rule {
        premise: new_premise,
        negative_premise: vec![],
        conclusion: new_conclusions,
        filters: rule.filters.clone(),
    }
}

impl Reasoner {
    /// Returns all variable bindings that satisfy `query` via backward chaining.
    pub fn backward_chaining(&self, query: &TriplePattern) -> Vec<HashMap<String, Term>> {
        let bindings = HashMap::new();
        let mut variable_counter = 0;
        self.backward_chaining_helper(query, &bindings, 0, &mut variable_counter)
    }



    /// Recursive helper for backward chaining. Depth-limited to prevent infinite loops.
    fn backward_chaining_helper(
        &self,
        query: &TriplePattern,
        bindings: &HashMap<String, Term>,
        depth: usize,
        variable_counter: &mut usize,
    ) -> Vec<HashMap<String, Term>> {
        const MAX_DEPTH: usize = 10;
        if depth > MAX_DEPTH {
            return Vec::new();
        }

        let substituted = substitute(query, bindings);
        let mut results = Vec::new();
        let all_facts: Vec<Triple> = self.index_manager.query(None, None, None);

        for fact in &all_facts {
            let fact_pattern = fact.to_pattern();
            if let Some(new_bindings) = unify_patterns(&substituted, &fact_pattern, bindings) {
                results.push(new_bindings);
            }
        }

        // Match with rules
        for rule in &self.rules {
            let renamed_rule = rename_rule_variables(rule, variable_counter);

            // Try to unify with each conclusion in the rule
            for conclusion in &renamed_rule.conclusion {
                if let Some(rb) = unify_patterns(conclusion, &substituted, bindings) {
                    let mut premise_results = vec![rb.clone()];
                    for prem in &renamed_rule.premise {
                        let mut new_premise_results = Vec::new();
                        for b in &premise_results {
                            let sub_res =
                                self.backward_chaining_helper(prem, b, depth + 1, variable_counter);
                            new_premise_results.extend(sub_res);
                        }
                        premise_results = new_premise_results;
                    }
                    results.extend(premise_results);
                }
            }
        }

        results
    }
}