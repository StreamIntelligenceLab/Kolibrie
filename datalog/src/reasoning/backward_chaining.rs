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
    // We CLONE the bindings so that we can make a new bindings object that can be returned in case they matchs
    let mut new_bindings = bindings.clone();

    if !unify_terms(&pattern1.0, &pattern2.0, &mut new_bindings) {
        return None; // Failed to unify pattern (e.g. Alice, Bob)
    }
    if !unify_terms(&pattern1.1, &pattern2.1, &mut new_bindings) {
        return None; // Failed to unify pattern: predicates don't match (e.g. likes, happyAbout)
    }
    if !unify_terms(&pattern1.2, &pattern2.2, &mut new_bindings) {
        return None; // Failed to unify pattern: objects don't match (e.g. Pizza and IceCream)
    }

    Some(new_bindings)
}

fn unify_terms(term1: &Term, term2: &Term, bindings: &mut HashMap<String, Term>) -> bool {
    let term1 = resolve_term(term1, bindings);
    let term2 = resolve_term(term2, bindings);

    match (&term1, &term2) {
        (Term::Constant(c1), Term::Constant(c2)) => c1 == c2, // Returns false if both are constants and inequal
        (Term::Variable(v), Term::Constant(c)) | (Term::Constant(c), Term::Variable(v)) => {
            // v.clone(): another clone is made here?

            bindings.insert(v.clone(), Term::Constant(*c));
            true
        }
        (Term::Variable(v1), Term::Variable(v2)) => {
            if v1 != v2 {
                bindings.insert(v1.clone(), Term::Variable(v2.clone()));
            }
            true
        }
    }
}

pub fn resolve_term<'a>(term: &'a Term, bindings: &'a HashMap<String, Term>) -> Term {
    match term {
        Term::Variable(v) => {
            // Bound term is the term that is bound to the variable
            if let Some(bound_term) = bindings.get(v) {
                resolve_term(bound_term, bindings) // Resolve the bound term recursively through the bindings
            } else {
                // Can be that a variable maps to nothing, in that case we simply clone the term.
                term.clone() // Clone the term to get a result. Why clone exactly?
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
                    *counter += 1; // Counter is incremented HERE
                    var_map.insert(v.clone(), new_v.clone()); // Why create a map between variable names?
                    Term::Variable(new_v)
                }
            }
            Term::Constant(c) => Term::Constant(*c),
        }
    }

    let mut new_premise = Vec::new();
    for p in &rule.premise {
        let s = rename_term(&p.0, &mut var_map, counter);
        let p_term = rename_term(&p.1, &mut var_map, counter);
        let o = rename_term(&p.2, &mut var_map, counter);
        new_premise.push((s, p_term, o));
    }

    // Rename all conclusions
    let mut new_conclusions = Vec::new();
    for conclusion in &rule.conclusion {
        let conclusion_s = rename_term(&conclusion.0, &mut var_map, counter);
        let conclusion_p = rename_term(&conclusion.1, &mut var_map, counter);
        let conclusion_o = rename_term(&conclusion.2, &mut var_map, counter);
        new_conclusions.push((conclusion_s, conclusion_p, conclusion_o));
    }

    Rule {
        premise: new_premise,
        conclusion: new_conclusions,
        filters: rule.filters.clone(),
    }
}

impl Reasoner {
    /// Runs backward chaining inference over the knowledge graph for the given query.
    ///
    /// This function takes a triple pattern as a query and returns all sets of variable
    /// bindings that satisfy the query, by invoking a recursive reasoning algorithm
    /// (see `backward_chaining_helper`). Returns a vector of solution bindings, where
    /// each binding is a mapping from variable names to their concrete values.
    ///
    /// # Arguments
    /// - `query`: The triple pattern to match in the knowledge graph.
    ///
    /// # Returns
    /// A vector of hashmaps (`Vec<HashMap<String, Term>>`), each representing one set
    /// of matched variable bindings.
    pub fn backward_chaining(&self, query: &TriplePattern) -> Vec<HashMap<String, Term>> {
        let bindings = HashMap::new();
        let mut variable_counter = 0; // What is this?
        self.backward_chaining_helper(query, &bindings, 0, &mut variable_counter)
    }



    /// Recursively applies backward chaining resolution to derive all possible bindings
    /// for a query, given existing bindings and the current recursion depth.
    ///
    /// This helper explores both direct facts and rule-based inferences within the knowledge
    /// graph. It enforces a maximum recursion depth to prevent infinite loops, attempts
    /// to unify the current query with known facts, and recursively tries rule premises
    /// when rules are applicable.
    ///
    /// # Arguments
    /// - `query`: The pattern being resolved at this recursion level.
    /// - `bindings`: Existing variable bindings accumulated so far.
    /// - `depth`: The current recursion depth (used for limiting inference).
    /// - `variable_counter`: Mutable counter ensuring uniquely-named variables in rule applications.
    ///
    /// # Returns
    /// A vector of hashmaps (`Vec<Binding>`), each containing a complete set of
    /// bindings from successfully resolved inference chains.
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

        // Get a substituted query (using the bindings we already have) that can be matched against known facts
        // From the DB
        let substituted = substitute(query, bindings);

        let mut results = Vec::new();
        // Get facts from the index manager
        let all_facts: Vec<Triple> = self.index_manager.query(None, None, None);

        for fact in &all_facts {
            let fact_pattern = fact.to_pattern();

            // new_bindings are like the solutions we get from matching the (substituted) query
            // With the facts from the db.
            // E.g. if you can unify a variable <?X with Alice to get X -> Alice>, <?Y with IceCream to get Y -> IceCream>
            // The entire triple pattern <?X likes ?Y> becomes matched with the fact <Alice, likes, IceCream> and
            // bindings become {X -> Alice, Y -> IceCream}
            if let Some(new_bindings) = unify_patterns(&substituted, &fact_pattern, bindings) {
                results.push(new_bindings);
            }
        }

        // match with rules
        for rule in &self.rules {
            let renamed_rule = rename_rule_variables(rule, variable_counter);

            // Try to unify with each conclusion in the rule
            for conclusion in &renamed_rule.conclusion {
                // rb: rule bindings?
                // &substituted is already substitued, so the bindings won't apply here right?
                if let Some(rb) = unify_patterns(conclusion, &substituted, bindings) {

                    // The query matches the rule conclusion according to current bindings

                    // We have a match => we need all premises to succeed
                    // All premises: e.g. if you have an AND rule, then for the conclusion to hold as a fact, the premises should be facts

                    // premise_results:
                    let mut premise_results = vec![rb.clone()];
                    for prem in &renamed_rule.premise {
                        // Vector of bindings
                        let mut new_premise_results = Vec::new();

                        // b is in each premise result. This acts as the new 'query' in our backward chaining algorithm (the conclusion)

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