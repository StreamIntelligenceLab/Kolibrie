use crate::dictionary::Dictionary;
use crate::triple::Triple;
use std::collections::{BTreeSet, HashMap};
// Logic part: Knowledge Graph

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Term {
    Variable(String),
    Constant(u32),
}

pub type TriplePattern = (Term, Term, Term);

#[derive(Debug, Clone)]
pub struct Rule {
    pub premise: Vec<TriplePattern>,
    pub conclusion: TriplePattern,
}

#[derive(Debug, Clone)]
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox: Assertions about individuals (instances)
    pub tbox: BTreeSet<Triple>, // TBox: Concepts and relationships (schema)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // List of dynamic rules
}

impl KnowledgeGraph {
    pub fn new() -> Self {
        Self {
            abox: BTreeSet::new(),
            tbox: BTreeSet::new(),
            dictionary: Dictionary::new(),
            rules: Vec::new(),
        }
    }

    /// Add a TBox triple (schema-level information)
    pub fn add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let triple = Triple {
            subject: self.dictionary.encode(subject),
            predicate: self.dictionary.encode(predicate),
            object: self.dictionary.encode(object),
        };
        self.tbox.insert(triple);
    }

    /// Add an ABox triple (instance-level information)
    pub fn add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let triple = Triple {
            subject: self.dictionary.encode(subject),
            predicate: self.dictionary.encode(predicate),
            object: self.dictionary.encode(object),
        };
        self.abox.insert(triple);
    }

    /// Query the ABox for instance-level assertions
    pub fn query_abox(
        &mut self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> Vec<Triple> {
        let subject_id = subject.map(|s| self.dictionary.encode(s));
        let predicate_id = predicate.map(|p| self.dictionary.encode(p));
        let object_id = object.map(|o| self.dictionary.encode(o));

        self.query(self.abox.iter(), subject_id, predicate_id, object_id)
    }

    /// Query the TBox for schema-level assertions
    pub fn query_tbox(
        &mut self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> Vec<Triple> {
        let subject_id = subject.map(|s| self.dictionary.encode(s));
        let predicate_id = predicate.map(|p| self.dictionary.encode(p));
        let object_id = object.map(|o| self.dictionary.encode(o));

        self.query(self.tbox.iter(), subject_id, predicate_id, object_id)
    }

    /// Helper function to handle querying both ABox and TBox
    fn query<'a, I>(
        &self, // No need for &mut self anymore
        triples: I,
        subject_id: Option<u32>,
        predicate_id: Option<u32>,
        object_id: Option<u32>,
    ) -> Vec<Triple>
    where
        I: Iterator<Item = &'a Triple>,
    {
        triples
            .filter(|triple| {
                subject_id.map_or(true, |sid| triple.subject == sid)
                    && predicate_id.map_or(true, |pid| triple.predicate == pid)
                    && object_id.map_or(true, |oid| triple.object == oid)
            })
            .cloned()
            .collect()
    }

    /// Add a dynamic rule to the graph
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    /// Infer new facts based on dynamic rules
    pub fn infer_new_facts(&mut self) -> Vec<Triple> {
        let mut inferred_facts = Vec::new();
        let abox_facts: Vec<Triple> = self.abox.iter().cloned().collect();
        let rules = self.rules.clone();

        for rule in &rules {
            if rule.premise.len() == 1 {
                // Handle single-premise rule
                for triple in &abox_facts {
                    let mut variable_bindings = HashMap::new();

                    if self.matches(&rule.premise[0], triple, &mut variable_bindings) {
                        let subject = match &rule.conclusion.0 {
                            Term::Variable(var_name) => *variable_bindings.get(var_name).unwrap(),
                            Term::Constant(value) => *value,
                        };

                        let predicate = match &rule.conclusion.1 {
                            Term::Variable(var_name) => *variable_bindings.get(var_name).unwrap(),
                            Term::Constant(value) => *value,
                        };

                        let object = match &rule.conclusion.2 {
                            Term::Variable(var_name) => *variable_bindings.get(var_name).unwrap(),
                            Term::Constant(value) => *value,
                        };

                        let inferred_triple = Triple {
                            subject,
                            predicate,
                            object,
                        };

                        if self.abox.insert(inferred_triple.clone()) {
                            inferred_facts.push(inferred_triple.clone());
                            println!("Inferred new fact: {:?}", inferred_triple.clone());
                        }
                    }
                }
            } else if rule.premise.len() == 2 {
                // Handle two-premise rule (original logic remains)
                for triple_1 in &abox_facts {
                    for triple_2 in &abox_facts {
                        if triple_1.object != triple_2.subject {
                            continue;
                        }

                        let mut variable_bindings = HashMap::new();

                        if self.matches(&rule.premise[0], triple_1, &mut variable_bindings)
                            && self.matches(&rule.premise[1], triple_2, &mut variable_bindings)
                        {
                            let subject = match &rule.conclusion.0 {
                                Term::Variable(var_name) => *variable_bindings.get(var_name).unwrap(),
                                Term::Constant(value) => *value,
                            };

                            let predicate = match &rule.conclusion.1 {
                                Term::Variable(var_name) => *variable_bindings.get(var_name).unwrap(),
                                Term::Constant(value) => *value,
                            };

                            let object = match &rule.conclusion.2 {
                                Term::Variable(var_name) => *variable_bindings.get(var_name).unwrap(),
                                Term::Constant(value) => *value,
                            };

                            let inferred_triple = Triple {
                                subject,
                                predicate,
                                object,
                            };

                            if self.abox.insert(inferred_triple.clone()) {
                                inferred_facts.push(inferred_triple.clone());
                                println!("Inferred new fact: {:?}", inferred_triple.clone());
                            }
                        }
                    }
                }
            }
        }

        inferred_facts
    }

    /// Check if a triple matches a rule pattern and bind variables
    fn matches(
        &self,
        pattern: &(Term, Term, Term),
        triple: &Triple,
        variable_bindings: &mut std::collections::HashMap<String, u32>,
    ) -> bool {
        // Match the subject
        let subject_match = match &pattern.0 {
            Term::Variable(var_name) => {
                if let Some(&bound_value) = variable_bindings.get(var_name) {
                    bound_value == triple.subject
                } else {
                    variable_bindings.insert(var_name.clone(), triple.subject);
                    true
                }
            }
            Term::Constant(value) => *value == triple.subject,
        };

        // Match the predicate
        let predicate_match = match &pattern.1 {
            Term::Variable(var_name) => {
                if let Some(&bound_value) = variable_bindings.get(var_name) {
                    bound_value == triple.predicate
                } else {
                    variable_bindings.insert(var_name.clone(), triple.predicate);
                    true
                }
            }
            Term::Constant(value) => *value == triple.predicate,
        };

        // Match the object
        let object_match = match &pattern.2 {
            Term::Variable(var_name) => {
                if let Some(&bound_value) = variable_bindings.get(var_name) {
                    bound_value == triple.object
                } else {
                    variable_bindings.insert(var_name.clone(), triple.object);
                    true
                }
            }
            Term::Constant(value) => *value == triple.object,
        };

        subject_match && predicate_match && object_match
    }

    pub fn backward_chaining(&self, query: &TriplePattern) -> Vec<HashMap<String, Term>> {
        let bindings = HashMap::new();
        let mut variable_counter = 0;
        self.backward_chaining_helper(query, &bindings, 0, &mut variable_counter)
    }

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

        let mut results = Vec::new();

        // Substitute current bindings into the query
        let substituted_query = substitute(query, bindings);

        // Unify with facts in abox and tbox
        for triple in self.abox.iter().chain(self.tbox.iter()) {
            let fact_pattern = triple_to_pattern(triple);
            if let Some(new_bindings) = unify_patterns(&substituted_query, &fact_pattern, bindings)
            {
                results.push(new_bindings);
            }
        }

        // Unify with rules
        for rule in &self.rules {
            let renamed_rule = rename_rule_variables(rule, variable_counter);

            if let Some(rule_bindings) =
                unify_patterns(&renamed_rule.conclusion, &substituted_query, bindings)
            {
                let mut premise_results = vec![rule_bindings.clone()];
                for premise in &renamed_rule.premise {
                    let mut new_premise_results = Vec::new();
                    for b in &premise_results {
                        let res =
                            self.backward_chaining_helper(premise, b, depth + 1, variable_counter);
                        new_premise_results.extend(res);
                    }
                    premise_results = new_premise_results;
                }
                results.extend(premise_results);
            }
        }

        results
    }
}

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
    }
}

fn triple_to_pattern(triple: &Triple) -> TriplePattern {
    (
        Term::Constant(triple.subject),
        Term::Constant(triple.predicate),
        Term::Constant(triple.object),
    )
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
        }
    }

    let mut new_premise = Vec::new();
    for p in &rule.premise {
        let s = rename_term(&p.0, &mut var_map, counter);
        let p_term = rename_term(&p.1, &mut var_map, counter);
        let o = rename_term(&p.2, &mut var_map, counter);
        new_premise.push((s, p_term, o));
    }

    let conclusion_s = rename_term(&rule.conclusion.0, &mut var_map, counter);
    let conclusion_p = rename_term(&rule.conclusion.1, &mut var_map, counter);
    let conclusion_o = rename_term(&rule.conclusion.2, &mut var_map, counter);

    Rule {
        premise: new_premise,
        conclusion: (conclusion_s, conclusion_p, conclusion_o),
    }
}