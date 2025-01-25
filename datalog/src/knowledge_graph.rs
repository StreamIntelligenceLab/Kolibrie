use shared::dictionary::Dictionary;
use shared::triple::Triple;
use std::collections::{BTreeSet, HashMap};

/// A trie-like index that maps:
///   subject -> predicate -> set of objects
#[derive(Debug, Default, Clone)]
pub struct TrieIndex {
    // subject -> (predicate -> set of objects)
    data: HashMap<u32, HashMap<u32, BTreeSet<u32>>>,
}

impl TrieIndex {
    /// Create a new, empty TrieIndex.
    pub fn new() -> Self {
        TrieIndex {
            data: HashMap::new(),
        }
    }

    pub fn contains(&self, triple: &Triple) -> bool {
        if let Some(subj_map) = self.data.get(&triple.subject) {
            if let Some(obj_set) = subj_map.get(&triple.predicate) {
                return obj_set.contains(&triple.object);
            }
        }
        false
    }

    /// Insert a triple into the trie.
    /// This does *not* prevent duplicates; typically you'd keep an external set
    /// or something that ensures you only insert “new” facts once.
    pub fn insert_raw(&mut self, triple: &Triple) {
        let subject_map = self.data
            .entry(triple.subject)
            .or_insert_with(HashMap::new);

        let object_set = subject_map
            .entry(triple.predicate)
            .or_insert_with(BTreeSet::new);

        object_set.insert(triple.object);
    }

    pub fn insert(&mut self, triple: &Triple) -> bool {
        if self.contains(triple) {
            return false; // no-op, already in the index
        }
        self.insert_raw(triple);
        true
    }

    /// Remove a triple from the trie, if present.
    pub fn remove(&mut self, triple: &Triple) {
        if let Some(subject_map) = self.data.get_mut(&triple.subject) {
            if let Some(object_set) = subject_map.get_mut(&triple.predicate) {
                object_set.remove(&triple.object);
                // If the object set is now empty, remove the predicate entry.
                if object_set.is_empty() {
                    subject_map.remove(&triple.predicate);
                }
            }
            // If the subject map is now empty, remove the subject entry.
            if subject_map.is_empty() {
                self.data.remove(&triple.subject);
            }
        }
    }

    /// Query the index with optional subject/predicate/object.
    ///
    /// If any field is `None`, it acts like a wildcard:
    /// it matches all possible values in that position.
    ///
    /// Returns a `Vec<Triple>` matching the pattern.
    pub fn query(
        &self,
        subject_id: Option<u32>,
        predicate_id: Option<u32>,
        object_id: Option<u32>,
    ) -> Vec<Triple> {
        let mut results = Vec::new();

        let subjects = match subject_id {
            Some(sid) => vec![sid],
            None => self.data.keys().cloned().collect(),
        };

        for s in subjects {
            if let Some(subj_map) = self.data.get(&s) {
                let predicates = match predicate_id {
                    Some(pid) => vec![pid],
                    None => subj_map.keys().cloned().collect(),
                };

                for p in predicates {
                    if let Some(objects) = subj_map.get(&p) {
                        match object_id {
                            Some(oid) => {
                                if objects.contains(&oid) {
                                    results.push(Triple { subject: s, predicate: p, object: oid });
                                }
                            }
                            None => {
                                for &o in objects {
                                    results.push(Triple { subject: s, predicate: p, object: o });
                                }
                            }
                        }
                    }
                }
            }
        }
        results
    }

    pub fn dump_triples(&self) -> Vec<Triple> {
        let mut all = Vec::new();
        for (&subj, pred_map) in &self.data {
            for (&pred, obj_set) in pred_map {
                for &obj in obj_set {
                    all.push(Triple {
                        subject: subj,
                        predicate: pred,
                        object: obj,
                    });
                }
            }
        }
        all
    }
}

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
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // List of dynamic rules

    pub abox_index: TrieIndex,
    pub tbox_index: TrieIndex,
}

impl KnowledgeGraph {
    pub fn new() -> Self {
        Self {
            dictionary: Dictionary::new(),
            rules: Vec::new(),
            abox_index: TrieIndex::new(),
            tbox_index: TrieIndex::new(),
        }
    }

    /// Add a TBox triple (schema-level information)
    pub fn add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let triple = Triple {
            subject: self.dictionary.encode(subject),
            predicate: self.dictionary.encode(predicate),
            object: self.dictionary.encode(object),
        };
        self.tbox_index.insert(&triple);
    }

    /// Add an ABox triple (instance-level information)
    pub fn add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let triple = Triple {
            subject: self.dictionary.encode(subject),
            predicate: self.dictionary.encode(predicate),
            object: self.dictionary.encode(object),
        };
        self.abox_index.insert(&triple);
    }

    /// Query the ABox for instance-level assertions (using TrieIndex now).
    pub fn query_abox(
        &mut self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> Vec<Triple> {
        let subject_id = subject.map(|s| self.dictionary.encode(s));
        let predicate_id = predicate.map(|p| self.dictionary.encode(p));
        let object_id = object.map(|o| self.dictionary.encode(o));

        // Use the trie index instead of scanning the entire BTreeSet.
        self.abox_index.query(subject_id, predicate_id, object_id)
    }

    /// Query the TBox for schema-level assertions (using TrieIndex now).
    pub fn query_tbox(
        &mut self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> Vec<Triple> {
        let subject_id = subject.map(|s| self.dictionary.encode(s));
        let predicate_id = predicate.map(|p| self.dictionary.encode(p));
        let object_id = object.map(|o| self.dictionary.encode(o));

        self.tbox_index.query(subject_id, predicate_id, object_id)
    }

    /// Add a dynamic rule to the graph
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    pub fn infer_new_facts(&mut self) -> Vec<Triple> {
        let mut inferred_facts = Vec::new();
        // Dump all current ABox facts
        let abox_facts = self.abox_index.dump_triples();
        let rules = self.rules.clone();

        for rule in &rules {
            match rule.premise.len() {
                1 => {
                    // Single-premise rule
                    for triple in &abox_facts {
                        let mut variable_bindings = HashMap::new();
                        if matches_rule_pattern(&rule.premise[0], triple, &mut variable_bindings) {
                            let inferred = construct_triple(&rule.conclusion, &variable_bindings);
                            // Insert only if new
                            if self.abox_index.insert(&inferred) {
                                inferred_facts.push(inferred.clone());
                                println!("Inferred new fact: {:?}", inferred);
                            }
                        }
                    }
                }
                2 => {
                    // Two-premise rule
                    for triple1 in &abox_facts {
                        for triple2 in &abox_facts {
                            // For chaining rules that require triple1.object == triple2.subject
                            if triple1.object != triple2.subject {
                                continue;
                            }
                            let mut variable_bindings = HashMap::new();
                            if matches_rule_pattern(&rule.premise[0], triple1, &mut variable_bindings)
                                && matches_rule_pattern(&rule.premise[1], triple2, &mut variable_bindings)
                            {
                                let inferred = construct_triple(&rule.conclusion, &variable_bindings);
                                if self.abox_index.insert(&inferred) {
                                    inferred_facts.push(inferred.clone());
                                    println!("Inferred new fact: {:?}", inferred);
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Extend to more premises if needed
                }
            }
        }

        inferred_facts
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

        let substituted = substitute(query, bindings);

        let mut results = Vec::new();
        // 1. Try to match with ABox + TBox facts
        let all_facts: Vec<Triple> = self.abox_index.dump_triples()
            .into_iter()
            .chain(self.tbox_index.dump_triples().into_iter())
            .collect();

        for fact in &all_facts {
            let fact_pattern = triple_to_pattern(fact);
            if let Some(new_bindings) = unify_patterns(&substituted, &fact_pattern, bindings) {
                results.push(new_bindings);
            }
        }

        // 2. Try to match with rules
        for rule in &self.rules {
            let renamed_rule = rename_rule_variables(rule, variable_counter);

            if let Some(rb) = unify_patterns(&renamed_rule.conclusion, &substituted, bindings) {
                // We have a match => we need all premises to succeed
                let mut premise_results = vec![rb.clone()];
                for prem in &renamed_rule.premise {
                    let mut new_premise_results = Vec::new();
                    for b in &premise_results {
                        let sub_res = self.backward_chaining_helper(prem, b, depth + 1, variable_counter);
                        new_premise_results.extend(sub_res);
                    }
                    premise_results = new_premise_results;
                }
                results.extend(premise_results);
            }
        }

        results
    }

    /// A convenience method to run the Datalog engine on the current KG
    /// and then query it.
    pub fn datalog_query_kg(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>> {
        // Construct a new Datalog engine from this KG.
        let mut engine = DatalogEngine::new_from_kg(self);

        // Run bottom-up inference until fixpoint.
        engine.run_datalog();

        // Query the saturated facts.
        engine.datalog_query(query)
    }

    pub fn datalog_inferred_query(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>> {
        let mut engine = DatalogEngine::new_from_kg(self);
        let original = engine.facts.dump_triples().into_iter().collect::<BTreeSet<_>>();
        engine.run_datalog();
        let all_after = engine.facts.dump_triples().into_iter().collect::<BTreeSet<_>>();

        // newly inferred = all_after - original
        let inferred = all_after.difference(&original).cloned().collect::<Vec<_>>();
        
        // unify with the query
        let mut results = Vec::new();
        for fact in inferred {
            let mut vb = HashMap::new();
            if matches_rule_pattern(query, &fact, &mut vb) {
                results.push(vb);
            }
        }
        results
    }
    
    pub fn infer_new_facts_optimized(&mut self) -> Vec<Triple> {
        // We'll store all "old" facts in old_facts. Initially, this is just our current ABox.
        let mut old_triples: BTreeSet<Triple> =
            self.abox_index.dump_triples().into_iter().collect();

        let mut delta: BTreeSet<Triple> = old_triples.clone();
        let mut new_inferred = Vec::new();

        loop {
            let mut next_delta = BTreeSet::new();

            for rule in &self.rules {
                match rule.premise.len() {
                    1 => {
                        // Single-premise rule: only match the premise with the "delta" facts
                        let pattern = &rule.premise[0];
                        for fact in &delta {
                            let mut var_bindings = HashMap::new();
                            if matches_rule_pattern(pattern, fact, &mut var_bindings) {
                                let inferred = construct_triple(&rule.conclusion, &var_bindings);
                                if !old_triples.contains(&inferred) {
                                    next_delta.insert(inferred);
                                }
                            }
                        }
                    }
                    2 => {
                        // Two-premise rule: match premise1 with delta, premise2 with old
                        let p1 = &rule.premise[0];
                        let p2 = &rule.premise[1];

                        // (A) fact1 from delta, fact2 from old
                        for fact1 in &delta {
                            for fact2 in &old_triples {
                                if fact1.object != fact2.subject {
                                    continue;
                                }
                                let mut var_bindings = HashMap::new();
                                if matches_rule_pattern(p1, fact1, &mut var_bindings)
                                    && matches_rule_pattern(p2, fact2, &mut var_bindings)
                                {
                                    let inferred = construct_triple(&rule.conclusion, &var_bindings);
                                    if !old_triples.contains(&inferred) {
                                        next_delta.insert(inferred);
                                    }
                                }
                            }
                        }
                        // (B) fact1 from old, fact2 from delta
                        for fact1 in &old_triples {
                            for fact2 in &delta {
                                if fact1.object != fact2.subject {
                                    continue;
                                }
                                let mut var_bindings = HashMap::new();
                                if matches_rule_pattern(p1, fact1, &mut var_bindings)
                                    && matches_rule_pattern(p2, fact2, &mut var_bindings)
                                {
                                    let inferred = construct_triple(&rule.conclusion, &var_bindings);
                                    if !old_triples.contains(&inferred) {
                                        next_delta.insert(inferred);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }

            if next_delta.is_empty() {
                // fixpoint
                break;
            }

            // Insert new facts into the trie, update old_triples
            for t in &next_delta {
                if self.abox_index.insert(t) {
                    // Inserted successfully
                    new_inferred.push(t.clone());
                }
            }
            old_triples.extend(next_delta);
            delta = old_triples.clone(); // or just next_delta if you prefer a standard semi-naive approach
        }

        new_inferred
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

#[derive(Debug)]
pub struct DatalogEngine {
    /// Facts: each fact is a ground triple (i.e., no variables).
    /// We’ll store them in a set to avoid duplicates.
    pub facts: TrieIndex,

    /// Datalog rules are the same as your dynamic rules: 
    /// premises are patterns (variables allowed), 
    /// conclusion is a single pattern (variables allowed).
    pub rules: Vec<Rule>,
}

impl DatalogEngine {
    /// Construct a new DatalogEngine from an existing KnowledgeGraph
    pub fn new_from_kg(kg: &KnowledgeGraph) -> Self {
        // We treat ABox as facts. TBox can also be added as "facts" if it is relevant
        // to your logic, or left out if it is purely schema information.
        // Here we just copy the ABox facts as our initial Datalog facts.
        Self {
            facts: kg.abox_index.clone(),
            rules: kg.rules.clone(),
        }
    }

    /// Perform naive bottom-up evaluation until no new facts are derived.
    ///
    /// For each rule in `self.rules`, try to match it against known `self.facts`.
    /// Derive new facts.
    /// Insert new facts into `self.facts`.
    /// Repeat until a fixpoint is reached (no new facts).
    pub fn run_datalog(&mut self) {
        let mut changed = true;
        while changed {
            changed = false;
            // Snapshot current facts
            let current_facts = self.facts.dump_triples();

            // For each rule, see if we can derive new facts
            for rule in &self.rules {
                match rule.premise.len() {
                    1 => {
                        let p = &rule.premise[0];
                        for fact in &current_facts {
                            let mut vmap = HashMap::new();
                            if matches_rule_pattern(p, fact, &mut vmap) {
                                let inferred = construct_triple(&rule.conclusion, &vmap);
                                // Insert if new
                                if self.facts.insert(&inferred) {
                                    changed = true;
                                }
                            }
                        }
                    }
                    2 => {
                        let p1 = &rule.premise[0];
                        let p2 = &rule.premise[1];
                        // Nested loop over all facts
                        for f1 in &current_facts {
                            for f2 in &current_facts {
                                // e.g. transitivity requirement
                                if f1.object != f2.subject {
                                    continue;
                                }
                                let mut vmap = HashMap::new();
                                if matches_rule_pattern(p1, f1, &mut vmap)
                                    && matches_rule_pattern(p2, f2, &mut vmap)
                                {
                                    let inferred = construct_triple(&rule.conclusion, &vmap);
                                    if self.facts.insert(&inferred) {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Query the derived facts in a Datalog style (i.e., after `run_datalog`).
    /// Returns variable bindings for matches.
    pub fn datalog_query(&self, pattern: &TriplePattern) -> Vec<HashMap<String, u32>> {
        let mut results = Vec::new();
        let all_facts = self.facts.dump_triples();
        for fact in &all_facts {
            let mut vmap = HashMap::new();
            if matches_rule_pattern(pattern, fact, &mut vmap) {
                results.push(vmap);
            }
        }
        results
    }
}

/// Construct a new Triple from a conclusion pattern and bound variables.
/// If the conclusion pattern has a variable, substitute its bound value;
/// if it's a constant, just use the constant.
fn construct_triple(conclusion: &TriplePattern, vars: &HashMap<String, u32>) -> Triple {
    let subject = match &conclusion.0 {
        Term::Variable(v) => *vars.get(v).unwrap(),
        Term::Constant(c) => *c,
    };
    let predicate = match &conclusion.1 {
        Term::Variable(v) => *vars.get(v).unwrap(),
        Term::Constant(c) => *c,
    };
    let object = match &conclusion.2 {
        Term::Variable(v) => *vars.get(v).unwrap(),
        Term::Constant(c) => *c,
    };

    Triple {
        subject,
        predicate,
        object,
    }
}

fn matches_rule_pattern(
    pattern: &TriplePattern,
    fact: &Triple,
    variable_bindings: &mut HashMap<String, u32>,
) -> bool {
    // Subject
    let s_ok = match &pattern.0 {
        Term::Variable(v) => {
            if let Some(&bound) = variable_bindings.get(v) {
                bound == fact.subject
            } else {
                variable_bindings.insert(v.clone(), fact.subject);
                true
            }
        }
        Term::Constant(c) => *c == fact.subject,
    };
    if !s_ok {
        return false;
    }

    // Predicate
    let p_ok = match &pattern.1 {
        Term::Variable(v) => {
            if let Some(&bound) = variable_bindings.get(v) {
                bound == fact.predicate
            } else {
                variable_bindings.insert(v.clone(), fact.predicate);
                true
            }
        }
        Term::Constant(c) => *c == fact.predicate,
    };
    if !p_ok {
        return false;
    }

    // Object
    let o_ok = match &pattern.2 {
        Term::Variable(v) => {
            if let Some(&bound) = variable_bindings.get(v) {
                bound == fact.object
            } else {
                variable_bindings.insert(v.clone(), fact.object);
                true
            }
        }
        Term::Constant(c) => *c == fact.object,
    };

    s_ok && p_ok && o_ok
}