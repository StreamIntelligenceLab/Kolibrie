/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;
use std::rc::Rc;

use shared::rule::Rule;
use shared::terms::Term;
use shared::triple::Triple;

pub struct RuleIndexer {
    pub rules: Vec<Rc<Rule>>,
    spo:Vec<Rc<Rule>>,
    s:HashMap<u32,  Vec<Rc<Rule>>>,
    p:HashMap<u32, Vec<Rc<Rule>>>,
    o:HashMap<u32, Vec<Rc<Rule>>>,
    sp:HashMap<u32,  HashMap<u32,Vec<Rc<Rule>>>>,
    po:HashMap<u32,  HashMap<u32,Vec<Rc<Rule>>>>,
    so:HashMap<u32,  HashMap<u32,Vec<Rc<Rule>>>>,
    spo_all:HashMap<u32,  HashMap<u32, HashMap<u32,Vec<Rc<Rule>>>>>,
}



impl RuleIndexer {
    pub fn len(&self) -> usize {
        self.spo.len() + self.s.len() + self.o.len() + self.p.len() +
            self.sp.len() + self.po.len() + self.so.len()
    }
    pub fn new() -> RuleIndexer {
        RuleIndexer {
            rules: Vec::new(),
            s:HashMap::new(),
            p:HashMap::new(),
            o:HashMap::new(),
            so:HashMap::new(),
            po:HashMap::new(),
            sp:HashMap::new(),
            spo:Vec::new(),
            spo_all: HashMap::new()}
    }
    fn is_term(term: &Term) ->bool{
        matches!(term, Term::Constant(_))
    }

    fn is_var(term: &Term) ->bool{
        matches!(term, Term::Variable(_))
    }
    fn add_rule_to_map(map: &mut HashMap<u32, Vec<Rc<Rule>>>, key_term: &Term, rule: &Rc<Rule>) {
        if let Term::Constant(key) = key_term {
            let entry = map.entry(key.clone()).or_insert_with(Vec::new);
            if !entry.contains(rule) {
                entry.push(rule.clone());
            }
        }
    }
    fn add_rule_to_nested_map(map: &mut HashMap<u32, HashMap<u32, Vec<Rc<Rule>>>>, key1: &Term, key2: &Term, rule: &Rc<Rule>) {
        if let (Term::Constant(k1), Term::Constant(k2)) = (key1, key2) {
            let inner = map.entry(k1.clone()).or_insert_with(HashMap::new);
            let rules = inner.entry(k2.clone()).or_insert_with(Vec::new);

            if !rules.contains(rule) {
                rules.push(rule.clone());
            }
        }
    }
    fn add_rc(&mut self, rule: Rc<Rule>){
        self.rules.push(rule.clone());
        for (s,p,o)  in rule.premise.iter(){
            // s match
            if Self::is_term(s) && Self::is_var(p) && Self::is_var(o) {
                Self::add_rule_to_map(&mut self.s, s, &rule);
            }

            // p match
            if Self::is_var(s) && Self::is_term(p) && Self::is_var(o) {
                Self::add_rule_to_map(&mut self.p, p, &rule);
            }
            // o match
            if Self::is_var(s) && Self::is_var(p) && Self::is_term(o) {
                Self::add_rule_to_map(&mut self.o, o, &rule);
            }

            // sp: subject and predicate are constants, object is variable
            if Self::is_term(s) && Self::is_term(p) && Self::is_var(o) {
                Self::add_rule_to_nested_map(&mut self.sp, s, p, &rule);
            }

            // so: subject and object are constants, predicate is variable
            if Self::is_term(s) && Self::is_var(p) && Self::is_term(o) {
                Self::add_rule_to_nested_map(&mut self.so, s, o, &rule);
            }

            // po: predicate and object are constants, subject is variable
            if Self::is_var(s) && Self::is_term(p) && Self::is_term(o) {
                Self::add_rule_to_nested_map(&mut self.po, p, o, &rule);
            }
            //spo
            if Self::is_term(s) && Self::is_term(p) && Self::is_term(o){
                if let (Term::Constant(s_cons), Term::Constant(p_cons), Term::Constant(o_cons)) = (s, p ,o){
                    let rules = self
                        .spo_all
                        .entry(s_cons.clone())
                        .or_insert_with(HashMap::new)
                        .entry(p_cons.clone())
                        .or_insert_with(HashMap::new)
                        .entry(o_cons.clone())
                        .or_insert_with(Vec::new);

                    if !rules.contains(&rule) {
                        rules.push(rule.clone());
                    }
                }

            }
            //?s?p?o
            if Self::is_var(s) && Self::is_var(p) && Self::is_var(o){
                if !self.spo.contains(&rule) {self.spo.push(rule.clone())};

            }

        }
    }
    pub fn add(&mut self, rule: Rule){
        let clone_rule = Rc::new(rule);
        self.add_rc(clone_rule);
    }

    pub fn add_ref(&mut self, rule:  & Rule ){
        let clone_rule = Rc::new(rule.clone());
        self.add_rc(clone_rule);
    }

    pub fn find_match(&self, triple: &Triple) ->Vec<&Rule>{
        let mut matched_triples: Vec<&Rule> = Vec::new();
        //check s
        if let Some(rule) = self.s.get(&triple.subject){
            rule.iter().for_each(|r|matched_triples.push(r));
        }
        //check p
        if let Some(rule) = self.p.get(&triple.predicate){
            rule.iter().for_each(|r|matched_triples.push(r));
        }
        //check o
        if let Some(rule) = self.o.get(&triple.object){
            rule.iter().for_each(|r|matched_triples.push(r));
        }
        //check so
        if let Some(s_rules) = self.so.get(&triple.subject){
            if let Some(rules) = s_rules.get(&triple.object) {
                rules.iter().for_each(|r| matched_triples.push(r));
            }
        }
        //check po
        if let Some(p_rules) = self.po.get(&triple.predicate){
            if let Some(rules) = p_rules.get(&triple.object) {
                rules.iter().for_each(|r| matched_triples.push(r));
            }
        }
        //check sp
        if let Some(s_rules) = self.sp.get(&triple.subject){
            if let Some(rules) = s_rules.get(&triple.predicate) {
                rules.iter().for_each(|r| matched_triples.push(r));
            }
        }
        //check spo
        if let Some(s_rules) = self.spo_all.get(&triple.subject){
            if let Some(p_rules) = s_rules.get(&triple.predicate) {
                if let Some(rules) = p_rules.get(&triple.object) {
                    rules.iter().for_each(|r| matched_triples.push(r));
                }
            }
        }
        self.spo.iter().for_each(|r| matched_triples.push(r));

        matched_triples
    }
}
#[cfg(test)]
mod tests {
    use crate::knowledge_graph::KnowledgeGraph;

    use super::*;

    #[test]
    fn test_adding_s_rule() {
        let mut kg = KnowledgeGraph::new();
        let rule1 = Rule {
            premise: vec![
                (Term::Constant(kg.dictionary.encode("parent")),
                 Term::Variable("X".to_string()),
                 Term::Variable("Y".to_string()),
                )
            ],
            conclusion: vec![(Term::Variable("X".to_string()),
                              Term::Constant(kg.dictionary.encode("ancestor")),
                              Term::Variable("Y".to_string())),],
            filters: vec![],
        };

        let mut rule_index = RuleIndexer::new();
        rule_index.add(rule1);
        assert_eq!(rule_index.s.len(),1);
    }
    #[test]
    fn test_adding_p_rule() {
        let mut kg = KnowledgeGraph::new();
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

        let mut rule_index = RuleIndexer::new();
        rule_index.add(rule1);
        assert_eq!(rule_index.p.len(),1);
    }
    #[test]
    fn test_adding_sp_rule() {
        let mut kg = KnowledgeGraph::new();
        let rule1 = Rule {
            premise: vec![
                (Term::Constant(kg.dictionary.encode("a")),
                 Term::Constant(kg.dictionary.encode("b")),
                 Term::Variable("Y".to_string())),
            ],
            conclusion: vec![(Term::Variable("X".to_string()),
                              Term::Constant(kg.dictionary.encode("ancestor")),
                              Term::Variable("Y".to_string())),],
            filters: vec![],
        };

        let mut rule_index = RuleIndexer::new();
        rule_index.add(rule1);
        assert_eq!(rule_index.sp.len(),1);
    }
    #[test]
    fn test_adding_so_rule() {
        let mut kg = KnowledgeGraph::new();
        let rule1 = Rule {
            premise: vec![
                (Term::Constant(kg.dictionary.encode("a")),
                 Term::Variable("Y".to_string()),
                 Term::Constant(kg.dictionary.encode("b")),
                 ),
            ],
            conclusion: vec![(Term::Variable("X".to_string()),
                              Term::Constant(kg.dictionary.encode("ancestor")),
                              Term::Variable("Y".to_string())),],
            filters: vec![],
        };

        let mut rule_index = RuleIndexer::new();
        rule_index.add(rule1);
        assert_eq!(rule_index.so.len(),1);
    }
    #[test]
    fn test_adding_po_rule() {
        let mut kg = KnowledgeGraph::new();
        let rule1 = Rule {
            premise: vec![
                (Term::Variable("Y".to_string()),
                 Term::Constant(kg.dictionary.encode("a")),
                 Term::Constant(kg.dictionary.encode("b")),
                ),
            ],
            conclusion: vec![(Term::Variable("X".to_string()),
                              Term::Constant(kg.dictionary.encode("ancestor")),
                              Term::Variable("Y".to_string())),],
            filters: vec![],
        };

        let mut rule_index = RuleIndexer::new();
        rule_index.add(rule1);
        assert_eq!(rule_index.po.len(),1);
    }
    #[test]
    fn test_adding_spo_rule() {
        let mut kg = KnowledgeGraph::new();
        let rule1 = Rule {
            premise: vec![
                (Term::Constant(kg.dictionary.encode("a")),
                 Term::Constant(kg.dictionary.encode("b")),
                 Term::Constant(kg.dictionary.encode("c"))
                ),
            ],
            conclusion: vec![(Term::Variable("X".to_string()),
                              Term::Constant(kg.dictionary.encode("ancestor")),
                              Term::Variable("Y".to_string())),],
            filters: vec![],
        };

        let mut rule_index = RuleIndexer::new();
        rule_index.add(rule1);
        assert_eq!(rule_index.spo_all.len(),1);
    }
    #[test]
    fn test_adding_all_var_rule() {
        let mut kg = KnowledgeGraph::new();
        let rule1 = Rule {
            premise: vec![
                (Term::Variable("X".to_string()),
                 Term::Variable("Y".to_string()),
                 Term::Variable("Z".to_string())
                ),
            ],
            conclusion: vec![(Term::Variable("X".to_string()),
                              Term::Constant(kg.dictionary.encode("ancestor")),
                              Term::Variable("Y".to_string())),],
            filters: vec![],
        };

        let mut rule_index = RuleIndexer::new();
        rule_index.add(rule1);
        assert_eq!(rule_index.spo.len(),1);
    }


    #[test]
    fn test_retrieving_p_rule() {
        let mut kg = KnowledgeGraph::new();
        let rule1 = Rule {
            premise: vec![
                (Term::Variable("X".to_string()),
                 Term::Constant(kg.dictionary.encode("b")),
                 Term::Variable("Y".to_string())),
            ],
            conclusion: vec![(Term::Variable("X".to_string()),
                              Term::Constant(kg.dictionary.encode("ancestor")),
                              Term::Variable("Y".to_string())),],
            filters: vec![],
        };

        let mut rule_index = RuleIndexer::new();
        rule_index.add(rule1);
        let triple = Triple{subject: kg.dictionary.encode("a"), predicate: kg.dictionary.encode("b"),object: kg.dictionary.encode("c")};
        let rules = rule_index.find_match(&triple);
        assert_eq!(rules.len(),1);
    }
}
