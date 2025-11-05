#[cfg(not(test))]
use log::{info, warn,trace, debug}; // Use log crate when building application
use std::fmt::Write;

#[cfg(test)]
use std::{println as info, println as warn, println as trace, println as debug};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use shared::index_manager::UnifiedIndex;
use shared::rule::Rule;
use shared::terms::TriplePattern;
use crate::ruleindex::RuleIndexer;
use shared::triple::Triple;


pub struct Reasoner;

impl Reasoner{
    pub fn materialize(&mut self, triple_index: &mut UnifiedIndex, rules_index: &RuleIndexer) -> Vec<Triple>{
        let mut inferred = Vec::new();
        let mut counter = 0;
        while counter < triple_index.len() {
            let process_quad = triple_index.get(counter).unwrap();
            //let matching_rules = self.find_matching_rules(process_quad);
            let matching_rules = rules_index.find_match(process_quad);
            let matching_rules : Vec<Rule> = matching_rules.clone().into_iter().flat_map(|r|Self::substitute_rule(process_quad,r)).collect();
            debug!("Matching Rules: {:?}",TripleStore::decode_rules(matching_rules.as_slice()));

            let new_triples = Self::infer_rule_heads(triple_index, Some(counter+1), matching_rules);
            for triple in new_triples{
                if !triple_index.contains(&triple) {
                    debug!("Inferred: {:?}",TripleStore::decode_triple(&triple));
                    inferred.push(triple.clone());
                    triple_index.add(triple);
                }
            }
            counter+=1;
        }

        inferred
    }
//
//     pub fn infer_rule_heads(triple_index: &TripleIndex, counter: Option<usize>, matching_rules: Vec<Rule>) -> Vec<Triple> {
//         let mut new_triples = Vec::new();
//         for rule in matching_rules {
//             if let Some(temp_bindings) = SimpleQueryEngine::query(triple_index, &rule.body, counter) {
//                 let new_heads = Reasoner::substitute_head_with_bindings(&rule.head, &temp_bindings);
//
//                 for new_head in new_heads {
//                     new_triples.push(new_head);
//                 }
//             }
//         }
//         new_triples
//     }
//
//     fn substitute_head_with_bindings(head: &Triple, binding: &Binding) ->Vec<Triple>{
//         if binding.len() == 0{
//             return vec![head.clone()];
//         }
//         let mut new_heads = Vec::new();
//         let mut s: &usize;
//         let mut p: &usize;
//         let mut o: &usize;
//         for result_counter in 0..binding.len(){
//             match &head.s{
//                 VarOrTerm::Var(s_var)=> s = binding.get(&s_var.name).unwrap().get(result_counter).unwrap(),
//                 VarOrTerm::Term(s_term)=> s = &s_term.iri
//             }
//             match &head.p{
//                 VarOrTerm::Var(p_var)=> p = binding.get(&p_var.name).unwrap().get(result_counter).unwrap(),
//                 VarOrTerm::Term(p_term)=> p = &p_term.iri
//             }
//             match &head.o{
//                 VarOrTerm::Var(o_var)=> o = binding.get(&o_var.name).unwrap().get(result_counter).unwrap(),
//                 VarOrTerm::Term(o_term)=> o = &o_term.iri
//             }
//             new_heads.push(Triple{s:VarOrTerm::Term(TermImpl{iri:s.clone()}),p:VarOrTerm::Term(TermImpl{iri:p.clone()}),o:VarOrTerm::Term(TermImpl{iri:o.clone()}), g: None})
//         }
//
//         new_heads
//     }
//     fn subsitute_binding(var_name:&usize,binding:&Binding,binding_counter:&usize) -> VarOrTerm{
//         if let Some(s) = binding.get(var_name){
//             let iri = s.get(*binding_counter).unwrap().clone();
//             VarOrTerm::new_encoded_term(iri)
//         }else{
//             VarOrTerm::new_encoded_var(var_name.clone())
//         }
//
//     }
//     pub fn substitute_triple_with_bindings(head: &Triple, binding: &Binding) ->Vec<Triple>{
//         let mut new_heads = Vec::new();
//         let mut s: VarOrTerm;
//         let mut p: VarOrTerm;
//         let mut o: VarOrTerm;
//         for result_counter in 0..binding.len(){
//             match &head.s{
//                 VarOrTerm::Var(s_var)=> s = Self::subsitute_binding(&s_var.name,binding,&result_counter),
//                 VarOrTerm::Term(s_term)=> s = VarOrTerm::new_encoded_term(s_term.iri.clone())
//             }
//             match &head.p{
//                 VarOrTerm::Var(p_var)=> p = Self::subsitute_binding(&p_var.name,binding,&result_counter),
//                 VarOrTerm::Term(p_term)=> p = VarOrTerm::new_encoded_term(p_term.iri.clone())
//             }
//             match &head.o{
//                 VarOrTerm::Var(o_var)=> o = Self::subsitute_binding(&o_var.name,binding,&result_counter),
//                 VarOrTerm::Term(o_term)=> o = VarOrTerm::new_encoded_term(o_term.iri.clone())
//             }
//             new_heads.push(Triple{s,p,o,g:None})
//         }
//
//         new_heads
//     }

pub fn query(query_triple:&TriplePattern, match_triple:&Triple) -> Option<Binding>{
    let mut bindings = Binding::new();
    let Triple{subject,predicate,object} = match_triple;
    match &query_triple.s{
        VarOrTerm::Var(s_var)=> bindings.add(&s_var.name,s.as_term().iri),
        VarOrTerm::Term(s_term)=>if let (TermImpl{iri}, TermImpl{iri:iri2})= (s_term,s.as_term()) {
            if !iri.eq(iri2){return None;}
        }
    }
    match &query_triple.p{
        VarOrTerm::Var(p_var)=> bindings.add(&p_var.name,p.as_term().iri),
        VarOrTerm::Term(p_term)=>if let (TermImpl{iri}, TermImpl{iri:iri2})= (p_term,p.as_term()) {
            if !iri.eq(iri2){return None;}
        }
    }
    match &query_triple.o{
        VarOrTerm::Var(o_var)=> bindings.add(&o_var.name,o.as_term().iri),
        VarOrTerm::Term(o_term)=>if let (TermImpl{iri}, TermImpl{iri:iri2})= (o_term,o.as_term()) {
            if !iri.eq(iri2){return None;}
        }
    }

    Some(bindings)
}
    fn evaluate_triple_pattern_on_single_triple(pattern: &TriplePattern, subject_id: Option<u32>, predicate_id: Option<u32>, object_id: Option<u32>, triple: &Triple) -> Result<Option<BTreeMap<String, u32>>, Option<BTreeMap<String, u32>>> {
        // Fast ID-based filtering
        if let Some(s_id) = subject_id {
            if triple.subject != s_id { return Err(None); }
        }
        if let Some(p_id) = predicate_id {
            if triple.predicate != p_id { return Err(None); }
        }
        if let Some(o_id) = object_id {
            if triple.object != o_id { return Err(None); }
        }

        // Build result only if match
        let mut result = BTreeMap::new();

        if let Some(var) = &pattern.subject {
            if var.starts_with('?') {
                result.insert(var.clone(), triple.subject);
            }
        }
        if let Some(var) = &pattern.predicate {
            if var.starts_with('?') {
                result.insert(var.clone(), triple.predicate);
            }
        }
        if let Some(var) = &pattern.object {
            if var.starts_with('?') {
                result.insert(var.clone(), triple.object);
            }
        }

        Ok(Some(result))
    }
    pub fn substitute_rule(matching_triple: &Triple, matching_rule: &Rule) -> Vec<Rule> {
        let mut results = Vec::new();
        for body_triple in matching_rule.premise.iter() {
            if let Some(bindings) = query(body_triple, matching_triple) {
                if bindings.len() > 1 {
                    panic!("Multiple bindings found!");
                }else if bindings.len() == 0{
                    return vec![matching_rule.clone()];
                }
                let new_body = Self::substitute_rule_body_with_binding(matching_rule, &bindings);
                let new_head = Reasoner::substitute_triple_with_bindings(&matching_rule.head, &bindings).get(0).unwrap().clone();
                results.push(Rule { body: new_body, head: new_head });
            }
        }
        results
    }
//
//     pub fn substitute_rule_body_with_binding(matching_rule: &Rule, bindings: &Binding) -> Vec<Triple> {
//         let mut new_body = Vec::new();
//         for body_triple_subs in matching_rule.body.iter() {
//             let substituted = Reasoner::substitute_triple_with_bindings(body_triple_subs, &bindings);
//             new_body.push(substituted.get(0).unwrap().clone());
//         }
//         new_body
//     }
}
// pub struct CSpriteReasoner;
//
// impl CSpriteReasoner{
//     pub fn materialize(&mut self, new_data: &Vec<(i32, Rc<Triple>)>, triple_index: &mut TripleIndex, rules_index: &RuleIndex, window: &mut ImarsWindow<Triple>) -> Vec<(i32, Rc<Triple>)>{
//         let mut inferred = Vec::new();
//         let mut counter = 0;
//         let mut pending_changes = Vec::new();
//         new_data.into_iter().for_each(|i|pending_changes.push(i.clone()));
//         while counter < pending_changes.len() {
//             let (_ts,process_quad) = pending_changes.get(counter).unwrap();
//             //trace!("Processing: {:?}",decode_triple(process_quad));
//             //let matching_rules = self.find_matching_rules(process_quad);
//             let matching_rules = rules_index.find_match(process_quad);
//             trace!("Found Rules: {:?}",matching_rules);
//             let mut new_triples = Vec::new();
//
//             for rule in matching_rules{
//                 if let Some(mut temp_bindings) = SimpleQueryEngine::query(triple_index, &rule.body,None) {
//                     let new_heads = Reasoner::substitute_head_with_bindings(&rule.head, &temp_bindings);
//                     let reconstructed = CSpriteReasoner::reconstruct_triples_from_bindings(&mut temp_bindings, rule);
//                     for i in 0..new_heads.len(){
//                         let new_head = new_heads.get(i).unwrap().clone();
//                         //println!("Inferred head: {:?}", Self::decode_triple(&new_head,encoder));
//                         //compute time stamp
//                         let triples = reconstructed.get(i).unwrap();
//                         //println!("Triples: {:?}", triples);
//                         // let min_ts: Vec<Option<i32>> =triples.iter().map(|t|window.get_time_stamp(Rc::new(t.clone()))).collect();
//                         // let min_ts =triples.iter().map(|t|window.get_time_stamp(Rc::new(t.clone()))).filter(|t|t.is_some()).min().unwrap().unwrap();//todo update to reference only
//                         let items : Vec<(i32,&Triple)>= triples.iter().map(|t|(window.get_time_stamp(Rc::new(t.clone())),t)).filter(|(ts,_t)|ts.is_some()).map(|(ts,t)|(ts.unwrap(),t)).collect();
//                         let (min_ts, min_triple) = items.iter().fold(items[0], |acc, &item| {
//                                 if acc.0 <= item.0 { acc } else { item }
//                             });
//                         println!(" min {:?} {:?}", min_ts, min_triple);
//                        new_triples.push((min_ts.clone(),new_head.clone(),min_triple.clone()));
//                     }
//
//                 }
//             }
//             for (ts, new_triple,min_triple) in new_triples{
//                 if !triple_index.contains(&new_triple) {
//                     //trace!("Inferred: {:?}",self.decode_triple(&triple));
//                     let triple_ref = Rc::new(new_triple);
//                     inferred.push((ts,triple_ref.clone()));
//                     //add to maintanance program
//                     println!("Adding To window {:?},{:?}", ts, triple_ref);
//                    // window.add_without_update(triple_ref.clone(),ts);
//                    window.add_after(triple_ref.clone(), Rc::new(min_triple.clone()),ts);
//                     pending_changes.push((ts,triple_ref.clone()));
//
//                     triple_index.add_ref(triple_ref);
//
//
//                 }
//             }
//             counter+=1;
//         }
//
//         inferred
//     }
//     fn decode_triple(triple: &Triple) -> String {
//         let mut res = String::new();
//
//             let decoded_s = Encoder::decode(&triple.s.to_encoded()).unwrap();
//             let decoded_p = Encoder::decode(&triple.p.to_encoded()).unwrap();
//             let decoded_o = Encoder::decode(&triple.o.to_encoded()).unwrap();
//
//             write!(&mut res, "{} {} {}.\n", decoded_s, decoded_p, decoded_o).unwrap();
//
//         res
//     }
//     fn reconstruct_triples_from_bindings(result_bindings: &mut Binding, rule: &Rule) -> Vec<Vec<Triple>>{
//
//         let mut counter = 0;
//         let mut all_triples = Vec::new();
//         while counter < result_bindings.len() {
//             let mut triples = Vec::new();
//             for triple in rule.body.iter() {
//                 let mut s;
//                 let mut p;
//                 let mut o;
//                 if triple.s.is_var() {
//                     s = VarOrTerm::new_encoded_term(*result_bindings.get(&triple.s.as_var().name).unwrap().get(counter).unwrap());
//                 } else {
//                     s = VarOrTerm::new_encoded_term(triple.s.as_term().iri);
//                 }
//                 if triple.p.is_var() {
//                     p = VarOrTerm::new_encoded_term(*result_bindings.get(&triple.p.as_var().name).unwrap().get(counter).unwrap());
//                 } else {
//                     p = VarOrTerm::new_encoded_term(triple.p.as_term().iri);
//                 }
//                 if triple.o.is_var() {
//                     o = VarOrTerm::new_encoded_term(*result_bindings.get(&triple.o.as_var().name).unwrap().get(counter).unwrap());
//                 } else {
//                     o = VarOrTerm::new_encoded_term(triple.o.as_term().iri);
//                 }
//                 triples.push(Triple { s, p, o, g: None });
//             }
//             counter+=1;
//             all_triples.push(triples);
//         }
//         all_triples
//     }
// }
// #[test]
// #[ignore]
// fn test_reconstruct_from_bindings(){
//     let data="{?a in ?c}=>{?a in ?c}";
//     let ( _content, rules) = Parser::parse(data.to_string());
//     println!("encoded {:?}", rules);
//
//     let mut result_bindings: Binding = Binding::new();
//     result_bindings.add(&0, 10);
//     result_bindings.add(&2, 11);
//     result_bindings.add(&3, 12);
//     let val_triple = vec![vec![Triple{s:VarOrTerm::new_encoded_term(10),p:VarOrTerm::new_encoded_term(1),o:VarOrTerm::new_encoded_term(11),g: None}]];
//     for rule in rules{
//         let triples = CSpriteReasoner::reconstruct_triples_from_bindings(&mut result_bindings, &rule);
//
//         assert_eq!(val_triple, triples);
//
//     }
//
// }
// pub fn query(query_triple:&Triple, match_triple:&Triple) -> Option<Binding>{
//     let mut bindings = Binding::new();
//     let Triple{s,p,o, g} = match_triple;
//         match &query_triple.s{
//             VarOrTerm::Var(s_var)=> bindings.add(&s_var.name,s.as_term().iri),
//             VarOrTerm::Term(s_term)=>if let (TermImpl{iri}, TermImpl{iri:iri2})= (s_term,s.as_term()) {
//                 if !iri.eq(iri2){return None;}
//             }
//         }
//         match &query_triple.p{
//             VarOrTerm::Var(p_var)=> bindings.add(&p_var.name,p.as_term().iri),
//             VarOrTerm::Term(p_term)=>if let (TermImpl{iri}, TermImpl{iri:iri2})= (p_term,p.as_term()) {
//                 if !iri.eq(iri2){return None;}
//             }
//         }
//         match &query_triple.o{
//             VarOrTerm::Var(o_var)=> bindings.add(&o_var.name,o.as_term().iri),
//             VarOrTerm::Term(o_term)=>if let (TermImpl{iri}, TermImpl{iri:iri2})= (o_term,o.as_term()) {
//                 if !iri.eq(iri2){return None;}
//             }
//         }
//
//     Some(bindings)
// }
//
// #[test]
// fn test_rule_substitution(){
//     let data=":a in :b.\n\
//                 {?a in ?b.?b in ?c}=>{?a in ?c}\n\
//                 {:a in :b.:b in ?c}=>{:a in ?c}\n\
//                 {?a in :a.:a in :b}=>{?a in :b}";
//     let (content, rules) = Parser::parse(data.to_string());
//     let matching_triple = content.get(0).unwrap();
//     let matching_rule = rules.get(0).unwrap();
//     let results = Reasoner::substitute_rule(matching_triple, matching_rule);
//     assert_eq!(&rules[1..],results);
// }

