/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::terms::{Term, TriplePattern};
use crate::triple::Triple;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum GraphId {
    Default,
    Named(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Quad {
    pub subject: u32,
    pub predicate: u32,
    pub object: u32,
    pub graph: GraphId,
}

impl Quad {
    pub fn triple(&self) -> Triple {
        Triple {
            subject: self.subject,
            predicate: self.predicate,
            object: self.object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphTerm {
    Default,
    Named(u32),
    Variable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuadPattern {
    pub subject: Term,
    pub predicate: Term,
    pub object: Term,
    pub graph: GraphTerm,
}

type NestedIndex = HashMap<u32, HashMap<u32, HashSet<u32>>>;
type GraphNestedIndex = HashMap<GraphId, NestedIndex>;
type SpoGraphIndex = HashMap<u32, HashMap<u32, HashMap<u32, HashSet<GraphId>>>>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatasetIndex {
    // Graph-leading indexes for graph-scoped patterns.
    gspo: GraphNestedIndex,
    gpos: GraphNestedIndex,
    gosp: GraphNestedIndex,
    // Triple-to-graph index for GRAPH ?g and membership checks.
    spog: SpoGraphIndex,
}

impl DatasetIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_triple(&mut self, triple: &Triple) -> bool {
        self.insert_quad(&Quad {
            subject: triple.subject,
            predicate: triple.predicate,
            object: triple.object,
            graph: GraphId::Default,
        })
    }

    pub fn delete_triple(&mut self, triple: &Triple) -> bool {
        self.delete_quad(&Quad {
            subject: triple.subject,
            predicate: triple.predicate,
            object: triple.object,
            graph: GraphId::Default,
        })
    }

    pub fn insert(&mut self, triple: &Triple) -> bool {
        self.insert_triple(triple)
    }

    pub fn delete(&mut self, triple: &Triple) -> bool {
        self.delete_triple(triple)
    }

    pub fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        self.query_default(s, p, o)
    }

    pub fn insert_quad(&mut self, quad: &Quad) -> bool {
        if self.contains_quad(quad) {
            return false;
        }

        let Quad {
            subject: s,
            predicate: p,
            object: o,
            graph: g,
        } = *quad;

        self.gspo
            .entry(g)
            .or_default()
            .entry(s)
            .or_default()
            .entry(p)
            .or_default()
            .insert(o);
        self.gpos
            .entry(g)
            .or_default()
            .entry(p)
            .or_default()
            .entry(o)
            .or_default()
            .insert(s);
        self.gosp
            .entry(g)
            .or_default()
            .entry(o)
            .or_default()
            .entry(s)
            .or_default()
            .insert(p);
        self.spog
            .entry(s)
            .or_default()
            .entry(p)
            .or_default()
            .entry(o)
            .or_default()
            .insert(g);
        true
    }

    pub fn delete_quad(&mut self, quad: &Quad) -> bool {
        if !self.contains_quad(quad) {
            return false;
        }

        let Quad {
            subject: s,
            predicate: p,
            object: o,
            graph: g,
        } = *quad;

        remove_from_graph_index(&mut self.gspo, g, s, p, o);
        remove_from_graph_index(&mut self.gpos, g, p, o, s);
        remove_from_graph_index(&mut self.gosp, g, o, s, p);
        remove_from_spog(&mut self.spog, s, p, o, g);
        true
    }

    pub fn contains_quad(&self, quad: &Quad) -> bool {
        self.spog
            .get(&quad.subject)
            .and_then(|pred_map| pred_map.get(&quad.predicate))
            .and_then(|obj_map| obj_map.get(&quad.object))
            .is_some_and(|graphs| graphs.contains(&quad.graph))
    }

    pub fn query_default(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        self.query_graph(GraphId::Default, s, p, o)
            .into_iter()
            .map(|quad| quad.triple())
            .collect()
    }

    pub fn query_graph(
        &self,
        graph: GraphId,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Vec<Quad> {
        let mut results = Vec::new();

        match (s, p, o) {
            (Some(ss), Some(pp), Some(oo)) => {
                let quad = Quad {
                    subject: ss,
                    predicate: pp,
                    object: oo,
                    graph,
                };
                if self.contains_quad(&quad) {
                    results.push(quad);
                }
            }
            (Some(ss), Some(pp), None) => {
                if let Some(objects) = self
                    .gspo
                    .get(&graph)
                    .and_then(|subj_map| subj_map.get(&ss))
                    .and_then(|pred_map| pred_map.get(&pp))
                {
                    results.extend(objects.iter().map(|&oo| Quad {
                        subject: ss,
                        predicate: pp,
                        object: oo,
                        graph,
                    }));
                }
            }
            (Some(ss), None, Some(oo)) => {
                if let Some(predicates) = self
                    .gosp
                    .get(&graph)
                    .and_then(|obj_map| obj_map.get(&oo))
                    .and_then(|subj_map| subj_map.get(&ss))
                {
                    results.extend(predicates.iter().map(|&pp| Quad {
                        subject: ss,
                        predicate: pp,
                        object: oo,
                        graph,
                    }));
                }
            }
            (None, Some(pp), Some(oo)) => {
                if let Some(subjects) = self
                    .gpos
                    .get(&graph)
                    .and_then(|pred_map| pred_map.get(&pp))
                    .and_then(|obj_map| obj_map.get(&oo))
                {
                    results.extend(subjects.iter().map(|&ss| Quad {
                        subject: ss,
                        predicate: pp,
                        object: oo,
                        graph,
                    }));
                }
            }
            (Some(ss), None, None) => {
                if let Some(pred_map) = self.gspo.get(&graph).and_then(|subj_map| subj_map.get(&ss))
                {
                    for (&pp, objects) in pred_map {
                        results.extend(objects.iter().map(|&oo| Quad {
                            subject: ss,
                            predicate: pp,
                            object: oo,
                            graph,
                        }));
                    }
                }
            }
            (None, Some(pp), None) => {
                if let Some(obj_map) = self.gpos.get(&graph).and_then(|pred_map| pred_map.get(&pp))
                {
                    for (&oo, subjects) in obj_map {
                        results.extend(subjects.iter().map(|&ss| Quad {
                            subject: ss,
                            predicate: pp,
                            object: oo,
                            graph,
                        }));
                    }
                }
            }
            (None, None, Some(oo)) => {
                if let Some(subj_map) = self.gosp.get(&graph).and_then(|obj_map| obj_map.get(&oo)) {
                    for (&ss, predicates) in subj_map {
                        results.extend(predicates.iter().map(|&pp| Quad {
                            subject: ss,
                            predicate: pp,
                            object: oo,
                            graph,
                        }));
                    }
                }
            }
            (None, None, None) => {
                if let Some(subj_map) = self.gspo.get(&graph) {
                    for (&ss, pred_map) in subj_map {
                        for (&pp, objects) in pred_map {
                            results.extend(objects.iter().map(|&oo| Quad {
                                subject: ss,
                                predicate: pp,
                                object: oo,
                                graph,
                            }));
                        }
                    }
                }
            }
        }

        results
    }

    pub fn query_named_graphs(
        &self,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
        visible_graphs: Option<&HashSet<GraphId>>,
    ) -> Vec<Quad> {
        let mut results = Vec::new();

        if let (Some(ss), Some(pp), Some(oo)) = (s, p, o) {
            if let Some(graphs) = self
                .spog
                .get(&ss)
                .and_then(|pred_map| pred_map.get(&pp))
                .and_then(|obj_map| obj_map.get(&oo))
            {
                results.extend(
                    graphs
                        .iter()
                        .copied()
                        .filter(|g| *g != GraphId::Default)
                        .filter(|g| visible_graphs.map_or(true, |visible| visible.contains(g)))
                        .map(|graph| Quad {
                            subject: ss,
                            predicate: pp,
                            object: oo,
                            graph,
                        }),
                );
                return results;
            }
        }

        for graph in self.named_graphs() {
            if visible_graphs.map_or(false, |visible| !visible.contains(&graph)) {
                continue;
            }
            results.extend(self.query_graph(graph, s, p, o));
        }

        results
    }

    pub fn query_quads(
        &self,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
        graph: Option<GraphId>,
    ) -> Vec<Quad> {
        match graph {
            Some(g) => self.query_graph(g, s, p, o),
            None => {
                let mut results = self.query_graph(GraphId::Default, s, p, o);
                results.extend(self.query_named_graphs(s, p, o, None));
                results
            }
        }
    }

    pub fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
        let (s, p, o) = pattern;
        let sub = constant_id(s);
        let pre = constant_id(p);
        let obj = constant_id(o);
        self.query_default(sub, pre, obj)
    }

    pub fn graph_exists(&self, graph: GraphId) -> bool {
        self.gspo.contains_key(&graph)
    }

    pub fn named_graphs(&self) -> Vec<GraphId> {
        self.gspo
            .keys()
            .copied()
            .filter(|graph| *graph != GraphId::Default)
            .collect()
    }

    pub fn graphs_for_triple(&self, triple: &Triple) -> Vec<GraphId> {
        self.spog
            .get(&triple.subject)
            .and_then(|pred_map| pred_map.get(&triple.predicate))
            .and_then(|obj_map| obj_map.get(&triple.object))
            .map(|graphs| graphs.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn clear_graph(&mut self, graph: GraphId) {
        let quads = self.query_graph(graph, None, None, None);
        for quad in quads {
            self.delete_quad(&quad);
        }
    }

    pub fn clear(&mut self) {
        self.gspo.clear();
        self.gpos.clear();
        self.gosp.clear();
        self.spog.clear();
    }

    pub fn len_graph(&self, graph: GraphId) -> usize {
        self.query_graph(graph, None, None, None).len()
    }

    pub fn len_default(&self) -> usize {
        self.len_graph(GraphId::Default)
    }
}

fn constant_id(term: &Term) -> Option<u32> {
    match term {
        Term::Constant(id) => Some(*id),
        Term::Variable(_) | Term::QuotedTriple(_) => None,
    }
}

fn remove_from_graph_index(
    index: &mut GraphNestedIndex,
    graph: GraphId,
    key1: u32,
    key2: u32,
    value: u32,
) {
    let remove_graph = if let Some(nested) = index.get_mut(&graph) {
        remove_from_nested_index(nested, key1, key2, value);
        nested.is_empty()
    } else {
        false
    };
    if remove_graph {
        index.remove(&graph);
    }
}

fn remove_from_nested_index(index: &mut NestedIndex, key1: u32, key2: u32, value: u32) {
    if let Some(inner_map) = index.get_mut(&key1) {
        if let Some(set) = inner_map.get_mut(&key2) {
            set.remove(&value);
            if set.is_empty() {
                inner_map.remove(&key2);
            }
        }
        if inner_map.is_empty() {
            index.remove(&key1);
        }
    }
}

fn remove_from_spog(index: &mut SpoGraphIndex, s: u32, p: u32, o: u32, graph: GraphId) {
    if let Some(pred_map) = index.get_mut(&s) {
        if let Some(obj_map) = pred_map.get_mut(&p) {
            if let Some(graphs) = obj_map.get_mut(&o) {
                graphs.remove(&graph);
                if graphs.is_empty() {
                    obj_map.remove(&o);
                }
            }
            if obj_map.is_empty() {
                pred_map.remove(&p);
            }
        }
        if pred_map.is_empty() {
            index.remove(&s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triple(s: u32, p: u32, o: u32) -> Triple {
        Triple {
            subject: s,
            predicate: p,
            object: o,
        }
    }

    #[test]
    fn default_triples_are_stored_in_default_graph() {
        let mut index = DatasetIndex::new();
        let t = triple(1, 2, 3);
        assert!(index.insert_triple(&t));
        assert_eq!(index.query_default(Some(1), Some(2), Some(3)), vec![t]);
        assert_eq!(
            index.query_named_graphs(Some(1), Some(2), Some(3), None),
            Vec::<Quad>::new()
        );
    }

    #[test]
    fn same_triple_can_exist_in_multiple_named_graphs() {
        let mut index = DatasetIndex::new();
        let q1 = Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Named(10),
        };
        let q2 = Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Named(11),
        };
        assert!(index.insert_quad(&q1));
        assert!(index.insert_quad(&q2));

        let mut graphs = index.graphs_for_triple(&q1.triple());
        graphs.sort();
        assert_eq!(graphs, vec![GraphId::Named(10), GraphId::Named(11)]);
    }

    #[test]
    fn delete_is_graph_scoped() {
        let mut index = DatasetIndex::new();
        let q1 = Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Named(10),
        };
        let q2 = Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Named(11),
        };
        index.insert_quad(&q1);
        index.insert_quad(&q2);

        assert!(index.delete_quad(&q1));
        assert!(!index.contains_quad(&q1));
        assert!(index.contains_quad(&q2));
    }
}
