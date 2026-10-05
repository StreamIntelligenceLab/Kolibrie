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
use std::collections::{BTreeSet, HashMap, HashSet};

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphTerm {
    Default,
    Named(u32),
    Variable(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
    gspo: GraphNestedIndex,
    gpos: GraphNestedIndex,
    gosp: GraphNestedIndex,
    spog: SpoGraphIndex,
    #[serde(default)]
    named_graphs: HashSet<u32>,
    #[serde(skip)]
    generation: u64,
    #[serde(skip)]
    resets: u64,
}

impl DatasetIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mutation counter; compare readings with `wrapping_sub`, not `<`
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Number of non-empty `clear()` calls
    pub fn resets(&self) -> u64 {
        self.resets
    }

    fn record_change(&mut self) {
        self.generation = self.generation.wrapping_add(1);
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
        // Registering a new named graph counts as a mutation
        let named_a_graph = match quad.graph {
            GraphId::Named(graph) => self.named_graphs.insert(graph),
            GraphId::Default => false,
        };

        if self.contains_quad(quad) {
            if named_a_graph {
                self.record_change();
            }
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
        self.record_change();
        true
    }

    pub fn delete_quad(&mut self, quad: &Quad) -> bool {
        if !self.contains_quad(quad) {
            return false;
        }

        if let GraphId::Named(graph) = quad.graph {
            // Materialize graph identity before deleting the last quad from an
            // index deserialized from the pre-catalog representation.
            self.named_graphs.insert(graph);
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
        self.record_change();
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

    /// Queries a logical default graph formed by merging the supplied source graphs
    pub fn query_merged_graphs(
        &self,
        source_graphs: &[GraphId],
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Vec<Triple> {
        source_graphs
            .iter()
            .flat_map(|graph| self.query_graph(*graph, s, p, o))
            .map(|quad| quad.triple())
            .collect::<BTreeSet<_>>()
            .into_iter()
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
        match graph {
            GraphId::Default => true,
            GraphId::Named(graph) => {
                self.named_graphs.contains(&graph)
                    // Non-empty graphs in indexes serialized before the graph
                    // catalog was added remain discoverable.
                    || self.gspo.contains_key(&GraphId::Named(graph))
            }
        }
    }

    pub fn named_graphs(&self) -> Vec<GraphId> {
        let mut graphs: HashSet<u32> = self.named_graphs.clone();
        graphs.extend(self.gspo.keys().filter_map(|graph| match graph {
            GraphId::Default => None,
            GraphId::Named(graph) => Some(*graph),
        }));

        let mut graphs: Vec<_> = graphs.into_iter().map(GraphId::Named).collect();
        graphs.sort_unstable();
        graphs
    }

    /// Returns every graph identity, including the always-present default
    /// graph and named graphs without any quads.
    pub fn graphs(&self) -> Vec<GraphId> {
        let mut graphs = vec![GraphId::Default];
        graphs.extend(self.named_graphs());
        graphs
    }

    /// Creates a graph identity without requiring a quad to be inserted.
    ///
    /// Returns `true` only when a new named graph was created. The default
    /// graph always exists and therefore returns `false`.
    pub fn create_graph(&mut self, graph: GraphId) -> bool {
        match graph {
            GraphId::Default => false,
            GraphId::Named(graph) => {
                let existed = self.graph_exists(GraphId::Named(graph));
                self.named_graphs.insert(graph);
                if !existed {
                    self.record_change();
                }
                !existed
            }
        }
    }

    /// Drops a graph identity and all of its quads.
    ///
    /// Dropping the default graph clears it but cannot remove its identity.
    /// A missing named graph returns `false`.
    pub fn drop_graph(&mut self, graph: GraphId) -> bool {
        match graph {
            GraphId::Default => {
                self.clear_graph(GraphId::Default);
                true
            }
            GraphId::Named(graph_id) => {
                let graph = GraphId::Named(graph_id);
                if !self.graph_exists(graph) {
                    return false;
                }
                // Removing the graph identity counts as one additional mutation
                self.clear_graph(graph);
                self.named_graphs.remove(&graph_id);
                self.record_change();
                true
            }
        }
    }

    /// Returns a complete, deterministic quad snapshot suitable for rebuilding
    /// all indexes without collapsing named graphs into the default graph.
    pub fn all_quads(&self) -> Vec<Quad> {
        let mut quads = Vec::new();
        for graph in self.graphs() {
            quads.extend(self.query_graph(graph, None, None, None));
        }
        quads.sort_unstable();
        quads
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
        // Clearing a named graph retains its identity, including for an old
        // deserialized index whose catalog is populated lazily.
        if let GraphId::Named(graph_id) = graph {
            if self.graph_exists(graph) && self.named_graphs.insert(graph_id) {
                // Register a legacy (pre-catalog) graph
                self.record_change();
            }
        }

        // delete_quad records one mutation per removed quad
        let quads = self.query_graph(graph, None, None, None);
        for quad in quads {
            self.delete_quad(&quad);
        }
    }

    pub fn clear(&mut self) {
        // No-op on an empty dataset
        if self.gspo.is_empty() && self.named_graphs.is_empty() {
            return;
        }
        self.gspo.clear();
        self.gpos.clear();
        self.gosp.clear();
        self.spog.clear();
        self.named_graphs.clear();
        self.resets = self.resets.wrapping_add(1);
        self.record_change();
    }

    /// Visits every quad without allocating; order is unspecified
    pub fn for_each_quad(&self, mut visit: impl FnMut(GraphId, u32, u32, u32)) {
        for (&graph, subjects) in &self.gspo {
            for (&subject, predicates) in subjects {
                for (&predicate, objects) in predicates {
                    for &object in objects {
                        visit(graph, subject, predicate, object);
                    }
                }
            }
        }
    }

    /// Visits every quad of one graph in place
    pub fn for_each_quad_in_graph(&self, graph: GraphId, mut visit: impl FnMut(u32, u32, u32)) {
        let Some(subjects) = self.gspo.get(&graph) else {
            return;
        };
        for (&subject, predicates) in subjects {
            for (&predicate, objects) in predicates {
                for &object in objects {
                    visit(subject, predicate, object);
                }
            }
        }
    }

    /// Counts one graph's quads without materializing them
    pub fn count_graph(&self, graph: GraphId) -> usize {
        self.gspo.get(&graph).map_or(0, |subjects| {
            subjects
                .values()
                .map(|predicates| predicates.values().map(HashSet::len).sum::<usize>())
                .sum()
        })
    }

    /// Counts every quad without materializing them
    pub fn count_quads(&self) -> usize {
        self.gspo
            .keys()
            .map(|&graph| self.count_graph(graph))
            .sum()
    }

    pub fn len_graph(&self, graph: GraphId) -> usize {
        self.count_graph(graph)
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

    #[test]
    fn default_graph_always_exists() {
        let mut index = DatasetIndex::new();
        assert!(index.graph_exists(GraphId::Default));
        assert_eq!(index.graphs(), vec![GraphId::Default]);
        assert!(!index.create_graph(GraphId::Default));

        index.insert_triple(&triple(1, 2, 3));
        assert!(index.drop_graph(GraphId::Default));
        assert!(index.graph_exists(GraphId::Default));
        assert!(index.query_default(None, None, None).is_empty());
    }

    #[test]
    fn empty_named_graph_has_an_identity() {
        let mut index = DatasetIndex::new();
        let graph = GraphId::Named(42);

        assert!(index.create_graph(graph));
        assert!(!index.create_graph(graph));
        assert!(index.graph_exists(graph));
        assert_eq!(index.named_graphs(), vec![graph]);
        assert_eq!(index.graphs(), vec![GraphId::Default, graph]);
        assert!(index.query_graph(graph, None, None, None).is_empty());
    }

    #[test]
    fn insert_and_delete_preserve_named_graph_identity() {
        let mut index = DatasetIndex::new();
        let quad = Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Named(10),
        };

        assert!(index.insert_quad(&quad));
        assert!(index.graph_exists(quad.graph));
        assert!(index.delete_quad(&quad));
        assert!(index.graph_exists(quad.graph));
        assert_eq!(index.named_graphs(), vec![quad.graph]);
    }

    #[test]
    fn deleting_from_legacy_index_materializes_graph_identity() {
        let mut index = DatasetIndex::new();
        let quad = Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Named(10),
        };
        index.insert_quad(&quad);
        // Models deserialization of the representation from before the
        // explicit catalog was added.
        index.named_graphs.clear();

        assert!(index.delete_quad(&quad));
        assert!(index.graph_exists(quad.graph));
        assert_eq!(index.named_graphs(), vec![quad.graph]);
    }

    #[test]
    fn clear_graph_retains_identity_and_drop_removes_it() {
        let mut index = DatasetIndex::new();
        let graph = GraphId::Named(10);
        index.insert_quad(&Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph,
        });

        index.clear_graph(graph);
        assert!(index.graph_exists(graph));
        assert!(index.query_graph(graph, None, None, None).is_empty());

        assert!(index.drop_graph(graph));
        assert!(!index.graph_exists(graph));
        assert!(!index.drop_graph(graph));
    }

    #[test]
    fn clear_resets_named_graph_catalog() {
        let mut index = DatasetIndex::new();
        let graph = GraphId::Named(10);
        index.insert_quad(&Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph,
        });

        index.clear();
        assert!(!index.graph_exists(graph));
        assert_eq!(index.graphs(), vec![GraphId::Default]);
        assert!(index.all_quads().is_empty());
    }

    #[test]
    fn all_quads_keeps_default_and_named_graph_scope() {
        let mut index = DatasetIndex::new();
        let default_quad = Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Default,
        };
        let named_quad = Quad {
            subject: 1,
            predicate: 2,
            object: 3,
            graph: GraphId::Named(10),
        };
        index.insert_quad(&default_quad);
        index.insert_quad(&named_quad);
        index.create_graph(GraphId::Named(11));

        assert_eq!(index.all_quads(), vec![default_quad, named_quad]);
        assert_eq!(
            index.graphs(),
            vec![GraphId::Default, GraphId::Named(10), GraphId::Named(11)]
        );
    }

    #[test]
    fn merged_graph_query_deduplicates_triples_across_sources() {
        let mut index = DatasetIndex::new();
        let shared = triple(1, 2, 3);
        let unique = triple(4, 2, 5);
        let first = GraphId::Named(10);
        let second = GraphId::Named(11);

        index.insert_quad(&Quad {
            subject: shared.subject,
            predicate: shared.predicate,
            object: shared.object,
            graph: first,
        });
        index.insert_quad(&Quad {
            subject: shared.subject,
            predicate: shared.predicate,
            object: shared.object,
            graph: second,
        });
        index.insert_quad(&Quad {
            subject: unique.subject,
            predicate: unique.predicate,
            object: unique.object,
            graph: second,
        });

        assert_eq!(
            index.query_merged_graphs(&[first, second, first], None, None, None),
            vec![shared.clone(), unique]
        );
        assert_eq!(
            index.query_merged_graphs(&[first, second], Some(1), Some(2), Some(3)),
            vec![shared]
        );
        assert!(index.query_merged_graphs(&[], None, None, None).is_empty());
    }

    fn named(id: u32) -> GraphId {
        GraphId::Named(id)
    }

    fn quad(s: u32, p: u32, o: u32, graph: GraphId) -> Quad {
        Quad {
            subject: s,
            predicate: p,
            object: o,
            graph,
        }
    }

    /// Returns the generation delta caused by `change`
    fn advance(index: &mut DatasetIndex, change: impl FnOnce(&mut DatasetIndex)) -> u64 {
        let before = index.generation();
        change(index);
        index.generation().wrapping_sub(before)
    }

    #[test]
    fn a_successful_quad_mutation_advances_the_generation_once() {
        let mut index = DatasetIndex::new();
        assert_eq!(advance(&mut index, |i| assert!(i.insert_triple(&triple(1, 2, 3)))), 1);
        assert_eq!(advance(&mut index, |i| assert!(i.delete_triple(&triple(1, 2, 3)))), 1);
        assert_eq!(
            advance(&mut index, |i| assert!(i.insert_quad(&quad(1, 2, 3, named(9))))),
            1
        );
        assert_eq!(
            advance(&mut index, |i| assert!(i.delete_quad(&quad(1, 2, 3, named(9))))),
            1
        );
    }

    #[test]
    fn a_graph_mutation_advances_the_generation_once() {
        let mut index = DatasetIndex::new();
        assert_eq!(advance(&mut index, |i| assert!(i.create_graph(named(5)))), 1);
        assert_eq!(advance(&mut index, |i| assert!(i.drop_graph(named(5)))), 1);
    }

    #[test]
    fn a_mutation_that_changes_nothing_leaves_the_generation_alone() {
        let mut index = DatasetIndex::new();
        index.insert_triple(&triple(1, 2, 3));
        index.create_graph(named(5));

        // Duplicate insert
        assert_eq!(advance(&mut index, |i| assert!(!i.insert_triple(&triple(1, 2, 3)))), 0);
        // Delete of a missing quad
        assert_eq!(advance(&mut index, |i| assert!(!i.delete_triple(&triple(9, 9, 9)))), 0);
        // Existing graph created, missing graph dropped
        assert_eq!(advance(&mut index, |i| assert!(!i.create_graph(named(5)))), 0);
        assert_eq!(advance(&mut index, |i| assert!(!i.drop_graph(named(404)))), 0);
        // Default graph cannot be created
        assert_eq!(advance(&mut index, |i| assert!(!i.create_graph(GraphId::Default))), 0);
    }

    #[test]
    fn clearing_an_empty_dataset_changes_nothing() {
        let mut index = DatasetIndex::new();
        assert_eq!(advance(&mut index, |i| i.clear()), 0);

        index.insert_triple(&triple(1, 2, 3));
        assert!(advance(&mut index, |i| i.clear()) >= 1);
        assert_eq!(advance(&mut index, |i| i.clear()), 0);
    }

    #[test]
    fn only_a_real_full_clear_counts_as_a_reset() {
        let mut index = DatasetIndex::new();
        index.clear();
        assert_eq!(index.resets(), 0);

        index.insert_quad(&quad(1, 2, 3, named(7)));
        index.clear_graph(named(7));
        assert_eq!(index.resets(), 0, "clearing one graph is ordinary deletion");

        index.insert_triple(&triple(1, 2, 3));
        index.clear();
        assert_eq!(index.resets(), 1);
    }

    #[test]
    fn clearing_a_graph_records_one_change_per_removed_quad() {
        let mut index = DatasetIndex::new();
        index.insert_quad(&quad(1, 2, 3, named(7)));
        index.insert_quad(&quad(4, 5, 6, named(7)));

        assert_eq!(advance(&mut index, |i| i.clear_graph(named(7))), 2);
        // Second clear removes nothing
        assert_eq!(advance(&mut index, |i| i.clear_graph(named(7))), 0);
    }

    /// Delete/insert pairs count as mutations even when the size is unchanged
    #[test]
    fn a_constant_size_replacement_still_advances_the_generation() {
        let mut index = DatasetIndex::new();
        for i in 0..10 {
            index.insert_triple(&triple(i, 1, i));
        }
        let before = index.generation();
        let size_before = index.count_quads();

        for i in 0..10 {
            assert!(index.delete_triple(&triple(i, 1, i)));
            assert!(index.insert_triple(&triple(i + 100, 1, i)));
        }

        assert_eq!(index.count_quads(), size_before);
        assert_eq!(index.generation().wrapping_sub(before), 20);
    }

    #[test]
    fn naming_a_graph_through_a_duplicate_quad_is_still_a_change() {
        let mut index = DatasetIndex::new();
        index.insert_quad(&quad(1, 2, 3, named(4)));
        // Same quad, graph already named
        assert_eq!(
            advance(&mut index, |i| assert!(!i.insert_quad(&quad(1, 2, 3, named(4))))),
            0
        );
    }

    #[test]
    fn counting_agrees_with_materialising() {
        let mut index = DatasetIndex::new();
        assert_eq!(index.count_quads(), 0);
        index.insert_triple(&triple(1, 2, 3));
        index.insert_quad(&quad(4, 5, 6, named(7)));
        index.insert_quad(&quad(8, 9, 10, named(7)));
        index.create_graph(named(8));

        assert_eq!(index.count_quads(), index.all_quads().len());
        assert_eq!(index.count_graph(GraphId::Default), 1);
        assert_eq!(index.count_graph(named(7)), 2);
        assert_eq!(index.count_graph(named(8)), 0);
        assert_eq!(index.count_graph(named(404)), 0);
    }

    #[test]
    fn visiting_quads_yields_exactly_the_snapshot() {
        let mut index = DatasetIndex::new();
        index.insert_triple(&triple(1, 2, 3));
        index.insert_quad(&quad(4, 5, 6, named(7)));
        index.create_graph(named(8));

        let mut visited = Vec::new();
        index.for_each_quad(|graph, s, p, o| visited.push(quad(s, p, o, graph)));
        visited.sort_unstable();

        assert_eq!(visited, index.all_quads());
    }
}
