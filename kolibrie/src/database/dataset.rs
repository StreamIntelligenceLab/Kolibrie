/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Adding, removing, looking up, counting and merging triples and quads

use super::{reencode_term_id, KolibrieDB};
use shared::dataset_index::{DatasetIndex, GraphId, Quad};
use shared::dictionary::Dictionary;
use shared::term_encoder::SharedTerms;
use shared::triple::Triple;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

impl KolibrieDB {
    /// Adds an already-encoded triple to the default graph
    pub fn add_triple(&mut self, triple: Triple) {
        self.dataset_index.insert_triple(&triple);
    }

    /// Removes a triple from the default graph
    pub fn delete_triple(&mut self, triple: &Triple) -> bool {
        self.dataset_index.delete_triple(triple)
    }

    /// Adds an encoded quad, creating its graph if needed
    pub fn add_quad(&mut self, quad: Quad) -> bool {
        self.dataset_index.insert_quad(&quad)
    }

    /// Removes a quad
    pub fn delete_quad(&mut self, quad: &Quad) -> bool {
        self.dataset_index.delete_quad(quad)
    }

    /// Interns the given terms and adds the resulting quad
    pub fn add_quad_parts(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
        graph: &str,
    ) -> bool {
        let subject_id = self.encode_term_star(subject);
        let predicate_id = self.encode_term_star(predicate);
        let object_id = self.encode_term_star(object);
        let graph_id = {
            let mut dict = self.dictionary.write();
            dict.encode(graph)
        };

        self.add_quad(Quad {
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
            graph: GraphId::Named(graph_id),
        })
    }

    /// Default-graph triples matching the bound positions; `None` is a wildcard
    pub fn query_default_triples(
        &self,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Vec<Triple> {
        self.dataset_index.query_default(s, p, o)
    }

    /// Quads in one graph matching the bound positions; `None` is a wildcard
    pub fn query_graph_quads(
        &self,
        graph: GraphId,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Vec<Quad> {
        self.dataset_index.query_graph(graph, s, p, o)
    }

    /// Encodes three lexical terms and adds the resulting triple
    pub fn add_triple_parts(&mut self, subject: &str, predicate: &str, object: &str) {
        let mut dict = self.dictionary.write();
        let subject_id = dict.encode(subject);
        let predicate_id = dict.encode(predicate);
        let object_id = dict.encode(object);
        drop(dict);

        let triple = Triple {
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
        };
        self.add_triple(triple);
    }

    /// Adds a triple carrying a provenance annotation, for probabilistic reasoning
    pub fn add_tagged_triple(
        &mut self,
        subject: &str,
        predicate: &str,
        object: &str,
        probability: f64,
    ) {
        let mut dict = self.dictionary.write();
        let s = dict.encode(subject);
        let p = dict.encode(predicate);
        let o = dict.encode(object);
        drop(dict);

        let triple = Triple {
            subject: s,
            predicate: p,
            object: o,
        };
        self.add_triple(triple.clone());
        self.probability_seeds.insert(triple, probability);
    }

    /// Encodes three lexical terms and deletes the resulting triple
    pub fn delete_triple_parts(&mut self, subject: &str, predicate: &str, object: &str) -> bool {
        let mut dict = self.dictionary.write();
        let subject_id = dict.encode(subject);
        let predicate_id = dict.encode(predicate);
        let object_id = dict.encode(object);
        drop(dict);

        let triple = Triple {
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
        };
        self.delete_triple(&triple)
    }

    /// Number of triples in the default graph
    pub fn default_triple_count(&self) -> usize {
        self.dataset_index.len_default()
    }

    /// Number of quads across every graph, including named ones
    pub fn quad_count(&self) -> usize {
        self.dataset_index.count_quads()
    }

    /// Every quad in the store, across all graphs
    pub fn all_quads(&self) -> Vec<Quad> {
        self.dataset_index.all_quads()
    }

    /// Every graph in the store, including the default graph
    pub fn graphs(&self) -> Vec<GraphId> {
        self.dataset_index.graphs()
    }

    /// The named graphs, excluding the default graph
    pub fn named_graphs(&self) -> Vec<GraphId> {
        self.dataset_index.named_graphs()
    }

    /// Creates `graph` if it does not exist
    pub fn create_graph(&mut self, graph: GraphId) -> bool {
        self.dataset_index.create_graph(graph)
    }

    /// Inserts a triple into the default graph, reporting whether it was new
    pub fn insert_triple(&mut self, triple: &Triple) -> bool {
        self.dataset_index.insert_triple(triple)
    }

    /// Removes every quad and graph, leaving the dictionary intact
    pub fn clear_dataset(&mut self) {
        self.dataset_index.clear();
    }

    /// Rebuilds every graph-scoped index, keeping named graphs distinct from the default
    pub fn build_all_indexes(&mut self) {
        let quads = self.dataset_index.all_quads();
        let named_graphs = self.dataset_index.named_graphs();
        let mut rebuilt = DatasetIndex::new();
        for graph in named_graphs {
            rebuilt.create_graph(graph);
        }
        for quad in quads {
            rebuilt.insert_quad(&quad);
        }
        self.dataset_index = rebuilt;
    }

    /// A new store containing both stores' data
    pub fn union(&mut self, other: &KolibrieDB) -> Self {
        let self_dict = self.dictionary.read();
        let other_dict = other.dictionary.read();
        let mut merged_dictionary = self_dict.clone();
        let self_quoted_triples = self.quoted_triple_store.read().unwrap();
        let other_quoted_triples = other.quoted_triple_store.read().unwrap();
        let mut merged_quoted_triples = self_quoted_triples.clone();
        let mut translated_ids = HashMap::new();

        let mut other_term_ids: Vec<_> = other_dict.id_to_string.keys().copied().collect();
        other_term_ids.sort_unstable();
        for id in other_term_ids {
            reencode_term_id(
                id,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
        }

        let mut other_quoted_ids: Vec<_> = other_quoted_triples
            .id_to_components
            .keys()
            .copied()
            .collect();
        other_quoted_ids.sort_unstable();
        for id in other_quoted_ids {
            reencode_term_id(
                id,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
        }

        let mut dataset_index = DatasetIndex::new();
        for graph in self.dataset_index.named_graphs() {
            dataset_index.create_graph(graph);
        }
        for quad in self.dataset_index.all_quads() {
            dataset_index.insert_quad(&quad);
        }

        for graph in other.dataset_index.named_graphs() {
            let GraphId::Named(graph_id) = graph else {
                continue;
            };
            let graph_id = reencode_term_id(
                graph_id,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            dataset_index.create_graph(GraphId::Named(graph_id));
        }
        for quad in other.dataset_index.all_quads() {
            let subject = reencode_term_id(
                quad.subject,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let predicate = reencode_term_id(
                quad.predicate,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let object = reencode_term_id(
                quad.object,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let graph = match quad.graph {
                GraphId::Default => GraphId::Default,
                GraphId::Named(graph_id) => GraphId::Named(reencode_term_id(
                    graph_id,
                    &other_dict,
                    &other_quoted_triples,
                    &mut merged_dictionary,
                    &mut merged_quoted_triples,
                    &mut translated_ids,
                )),
            };
            dataset_index.insert_quad(&Quad {
                subject,
                predicate,
                object,
                graph,
            });
        }

        let mut merged_seeds = self.probability_seeds.clone();
        for (triple, prob) in &other.probability_seeds {
            let subject = reencode_term_id(
                triple.subject,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let predicate = reencode_term_id(
                triple.predicate,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            let object = reencode_term_id(
                triple.object,
                &other_dict,
                &other_quoted_triples,
                &mut merged_dictionary,
                &mut merged_quoted_triples,
                &mut translated_ids,
            );
            merged_seeds.insert(
                Triple {
                    subject,
                    predicate,
                    object,
                },
                *prob,
            );
        }

        Self {
            dataset_index,
            dictionary: SharedTerms::from_dictionary(merged_dictionary),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            rule_map: HashMap::new(),
            model_decls: self.model_decls.clone(),
            neural_relation_decls: self.neural_relation_decls.clone(),
            ml_context: self.ml_context.clone(),
            train_neural_relation_decls: self.train_neural_relation_decls.clone(),
            neural_model_artifacts: self.neural_model_artifacts.clone(),
            neural_materialized_triples: self.neural_materialized_triples.clone(),
            ml_predict_materialized_triples: self.ml_predict_materialized_triples.clone(),
            probability_seeds: merged_seeds,
            cached_stats: None,
            exact_stats_generation: 0,
            planning_stats: None,
            planning_stats_policy: self.planning_stats_policy,
            stats_epoch: 0,
            stats_rebuild_count: 0,
            stats_rebuild_nanos: 0,
            quoted_triple_store: Arc::new(RwLock::new(merged_quoted_triples)),
            implicit_neural_materialization: self.implicit_neural_materialization,
            training_seed: self.training_seed,
        }
    }

    /// Runs a query and renders the result as text, for the demo server
    pub fn handle_query(&mut self, query: &str) -> String {
        let parts: Vec<&str> = query.split_whitespace().collect();

        if parts.len() != 3 {
            return "Invalid query format. Expected 'subject predicate object'.".to_string();
        }

        let subject = parts[0];
        let predicate = parts[1];
        let object = parts[2];

        let mut dict = self.dictionary.write();
        let subject_id = dict.encode(subject);
        let predicate_id = dict.encode(predicate);
        let object_id = dict.encode(object);

        let mut result = String::new();
        let matching_triples =
            self.query_default_triples(Some(subject_id), Some(predicate_id), Some(object_id));
        for triple in matching_triples {
            if triple.subject == subject_id
                && triple.predicate == predicate_id
                && triple.object == object_id
            {
                result.push_str(&format!(
                    "Subject: {}, Predicate: {}, Object: {}\n",
                    dict.decode(triple.subject).unwrap(),
                    dict.decode(triple.predicate).unwrap(),
                    dict.decode(triple.object).unwrap()
                ));
            }
        }
        drop(dict);

        if result.is_empty() {
            result = "No matching triples found.".to_string();
        }

        result
    }

    /// Return the decoded graph size for diagnostics
    pub fn debug_print_triples(&self) {
        println!(
            "TRIPLE_COUNT {}",
            self.query_default_triples(None, None, None).len()
        );
    }

    /// Triple to string
    pub fn triple_to_string(&self, triple: &Triple, dict: &Dictionary) -> String {
        let subject = dict.decode(triple.subject);
        let predicate = dict.decode(triple.predicate);
        let object = dict.decode(triple.object);
        format!(
            "{} {} {}",
            subject.unwrap(),
            predicate.unwrap(),
            object.unwrap()
        )
    }
}

impl shared::quad_source::QuadSource for KolibrieDB {
    fn query_default(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        self.dataset_index.query_default(s, p, o)
    }

    fn for_each_quad(&self, visit: &mut dyn FnMut(GraphId, u32, u32, u32)) {
        self.dataset_index
            .for_each_quad(|g, s, p, o| visit(g, s, p, o));
    }

    fn named_graphs(&self) -> Vec<GraphId> {
        self.dataset_index.named_graphs()
    }

    fn count_graph(&self, graph: GraphId) -> usize {
        self.dataset_index.count_graph(graph)
    }
}
