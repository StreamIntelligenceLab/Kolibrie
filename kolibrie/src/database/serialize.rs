/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Writing the store out as RDF/XML, N-Triples, N-Quads and Turtle

use super::KolibrieDB;
use shared::dataset_index::GraphId;
use shared::rdf_term;
use std::collections::BTreeMap;

impl KolibrieDB {
    /// Serialises the default graph as RDF/XML
    pub fn generate_rdf_xml(&mut self) -> String {
        let mut xml = String::new();
        xml.push_str("<?xml version=\"1.0\"?>\n");
        xml.push_str("<rdf:RDF");

        for (prefix, uri) in &self.prefixes {
            if prefix.is_empty() {
                xml.push_str(&format!(" xmlns=\"{}\"", uri));
            } else {
                xml.push_str(&format!(" xmlns:{}=\"{}\"", prefix, uri));
            }
        }
        xml.push_str(" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"");
        xml.push_str(">\n");

        let dict = self.dictionary.read();
        let mut subjects: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        let default_triples = self.query_default_triples(None, None, None);
        for triple in &default_triples {
            let subject = dict.decode(triple.subject);
            let predicate = dict.decode(triple.predicate);
            let object = dict.decode(triple.object);
            subjects
                .entry(subject.unwrap().to_string())
                .or_default()
                .push((predicate.unwrap().to_string(), object.unwrap().to_string()));
        }
        drop(dict);

        for (subject, po_pairs) in subjects {
            xml.push_str(&format!("  <rdf:Description rdf:about=\"{}\">\n", subject));
            for (predicate, object) in po_pairs {
                xml.push_str(&format!("    <{}>{}</{}>\n", predicate, object, predicate));
            }
            xml.push_str("  </rdf:Description>\n");
        }

        xml.push_str("</rdf:RDF>\n");
        xml
    }

    /// Serializes all triples as N-Triples-star format
    pub fn generate_ntriples(&self) -> String {
        self.write_ntriples(None)
    }

    fn write_ntriples(&self, skolem_base: Option<&str>) -> String {
        let mut output = String::new();
        for triple in self.query_default_triples(None, None, None) {
            let s = self.decode_any(triple.subject).unwrap_or_default();
            let p = self.decode_any(triple.predicate).unwrap_or_default();
            let o = self.decode_any(triple.object).unwrap_or_default();

            output.push_str(&format!(
                "{} {} {} .\n",
                Self::write_term(&s, skolem_base),
                Self::write_term(&p, skolem_base),
                Self::write_term(&o, skolem_base)
            ));
        }
        output
    }

    /// Serialises every graph as N-Quads, replacing blank nodes with Skolem IRIs under `base`
    pub fn generate_nquads_skolemised(&self, base: &str) -> String {
        self.write_nquads(Some(base))
    }

    /// Serialises the default graph as N-Triples, replacing blank nodes with Skolem IRIs under
    /// `base`
    pub fn generate_ntriples_skolemised(&self, base: &str) -> String {
        self.write_ntriples(Some(base))
    }

    /// Writes one term, replacing a blank node with a Skolem IRI when asked
    fn write_term(spelling: &str, skolem_base: Option<&str>) -> String {
        match skolem_base {
            Some(base) => rdf_term::to_ntriples_term(&rdf_term::skolemise(spelling, base)),
            None => rdf_term::to_ntriples_term(spelling),
        }
    }

    /// Serialises every graph as N-Quads, which unlike N-Triples preserves named graphs
    pub fn generate_nquads(&self) -> String {
        self.write_nquads(None)
    }

    fn write_nquads(&self, skolem_base: Option<&str>) -> String {
        let mut output = String::new();
        for quad in self.dataset_index.all_quads() {
            let s = self.decode_any(quad.subject).unwrap_or_default();
            let p = self.decode_any(quad.predicate).unwrap_or_default();
            let o = self.decode_any(quad.object).unwrap_or_default();

            let s_str = Self::write_term(&s, skolem_base);
            let p_str = Self::write_term(&p, skolem_base);
            let o_str = Self::write_term(&o, skolem_base);
            match quad.graph {
                GraphId::Default => {
                    output.push_str(&format!("{} {} {} .\n", s_str, p_str, o_str));
                }
                GraphId::Named(graph_id) => {
                    let graph = self.decode_any(graph_id).unwrap_or_default();
                    let graph = Self::write_term(&graph, skolem_base);
                    output.push_str(&format!("{} {} {} {} .\n", s_str, p_str, o_str, graph));
                }
            }
        }
        output
    }

    /// Serializes all triples as Turtle-star format with prefix declarations
    pub fn generate_turtle(&self) -> String {
        let mut output = String::new();

        for (prefix, uri) in &self.prefixes {
            output.push_str(&format!("@prefix {}: <{}> .\n", prefix, uri));
        }
        if !self.prefixes.is_empty() {
            output.push('\n');
        }

        let mut subjects: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
        for triple in self.query_default_triples(None, None, None) {
            let s = self.decode_any(triple.subject).unwrap_or_default();
            let p = self.decode_any(triple.predicate).unwrap_or_default();
            let o = self.decode_any(triple.object).unwrap_or_default();
            subjects.entry(s).or_default().entry(p).or_default().push(o);
        }

        for (subject, predicates) in &subjects {
            output.push_str(&rdf_term::to_ntriples_term(subject));

            let pred_count = predicates.len();
            for (i, (predicate, objects)) in predicates.iter().enumerate() {
                if i == 0 {
                    output.push(' ');
                } else {
                    output.push_str(" ;\n    ");
                }
                output.push_str(&rdf_term::to_ntriples_term(predicate));

                for (j, obj) in objects.iter().enumerate() {
                    if j > 0 {
                        output.push_str(" ,");
                    }
                    output.push(' ');
                    output.push_str(&rdf_term::to_ntriples_term(obj));
                }

                if i == pred_count - 1 {
                    output.push_str(" .\n");
                }
            }
        }
        output
    }
}
