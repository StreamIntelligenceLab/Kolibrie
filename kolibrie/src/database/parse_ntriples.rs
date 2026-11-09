/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reading N-Triples(-star) and N-Quads

use super::KolibrieDB;
use rayon::prelude::*;
use shared::dataset_index::{GraphId, Quad};
use shared::rdf_term;
use shared::rdf_term::TermTriple;
use shared::triple::Triple;

impl KolibrieDB {
    /// Parses N-Triples and adds them
    pub fn parse_ntriples_and_add(&mut self, ntriples_data: &str) {
        let partial_results = self.parse_ntriples(ntriples_data);

        let encoded_triples = self.encode_triples(partial_results);
        for encoded_triple in encoded_triples {
            self.add_triple(encoded_triple);
        }
    }

    /// Parses N-Triples into lexical triples **without** adding them, in parallel chunks of 1000
    /// lines
    pub fn parse_ntriples(&mut self, ntriples_data: &str) -> Vec<Vec<TermTriple>> {
        self.parse_ntriples_in_scope(ntriples_data, rdf_term::next_document_scope())
    }

    /// Parses N-Triples, relabelling blank nodes into a scope the caller chooses
    pub fn parse_ntriples_in_scope(
        &mut self,
        ntriples_data: &str,
        document: u64,
    ) -> Vec<Vec<TermTriple>> {
        let lines: Vec<&str> = ntriples_data.lines().collect();
        let chunk_size = 1000;
        let chunks: Vec<&[&str]> = lines.chunks(chunk_size).collect();

        let partial_results: Vec<Vec<TermTriple>> = chunks
            .par_iter()
            .map(|chunk| {
                let mut local_triples = Vec::new();

                for line in chunk.iter() {
                    let line = line.trim();

                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }

                    if !line.ends_with('.') {
                        eprintln!("KOLIBRIE_OPERATION_FAILED");
                        continue;
                    }

                    let line_without_dot = &line[..line.len() - 1].trim();

                    if let Some((subject, predicate, object)) =
                        self.parse_ntriples_line(line_without_dot)
                    {
                        local_triples.push(TermTriple {
                            subject: self.scope_blank_nodes(subject, document),
                            predicate: self.scope_blank_nodes(predicate, document),
                            object: self.scope_blank_nodes(object, document),
                        });
                    }
                }

                local_triples
            })
            .collect();
        partial_results
    }

    /// Interns lexical triples into encoded ones
    pub fn encode_triples(&mut self, non_encoded_triples: Vec<Vec<TermTriple>>) -> Vec<Triple> {
        let mut encoded_triples = Vec::new();
        for triple_strings in non_encoded_triples {
            for TermTriple {
                subject,
                predicate,
                object,
            } in triple_strings
            {
                let main_triple = Triple {
                    subject: self.encode_term_star(&subject),
                    predicate: self.encode_term_star(&predicate),
                    object: self.encode_term_star(&object),
                };
                encoded_triples.push(main_triple);
            }
        }
        encoded_triples
    }

    /// Parses and encodes N-Triples without adding them to the store
    pub fn parse_and_encode_ntriples(&mut self, ntriples_data: &str) -> Vec<Triple> {
        let partial_results = self.parse_ntriples(ntriples_data);

        self.encode_triples(partial_results)
    }

    /// Parses and encodes N-Triples in a scope the caller chooses
    pub fn parse_and_encode_ntriples_in_scope(
        &mut self,
        ntriples_data: &str,
        document: u64,
    ) -> Vec<Triple> {
        let partial_results = self.parse_ntriples_in_scope(ntriples_data, document);

        self.encode_triples(partial_results)
    }

    /// Parses N-Quads and adds them, preserving named graphs
    pub fn parse_nquads_and_add(&mut self, nquads_data: &str) {
        let document = rdf_term::next_document_scope();
        for raw_line in nquads_data.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let line_without_dot = if line.ends_with('.') {
                line[..line.len() - 1].trim()
            } else {
                eprintln!("KOLIBRIE_OPERATION_FAILED");
                continue;
            };

            if let Some((subject, predicate, object, graph)) =
                self.parse_nquads_line(line_without_dot)
            {
                let subject = self.scope_blank_nodes(subject, document);
                let predicate = self.scope_blank_nodes(predicate, document);
                let object = self.scope_blank_nodes(object, document);
                let graph = graph.map(|name| self.scope_blank_nodes(name, document));
                match graph {
                    Some(graph) => {
                        self.add_quad_parts(&subject, &predicate, &object, &graph);
                    }
                    None => {
                        let quad = Quad {
                            subject: self.encode_term_star(&subject),
                            predicate: self.encode_term_star(&predicate),
                            object: self.encode_term_star(&object),
                            graph: GraphId::Default,
                        };
                        self.add_quad(quad);
                    }
                }
            }
        }
    }

    fn parse_nquads_line(&self, line: &str) -> Option<(String, String, String, Option<String>)> {
        let mut parts = self.parse_ntriples_parts(line);
        if !matches!(parts.len(), 3 | 4) {
            eprintln!("KOLIBRIE_OPERATION_FAILED");
            return None;
        }
        let subject = self.clean_ntriples_term(&parts.remove(0));
        let predicate = self.clean_ntriples_term(&parts.remove(0));
        let object = self.clean_ntriples_term(&parts.remove(0));
        let graph = (!parts.is_empty()).then(|| self.clean_ntriples_term(&parts.remove(0)));
        Some((subject, predicate, object, graph))
    }

    fn parse_ntriples_line(&self, line: &str) -> Option<(String, String, String)> {
        let parts = self.parse_ntriples_parts(line);
        if parts.len() == 3 {
            let subject = self.clean_ntriples_term(&parts[0]);
            let predicate = if parts[1] == "a" {
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string()
            } else {
                self.clean_ntriples_term(&parts[1])
            };
            let object = self.clean_ntriples_term(&parts[2]);
            Some((subject, predicate, object))
        } else {
            eprintln!("KOLIBRIE_OPERATION_FAILED");
            None
        }
    }

    fn parse_ntriples_parts(&self, line: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut current_part = String::new();
        let mut in_uri = false;
        let mut in_literal = false;
        let mut escaped = false;
        let mut qt_depth: i32 = 0;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '<' if !in_literal && !escaped => {
                    if chars.peek() == Some(&'<') && !in_uri {
                        current_part.push(ch);
                        current_part.push(chars.next().unwrap());
                        qt_depth += 1;
                    } else if qt_depth > 0 {
                        current_part.push(ch);
                        if chars.peek() == Some(&'<') {
                            current_part.push(chars.next().unwrap());
                            qt_depth += 1;
                        } else {
                        }
                    } else {
                        in_uri = true;
                        current_part.push(ch);
                    }
                }
                '>' if !in_literal && !escaped => {
                    if qt_depth > 0 && !in_uri {
                        current_part.push(ch);
                        if chars.peek() == Some(&'>') {
                            current_part.push(chars.next().unwrap());
                            qt_depth -= 1;
                            if qt_depth == 0 {
                                parts.push(current_part.trim().to_string());
                                current_part.clear();
                            }
                        }
                    } else if in_uri {
                        in_uri = false;
                        current_part.push(ch);
                        if qt_depth == 0 {
                            parts.push(current_part.trim().to_string());
                            current_part.clear();
                        }
                    } else {
                        current_part.push(ch);
                    }
                }
                '"' if !in_uri && !escaped => {
                    in_literal = !in_literal;
                    current_part.push(ch);
                    if !in_literal {
                        while let Some(&next_ch) = chars.peek() {
                            if next_ch == '^' || next_ch == '@' {
                                current_part.push(chars.next().unwrap());
                                if next_ch == '^' {
                                    if let Some(&second_caret) = chars.peek() {
                                        if second_caret == '^' {
                                            current_part.push(chars.next().unwrap());
                                            while let Some(&datatype_ch) = chars.peek() {
                                                if datatype_ch == '<' {
                                                    current_part.push(chars.next().unwrap());
                                                    let mut in_datatype_uri = true;
                                                    while let Some(&uri_ch) = chars.peek() {
                                                        current_part.push(chars.next().unwrap());
                                                        if uri_ch == '>' {
                                                            in_datatype_uri = false;
                                                            break;
                                                        }
                                                    }
                                                    if !in_datatype_uri {
                                                        break;
                                                    }
                                                } else if datatype_ch.is_whitespace() {
                                                    break;
                                                } else {
                                                    current_part.push(chars.next().unwrap());
                                                }
                                            }
                                        }
                                    }
                                } else if next_ch == '@' {
                                    while let Some(&lang_ch) = chars.peek() {
                                        if lang_ch.is_alphanumeric() || lang_ch == '-' {
                                            current_part.push(chars.next().unwrap());
                                        } else {
                                            break;
                                        }
                                    }
                                }
                                break;
                            } else if next_ch.is_whitespace() {
                                break;
                            } else {
                                break;
                            }
                        }
                        if qt_depth == 0 {
                            parts.push(current_part.trim().to_string());
                            current_part.clear();
                        }
                    }
                }
                '\\' if (in_uri || in_literal) && !escaped => {
                    escaped = true;
                    current_part.push(ch);
                }
                ' ' | '\t' if !in_uri && !in_literal && !escaped && qt_depth == 0 => {
                    if !current_part.is_empty() {
                        parts.push(current_part.trim().to_string());
                        current_part.clear();
                    }
                }
                _ => {
                    escaped = false;
                    current_part.push(ch);
                }
            }
        }

        if !current_part.is_empty() {
            parts.push(current_part.trim().to_string());
        }

        parts
    }

    fn clean_ntriples_term(&self, term: &str) -> String {
        let term = term.trim();

        if term.starts_with("<<") && term.ends_with(">>") {
            return term.to_string();
        }

        if term.starts_with('<') && term.ends_with('>') {
            return term[1..term.len() - 1].to_string();
        }

        if term.starts_with('"') {
            if let Some(canonical) = rdf_term::canonicalise_literal(term) {
                return canonical;
            }
        }

        term.to_string()
    }
}
