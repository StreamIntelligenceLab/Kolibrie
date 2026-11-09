/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reading the N3 subset this engine supports

use super::{reencode_term_id, KolibrieDB};
use rayon::prelude::*;
use shared::dictionary::Dictionary;
use shared::quoted_triple_store::QuotedTripleStore;
use shared::rdf_term;
use shared::term_encoder::SharedTerms;
use shared::triple::Triple;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

impl KolibrieDB {
    /// Parses the N3 subset this engine supports and adds the triples
    pub fn parse_n3(&mut self, n3_data: &str) {
        let document = rdf_term::next_document_scope();
        let lines: Vec<String> = n3_data.lines().map(|l| l.trim().to_string()).collect();
        let chunk_size = 1000;
        let chunks: Vec<Vec<String>> = lines.chunks(chunk_size).map(|c| c.to_vec()).collect();

        type ChunkResult = (
            Vec<Triple>,
            SharedTerms,
            Arc<RwLock<QuotedTripleStore>>,
            HashMap<String, String>,
        );
        let partial_results: Vec<ChunkResult> = chunks
            .par_iter()
            .map(|chunk| {
                let mut local_db = KolibrieDB::new();
                let mut statement = String::new();

                for raw_line in chunk {
                    let mut line = raw_line.as_str();
                    if let Some(comment_start) = line.find('#') {
                        line = &line[..comment_start];
                        line = line.trim();
                    }
                    if line.is_empty() {
                        continue;
                    }
                    if line.starts_with("@prefix") {
                        let line = line.trim_start_matches("@prefix").trim_end_matches('.');
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            let prefix = parts[0].trim_end_matches(':').to_string();
                            let uri = parts[1]
                                .trim_start_matches('<')
                                .trim_end_matches('>')
                                .to_string();
                            local_db.prefixes.insert(prefix, uri);
                        } else {
                            eprintln!("KOLIBRIE_OPERATION_FAILED");
                        }
                    } else {
                        statement.push_str(line);
                        statement.push(' ');
                        if line.ends_with('.') {
                            local_db.parse_statement(statement.trim(), document);
                            statement.clear();
                        }
                    }
                }

                (
                    local_db.query_default_triples(None, None, None),
                    local_db.dictionary,
                    local_db.quoted_triple_store,
                    local_db.prefixes,
                )
            })
            .collect();

        for (triples, dict_arc, quoted_arc, pref) in partial_results {
            let translated: Vec<Triple> = {
                let source_dictionary = dict_arc.read();
                let source_quoted = quoted_arc.read().unwrap_or_else(|p| p.into_inner());
                let mut target_dictionary = self.dictionary.write();
                let mut target_quoted = self
                    .quoted_triple_store
                    .write()
                    .unwrap_or_else(|p| p.into_inner());
                let mut seen = HashMap::new();
                let reencode = |id: u32,
                                target_dictionary: &mut Dictionary,
                                target_quoted: &mut QuotedTripleStore,
                                seen: &mut HashMap<u32, u32>| {
                    reencode_term_id(
                        id,
                        &source_dictionary,
                        &source_quoted,
                        target_dictionary,
                        target_quoted,
                        seen,
                    )
                };
                triples
                    .iter()
                    .map(|triple| Triple {
                        subject: reencode(
                            triple.subject,
                            &mut target_dictionary,
                            &mut target_quoted,
                            &mut seen,
                        ),
                        predicate: reencode(
                            triple.predicate,
                            &mut target_dictionary,
                            &mut target_quoted,
                            &mut seen,
                        ),
                        object: reencode(
                            triple.object,
                            &mut target_dictionary,
                            &mut target_quoted,
                            &mut seen,
                        ),
                    })
                    .collect()
            };
            for triple in translated {
                self.add_triple(triple);
            }
            for (k, v) in pref {
                self.prefixes.insert(k, v);
            }
        }
    }

    fn parse_statement(&mut self, statement: &str, document: u64) {
        let mut tokens = statement.split_whitespace().peekable();
        let mut subject = String::new();
        let mut predicate = String::new();
        let mut current_state = "subject";

        while let Some(token) = tokens.next() {
            match token {
                ";" => {
                    predicate.clear();
                    current_state = "predicate";
                }
                "." => {
                    break;
                }
                _ => match current_state {
                    "subject" => {
                        subject = token.to_string();
                        current_state = "predicate";
                    }
                    "predicate" => {
                        predicate = token.to_string();
                        current_state = "object";
                    }
                    "object" => {
                        let mut object = token.to_string();

                        while let Some(next_token) = tokens.peek() {
                            if *next_token == ";" || *next_token == "." || *next_token == "," {
                                break;
                            }
                            let next_token = tokens.next().unwrap();
                            object.push(' ');
                            object.push_str(next_token);
                        }

                        let resolved_subject = self.resolve_term(&subject);
                        let resolved_subject = self.scope_blank_nodes(resolved_subject, document);
                        let resolved_predicate = self.resolve_term(&predicate);
                        let resolved_predicate =
                            self.scope_blank_nodes(resolved_predicate, document);
                        let resolved_object = self.resolve_term(&object);
                        let resolved_object = self.scope_blank_nodes(resolved_object, document);

                        let mut dict = self.dictionary.write();
                        let triple = Triple {
                            subject: dict.encode(&resolved_subject),
                            predicate: dict.encode(&resolved_predicate),
                            object: dict.encode(&resolved_object),
                        };
                        drop(dict);
                        self.add_triple(triple);

                        current_state = "predicate";
                    }
                    _ => {}
                },
            }
        }
    }
}
