/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reading Turtle and Turtle-star

use super::KolibrieDB;
use shared::rdf_term;
use shared::triple::Triple;

impl KolibrieDB {
    /// Parses Turtle and adds the triples, honouring `@prefix` declarations and the `;` and `,`
    /// abbreviations
    pub fn parse_turtle(&mut self, turtle_data: &str) {
        let document = rdf_term::next_document_scope();
        for raw_line in turtle_data.lines() {
            let line = raw_line.trim();

            if line.is_empty() || line.starts_with("#") {
                continue;
            }

            if line.starts_with("@prefix") || line.starts_with("PREFIX") {
                let prefix_line = line
                    .trim_start_matches("@prefix")
                    .trim_start_matches("PREFIX")
                    .trim_end_matches('.')
                    .trim();

                let parts: Vec<&str> = prefix_line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let prefix = parts[0].trim_end_matches(':').to_string();
                    let uri = parts[1]
                        .trim_start_matches('<')
                        .trim_end_matches('>')
                        .to_string();
                    self.prefixes.insert(prefix, uri);
                } else {
                    eprintln!("KOLIBRIE_OPERATION_FAILED");
                }
                continue;
            }

            let tokens = Self::tokenize_turtle_star_line(line);

            let mut subject_raw: Option<String> = None;
            let mut predicate_raw: Option<String> = None;
            let mut object_tokens: Vec<String> = Vec::new();

            let mut expect_subject = true;
            let mut expect_predicate = false;
            let mut expect_object = false;

            let flush_object = |this: &mut Self,
                                subject_raw: &Option<String>,
                                predicate_raw: &Option<String>,
                                object_tokens: &mut Vec<String>| {
                if let (Some(s_raw), Some(p_raw)) = (subject_raw.as_ref(), predicate_raw.as_ref()) {
                    if object_tokens.is_empty() {
                        return;
                    }

                    let object_raw = object_tokens.join(" ");

                    let (object_part, annotations) = if let Some(ann_start) = object_raw.find("{|")
                    {
                        let obj = object_raw[..ann_start].trim().to_string();

                        if let Some(ann_end) = object_raw.find("|}") {
                            let ann_content = object_raw[ann_start + 2..ann_end].trim();
                            let ann_parts: Vec<&str> =
                                ann_content.splitn(2, char::is_whitespace).collect();

                            if ann_parts.len() == 2 {
                                (
                                    obj,
                                    vec![(ann_parts[0].to_string(), ann_parts[1].to_string())],
                                )
                            } else {
                                (obj, vec![])
                            }
                        } else {
                            (object_raw, vec![])
                        }
                    } else {
                        (object_raw, vec![])
                    };

                    let subject =
                        this.resolve_query_term(&Self::clean_turtle_term(s_raw), &this.prefixes);
                    let subject = this.scope_blank_nodes(subject, document);
                    let predicate =
                        this.resolve_query_term(&Self::clean_turtle_term(p_raw), &this.prefixes);
                    let predicate = this.scope_blank_nodes(predicate, document);
                    let object = this
                        .resolve_query_term(&Self::clean_turtle_term(&object_part), &this.prefixes);
                    let object = this.scope_blank_nodes(object, document);

                    if subject.starts_with("<<") || object.starts_with("<<") {
                        let s_id = this.encode_term_star(&subject);
                        let p_id = this.encode_term_star(&predicate);
                        let o_id = this.encode_term_star(&object);
                        let triple = Triple {
                            subject: s_id,
                            predicate: p_id,
                            object: o_id,
                        };
                        this.add_triple(triple);
                    } else {
                        let mut dict = this.dictionary.write();
                        let triple = Triple {
                            subject: dict.encode(&subject),
                            predicate: dict.encode(&predicate),
                            object: dict.encode(&object),
                        };
                        drop(dict);
                        this.add_triple(triple);
                    }

                    for (ann_pred, ann_obj) in &annotations {
                        let qt_str = format!("<< {} {} {} >>", subject, predicate, object);
                        let qt_id = this.encode_term_star(&qt_str);

                        let ann_p_id = this.encode_term_star(&this.resolve_query_term(
                            &Self::clean_turtle_term(ann_pred),
                            &this.prefixes,
                        ));
                        let ann_o_id =
                            this.encode_term_star(&this.resolve_query_term(
                                &Self::clean_turtle_term(ann_obj),
                                &this.prefixes,
                            ));

                        let ann_triple = Triple {
                            subject: qt_id,
                            predicate: ann_p_id,
                            object: ann_o_id,
                        };
                        this.add_triple(ann_triple);
                    }

                    object_tokens.clear();
                }
            };

            for token in tokens {
                match token.as_str() {
                    "." => {
                        flush_object(self, &subject_raw, &predicate_raw, &mut object_tokens);
                        subject_raw = None;
                        predicate_raw = None;
                        expect_subject = true;
                        expect_predicate = false;
                        expect_object = false;
                    }
                    ";" => {
                        flush_object(self, &subject_raw, &predicate_raw, &mut object_tokens);
                        predicate_raw = None;
                        expect_predicate = true;
                        expect_object = false;
                    }
                    "," => {
                        flush_object(self, &subject_raw, &predicate_raw, &mut object_tokens);
                        expect_object = true;
                    }
                    _ => {
                        if expect_subject {
                            subject_raw = Some(token);
                            expect_subject = false;
                            expect_predicate = true;
                        } else if expect_predicate {
                            predicate_raw = Some(token);
                            expect_predicate = false;
                            expect_object = true;
                        } else if expect_object {
                            object_tokens.push(token);
                        } else {
                            object_tokens.push(token);
                        }
                    }
                }
            }

            flush_object(self, &subject_raw, &predicate_raw, &mut object_tokens);
        }
    }

    /// Tokenize a Turtle-star line, keeping `<< ... >>` and punctuation structure intact
    fn tokenize_turtle_star_line(line: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut current = String::new();

        let mut depth = 0i32;
        let mut in_uri = false;
        let mut in_literal = false;
        let mut escaped = false;

        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            if escaped {
                current.push(ch);
                escaped = false;
                continue;
            }

            match ch {
                '\\' if in_literal => {
                    current.push(ch);
                    escaped = true;
                }

                '"' if !in_uri && depth == 0 => {
                    in_literal = !in_literal;
                    current.push(ch);
                }

                '"' if depth > 0 => {
                    in_literal = !in_literal;
                    current.push(ch);
                }

                '<' if !in_literal => {
                    if chars.peek() == Some(&'<') && !in_uri {
                        current.push(ch);
                        current.push(chars.next().unwrap());
                        depth += 1;
                    } else if depth > 0 {
                        current.push(ch);
                        if chars.peek() == Some(&'<') {
                            current.push(chars.next().unwrap());
                            depth += 1;
                        }
                    } else {
                        in_uri = true;
                        current.push(ch);
                    }
                }

                '>' if !in_literal => {
                    if depth > 0 && !in_uri {
                        current.push(ch);
                        if chars.peek() == Some(&'>') {
                            current.push(chars.next().unwrap());
                            depth -= 1;
                            if depth == 0 {
                                tokens.push(current.trim().to_string());
                                current.clear();
                            }
                        }
                    } else if in_uri {
                        in_uri = false;
                        current.push(ch);
                        if depth == 0 {
                            tokens.push(current.trim().to_string());
                            current.clear();
                        }
                    } else {
                        current.push(ch);
                    }
                }

                ';' | ',' | '.' if depth == 0 && !in_uri && !in_literal => {
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        tokens.push(trimmed);
                        current.clear();
                    }
                    tokens.push(ch.to_string());
                }

                ' ' | '\t' | '\n' | '\r' if depth == 0 && !in_uri && !in_literal => {
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        tokens.push(trimmed);
                        current.clear();
                    }
                }

                _ => {
                    current.push(ch);
                }
            }
        }

        let trimmed = current.trim().to_string();
        if !trimmed.is_empty() {
            tokens.push(trimmed);
        }

        tokens
    }

    fn clean_turtle_term(term: &str) -> String {
        let term = term.trim();
        if term.starts_with("<<") {
            term.to_string()
        } else if term.starts_with('<') && term.ends_with('>') {
            term.to_string()
        } else if term.starts_with('"') {
            term.to_string()
        } else {
            term.trim_matches('"').to_string()
        }
    }
}
