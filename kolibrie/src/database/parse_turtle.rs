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
use n3::syntax::{Node, Statement};
use n3::N3Error;
use shared::quoted_triple_store::QuotedTripleStore;
use shared::rdf_term;
use shared::term_encoder::TermsMut;
use shared::triple::Triple;
use std::sync::RwLock;

const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

impl KolibrieDB {
    /// Parses Turtle or Turtle-star and adds the triples, reporting a malformed document on
    /// standard error and adding nothing from it
    pub fn parse_turtle(&mut self, turtle_data: &str) {
        if let Err(error) = self.try_parse_turtle(turtle_data) {
            eprintln!("KOLIBRIE_OPERATION_FAILED: {error}");
        }
    }

    /// Parses Turtle or Turtle-star and adds the triples, returning how many it added; prefixes
    /// the store knows need no declaration, `<< s p o >>` is the quoted triple itself as RDF-star
    /// reads it, and collections become `rdf:first`/`rdf:rest` chains
    ///
    /// # Errors
    /// Returns [`N3Error::Syntax`] for malformed text and [`N3Error::Unsupported`] for N3 that
    /// Turtle cannot write, such as variables or formulas; nothing is added then
    pub fn try_parse_turtle(&mut self, turtle_data: &str) -> Result<usize, N3Error> {
        let mut known = String::new();
        for (prefix, iri) in &self.prefixes {
            known.push_str(&format!("@prefix {prefix}: <{iri}> .\n"));
        }
        let document = n3::parse_rdf_star(&format!("{known}{turtle_data}"), None)?;
        let mut writer = Writer {
            terms: self.dictionary.write(),
            quoted: &self.quoted_triple_store,
            scope: rdf_term::next_document_scope(),
            lists: 0,
            triples: Vec::new(),
        };
        for statement in &document.statements {
            let triple = writer.triple(statement, false)?;
            writer.triples.push(triple);
        }
        let triples = std::mem::take(&mut writer.triples);
        drop(writer);
        for (prefix, iri) in document.prefixes {
            self.prefixes.insert(prefix, iri);
        }
        let count = triples.len();
        for triple in triples {
            self.add_triple(triple);
        }
        Ok(count)
    }
}

/// Encodes a document's terms, holding the store's dictionary for the whole document
struct Writer<'a> {
    terms: TermsMut<'a>,
    quoted: &'a RwLock<QuotedTripleStore>,
    scope: u64,
    lists: usize,
    triples: Vec<Triple>,
}

impl Writer<'_> {
    /// `statement` as ids; `quoted` inside `<< … >>`, where a collection has nowhere to go
    fn triple(&mut self, statement: &Statement, quoted: bool) -> Result<Triple, N3Error> {
        Ok(Triple {
            subject: self.term(&statement.subject, quoted)?,
            predicate: self.term(&statement.predicate, quoted)?,
            object: self.term(&statement.object, quoted)?,
        })
    }

    fn term(&mut self, node: &Node, quoted: bool) -> Result<u32, N3Error> {
        match node {
            Node::Iri(iri) => Ok(self.encode(iri)),
            Node::Literal {
                lexical,
                datatype,
                language,
            } => Ok(self.encode(&rdf_term::spell_literal(
                lexical,
                datatype.as_deref(),
                language.as_deref(),
            ))),
            Node::Blank(label) => Ok(self.encode(&format!("_:{label}"))),
            Node::List(_) if quoted => Err(N3Error::Unsupported(
                "a collection inside a quoted triple".to_string(),
            )),
            Node::List(items) => self.list(items),
            Node::Formula(statements) if statements.len() == 1 => {
                let triple = self.triple(&statements[0], true)?;
                let mut quoted = self
                    .quoted
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                Ok(quoted.encode(triple.subject, triple.predicate, triple.object))
            }
            Node::Formula(_) => Err(N3Error::Unsupported(
                "a formula '{ … }', which Turtle cannot write".to_string(),
            )),
            Node::Var(name) => Err(N3Error::Unsupported(format!(
                "the variable '?{name}', which Turtle cannot write"
            ))),
        }
    }

    /// The head of an `rdf:first`/`rdf:rest` chain holding `items`, the chain queued as triples
    fn list(&mut self, items: &[Node]) -> Result<u32, N3Error> {
        let first = self.encode(RDF_FIRST);
        let rest = self.encode(RDF_REST);
        let mut tail = self.encode(RDF_NIL);
        let mut cells = Vec::with_capacity(items.len());
        for _ in items {
            self.lists += 1;
            let label = rdf_term::spell_blank_node(&format!(".list{}", self.lists), self.scope);
            cells.push(self.encode(&label));
        }
        for (cell, item) in cells.iter().zip(items).rev() {
            let value = self.term(item, false)?;
            self.triples.push(Triple {
                subject: *cell,
                predicate: first,
                object: value,
            });
            self.triples.push(Triple {
                subject: *cell,
                predicate: rest,
                object: tail,
            });
            tail = *cell;
        }
        Ok(tail)
    }

    fn encode(&mut self, spelling: &str) -> u32 {
        self.terms.encode(spelling)
    }
}
