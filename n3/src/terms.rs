/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Interns N3 terms, lists and formulas included, as Kolibrie dictionary ids

use crate::syntax::{Node, Statement};
use shared::dictionary::Dictionary;
use shared::rdf_term::{self, TermKind};
use shared::triple::Triple;
use std::collections::HashMap;

/// The structure behind an id that does not stand for a plain RDF term
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Compound {
    /// A list of member ids
    List(Vec<u32>),
    /// A quoted formula: its triples, sorted and without duplicates
    Formula(Vec<Triple>),
    /// A variable occurring inside a quoted formula, by name
    Var(String),
    /// A logic variable a backward proof introduced for a value not known yet, by serial
    /// number; a solution binds it under [`TermStore::logic_key`]
    Logic(u32),
}

/// A Kolibrie [`Dictionary`] plus the structure of the compound terms interned in it
#[derive(Debug, Clone, Default)]
pub struct TermStore {
    /// Every term's spelling, compound terms spelled as N3 text
    pub dictionary: Dictionary,
    compounds: HashMap<u32, Compound>,
    logic_vars: u32,
}

/// The dictionary spelling of a literal, quoted wherever the bare form would read as another term
pub fn literal_spelling(lexical: &str, datatype: Option<&str>, language: Option<&str>) -> String {
    let spelling = rdf_term::spell_literal(lexical, datatype, language);
    let ambiguous = !spelling.starts_with('"')
        && (rdf_term::kind_of(&spelling) != TermKind::Literal
            || spelling.starts_with(['(', '{', '?', '<', '"']));
    if ambiguous {
        format!("\"{}\"", rdf_term::escape_lexical(lexical))
    } else {
        spelling
    }
}

impl TermStore {
    /// An empty store
    pub fn new() -> Self {
        Self::default()
    }

    /// The structure behind `id`, or `None` for a plain RDF term
    pub fn compound(&self, id: u32) -> Option<&Compound> {
        self.compounds.get(&id)
    }

    /// The id of an IRI
    pub fn iri(&mut self, iri: &str) -> u32 {
        self.dictionary.encode(iri)
    }

    /// The id of an already-interned spelling, without interning anything
    pub fn lookup(&self, spelling: &str) -> Option<u32> {
        self.dictionary.string_to_id.get(spelling).copied()
    }

    /// Interns a node; variables inside it become quoted-variable terms
    pub fn encode(&mut self, node: &Node) -> u32 {
        match node {
            Node::Iri(iri) => self.dictionary.encode(iri),
            Node::Literal {
                lexical,
                datatype,
                language,
            } => {
                let spelling = literal_spelling(lexical, datatype.as_deref(), language.as_deref());
                self.dictionary.encode(&spelling)
            }
            Node::Blank(label) => self.dictionary.encode(&format!("_:{label}")),
            Node::Var(name) => self.intern_compound(Compound::Var(name.clone())),
            Node::List(items) => {
                let ids = items.iter().map(|item| self.encode(item)).collect();
                self.intern_compound(Compound::List(ids))
            }
            Node::Formula(statements) => {
                let triples = statements
                    .iter()
                    .map(|statement| self.encode_statement(statement))
                    .collect();
                self.formula(triples)
            }
        }
    }

    /// Interns the three terms of a statement
    pub fn encode_statement(&mut self, statement: &Statement) -> Triple {
        Triple {
            subject: self.encode(&statement.subject),
            predicate: self.encode(&statement.predicate),
            object: self.encode(&statement.object),
        }
    }

    /// The id of the list with these members
    pub fn list(&mut self, items: Vec<u32>) -> u32 {
        self.intern_compound(Compound::List(items))
    }

    /// The id of the formula with these triples
    pub fn formula(&mut self, mut triples: Vec<Triple>) -> u32 {
        triples.sort();
        triples.dedup();
        self.intern_compound(Compound::Formula(triples))
    }

    /// A logic variable no term has used before
    pub fn fresh_logic_var(&mut self) -> u32 {
        self.logic_vars += 1;
        self.intern_compound(Compound::Logic(self.logic_vars))
    }

    /// Whether any logic variable was ever made, so the matcher can skip following them
    pub fn has_logic_vars(&self) -> bool {
        self.logic_vars > 0
    }

    /// The binding key under which a solution records the value of logic variable `id`
    pub fn logic_key(id: u32) -> String {
        format!("#lv{id}")
    }

    /// The id of the quoted variable `name`
    pub fn quoted_var(&mut self, name: &str) -> u32 {
        self.intern_compound(Compound::Var(name.to_string()))
    }

    /// The id of the list with these members, if it was interned before
    pub fn find_list(&self, items: &[u32]) -> Option<u32> {
        self.lookup(&self.spell_list(items))
    }

    fn spell_list(&self, items: &[u32]) -> String {
        let members: Vec<String> = items.iter().map(|&id| self.n3(id)).collect();
        format!("({})", members.join(" "))
    }

    fn spell(&self, compound: &Compound) -> String {
        match compound {
            Compound::List(items) => self.spell_list(items),
            Compound::Formula(triples) => {
                let statements: Vec<String> = triples
                    .iter()
                    .map(|t| {
                        format!(
                            "{} {} {} .",
                            self.n3(t.subject),
                            self.n3(t.predicate),
                            self.n3(t.object)
                        )
                    })
                    .collect();
                format!("{{{}}}", statements.join(" "))
            }
            Compound::Var(name) => format!("?{name}"),
            Compound::Logic(serial) => format!("?#{serial}"),
        }
    }

    fn intern_compound(&mut self, compound: Compound) -> u32 {
        let spelling = self.spell(&compound);
        if let Some(id) = self.lookup(&spelling) {
            return id;
        }
        let id = self.dictionary.encode(&spelling);
        self.compounds.insert(id, compound);
        id
    }

    /// Writes `id` as N3 term syntax
    pub fn n3(&self, id: u32) -> String {
        let spelling = self.dictionary.decode(id).unwrap_or("");
        if self.compounds.contains_key(&id) {
            return spelling.to_string();
        }
        rdf_term::to_ntriples_term(spelling)
    }

    /// Writes a triple as one N3 statement
    pub fn n3_triple(&self, triple: &Triple) -> String {
        format!(
            "{} {} {} .",
            self.n3(triple.subject),
            self.n3(triple.predicate),
            self.n3(triple.object)
        )
    }

    /// Rebuilds the node `id` stands for
    pub fn node(&self, id: u32) -> Node {
        match self.compounds.get(&id) {
            Some(Compound::List(items)) => {
                Node::List(items.iter().map(|&i| self.node(i)).collect())
            }
            Some(Compound::Formula(triples)) => {
                Node::Formula(triples.iter().map(|t| self.statement(t)).collect())
            }
            Some(Compound::Var(name)) => Node::Var(name.clone()),
            Some(Compound::Logic(serial)) => Node::Var(format!("_{serial}")),
            None => {
                let spelling = self.dictionary.decode(id).unwrap_or("");
                match rdf_term::kind_of(spelling) {
                    TermKind::Iri => Node::Iri(spelling.to_string()),
                    TermKind::BlankNode => Node::Blank(spelling[2..].to_string()),
                    TermKind::Literal => match rdf_term::read_literal(spelling) {
                        Some(literal) => Node::Literal {
                            lexical: literal.lexical,
                            datatype: literal.datatype,
                            language: literal.language,
                        },
                        None => Node::Iri(spelling.to_string()),
                    },
                }
            }
        }
    }

    /// Rebuilds the statement a triple stands for
    pub fn statement(&self, triple: &Triple) -> Statement {
        Statement::new(
            self.node(triple.subject),
            self.node(triple.predicate),
            self.node(triple.object),
        )
    }
}
