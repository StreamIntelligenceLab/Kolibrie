/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The parsed form of an N3 document: terms, statements and formulas

use std::fmt;

/// The `log:` namespace
pub const LOG: &str = "http://www.w3.org/2000/10/swap/log#";
/// `log:implies`, the predicate `=>` abbreviates
pub const LOG_IMPLIES: &str = "http://www.w3.org/2000/10/swap/log#implies";
/// `log:impliedBy`, the predicate `<=` abbreviates
pub const LOG_IMPLIED_BY: &str = "http://www.w3.org/2000/10/swap/log#impliedBy";
/// `log:query`, which selects what a run outputs
pub const LOG_QUERY: &str = "http://www.w3.org/2000/10/swap/log#query";
/// `rdf:type`, the predicate `a` abbreviates
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
/// `owl:sameAs`, the predicate `=` abbreviates
pub const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";
/// `xsd:integer`
pub const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
/// `xsd:decimal`
pub const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
/// `xsd:double`
pub const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
/// `xsd:boolean`
pub const XSD_BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";

/// One N3 term as written, after prefixes and the base are resolved
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Node {
    /// An absolute IRI
    Iri(String),
    /// A literal: lexical form, datatype IRI, language tag
    Literal {
        /// The lexical form, unescaped
        lexical: String,
        /// The datatype IRI, absent for a plain or language-tagged literal
        datatype: Option<String>,
        /// The language tag, absent unless the literal has one
        language: Option<String>,
    },
    /// A blank node, by a label unique within the document
    Blank(String),
    /// A universally quantified variable, by name without `?`
    Var(String),
    /// A list `( … )`
    List(Vec<Node>),
    /// A quoted formula `{ … }`
    Formula(Vec<Statement>),
}

/// One triple of N3 terms
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Statement {
    /// The subject
    pub subject: Node,
    /// The predicate
    pub predicate: Node,
    /// The object
    pub object: Node,
}

/// A parsed N3 document
#[derive(Debug, Clone, Default)]
pub struct Document {
    /// The top-level statements, rules included as `log:implies` triples
    pub statements: Vec<Statement>,
    /// Prefix declarations in the order they were read
    pub prefixes: Vec<(String, String)>,
}

impl Node {
    /// An IRI node
    pub fn iri(iri: impl Into<String>) -> Self {
        Node::Iri(iri.into())
    }

    /// A literal node with an optional datatype
    pub fn typed(lexical: impl Into<String>, datatype: &str) -> Self {
        Node::Literal {
            lexical: lexical.into(),
            datatype: Some(datatype.to_string()),
            language: None,
        }
    }

    /// Whether the node contains no variable at any depth
    pub fn is_ground(&self) -> bool {
        match self {
            Node::Var(_) => false,
            Node::List(items) => items.iter().all(Node::is_ground),
            Node::Formula(statements) => statements.iter().all(Statement::is_ground),
            _ => true,
        }
    }
}

impl Statement {
    /// A statement from its three terms
    pub fn new(subject: Node, predicate: Node, object: Node) -> Self {
        Statement {
            subject,
            predicate,
            object,
        }
    }

    /// Whether no position contains a variable
    pub fn is_ground(&self) -> bool {
        self.subject.is_ground() && self.predicate.is_ground() && self.object.is_ground()
    }
}

/// Escapes a lexical form for a double-quoted N3 string
pub fn escape_string(lexical: &str) -> String {
    shared::rdf_term::escape_lexical(lexical)
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Iri(iri) => write!(f, "<{iri}>"),
            Node::Literal {
                lexical,
                datatype,
                language,
            } => {
                write!(f, "\"{}\"", escape_string(lexical))?;
                if let Some(tag) = language {
                    write!(f, "@{tag}")
                } else if let Some(iri) = datatype {
                    write!(f, "^^<{iri}>")
                } else {
                    Ok(())
                }
            }
            Node::Blank(label) => write!(f, "_:{label}"),
            Node::Var(name) => write!(f, "?{name}"),
            Node::List(items) => {
                write!(f, "(")?;
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, ")")
            }
            Node::Formula(statements) => {
                write!(f, "{{")?;
                for (index, statement) in statements.iter().enumerate() {
                    if index > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{statement}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {} .", self.subject, self.predicate, self.object)
    }
}
