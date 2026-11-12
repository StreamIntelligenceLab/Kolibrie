/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The `log:` built-ins that take terms apart or name them

use crate::matcher::Matcher;
use crate::rules::PTerm;
use crate::syntax::Node;
use crate::terms::Compound;
use shared::rdf_term::XSD_STRING;
use shared::terms::Binding;

const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
const RDF_DIR_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#dirLangString";
const RDF_LIST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#List";
const LOG_FORMULA: &str = "http://www.w3.org/2000/10/swap/log#Formula";
const LOG_LITERAL: &str = "http://www.w3.org/2000/10/swap/log#Literal";
const LOG_OTHER: &str = "http://www.w3.org/2000/10/swap/log#Other";
const SKOLEM_BASE: &str = "https://eyereasoner.github.io/.well-known/genid/";

/// A `log:` built-in over a single term
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralOp {
    /// `log:uri`: an IRI and the string that spells it
    Uri,
    /// `log:skolem`: an IRI that stands for the term
    Skolem,
    /// `log:dtlit`: a literal and its `(lexical datatype)` pair
    Dtlit,
    /// `log:langlit`: a literal and its `(lexical language)` pair
    Langlit,
    /// `log:rawType`: whether the term is a formula, literal, list or other
    RawType,
}

impl LiteralOp {
    /// The operation a `log:` local name names
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "uri" => LiteralOp::Uri,
            "skolem" => LiteralOp::Skolem,
            "dtlit" => LiteralOp::Dtlit,
            "langlit" => LiteralOp::Langlit,
            "rawType" => LiteralOp::RawType,
            _ => return None,
        })
    }

    /// Whether the atom can run computing from its subject alone
    pub(crate) fn forward(self, subject: &PTerm, ground: &dyn Fn(&PTerm) -> bool) -> bool {
        ground(subject)
    }

    /// Whether the atom can run in some mode with what is bound
    pub(crate) fn ready(
        self,
        subject: &PTerm,
        object: &PTerm,
        ground: &dyn Fn(&PTerm) -> bool,
    ) -> bool {
        match self {
            LiteralOp::Uri | LiteralOp::Dtlit | LiteralOp::Langlit => {
                ground(subject) || ground(object)
            }
            LiteralOp::Skolem | LiteralOp::RawType => ground(subject),
        }
    }

    /// Every extension of `binding` under which the atom holds
    pub(crate) fn evaluate(
        self,
        m: &mut Matcher<'_>,
        subject: &PTerm,
        object: &PTerm,
        binding: Binding,
    ) -> Vec<Binding> {
        let s = m.ground(subject, &binding);
        let o = m.ground(object, &binding);
        match self {
            LiteralOp::Uri => match (s.map(|id| m.node(id)), o.map(|id| m.node(id))) {
                (Some(Node::Iri(iri)), _) => {
                    let spelled = m.intern(&plain(iri));
                    m.unify_value(object, spelled, binding)
                }
                (
                    None,
                    Some(Node::Literal {
                        lexical,
                        datatype: None,
                        language: None,
                    }),
                ) if valid_iri(&lexical) => {
                    let iri = m.intern(&Node::Iri(lexical));
                    m.unify_value(subject, iri, binding)
                }
                _ => Vec::new(),
            },
            LiteralOp::Skolem => {
                let Some(id) = s else {
                    return Vec::new();
                };
                let name = format!("{SKOLEM_BASE}{}", fnv1a(&debug_text(m, id)));
                let iri = m.intern(&Node::Iri(name));
                m.unify_value(object, iri, binding)
            }
            LiteralOp::Dtlit | LiteralOp::Langlit => {
                if let Some(pair) = s.and_then(|id| m.list_items(id)) {
                    let [lexical, tag] = pair.as_slice() else {
                        return Vec::new();
                    };
                    let Some(lexical) = text(m, *lexical) else {
                        return Vec::new();
                    };
                    let tag = m.node(*tag);
                    let literal = match (self, tag) {
                        (LiteralOp::Langlit, tag) => match text_of(&tag) {
                            Some(language) if !language.is_empty() => Node::Literal {
                                lexical,
                                datatype: None,
                                language: Some(language),
                            },
                            _ => return Vec::new(),
                        },
                        (_, Node::Iri(datatype)) if datatype == XSD_STRING => plain(lexical),
                        (_, Node::Iri(datatype))
                            if datatype == RDF_LANG_STRING || datatype == RDF_DIR_LANG_STRING =>
                        {
                            return match o.map(|id| m.node(id)) {
                                Some(Node::Literal {
                                    lexical: given,
                                    language: Some(_),
                                    ..
                                }) if given == lexical => vec![binding],
                                _ => Vec::new(),
                            };
                        }
                        (_, Node::Iri(datatype)) => Node::Literal {
                            lexical,
                            datatype: Some(datatype),
                            language: None,
                        },
                        _ => return Vec::new(),
                    };
                    let id = m.intern(&literal);
                    return m.unify_value(object, id, binding);
                }
                let Some(Node::Literal {
                    lexical,
                    datatype,
                    language,
                }) = o.map(|id| m.node(id))
                else {
                    return Vec::new();
                };
                let second = match (self, language) {
                    (LiteralOp::Langlit, Some(language)) => plain(language),
                    (LiteralOp::Langlit, None) => return Vec::new(),
                    (_, Some(language)) => Node::Iri(
                        if language.contains("--") {
                            RDF_DIR_LANG_STRING
                        } else {
                            RDF_LANG_STRING
                        }
                        .to_string(),
                    ),
                    (_, None) => Node::Iri(datatype.unwrap_or_else(|| XSD_STRING.to_string())),
                };
                let first = m.intern(&plain(lexical));
                let second = m.intern(&second);
                let pair = m.store.list(vec![first, second]);
                m.unify_value(subject, pair, binding)
            }
            LiteralOp::RawType => {
                let Some(id) = s else {
                    return Vec::new();
                };
                let kind = match (m.store.compound(id), m.node(id)) {
                    (Some(Compound::Formula(_)), _) => LOG_FORMULA,
                    (Some(Compound::List(_)), _) => RDF_LIST,
                    (_, Node::Literal { .. }) => LOG_LITERAL,
                    _ => LOG_OTHER,
                };
                let kind = m.intern(&Node::Iri(kind.to_string()));
                m.unify_value(object, kind, binding)
            }
        }
    }
}

fn plain(text: String) -> Node {
    Node::Literal {
        lexical: text,
        datatype: None,
        language: None,
    }
}

fn text_of(node: &Node) -> Option<String> {
    match node {
        Node::Literal { lexical, .. } => Some(lexical.clone()),
        Node::Iri(iri) => Some(iri.clone()),
        _ => None,
    }
}

fn text(m: &Matcher<'_>, id: u32) -> Option<String> {
    text_of(&m.node(id))
}

/// Whether a string can be an IRI `log:uri` makes
fn valid_iri(value: &str) -> bool {
    !value.is_empty()
        && !value.chars().any(|c| {
            c.is_control()
                || c.is_whitespace()
                || matches!(c, '<' | '>' | '{' | '}' | '|' | '^' | '`' | '\\')
        })
}

/// A term written the way eyeron's `Debug` writes it, so skolem IRIs follow its scheme
fn debug_text(m: &Matcher<'_>, id: u32) -> String {
    if let Some(items) = m.list_items(id) {
        let members: Vec<String> = items.iter().map(|&item| debug_text(m, item)).collect();
        return format!("List([{}])", members.join(", "));
    }
    match m.node(id) {
        Node::Iri(iri) => format!("Iri({iri:?})"),
        Node::Blank(label) => format!("Blank({label:?})"),
        Node::Var(name) => format!("Var({name:?})"),
        Node::Literal {
            lexical,
            datatype,
            language,
        } => format!(
            "Literal(Literal {{ value: {lexical:?}, datatype: {datatype:?}, language: {language:?} }})"
        ),
        other => format!("{other}"),
    }
}

/// The 64-bit FNV-1a hash of `text` in hex
fn fnv1a(text: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}")
}
