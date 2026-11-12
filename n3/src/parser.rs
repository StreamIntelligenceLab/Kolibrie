/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Recursive-descent parser from N3 text to a [`Document`]

use crate::error::N3Error;
use crate::lexer::{tokenize, Token, TokenKind};
use crate::syntax::*;
use std::collections::HashMap;

/// Parses an N3 document, resolving relative IRIs against `base`
///
/// # Errors
/// Returns [`N3Error::Syntax`] for malformed text and [`N3Error::Unsupported`] for N3 this
/// parser does not handle yet, such as explicit quantifiers
pub fn parse_document(text: &str, base: Option<&str>) -> Result<Document, N3Error> {
    let tokens = tokenize(text)?;
    let mut parser = Parser {
        tokens,
        pos: 0,
        prefixes: WELL_KNOWN_PREFIXES
            .iter()
            .map(|(prefix, iri)| (prefix.to_string(), iri.to_string()))
            .collect(),
        document: Document::default(),
        base: base.map(str::to_string),
        scope: shared::rdf_term::next_document_scope(),
        fresh: 0,
        formulas: 0,
        blank_scopes: vec![HashMap::new()],
    };
    parser.parse()?;
    Ok(parser.document)
}

/// Prefixes usable without a declaration, as eyeron and cwm predefine them
const WELL_KNOWN_PREFIXES: [(&str, &str); 10] = [
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("log", "http://www.w3.org/2000/10/swap/log#"),
    ("math", "http://www.w3.org/2000/10/swap/math#"),
    ("string", "http://www.w3.org/2000/10/swap/string#"),
    ("list", "http://www.w3.org/2000/10/swap/list#"),
    ("time", "http://www.w3.org/2000/10/swap/time#"),
    ("crypto", "http://www.w3.org/2000/10/swap/crypto#"),
];

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    prefixes: HashMap<String, String>,
    document: Document,
    base: Option<String>,
    scope: u64,
    fresh: usize,
    formulas: usize,
    blank_scopes: Vec<HashMap<String, String>>,
}

fn is_word(kind: &TokenKind, word: &str) -> bool {
    matches!(kind, TokenKind::PName(p) if p == word)
}

fn is_word_ignore_case(kind: &TokenKind, word: &str) -> bool {
    matches!(kind, TokenKind::PName(p) if p.eq_ignore_ascii_case(word))
}

impl Parser {
    fn peek(&self) -> &TokenKind {
        &self.tokens[self.pos].kind
    }

    fn offset(&self) -> usize {
        self.tokens[self.pos].offset
    }

    fn advance(&mut self) -> TokenKind {
        let kind = self.tokens[self.pos].kind.clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        kind
    }

    fn expect(&mut self, expected: &TokenKind, what: &str) -> Result<(), N3Error> {
        if self.peek() == expected {
            self.advance();
            Ok(())
        } else {
            Err(N3Error::at(
                format!("expected {what}, found {:?}", self.peek()),
                self.offset(),
            ))
        }
    }

    fn parse(&mut self) -> Result<(), N3Error> {
        while *self.peek() != TokenKind::Eof {
            let mut out = Vec::new();
            if self.parse_directive()? {
                continue;
            }
            if *self.peek() == TokenKind::Dot {
                self.advance();
                continue;
            }
            self.parse_triples(&mut out)?;
            self.expect(&TokenKind::Dot, "'.'")?;
            self.document.statements.extend(out);
        }
        Ok(())
    }

    /// Handles a prefix, base or quantifier directive; false if the next token starts none
    fn parse_directive(&mut self) -> Result<bool, N3Error> {
        let kind = self.peek().clone();
        match &kind {
            TokenKind::AtKeyword(word) if word == "prefix" => {
                self.advance();
                self.parse_prefix_body()?;
                self.expect(&TokenKind::Dot, "'.' after @prefix")?;
                Ok(true)
            }
            k if is_word_ignore_case(k, "PREFIX") && self.next_is_prefix_name() => {
                self.advance();
                self.parse_prefix_body()?;
                Ok(true)
            }
            TokenKind::AtKeyword(word) if word == "base" => {
                self.advance();
                self.parse_base_body()?;
                self.expect(&TokenKind::Dot, "'.' after @base")?;
                Ok(true)
            }
            k if is_word_ignore_case(k, "BASE")
                && matches!(
                    self.tokens.get(self.pos + 1).map(|t| &t.kind),
                    Some(TokenKind::Iri(_))
                ) =>
            {
                self.advance();
                self.parse_base_body()?;
                Ok(true)
            }
            TokenKind::AtKeyword(word) if word == "version" => {
                self.advance();
                self.parse_version_body()?;
                self.expect(&TokenKind::Dot, "'.' after @version")?;
                Ok(true)
            }
            k if is_word_ignore_case(k, "VERSION")
                && matches!(
                    self.tokens.get(self.pos + 1).map(|t| &t.kind),
                    Some(TokenKind::Str(_))
                ) =>
            {
                self.advance();
                self.parse_version_body()?;
                Ok(true)
            }
            TokenKind::AtKeyword(word) if word == "forAll" || word == "forSome" => Err(
                N3Error::unsupported(format!("explicit quantification (@{word})")),
            ),
            TokenKind::AtKeyword(word) if word == "keywords" => {
                Err(N3Error::unsupported("@keywords"))
            }
            _ => Ok(false),
        }
    }

    fn next_is_prefix_name(&self) -> bool {
        matches!(
            self.tokens.get(self.pos + 1).map(|t| &t.kind),
            Some(TokenKind::PName(name)) if name.ends_with(':')
        )
    }

    fn parse_prefix_body(&mut self) -> Result<(), N3Error> {
        let offset = self.offset();
        let TokenKind::PName(name) = self.advance() else {
            return Err(N3Error::at("expected a prefix name", offset));
        };
        let Some(prefix) = name.strip_suffix(':') else {
            return Err(N3Error::at("a prefix name ends with ':'", offset));
        };
        let offset = self.offset();
        let TokenKind::Iri(iri) = self.advance() else {
            return Err(N3Error::at("expected the prefix IRI", offset));
        };
        let iri = self.resolve(&iri);
        self.prefixes.insert(prefix.to_string(), iri.clone());
        self.document.prefixes.push((prefix.to_string(), iri));
        Ok(())
    }

    fn parse_version_body(&mut self) -> Result<(), N3Error> {
        let offset = self.offset();
        match self.advance() {
            TokenKind::Str(_) => Ok(()),
            _ => Err(N3Error::at("expected the version string", offset)),
        }
    }

    fn parse_base_body(&mut self) -> Result<(), N3Error> {
        let offset = self.offset();
        let TokenKind::Iri(iri) = self.advance() else {
            return Err(N3Error::at("expected the base IRI", offset));
        };
        self.base = Some(self.resolve(&iri));
        Ok(())
    }

    fn at_statement_end(&self) -> bool {
        matches!(
            self.peek(),
            TokenKind::Dot | TokenKind::RBrace | TokenKind::RBracket | TokenKind::Eof
        )
    }

    fn parse_triples(&mut self, out: &mut Vec<Statement>) -> Result<(), N3Error> {
        let bracketed = *self.peek() == TokenKind::LBracket;
        let bare_true = is_word(self.peek(), "true");
        let subject = self.parse_path(out)?;
        if (bracketed || bare_true) && self.at_statement_end() {
            return Ok(());
        }
        self.parse_predicate_object_list(&subject, out)
    }

    fn parse_predicate_object_list(
        &mut self,
        subject: &Node,
        out: &mut Vec<Statement>,
    ) -> Result<(), N3Error> {
        loop {
            let (verb, inverse) = self.parse_verb(out)?;
            loop {
                let object = self.parse_path(out)?;
                if inverse {
                    out.push(Statement::new(object, verb.clone(), subject.clone()));
                } else {
                    out.push(Statement::new(subject.clone(), verb.clone(), object));
                }
                if *self.peek() == TokenKind::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            if *self.peek() != TokenKind::Semicolon {
                return Ok(());
            }
            while *self.peek() == TokenKind::Semicolon {
                self.advance();
            }
            if self.at_statement_end() {
                return Ok(());
            }
        }
    }

    /// Reads a verb, returning it and whether subject and object swap
    fn parse_verb(&mut self, out: &mut Vec<Statement>) -> Result<(Node, bool), N3Error> {
        let kind = self.peek().clone();
        match kind {
            TokenKind::Equals => {
                self.advance();
                Ok((Node::iri(OWL_SAME_AS), false))
            }
            TokenKind::Arrow => {
                self.advance();
                Ok((Node::iri(LOG_IMPLIES), false))
            }
            TokenKind::BackArrow => {
                self.advance();
                Ok((Node::iri(LOG_IMPLIED_BY), false))
            }
            ref k if is_word(k, "a") || matches!(k, TokenKind::AtKeyword(w) if w == "a") => {
                self.advance();
                Ok((Node::iri(RDF_TYPE), false))
            }
            ref k if is_word(k, "has") || matches!(k, TokenKind::AtKeyword(w) if w == "has") => {
                self.advance();
                Ok((self.parse_path(out)?, false))
            }
            ref k if is_word(k, "is") || matches!(k, TokenKind::AtKeyword(w) if w == "is") => {
                self.advance();
                let verb = self.parse_path(out)?;
                let offset = self.offset();
                let closing = self.advance();
                if !(is_word(&closing, "of")
                    || matches!(&closing, TokenKind::AtKeyword(w) if w == "of"))
                {
                    return Err(N3Error::at("expected 'of' after 'is <verb>'", offset));
                }
                Ok((verb, true))
            }
            TokenKind::Caret => Err(N3Error::unsupported("inverse verb '^'")),
            _ => Ok((self.parse_path(out)?, false)),
        }
    }

    fn parse_path(&mut self, out: &mut Vec<Statement>) -> Result<Node, N3Error> {
        let mut current = self.parse_term(out)?;
        loop {
            let forward = match self.peek() {
                TokenKind::Bang => true,
                TokenKind::Caret => false,
                _ => return Ok(current),
            };
            self.advance();
            let predicate = self.parse_term(out)?;
            let next = self.fresh_blank();
            if forward {
                out.push(Statement::new(current, predicate, next.clone()));
            } else {
                out.push(Statement::new(next.clone(), predicate, current));
            }
            current = next;
        }
    }

    fn fresh_blank(&mut self) -> Node {
        self.fresh += 1;
        Node::Blank(format!("kolibrie-doc-{}-.g{}", self.scope, self.fresh))
    }

    fn scoped_blank(&mut self, label: &str) -> Node {
        let depth = self.blank_scopes.len();
        if let Some(existing) = self.blank_scopes[depth - 1].get(label) {
            return Node::Blank(existing.clone());
        }
        let unique = if depth == 1 {
            shared::rdf_term::spell_blank_node(label, self.scope)
                .trim_start_matches("_:")
                .to_string()
        } else {
            format!("kolibrie-doc-{}-.f{}-{label}", self.scope, self.formulas)
        };
        self.blank_scopes[depth - 1].insert(label.to_string(), unique.clone());
        Node::Blank(unique)
    }

    fn parse_term(&mut self, out: &mut Vec<Statement>) -> Result<Node, N3Error> {
        let offset = self.offset();
        match self.advance() {
            TokenKind::Iri(iri) => Ok(Node::Iri(self.resolve(&iri))),
            TokenKind::PName(name) => self.pname_term(&name, offset),
            TokenKind::Var(name) => Ok(Node::Var(name)),
            TokenKind::Blank(label) => Ok(self.scoped_blank(&label)),
            TokenKind::Str(lexical) => self.finish_literal(lexical),
            TokenKind::Number(text) => Ok(number_literal(&text)),
            TokenKind::LBrace => self.parse_formula(),
            TokenKind::LBracket => {
                let blank = self.fresh_blank();
                if *self.peek() == TokenKind::RBracket {
                    self.advance();
                    return Ok(blank);
                }
                if is_word(self.peek(), "id") {
                    return Err(N3Error::unsupported("'[ id … ]' property lists"));
                }
                self.parse_predicate_object_list(&blank, out)?;
                self.expect(&TokenKind::RBracket, "']'")?;
                Ok(blank)
            }
            TokenKind::LParen => {
                let mut items = Vec::new();
                while *self.peek() != TokenKind::RParen {
                    if *self.peek() == TokenKind::Eof {
                        return Err(N3Error::at("unterminated list", offset));
                    }
                    items.push(self.parse_path(out)?);
                }
                self.advance();
                Ok(Node::List(items))
            }
            TokenKind::AtKeyword(word) if word == "a" => Ok(Node::iri(RDF_TYPE)),
            TokenKind::AtKeyword(word) if word == "true" || word == "false" => {
                Ok(Node::typed(word, XSD_BOOLEAN))
            }
            other => Err(N3Error::at(
                format!("expected a term, found {other:?}"),
                offset,
            )),
        }
    }

    fn parse_formula(&mut self) -> Result<Node, N3Error> {
        self.formulas += 1;
        self.blank_scopes.push(HashMap::new());
        let mut statements = Vec::new();
        loop {
            match self.peek() {
                TokenKind::RBrace => {
                    self.advance();
                    break;
                }
                TokenKind::Eof => {
                    return Err(N3Error::at("unterminated formula", self.offset()));
                }
                TokenKind::Dot => {
                    self.advance();
                }
                _ => {
                    if self.parse_directive()? {
                        continue;
                    }
                    self.parse_triples(&mut statements)?;
                    if !matches!(self.peek(), TokenKind::Dot | TokenKind::RBrace) {
                        return Err(N3Error::at(
                            "expected '.' or '}' in a formula",
                            self.offset(),
                        ));
                    }
                }
            }
        }
        self.blank_scopes.pop();
        Ok(Node::Formula(statements))
    }

    fn pname_term(&mut self, name: &str, offset: usize) -> Result<Node, N3Error> {
        match name {
            "a" => return Ok(Node::iri(RDF_TYPE)),
            "true" | "false" => return Ok(Node::typed(name, XSD_BOOLEAN)),
            _ => {}
        }
        Ok(Node::Iri(self.expand(name, offset)?))
    }

    fn expand(&self, name: &str, offset: usize) -> Result<String, N3Error> {
        if let Some((prefix, local)) = name.split_once(':') {
            let Some(namespace) = self.prefixes.get(prefix) else {
                return Err(N3Error::at(format!("unknown prefix '{prefix}:'"), offset));
            };
            return Ok(format!("{namespace}{local}"));
        }
        match self.prefixes.get("") {
            Some(namespace) => Ok(format!("{namespace}{name}")),
            None => Err(N3Error::at(
                format!("bare word '{name}' with no ':' prefix"),
                offset,
            )),
        }
    }

    fn finish_literal(&mut self, lexical: String) -> Result<Node, N3Error> {
        match self.peek().clone() {
            TokenKind::Lang(tag) => {
                self.advance();
                Ok(Node::Literal {
                    lexical,
                    datatype: None,
                    language: Some(tag.to_ascii_lowercase()),
                })
            }
            TokenKind::HatHat => {
                self.advance();
                let offset = self.offset();
                let datatype = match self.advance() {
                    TokenKind::Iri(iri) => self.resolve(&iri),
                    TokenKind::PName(name) => self.expand(&name, offset)?,
                    _ => return Err(N3Error::at("a datatype must be an IRI", offset)),
                };
                Ok(Node::Literal {
                    lexical,
                    datatype: (datatype != shared::rdf_term::XSD_STRING).then_some(datatype),
                    language: None,
                })
            }
            _ => Ok(Node::Literal {
                lexical,
                datatype: None,
                language: None,
            }),
        }
    }

    fn resolve(&self, reference: &str) -> String {
        match &self.base {
            Some(base) => resolve_iri(base, reference),
            None => reference.to_string(),
        }
    }
}

fn number_literal(text: &str) -> Node {
    let datatype = if text.contains(['e', 'E']) {
        XSD_DOUBLE
    } else if text.contains('.') {
        XSD_DECIMAL
    } else {
        XSD_INTEGER
    };
    Node::typed(text, datatype)
}

fn has_scheme(reference: &str) -> bool {
    match reference.find(':') {
        Some(colon) => {
            let scheme = &reference[..colon];
            !scheme.is_empty()
                && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        }
        None => false,
    }
}

fn remove_dot_segments(path: &str) -> String {
    let mut output: Vec<&str> = Vec::new();
    let segments: Vec<&str> = path.split('/').collect();
    for (index, segment) in segments.iter().enumerate() {
        let last = index + 1 == segments.len();
        match *segment {
            "." => {
                if last {
                    output.push("");
                }
            }
            ".." => {
                if output.len() > 1 {
                    output.pop();
                }
                if last {
                    output.push("");
                }
            }
            other => output.push(other),
        }
    }
    output.join("/")
}

/// Resolves `reference` against `base` as RFC 3986 §5.2 describes
pub fn resolve_iri(base: &str, reference: &str) -> String {
    if has_scheme(reference) {
        return reference.to_string();
    }
    let (base_no_fragment, _) = base.split_once('#').unwrap_or((base, ""));
    if reference.is_empty() {
        return base_no_fragment.to_string();
    }
    if reference.starts_with('#') {
        return format!("{base_no_fragment}{reference}");
    }
    let scheme_end = base.find(':').map_or(0, |i| i + 1);
    let scheme = &base[..scheme_end];
    let rest = &base[scheme_end..];
    if let Some(network) = reference.strip_prefix("//") {
        return format!("{scheme}//{network}");
    }
    let (authority, base_path) = match rest.strip_prefix("//") {
        Some(after) => {
            let end = after.find(['/', '?', '#']).unwrap_or(after.len());
            (format!("//{}", &after[..end]), &after[end..])
        }
        None => (String::new(), rest),
    };
    let base_path = base_path.split(['?', '#']).next().unwrap_or("");
    if reference.starts_with('?') {
        return format!("{scheme}{authority}{base_path}{reference}");
    }
    let (ref_path, ref_tail) = match reference.find(['?', '#']) {
        Some(i) => (&reference[..i], &reference[i..]),
        None => (reference, ""),
    };
    let merged = if ref_path.starts_with('/') {
        ref_path.to_string()
    } else {
        match base_path.rfind('/') {
            Some(slash) => format!("{}{}", &base_path[..=slash], ref_path),
            None if !authority.is_empty() => format!("/{ref_path}"),
            None => ref_path.to_string(),
        }
    };
    format!(
        "{scheme}{authority}{}{ref_tail}",
        remove_dot_segments(&merged)
    )
}
