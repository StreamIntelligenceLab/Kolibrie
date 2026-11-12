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
use std::collections::{HashMap, HashSet};

/// Parses an N3 document, resolving relative IRIs against `base`
///
/// # Errors
/// Returns [`N3Error::Syntax`] for malformed text and [`N3Error::Unsupported`] for N3 this
/// parser does not handle yet, such as explicit quantifiers
pub fn parse_document(text: &str, base: Option<&str>) -> Result<Document, N3Error> {
    parse_with(text, base, false, "")
}

/// Parses TriG: N3 statements plus named graphs `g { … }` and `GRAPH g { … }`, each read as
/// `g log:nameOf { … }`, and default-graph blocks `{ … }`
///
/// # Errors
/// As [`parse_document`]
pub fn parse_trig(text: &str, base: Option<&str>) -> Result<Document, N3Error> {
    parse_with(text, base, true, "")
}

/// Whether text is an RDF Message Log: a `VERSION "…-messages"` directive or a `MESSAGE` line
pub fn is_message_log(text: &str) -> bool {
    text.lines()
        .any(|line| is_message_version(line.trim()) || is_message_delimiter(line.trim()))
}

fn is_message_version(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    (lower.starts_with("version ") || lower.starts_with("@version ")) && lower.contains("-messages")
}

fn is_message_delimiter(line: &str) -> bool {
    line.eq_ignore_ascii_case("MESSAGE") || line.eq_ignore_ascii_case("@message .")
}

const MESSAGE: &str = "https://eyereasoner.github.io/eyeling/vocab/message#";
const STREAM: &str = "urn:eyeron:rdf-message-stream:1";

/// Parses an RDF Message Log into the replay view eyeron gives it: one stream, an ordered
/// envelope per message, and each non-empty message's triples as a named payload graph
///
/// # Errors
/// As [`parse_document`], for the prefixes and each message
pub fn parse_message_log(text: &str, base: Option<&str>) -> Result<Document, N3Error> {
    let mut prefixes = String::new();
    let mut messages = vec![String::new()];
    for line in text.lines() {
        let trimmed = line.trim();
        let directive = |keyword: &str, n3: &str| -> Option<String> {
            let head = trimmed.get(..keyword.len())?;
            if !head.eq_ignore_ascii_case(keyword) || !trimmed[keyword.len()..].starts_with(' ') {
                return None;
            }
            let rest = trimmed[keyword.len()..].trim();
            Some(format!(
                "{n3} {} .\n",
                rest.strip_suffix('.').unwrap_or(rest).trim()
            ))
        };
        if is_message_version(trimmed) {
            continue;
        }
        if is_message_delimiter(trimmed) {
            messages.push(String::new());
            continue;
        }
        if let Some(prefix) = directive("PREFIX", "@prefix").or_else(|| directive("BASE", "@base"))
        {
            prefixes.push_str(&prefix);
            continue;
        }
        if let Some(current) = messages.last_mut() {
            current.push_str(line);
            current.push('\n');
        }
    }
    let iri = |text: String| Node::Iri(text);
    let message = |local: &str| iri(format!("{MESSAGE}{local}"));
    let stream = iri(STREAM.to_string());
    let envelopes: Vec<Node> = (1..=messages.len())
        .map(|i| iri(format!("{STREAM}:envelope:{i}")))
        .collect();
    let mut document = parse_document(&prefixes, base)?;
    let facts = &mut document.statements;
    facts.push(Statement::new(
        stream.clone(),
        iri(RDF_TYPE.to_string()),
        message("RDFMessageStream"),
    ));
    facts.push(Statement::new(
        stream.clone(),
        message("orderedEnvelopes"),
        Node::List(envelopes.clone()),
    ));
    if let Some(first) = envelopes.first() {
        facts.push(Statement::new(
            stream.clone(),
            message("firstEnvelope"),
            first.clone(),
        ));
    }
    for (index, body) in messages.iter().enumerate() {
        let envelope = envelopes[index].clone();
        facts.push(Statement::new(
            stream.clone(),
            message("envelope"),
            envelope.clone(),
        ));
        facts.push(Statement::new(
            envelope.clone(),
            iri(RDF_TYPE.to_string()),
            message("MessageEnvelope"),
        ));
        facts.push(Statement::new(
            envelope.clone(),
            message("offset"),
            Node::typed(index.to_string(), XSD_INTEGER),
        ));
        if let Some(next) = envelopes.get(index + 1) {
            facts.push(Statement::new(
                envelope.clone(),
                message("nextEnvelope"),
                next.clone(),
            ));
        }
        let empty = body.lines().all(|line| {
            let trimmed = line.trim();
            trimmed.is_empty() || trimmed.starts_with('#')
        });
        if empty {
            facts.push(Statement::new(
                envelope,
                message("payloadKind"),
                message("empty"),
            ));
            continue;
        }
        // Labels are the message's own, spelled as eyeron spells them so proofs can name them.
        let payload_document = parse_with(
            &format!("{prefixes}\n{body}"),
            base,
            false,
            &format!("m{}_", index + 1),
        )?;
        let payload = iri(format!("{STREAM}:payload:{}", index + 1));
        facts.push(Statement::new(
            envelope.clone(),
            message("payloadKind"),
            message("nonEmpty"),
        ));
        facts.push(Statement::new(
            envelope,
            message("payloadGraph"),
            payload.clone(),
        ));
        facts.push(Statement::new(
            payload,
            iri(format!("{LOG}nameOf")),
            Node::Formula(payload_document.statements),
        ));
    }
    Ok(document)
}

/// Parses `text`, writing `label_prefix` before every blank node label the text spells
fn parse_with(
    text: &str,
    base: Option<&str>,
    trig: bool,
    label_prefix: &str,
) -> Result<Document, N3Error> {
    let mut tokens = tokenize(text)?;
    if !label_prefix.is_empty() {
        for token in &mut tokens {
            if let TokenKind::Blank(label) = &mut token.kind {
                label.insert_str(0, label_prefix);
            }
        }
    }
    let source_labels = tokens
        .iter()
        .filter_map(|token| match &token.kind {
            TokenKind::Blank(label) | TokenKind::Var(label) => Some(label.clone()),
            _ => None,
        })
        .collect();
    let mut parser = Parser {
        trig,
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
        quantifiers: vec![HashMap::new()],
        quantified: 0,
        local_names: false,
        source_labels,
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
    trig: bool,
    tokens: Vec<Token>,
    pos: usize,
    prefixes: HashMap<String, String>,
    document: Document,
    base: Option<String>,
    scope: u64,
    fresh: usize,
    formulas: usize,
    blank_scopes: Vec<HashMap<String, String>>,
    /// The IRIs `@forAll` and `@forSome` bind in each open graph, outermost first
    quantifiers: Vec<HashMap<String, Node>>,
    quantified: usize,
    /// Whether some formula already named its binders canonically, so later ones must shift them
    local_names: bool,
    /// The blank node and variable labels the source spells, which generated names avoid
    source_labels: HashSet<String>,
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
            if self.trig && self.parse_graph_block()? {
                continue;
            }
            self.parse_triples(&mut out)?;
            self.expect(&TokenKind::Dot, "'.'")?;
            self.document.statements.extend(out);
        }
        Ok(())
    }

    /// In TriG, reads `g { … }`, `GRAPH g { … }` or a default-graph `{ … }`; false if the next
    /// tokens start none of them
    fn parse_graph_block(&mut self) -> Result<bool, N3Error> {
        let next = self.tokens.get(self.pos + 1).map(|t| t.kind.clone());
        if *self.peek() == TokenKind::LBrace {
            let mut out = Vec::new();
            let Node::Formula(statements) = self.parse_term(&mut out)? else {
                return Err(N3Error::at("expected a graph", self.offset()));
            };
            self.document.statements.extend(statements);
            return Ok(true);
        }
        let keyword = is_word_ignore_case(self.peek(), "GRAPH");
        let labelled = match self.peek() {
            TokenKind::Iri(_) | TokenKind::PName(_) | TokenKind::Blank(_) => {
                next == Some(TokenKind::LBrace) || keyword
            }
            _ => false,
        };
        if !labelled {
            return Ok(false);
        }
        if keyword {
            self.advance();
        }
        let mut out = Vec::new();
        let label = self.parse_term(&mut out)?;
        let graph = self.parse_term(&mut out)?;
        if !matches!(graph, Node::Formula(_)) {
            return Err(N3Error::at(
                "expected '{' after a graph name",
                self.offset(),
            ));
        }
        self.document.statements.push(Statement::new(
            label,
            Node::iri(format!("{LOG}nameOf")),
            graph,
        ));
        self.document.statements.extend(out);
        if *self.peek() == TokenKind::Dot {
            self.advance();
        }
        Ok(true)
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
            TokenKind::AtKeyword(word) if word == "forAll" || word == "forSome" => {
                let universal = word == "forAll";
                self.advance();
                loop {
                    let offset = self.offset();
                    let iri = match self.advance() {
                        TokenKind::Iri(iri) => self.resolve(&iri),
                        TokenKind::PName(name) => self.expand(&name, offset)?,
                        _ => return Err(N3Error::at("a quantifier names IRIs", offset)),
                    };
                    self.quantified += 1;
                    let node = if universal {
                        Node::Var(format!("__q{}", self.quantified))
                    } else if self.blank_scopes.len() == 1 {
                        Node::Blank(format!(
                            "kolibrie-doc-{}-.__q{}",
                            self.scope, self.quantified
                        ))
                    } else {
                        Node::Blank(format!(
                            "kolibrie-doc-{}-.f{}-.__q{}",
                            self.scope, self.formulas, self.quantified
                        ))
                    };
                    let depth = self.blank_scopes.len();
                    self.quantifiers[depth - 1].insert(iri, node);
                    if *self.peek() != TokenKind::Comma {
                        break;
                    }
                    self.advance();
                }
                self.expect(&TokenKind::Dot, "'.' after a quantifier")?;
                Ok(true)
            }
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
                // eyeron lists a statement before those its object's brackets imply
                let mut nested = Vec::new();
                let object = self.parse_path(&mut nested)?;
                let statement = if inverse {
                    Statement::new(object, verb.clone(), subject.clone())
                } else {
                    Statement::new(subject.clone(), verb.clone(), object)
                };
                out.push(rule_sides(statement));
                out.append(&mut nested);
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
            let next = self.fresh_blank("path");
            if forward {
                out.push(Statement::new(current, predicate, next.clone()));
            } else {
                out.push(Statement::new(next.clone(), predicate, current));
            }
            current = next;
        }
    }

    /// A blank node the syntax implies, named as eyeron names it, `kind` then a counter shared by
    /// every kind, skipping labels the source spells itself
    fn fresh_blank(&mut self, kind: &str) -> Node {
        loop {
            self.fresh += 1;
            let candidate = format!("{kind}{}", self.fresh);
            if !self.source_labels.contains(&candidate) {
                return Node::Blank(if self.blank_scopes.len() > 1 {
                    format!(
                        "kolibrie-doc-{}-.f{}-.{candidate}",
                        self.scope, self.formulas
                    )
                } else {
                    format!("kolibrie-doc-{}-.{candidate}", self.scope)
                });
            }
        }
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
            TokenKind::Iri(iri) => {
                let iri = self.resolve(&iri);
                Ok(self.quantified_or(iri))
            }
            TokenKind::PName(name) => self.pname_term(&name, offset),
            TokenKind::Var(name) => Ok(Node::Var(name)),
            TokenKind::Blank(label) => Ok(self.scoped_blank(&label)),
            TokenKind::Str(lexical) => self.finish_literal(lexical),
            TokenKind::Number(text) => Ok(number_literal(&text)),
            TokenKind::LBrace => self.parse_formula(),
            TokenKind::LBracket => {
                let blank = self.fresh_blank("b");
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
            TokenKind::LTriple => {
                let parenthesised = *self.peek() == TokenKind::LParen;
                if parenthesised {
                    self.advance();
                }
                let subject = self.parse_term(out)?;
                let (predicate, _) = self.parse_verb(out)?;
                let object = self.parse_term(out)?;
                if parenthesised {
                    self.expect(&TokenKind::RParen, "')'")?;
                }
                self.expect(&TokenKind::RTriple, "'>>'")?;
                let triple = Node::Formula(vec![Statement::new(subject, predicate, object)]);
                if parenthesised {
                    return Ok(triple);
                }
                let reifier = self.fresh_blank("reif");
                out.push(Statement::new(
                    reifier.clone(),
                    Node::iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"),
                    triple,
                ));
                Ok(reifier)
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
        self.quantifiers.push(HashMap::new());
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
        let own: Vec<Node> = self
            .quantifiers
            .pop()
            .unwrap_or_default()
            .into_values()
            .collect();
        if own.is_empty() && !self.local_names {
            return Ok(Node::Formula(statements));
        }
        self.local_names = true;
        Ok(Node::Formula(canonical_binders(statements, &own)))
    }

    fn pname_term(&mut self, name: &str, offset: usize) -> Result<Node, N3Error> {
        match name {
            "a" => return Ok(Node::iri(RDF_TYPE)),
            "true" | "false" => return Ok(Node::typed(name, XSD_BOOLEAN)),
            _ => {}
        }
        let iri = self.expand(name, offset)?;
        Ok(self.quantified_or(iri))
    }

    /// The variable or blank node a quantifier binds `iri` to, innermost first, or the IRI
    fn quantified_or(&self, iri: String) -> Node {
        self.quantifiers
            .iter()
            .rev()
            .find_map(|scope| scope.get(&iri).cloned())
            .unwrap_or(Node::Iri(iri))
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

/// The prefix of a variable a formula's own `@forAll` binds, once named canonically
pub(crate) const LOCAL_UNIVERSAL: &str = "__l";
/// The prefix of a blank node a formula's own `@forSome` binds, once named canonically
pub(crate) const LOCAL_EXISTENTIAL: &str = "kolibrie-l";

/// A formula's statements with the names its own quantifiers bind renamed by order of first
/// occurrence, so formulas that differ only in those names are the same term
///
/// A name is the binder's depth above the term's own formula and a counter: `__l0.1` is this
/// formula's first universal, and a nested formula's own names move from `__l0.n` to `__l1.n`.
fn canonical_binders(statements: Vec<Statement>, own: &[Node]) -> Vec<Statement> {
    fn visit(node: &Node, own: &[Node], order: &mut Vec<Node>) {
        match node {
            Node::List(items) => items.iter().for_each(|item| visit(item, own, order)),
            Node::Formula(inner) => inner.iter().for_each(|s| visit_statement(s, own, order)),
            other if own.contains(other) && !order.contains(other) => order.push(other.clone()),
            _ => {}
        }
    }
    fn visit_statement(statement: &Statement, own: &[Node], order: &mut Vec<Node>) {
        for node in [&statement.subject, &statement.predicate, &statement.object] {
            visit(node, own, order);
        }
    }
    fn shift(name: &str, prefix: &str) -> Option<String> {
        let (depth, rest) = name.strip_prefix(prefix)?.split_once('.')?;
        let depth: usize = depth.parse().ok()?;
        Some(format!("{prefix}{}.{rest}", depth + 1))
    }
    fn rename(node: &Node, order: &[Node]) -> Node {
        if let Some(position) = order.iter().position(|o| o == node) {
            return match node {
                Node::Var(_) => Node::Var(format!("{LOCAL_UNIVERSAL}0.{}", position + 1)),
                _ => Node::Blank(format!("{LOCAL_EXISTENTIAL}0.{}", position + 1)),
            };
        }
        match node {
            Node::Var(name) => {
                Node::Var(shift(name, LOCAL_UNIVERSAL).unwrap_or_else(|| name.clone()))
            }
            Node::Blank(label) => {
                Node::Blank(shift(label, LOCAL_EXISTENTIAL).unwrap_or_else(|| label.clone()))
            }
            Node::List(items) => Node::List(items.iter().map(|i| rename(i, order)).collect()),
            Node::Formula(inner) => {
                Node::Formula(inner.iter().map(|s| rename_statement(s, order)).collect())
            }
            other => other.clone(),
        }
    }
    fn rename_statement(statement: &Statement, order: &[Node]) -> Statement {
        Statement::new(
            rename(&statement.subject, order),
            rename(&statement.predicate, order),
            rename(&statement.object, order),
        )
    }
    let mut order = Vec::new();
    for statement in &statements {
        visit_statement(statement, own, &mut order);
    }
    statements
        .iter()
        .map(|s| rename_statement(s, &order))
        .collect()
}

/// A `log:implies` or `log:impliedBy` statement with `true` as a side read as the empty formula,
/// as N3 means it
fn rule_sides(statement: Statement) -> Statement {
    let is_rule = matches!(&statement.predicate,
        Node::Iri(iri) if iri == LOG_IMPLIES || iri == LOG_IMPLIED_BY);
    if !is_rule {
        return statement;
    }
    let side = |node: Node| match &node {
        Node::Literal {
            lexical,
            datatype: Some(datatype),
            ..
        } if lexical == "true" && datatype == XSD_BOOLEAN => Node::Formula(Vec::new()),
        _ => node,
    };
    Statement::new(
        side(statement.subject),
        statement.predicate,
        side(statement.object),
    )
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
