/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::error::RdfMessageError;
use super::format::MessageBaseFormat;

/// A lexical unit produced by the scanner
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Chunk {
    /// A `VERSION` / `@version` announcement
    Version(String),
    /// A prefix directive; `name` excludes the trailing colon
    Prefix { name: String, iri: String },
    /// A base directive
    Base(String),
    /// A `MESSAGE` / `@message .` delimiter
    Message,
    /// One triple/quad or TriG graph block, as raw text
    Statement(String),
}

/// Scan a document into chunks per its base format
pub(crate) fn scan(input: &str, format: MessageBaseFormat) -> Result<Vec<Chunk>, RdfMessageError> {
    if format.is_line_based() {
        scan_line_based(input)
    } else {
        Lexer::new(input).scan_all()
    }
}

fn scan_line_based(input: &str) -> Result<Vec<Chunk>, RdfMessageError> {
    let mut chunks = Vec::new();
    for raw_line in input.lines() {
        let code = strip_line_comment(raw_line).trim();
        if code.is_empty() {
            continue;
        }

        let (head, rest) = split_first_token(code);
        match head {
            "MESSAGE" if rest.trim().is_empty() => {
                chunks.push(Chunk::Message);
                continue;
            }
            "VERSION" => {
                let label = rest.trim();
                if label.is_empty() {
                    return Err(RdfMessageError::MalformedDirective(code.to_string()));
                }
                chunks.push(Chunk::Version(label.to_string()));
                continue;
            }
            _ => {}
        }

        chunks.push(Chunk::Statement(code.to_string()));
    }
    Ok(chunks)
}

/// The part of `line` before an unquoted `#` comment
fn strip_line_comment(line: &str) -> &str {
    let mut in_iri = false;
    let mut in_literal = false;
    let mut escaped = false;
    for (idx, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_literal => escaped = true,
            '"' if !in_iri => in_literal = !in_literal,
            '<' if !in_literal => in_iri = true,
            '>' if in_iri => in_iri = false,
            '#' if !in_iri && !in_literal => return &line[..idx],
            _ => {}
        }
    }
    line
}

fn split_first_token(s: &str) -> (&str, &str) {
    match s.find(char::is_whitespace) {
        Some(idx) => (&s[..idx], &s[idx..]),
        None => (s, ""),
    }
}

struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    fn new(input: &str) -> Self {
        Lexer {
            chars: input.chars().collect(),
            pos: 0,
        }
    }

    fn eof(&self) -> bool {
        self.pos >= self.chars.len()
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, k: usize) -> Option<char> {
        self.chars.get(self.pos + k).copied()
    }

    fn scan_all(&mut self) -> Result<Vec<Chunk>, RdfMessageError> {
        let mut chunks = Vec::new();
        loop {
            self.skip_ws_and_comments();
            if self.eof() {
                break;
            }
            chunks.push(self.next_chunk()?);
        }
        Ok(chunks)
    }

    /// Consume the next directive or statement
    fn next_chunk(&mut self) -> Result<Chunk, RdfMessageError> {
        let keyword = self.peek_keyword();
        match keyword.as_str() {
            "MESSAGE" => {
                self.consume_word();
                Ok(Chunk::Message)
            }
            "PREFIX" => {
                self.consume_word();
                let (name, iri) = self.read_prefix_body()?;
                Ok(Chunk::Prefix { name, iri })
            }
            "BASE" => {
                self.consume_word();
                self.skip_ws_and_comments();
                let iri = self.read_iri()?;
                Ok(Chunk::Base(iri))
            }
            "VERSION" => {
                self.consume_word();
                self.skip_ws_and_comments();
                let label = self.read_string_literal()?;
                Ok(Chunk::Version(label))
            }
            "@prefix" => {
                self.consume_word();
                let (name, iri) = self.read_prefix_body()?;
                self.expect_dot()?;
                Ok(Chunk::Prefix { name, iri })
            }
            "@base" => {
                self.consume_word();
                self.skip_ws_and_comments();
                let iri = self.read_iri()?;
                self.expect_dot()?;
                Ok(Chunk::Base(iri))
            }
            "@version" => {
                self.consume_word();
                self.skip_ws_and_comments();
                let label = self.read_string_literal()?;
                self.expect_dot()?;
                Ok(Chunk::Version(label))
            }
            "@message" => {
                self.consume_word();
                self.expect_dot()?;
                Ok(Chunk::Message)
            }
            _ => {
                let statement = self.read_statement()?;
                Ok(Chunk::Statement(statement))
            }
        }
    }

    /// Peek the leading keyword token (`[A-Za-z@]+`)
    fn peek_keyword(&self) -> String {
        let mut out = String::new();
        let mut k = 0;
        while let Some(c) = self.peek_at(k) {
            if c == '@' || c.is_ascii_alphabetic() {
                out.push(c);
                k += 1;
            } else {
                break;
            }
        }
        out
    }

    /// Consume a leading keyword token seen with [`peek_keyword`]
    fn consume_word(&mut self) {
        while let Some(c) = self.peek() {
            if c == '@' || c.is_ascii_alphabetic() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn skip_ws_and_comments(&mut self) {
        loop {
            self.skip_ws();
            if self.peek() == Some('#') {
                while let Some(c) = self.peek() {
                    self.pos += 1;
                    if c == '\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Read `name: <iri>`, shared by `PREFIX` and `@prefix`
    fn read_prefix_body(&mut self) -> Result<(String, String), RdfMessageError> {
        self.skip_ws_and_comments();
        // name: up to the next whitespace or '<', trailing ':' stripped
        let mut name = String::new();
        while let Some(c) = self.peek() {
            if c.is_whitespace() || c == '<' {
                break;
            }
            name.push(c);
            self.pos += 1;
        }
        let name = name.trim_end_matches(':').to_string();
        self.skip_ws_and_comments();
        let iri = self.read_iri()?;
        Ok((name, iri))
    }

    /// Read a `<...>` IRI, returning the inner text
    fn read_iri(&mut self) -> Result<String, RdfMessageError> {
        if self.peek() != Some('<') {
            return Err(RdfMessageError::MalformedDirective(
                "expected '<' starting an IRI".to_string(),
            ));
        }
        self.pos += 1;
        let mut iri = String::new();
        while let Some(c) = self.peek() {
            self.pos += 1;
            if c == '>' {
                return Ok(iri);
            }
            iri.push(c);
        }
        Err(RdfMessageError::MalformedDirective(
            "unterminated IRI".to_string(),
        ))
    }

    /// Read a single- or triple-quoted string literal
    fn read_string_literal(&mut self) -> Result<String, RdfMessageError> {
        let quote = match self.peek() {
            Some(q @ ('"' | '\'')) => q,
            _ => {
                return Err(RdfMessageError::MalformedDirective(
                    "expected a string literal".to_string(),
                ))
            }
        };
        let triple = self.peek_at(1) == Some(quote) && self.peek_at(2) == Some(quote);
        let opener = if triple { 3 } else { 1 };
        self.pos += opener;
        let mut out = String::new();
        while let Some(c) = self.peek() {
            if c == '\\' {
                out.push(c);
                self.pos += 1;
                if let Some(next) = self.peek() {
                    out.push(next);
                    self.pos += 1;
                }
                continue;
            }
            if c == quote {
                if triple {
                    if self.peek_at(1) == Some(quote) && self.peek_at(2) == Some(quote) {
                        self.pos += 3;
                        return Ok(out);
                    }
                } else {
                    self.pos += 1;
                    return Ok(out);
                }
            }
            out.push(c);
            self.pos += 1;
        }
        Err(RdfMessageError::UnterminatedLiteral)
    }

    /// Expect and consume a terminating `.`
    fn expect_dot(&mut self) -> Result<(), RdfMessageError> {
        self.skip_ws_and_comments();
        if self.peek() == Some('.') {
            self.pos += 1;
            Ok(())
        } else {
            Err(RdfMessageError::MalformedDirective(
                "expected '.' terminating a directive".to_string(),
            ))
        }
    }

    fn read_statement(&mut self) -> Result<String, RdfMessageError> {
        let start = self.pos;
        let mut brace = 0i32;
        let mut bracket = 0i32;
        let mut paren = 0i32;
        let mut qt = 0i32;
        let mut opened_block = false;

        while let Some(c) = self.peek() {
            match c {
                '#' => {
                    // comment to end of line
                    while let Some(cc) = self.peek() {
                        self.pos += 1;
                        if cc == '\n' {
                            break;
                        }
                    }
                }
                '"' | '\'' => {
                    self.read_string_literal()?;
                }
                '<' => {
                    if self.peek_at(1) == Some('<') {
                        qt += 1;
                        self.pos += 2;
                    } else {
                        // single IRI reference
                        self.read_iri()?;
                    }
                }
                '>' if qt > 0 && self.peek_at(1) == Some('>') => {
                    qt -= 1;
                    self.pos += 2;
                }
                '{' => {
                    brace += 1;
                    opened_block = true;
                    self.pos += 1;
                }
                '}' => {
                    brace -= 1;
                    if brace < 0 {
                        return Err(RdfMessageError::UnbalancedDelimiters);
                    }
                    self.pos += 1;
                    if brace == 0 && opened_block {
                        let end = self.pos;
                        self.skip_ws_and_comments();
                        if self.peek() == Some('.') {
                            self.pos += 1;
                            return Ok(self.slice(start, self.pos));
                        }
                        return Ok(self.slice(start, end));
                    }
                }
                '[' => {
                    bracket += 1;
                    self.pos += 1;
                }
                ']' => {
                    bracket -= 1;
                    if bracket < 0 {
                        return Err(RdfMessageError::UnbalancedDelimiters);
                    }
                    self.pos += 1;
                }
                '(' => {
                    paren += 1;
                    self.pos += 1;
                }
                ')' => {
                    paren -= 1;
                    if paren < 0 {
                        return Err(RdfMessageError::UnbalancedDelimiters);
                    }
                    self.pos += 1;
                }
                '.' if brace == 0 && bracket == 0 && paren == 0 && qt == 0 => {
                    let next = self.peek_at(1);
                    let terminates = matches!(next, None | Some('#'))
                        || next.map(|c| c.is_whitespace()).unwrap_or(false);
                    self.pos += 1;
                    if terminates {
                        return Ok(self.slice(start, self.pos));
                    }
                }
                _ => {
                    self.pos += 1;
                }
            }
        }

        // EOF
        if brace != 0 || bracket != 0 || paren != 0 || qt != 0 {
            return Err(RdfMessageError::UnbalancedDelimiters);
        }
        let text = self.slice(start, self.pos);
        if text.trim().is_empty() {
            Err(RdfMessageError::MalformedDirective(
                "empty statement".to_string(),
            ))
        } else {
            Ok(text)
        }
    }

    fn slice(&self, start: usize, end: usize) -> String {
        self.chars[start..end].iter().collect::<String>().trim().to_string()
    }
}
