/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Splits N3 text into tokens

use crate::error::N3Error;

/// What a token is
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TokenKind {
    Iri(String),
    PName(String),
    Var(String),
    Blank(String),
    Str(String),
    Lang(String),
    Number(String),
    AtKeyword(String),
    Dot,
    Semicolon,
    Comma,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Arrow,
    BackArrow,
    Equals,
    HatHat,
    Bang,
    Caret,
    Eof,
}

/// A token and the byte offset it starts at
#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) offset: usize,
}

struct Lexer<'a> {
    text: &'a str,
    bytes: &'a [u8],
    pos: usize,
    tokens: Vec<Token>,
}

/// Tokenises a whole document
pub(crate) fn tokenize(text: &str) -> Result<Vec<Token>, N3Error> {
    let mut lexer = Lexer {
        text,
        bytes: text.as_bytes(),
        pos: 0,
        tokens: Vec::new(),
    };
    lexer.run()?;
    Ok(lexer.tokens)
}

fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-' || c == '\u{00B7}'
}

impl Lexer<'_> {
    fn peek_char(&self) -> Option<char> {
        self.text[self.pos..].chars().next()
    }

    fn char_at(&self, offset: usize) -> Option<char> {
        self.text.get(self.pos + offset..)?.chars().next()
    }

    fn push(&mut self, kind: TokenKind, offset: usize) {
        self.tokens.push(Token { kind, offset });
    }

    fn previous_is_string(&self) -> bool {
        matches!(
            self.tokens.last(),
            Some(Token {
                kind: TokenKind::Str(_),
                ..
            })
        )
    }

    fn run(&mut self) -> Result<(), N3Error> {
        while let Some(c) = self.peek_char() {
            let start = self.pos;
            match c {
                c if c.is_whitespace() => self.pos += c.len_utf8(),
                '#' => {
                    while let Some(c) = self.peek_char() {
                        if c == '\n' {
                            break;
                        }
                        self.pos += c.len_utf8();
                    }
                }
                '<' if self.char_at(1) == Some('=') => {
                    self.pos += 2;
                    self.push(TokenKind::BackArrow, start);
                }
                '<' => {
                    let iri = self.read_iri()?;
                    self.push(TokenKind::Iri(iri), start);
                }
                '"' | '\'' => {
                    let value = self.read_string(c)?;
                    self.push(TokenKind::Str(value), start);
                }
                '@' => {
                    self.pos += 1;
                    let word = self.read_while(is_name_char);
                    if word.is_empty() {
                        return Err(N3Error::at(
                            "expected a keyword or language tag after '@'",
                            start,
                        ));
                    }
                    if self.previous_is_string()
                        && start > 0
                        && matches!(self.bytes[start - 1], b'"' | b'\'')
                    {
                        self.push(TokenKind::Lang(word), start);
                    } else {
                        self.push(TokenKind::AtKeyword(word), start);
                    }
                }
                '?' => {
                    self.pos += 1;
                    let name = self.read_while(is_name_char);
                    if name.is_empty() {
                        return Err(N3Error::at("expected a variable name after '?'", start));
                    }
                    self.push(TokenKind::Var(name), start);
                }
                '_' if self.char_at(1) == Some(':') => {
                    self.pos += 2;
                    let label = self.read_local();
                    self.push(TokenKind::Blank(label), start);
                }
                '.' if self.char_at(1).is_some_and(|c| c.is_ascii_digit())
                    && !self.previous_ends_term() =>
                {
                    let number = self.read_number();
                    self.push(TokenKind::Number(number), start);
                }
                '.' => {
                    self.pos += 1;
                    self.push(TokenKind::Dot, start);
                }
                ';' => self.single(TokenKind::Semicolon),
                ',' => self.single(TokenKind::Comma),
                '{' => self.single(TokenKind::LBrace),
                '}' => self.single(TokenKind::RBrace),
                '[' => self.single(TokenKind::LBracket),
                ']' => self.single(TokenKind::RBracket),
                '(' => self.single(TokenKind::LParen),
                ')' => self.single(TokenKind::RParen),
                '!' => self.single(TokenKind::Bang),
                '=' if self.char_at(1) == Some('>') => {
                    self.pos += 2;
                    self.push(TokenKind::Arrow, start);
                }
                '=' => self.single(TokenKind::Equals),
                '^' if self.char_at(1) == Some('^') => {
                    self.pos += 2;
                    self.push(TokenKind::HatHat, start);
                }
                '^' => self.single(TokenKind::Caret),
                '+' | '-'
                    if self
                        .char_at(1)
                        .is_some_and(|c| c.is_ascii_digit() || c == '.') =>
                {
                    let number = self.read_number();
                    self.push(TokenKind::Number(number), start);
                }
                c if c.is_ascii_digit() => {
                    let number = self.read_number();
                    self.push(TokenKind::Number(number), start);
                }
                c if c == ':' || is_name_char(c) => {
                    let name = self.read_pname();
                    self.push(TokenKind::PName(name), start);
                }
                other => {
                    return Err(N3Error::at(
                        format!("unexpected character '{other}'"),
                        start,
                    ));
                }
            }
        }
        self.push(TokenKind::Eof, self.pos);
        Ok(())
    }

    fn previous_ends_term(&self) -> bool {
        matches!(
            self.tokens.last().map(|t| &t.kind),
            Some(
                TokenKind::Iri(_)
                    | TokenKind::PName(_)
                    | TokenKind::Var(_)
                    | TokenKind::Blank(_)
                    | TokenKind::Str(_)
                    | TokenKind::Lang(_)
                    | TokenKind::Number(_)
                    | TokenKind::RBrace
                    | TokenKind::RBracket
                    | TokenKind::RParen
            )
        )
    }

    fn single(&mut self, kind: TokenKind) {
        let start = self.pos;
        self.pos += 1;
        self.push(kind, start);
    }

    fn read_while(&mut self, keep: impl Fn(char) -> bool) -> String {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if !keep(c) {
                break;
            }
            self.pos += c.len_utf8();
        }
        self.text[start..self.pos].to_string()
    }

    /// Reads a local name, which may contain dots but never ends with one
    fn read_local(&mut self) -> String {
        let mut out = String::new();
        while let Some(c) = self.peek_char() {
            if c == '\\' {
                if let Some(escaped) = self.char_at(1) {
                    out.push(escaped);
                    self.pos += 1 + escaped.len_utf8();
                    continue;
                }
            }
            if c == '%' {
                out.push(c);
                self.pos += 1;
                continue;
            }
            if is_name_char(c) || c == ':' {
                out.push(c);
                self.pos += c.len_utf8();
                continue;
            }
            if c == '.'
                && self
                    .char_at(1)
                    .is_some_and(|n| is_name_char(n) || n == ':' || n == '%')
            {
                out.push(c);
                self.pos += 1;
                continue;
            }
            break;
        }
        out
    }

    fn read_pname(&mut self) -> String {
        let mut end = self.pos;
        while let Some(c) = self.text[end..].chars().next() {
            if !(is_name_char(c) || c == '.') {
                break;
            }
            end += c.len_utf8();
        }
        if self.bytes.get(end) == Some(&b':') {
            let prefix = self.text[self.pos..end].to_string();
            self.pos = end + 1;
            let local = self.read_local();
            format!("{prefix}:{local}")
        } else {
            let word = self.text[self.pos..end].trim_end_matches('.').to_string();
            self.pos += word.len();
            word
        }
    }

    fn read_number(&mut self) -> String {
        let start = self.pos;
        if matches!(self.peek_char(), Some('+') | Some('-')) {
            self.pos += 1;
        }
        while self.peek_char().is_some_and(|c| c.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.peek_char() == Some('.') && self.char_at(1).is_some_and(|c| c.is_ascii_digit()) {
            self.pos += 1;
            while self.peek_char().is_some_and(|c| c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek_char(), Some('e') | Some('E')) {
            let mut lookahead = 1;
            if matches!(self.char_at(1), Some('+') | Some('-')) {
                lookahead = 2;
            }
            if self.char_at(lookahead).is_some_and(|c| c.is_ascii_digit()) {
                self.pos += lookahead;
                while self.peek_char().is_some_and(|c| c.is_ascii_digit()) {
                    self.pos += 1;
                }
            }
        }
        self.text[start..self.pos].to_string()
    }

    fn read_iri(&mut self) -> Result<String, N3Error> {
        let start = self.pos;
        self.pos += 1;
        let mut out = String::new();
        loop {
            let Some(c) = self.peek_char() else {
                return Err(N3Error::at("unterminated IRI", start));
            };
            match c {
                '>' => {
                    self.pos += 1;
                    return Ok(out);
                }
                '\\' => {
                    self.pos += 1;
                    out.push(self.read_escape(start)?);
                }
                c if c.is_whitespace() => {
                    return Err(N3Error::at("whitespace inside an IRI", start));
                }
                c => {
                    out.push(c);
                    self.pos += c.len_utf8();
                }
            }
        }
    }

    fn read_hex(&mut self, digits: usize, start: usize) -> Result<char, N3Error> {
        let hex = self
            .text
            .get(self.pos..self.pos + digits)
            .ok_or_else(|| N3Error::at("truncated unicode escape", start))?;
        let value =
            u32::from_str_radix(hex, 16).map_err(|_| N3Error::at("bad unicode escape", start))?;
        self.pos += digits;
        char::from_u32(value).ok_or_else(|| N3Error::at("escape is not a character", start))
    }

    /// Reads the character after a backslash
    fn read_escape(&mut self, start: usize) -> Result<char, N3Error> {
        let Some(c) = self.peek_char() else {
            return Err(N3Error::at("dangling escape", start));
        };
        self.pos += c.len_utf8();
        Ok(match c {
            't' => '\t',
            'b' => '\u{0008}',
            'n' => '\n',
            'r' => '\r',
            'f' => '\u{000c}',
            'u' => self.read_hex(4, start)?,
            'U' => self.read_hex(8, start)?,
            other => other,
        })
    }

    fn read_string(&mut self, quote: char) -> Result<String, N3Error> {
        let start = self.pos;
        let triple: String = std::iter::repeat_n(quote, 3).collect();
        let long = self.text[self.pos..].starts_with(&triple);
        self.pos += if long { 3 } else { 1 };
        let mut out = String::new();
        loop {
            let Some(c) = self.peek_char() else {
                return Err(N3Error::at("unterminated string", start));
            };
            if long {
                if self.text[self.pos..].starts_with(&triple) {
                    // A run of more than three quotes ends with the last three.
                    let mut run = 0;
                    while self.char_at(run) == Some(quote) {
                        run += 1;
                    }
                    for _ in 0..run - 3 {
                        out.push(quote);
                    }
                    self.pos += run;
                    return Ok(out);
                }
            } else if c == quote {
                self.pos += 1;
                return Ok(out);
            } else if c == '\n' {
                return Err(N3Error::at("newline inside a short string", start));
            }
            if c == '\\' {
                self.pos += 1;
                out.push(self.read_escape(start)?);
            } else {
                out.push(c);
                self.pos += c.len_utf8();
            }
        }
    }
}
