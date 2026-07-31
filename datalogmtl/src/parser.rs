/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Parser for the **past-only, integer, arity-≤2** fragment of MeTeoR's textual
//! DatalogMTL syntax. Used to drive the datalogmtl engine from the same corpus
//! files that feed the MeTeoR reference reasoner, so results can be diffed.
//!
//! Supported fragment:
//!   - Operators (past): `Boxminus[a,b]`, `Diamondminus[a,b]`, `L Since[a,b] R`.
//!     Operators may stack (`Boxminus[1,2]Diamondminus[0,1]A(X)`).
//!   - Plain (operator-free) rule heads only.
//!   - Closed integer intervals `[a,b]` (and single point `[a]`); open bounds rejected.
//!   - Predicate arity 1 or 2. Arity 0 (propositional) and arity ≥3 rejected.
//!
//! RDF mapping (arity ≤2):
//!   - unary `A(x)`   ↔ triple `(x, rdf:type, A)`
//!   - binary `C(x,y)` ↔ triple `(x, C, y)`
//!
//! Future operators (`Boxplus`/`Diamondplus`/`Until`) and openness are outside the
//! fragment and produce a descriptive error.

use std::sync::{Arc, RwLock};
use shared::dictionary::Dictionary;
use shared::triple::Triple;
use shared::terms::{Term, TriplePattern};
use crate::syntax::{DatalogMTLRule, TemporalAtom, Interval};

/// Full IRI for the `rdf:type` predicate used to encode unary atoms.
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// A grounded temporal fact with a closed integer validity interval `[start, end]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalFact {
    pub triple: Triple,
    pub start: u64,
    pub end: u64,
}

// ────────────────────────────────────────────────────────────────
// Public entry points
// ────────────────────────────────────────────────────────────────

/// Parse a whole program (one rule per non-empty, non-comment line).
pub fn parse_program(
    text: &str,
    dict: &Arc<RwLock<Dictionary>>,
) -> Result<Vec<DatalogMTLRule>, String> {
    let mut rules = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = strip_line(raw);
        if line.is_empty() { continue; }
        let rule = parse_rule(&line, i + 1, dict)
            .map_err(|e| format!("program line {}: {}: {}", i + 1, raw.trim(), e))?;
        rules.push(rule);
    }
    Ok(rules)
}

/// Parse a data file (one fact per non-empty, non-comment line).
pub fn parse_data(
    text: &str,
    dict: &Arc<RwLock<Dictionary>>,
) -> Result<Vec<TemporalFact>, String> {
    let mut facts = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = strip_line(raw);
        if line.is_empty() { continue; }
        let fact = parse_fact(&line, dict)
            .map_err(|e| format!("data line {}: {}: {}", i + 1, raw.trim(), e))?;
        facts.push(fact);
    }
    Ok(facts)
}

// ────────────────────────────────────────────────────────────────
// Rule parsing
// ────────────────────────────────────────────────────────────────

fn parse_rule(
    line: &str,
    _lineno: usize,
    dict: &Arc<RwLock<Dictionary>>,
) -> Result<DatalogMTLRule, String> {
    let Some((head_str, body_str)) = line.split_once(":-") else {
        return Err("rule missing ':-'".into());
    };
    if body_str.contains("||") {
        return Err("negation ('||') is outside the supported fragment".into());
    }

    // Head must be a plain atom (no temporal operator).
    if leading_operator(head_str).is_some()
        || future_operator(head_str).is_some()
        || find_kw_depth0(head_str, "Since").is_some()
        || find_kw_depth0(head_str, "Until").is_some()
    {
        return Err("temporal operator in head is outside the supported fragment".into());
    }
    let head = atom_to_pattern(head_str, dict, true)?;

    let mut body = Vec::new();
    for lit in split_top_commas(body_str) {
        if lit.is_empty() { continue; }
        body.push(parse_literal(&lit, dict)?);
    }
    if body.is_empty() {
        return Err("rule has empty body".into());
    }

    Ok(DatalogMTLRule { id: head_str.to_string(), head, body })
}

/// Parse a single body literal into a (possibly nested) `TemporalAtom`.
fn parse_literal(s: &str, dict: &Arc<RwLock<Dictionary>>) -> Result<TemporalAtom, String> {
    // Binary Since: `L Since[a,b] R` (whitespace already stripped).
    if let Some(idx) = find_kw_depth0(s, "Since") {
        let left = &s[..idx];
        let after = &s[idx + "Since".len()..];
        let (interval, rest) = read_operator_interval(after)?;
        if left.is_empty() || rest.is_empty() {
            return Err("Since must have a literal on each side".into());
        }
        let phi = Box::new(parse_literal(left, dict)?);   // continuation (holds since)
        let psi = Box::new(parse_literal(rest, dict)?);   // trigger / reset
        return Ok(TemporalAtom::Since { interval, phi, psi });
    }
    if find_kw_depth0(s, "Until").is_some() {
        return Err("Until (future operator) is outside the supported fragment".into());
    }

    // Peel stacked unary operators (outermost first).
    let mut ops: Vec<(OpKind, Interval)> = Vec::new();
    let mut rest = s;
    loop {
        if let Some(fut) = future_operator(rest) {
            return Err(format!("future operator '{}' is outside the supported fragment", fut));
        }
        let Some(kind) = leading_operator(rest) else { break };
        let after = &rest[kind.name().len()..];
        let (interval, tail) = read_operator_interval(after)?;
        ops.push((kind, interval));
        rest = tail;
    }

    let mut atom = TemporalAtom::Base(atom_to_pattern(rest, dict, true)?);
    for (kind, interval) in ops.into_iter().rev() {
        atom = match kind {
            OpKind::Box => TemporalAtom::Box_ { interval, inner: Box::new(atom) },
            OpKind::Diamond => TemporalAtom::Diamond { interval, inner: Box::new(atom) },
        };
    }
    Ok(atom)
}

#[derive(Clone, Copy)]
enum OpKind { Box, Diamond }
impl OpKind {
    fn name(self) -> &'static str {
        match self { OpKind::Box => "Boxminus", OpKind::Diamond => "Diamondminus" }
    }
}

/// If `s` starts with a supported past operator keyword, return its kind.
fn leading_operator(s: &str) -> Option<OpKind> {
    if s.starts_with("Boxminus") { Some(OpKind::Box) }
    else if s.starts_with("Diamondminus") { Some(OpKind::Diamond) }
    else { None }
}

/// If `s` starts with a future/unsupported temporal operator keyword, return it.
fn future_operator(s: &str) -> Option<&'static str> {
    for kw in ["Boxplus", "Diamondplus", "SOMETIME", "ALWAYS"] {
        if s.starts_with(kw) { return Some(kw); }
    }
    None
}

// ────────────────────────────────────────────────────────────────
// Fact parsing
// ────────────────────────────────────────────────────────────────

fn parse_fact(line: &str, dict: &Arc<RwLock<Dictionary>>) -> Result<TemporalFact, String> {
    let Some((atom_str, span)) = line.rsplit_once('@') else {
        return Err("fact missing '@<interval>'".into());
    };
    let triple = atom_to_ground_triple(atom_str, dict)?;
    let (start, end) = parse_span(span)?;
    Ok(TemporalFact { triple, start, end })
}

/// Parse a fact interval: `t`, `[t]`, or `[l,r]`. Closed integers only.
fn parse_span(span: &str) -> Result<(u64, u64), String> {
    let span = span.trim();
    if span.starts_with('[') || span.starts_with('(') {
        let (open, inner, close, tail) = read_bracket(span)?;
        if !tail.is_empty() {
            return Err(format!("trailing characters after interval: '{}'", tail));
        }
        if open != '[' || close != ']' {
            return Err("open interval bounds are outside the supported fragment".into());
        }
        let parts: Vec<&str> = inner.split(',').collect();
        match parts.as_slice() {
            [p] => { let v = parse_int(p)?; Ok((v, v)) }
            [l, r] => {
                let (l, r) = (parse_int(l)?, parse_int(r)?);
                if l > r { return Err(format!("interval start {} > end {}", l, r)); }
                Ok((l, r))
            }
            _ => Err("interval must have one or two endpoints".into()),
        }
    } else {
        let v = parse_int(span)?;
        Ok((v, v))
    }
}

// ────────────────────────────────────────────────────────────────
// Atom ↔ RDF triple mapping
// ────────────────────────────────────────────────────────────────

/// Split an atom string `Pred(a,b)` into (functor, args). Nullary => empty args.
fn split_atom(s: &str) -> Result<(&str, Vec<&str>), String> {
    let s = s.trim();
    let Some(open) = s.find('(') else {
        return Ok((s, Vec::new())); // no parens: propositional (arity 0)
    };
    if !s.ends_with(')') {
        return Err(format!("malformed atom '{}'", s));
    }
    let functor = &s[..open];
    let inner = &s[open + 1..s.len() - 1];
    let args: Vec<&str> = inner.split(',').map(|a| a.trim()).collect();
    Ok((functor, args))
}

/// Map an atom to a triple *pattern* (rule context: uppercase args are variables).
fn atom_to_pattern(
    s: &str,
    dict: &Arc<RwLock<Dictionary>>,
    _rule_ctx: bool,
) -> Result<TriplePattern, String> {
    let (functor, args) = split_atom(s)?;
    if functor.is_empty() {
        return Err("empty predicate name".into());
    }
    match args.len() {
        0 => Err(format!("arity-0 (propositional) atom '{}' cannot be represented as an RDF triple", functor)),
        1 => {
            let subj = term_of(args[0], dict)?;
            let typ = Term::Constant(encode(dict, RDF_TYPE));
            let cls = Term::Constant(encode(dict, functor));
            Ok((subj, typ, cls))
        }
        2 => {
            let subj = term_of(args[0], dict)?;
            let pred = Term::Constant(encode(dict, functor));
            let obj = term_of(args[1], dict)?;
            Ok((subj, pred, obj))
        }
        n => Err(format!("arity-{} atom '{}' is outside the supported fragment (max 2)", n, functor)),
    }
}

/// Map an atom to a *ground* triple (data context: all args are constants).
fn atom_to_ground_triple(s: &str, dict: &Arc<RwLock<Dictionary>>) -> Result<Triple, String> {
    let (functor, args) = split_atom(s)?;
    if functor.is_empty() {
        return Err("empty predicate name".into());
    }
    let (subj, pred, obj) = match args.len() {
        0 => return Err(format!("arity-0 (propositional) fact '{}' cannot be represented as an RDF triple", functor)),
        1 => (encode(dict, args[0]), encode(dict, RDF_TYPE), encode(dict, functor)),
        2 => (encode(dict, args[0]), encode(dict, functor), encode(dict, args[1])),
        n => return Err(format!("arity-{} fact '{}' is outside the supported fragment (max 2)", n, functor)),
    };
    Ok(Triple { subject: subj, predicate: pred, object: obj })
}

/// A rule-context term: uppercase first letter => variable, else constant.
fn term_of(tok: &str, dict: &Arc<RwLock<Dictionary>>) -> Result<Term, String> {
    let tok = tok.trim();
    let Some(first) = tok.chars().next() else {
        return Err("empty argument".into());
    };
    if first.is_ascii_uppercase() {
        Ok(Term::Variable(tok.to_string()))
    } else {
        Ok(Term::Constant(encode(dict, tok)))
    }
}

fn encode(dict: &Arc<RwLock<Dictionary>>, s: &str) -> u32 {
    dict.write().unwrap().encode(s)
}

// ────────────────────────────────────────────────────────────────
// Low-level string helpers
// ────────────────────────────────────────────────────────────────

/// Strip comments (`#`/`//`) and all whitespace, mirroring MeTeoR's `replace(" ","")`.
fn strip_line(raw: &str) -> String {
    let mut s = raw;
    if let Some(i) = s.find('#') { s = &s[..i]; }
    if let Some(i) = s.find("//") { s = &s[..i]; }
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

fn parse_int(s: &str) -> Result<u64, String> {
    let s = s.trim();
    s.parse::<u64>().map_err(|_| {
        if s.parse::<f64>().is_ok() || s.parse::<i64>().is_ok() {
            format!("non-negative-integer time point required, got '{}'", s)
        } else {
            format!("invalid time point '{}'", s)
        }
    })
}

/// Read a bracketed group at the start of `s`. Returns
/// (open_char, inner, close_char, tail_after_close).
fn read_bracket(s: &str) -> Result<(char, &str, char, &str), String> {
    let bytes = s.as_bytes();
    let open = bytes[0] as char;
    if open != '[' && open != '(' {
        return Err(format!("expected '[' or '(', found '{}'", open));
    }
    let mut depth = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '[' | '(' => depth += 1,
            ']' | ')' => {
                depth -= 1;
                if depth == 0 {
                    let inner = &s[1..i];
                    let close = ch;
                    let tail = &s[i + 1..];
                    return Ok((open, inner, close, tail));
                }
            }
            _ => {}
        }
    }
    Err(format!("unbalanced brackets in '{}'", s))
}

/// Read a *closed integer* operator interval `[a,b]` (or `[a]`) at the start of `s`.
/// Returns the parsed `Interval` and the remaining tail.
fn read_operator_interval(s: &str) -> Result<(Interval, &str), String> {
    let (open, inner, close, tail) = read_bracket(s)?;
    if open != '[' || close != ']' {
        return Err("operator intervals must be closed '[a,b]' in the supported fragment".into());
    }
    let parts: Vec<&str> = inner.split(',').collect();
    let (start, end) = match parts.as_slice() {
        [p] => { let v = parse_int(p)?; (v, v) }
        [l, r] => (parse_int(l)?, parse_int(r)?),
        _ => return Err("operator interval must have one or two endpoints".into()),
    };
    if start > end {
        return Err(format!("operator interval start {} > end {}", start, end));
    }
    Ok((Interval { start, end }, tail))
}

/// Find keyword `kw` in `s` at bracket-depth 0. Returns byte index of match start.
fn find_kw_depth0(s: &str, kw: &str) -> Option<usize> {
    let mut depth = 0i32;
    let bytes = s.as_bytes();
    let kw_bytes = kw.as_bytes();
    for i in 0..bytes.len() {
        match bytes[i] as char {
            '[' | '(' => depth += 1,
            ']' | ')' => depth -= 1,
            _ => {
                if depth == 0 && bytes[i..].starts_with(kw_bytes) {
                    return Some(i);
                }
            }
        }
    }
    None
}

/// Split `s` on commas at bracket-depth 0 (MeTeoR `parse_body`).
fn split_top_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for ch in s.chars() {
        match ch {
            '[' | '(' => { depth += 1; cur.push(ch); }
            ']' | ')' => { depth -= 1; cur.push(ch); }
            ',' if depth == 0 => { out.push(std::mem::take(&mut cur)); }
            _ => cur.push(ch),
        }
    }
    out.push(cur);
    out
}
