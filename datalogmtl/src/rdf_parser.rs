/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Parser for **RDF triple-pattern** DatalogMTL syntax — the counterpart to
//! [`crate::parser`], which reads MeTeoR's `Pred(args)` syntax.
//!
//! Use this one whenever rules must line up with real RDF data: predicates and
//! constants are IRIs interned into the caller's [`Dictionary`], so the ids match
//! facts produced elsewhere. (The MeTeoR parser interns the bare functor, so
//! `inZone(D,Z)` yields a different id than `<http://…/inZone>` and silently
//! matches nothing.)
//!
//! # Rules
//!
//! ```text
//! PREFIX dront: <http://example.org/dront/>
//!
//! [sustainedGeofenceViolation]
//! (?d, dront:violatedZone, ?z) :-
//!     Box[0,30](?d, dront:inZone, ?z),
//!     (?z, dront:status, dront:Restricted).
//! ```
//!
//! Rules are terminated by a `.` at depth 0 and may span lines. An optional
//! `[label]` before the head becomes the rule id (otherwise the rule index is
//! used). Operators: `Diamond`/`Diamondminus`, `Box`/`Boxminus`, `Prev`, and
//! `Since[a,b](phi, psi)`; in [`Mode::Static`] also `Diamondplus`, `Boxplus` and
//! `Until[a,b](phi, psi)`. Operators stack: `Box[0,5]Diamond[0,2](?x, :p, ?y)`.
//!
//! # Stream shapes
//!
//! ```text
//! STREAM <http://utm.example.org/telemetry>
//!     PATTERN (?obs, rdf:type, sosa:Observation)
//!             (?obs, sosa:madeBySensor, ?drone)
//!     KEY ?drone
//!     STALENESS 10
//!     EXPIRY (?drone, utm:channelStatus, utm:expired)
//! .
//! ```
//!
//! Keywords are case-sensitive and blocks are separated by a depth-0 `.`.
//! `STREAM` and `PATTERN` are required; `KEY` defaults to empty and `STALENESS`
//! to `0`. The unit of `STALENESS` is whatever unit the caller's timestamps use.
//!
//! `EXPIRY` declares facts transmitted on behalf of a channel that has gone
//! stale — silence is not itself an event, so without it a rule has nothing to
//! observe when a source stops reporting. Only `KEY` variables may appear there,
//! since the key binding is all that is still known about an expired channel.
//! See [`crate::stream::ShapeIngester::expiry_facts`].
//!
//! # Terms
//!
//! | Form | Meaning |
//! |---|---|
//! | `?x` | variable |
//! | `<http://…>` | IRI constant (brackets stripped before interning) |
//! | `pfx:local` | IRI constant, expanded against a `PREFIX` declaration |
//! | `:name` | with no empty `PREFIX` declared, interned verbatim as `":name"` |
//! | `"lit"` | literal constant, interned with its quotes |
//!
//! `PREFIX` declarations are per text blob — rules and stream shapes are parsed
//! independently, so each must declare the prefixes it uses. Anything inside
//! `<…>` or `"…"` is opaque to the scanner, so `#` in an IRI fragment is not
//! mistaken for a comment and `.` in a host name does not terminate a block.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use shared::dictionary::Dictionary;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;

use crate::stream::{StalenessPolicy, StreamShape};
use crate::syntax::{DatalogMTLRule, Interval, Mode, TemporalAtom};

/// Prefix label (without the trailing `:`) → IRI namespace.
type PrefixMap = HashMap<String, String>;

// ── Public API ────────────────────────────────────────────────────────────────

/// Parse a rule program. `dict` receives every IRI/literal encountered.
pub fn parse_rules(
    text: &str,
    dict: &mut Dictionary,
    mode: Mode,
) -> Result<Vec<DatalogMTLRule>, String> {
    let (prefixes, cleaned) = extract_prefixes(text)?;

    let rule_strs = split_at_depth0(&cleaned, '.');
    let mut rules = Vec::new();

    for (i, rule_str) in rule_strs.iter().enumerate() {
        let rule_str = rule_str.trim();
        if rule_str.is_empty() {
            continue;
        }

        // Optional `[label]` before the head becomes the rule id. A head always
        // starts with `(` or a term, so a leading `[` is unambiguous.
        let (label, rule_str) = split_label(rule_str)?;
        let rule_str = rule_str.trim();

        let sep_pos = find_rule_sep(rule_str)
            .ok_or_else(|| format!("Rule {}: no ':-' found in: '{}'", i, rule_str))?;

        let head_str = rule_str[..sep_pos].trim();
        let body_str = rule_str[sep_pos + 2..].trim();

        let head = parse_triple_pattern(head_str, dict, &prefixes)?;
        let body_parts = split_at_depth0(body_str, ',');
        let mut body = Vec::new();
        for part in &body_parts {
            let part = part.trim();
            if !part.is_empty() {
                body.push(parse_temporal_atom(part, dict, &prefixes, mode)?);
            }
        }

        let id = label.unwrap_or_else(|| i.to_string());
        rules.push(DatalogMTLRule { id, head, body });
    }

    Ok(rules)
}

/// Parse `STREAM`/`PATTERN`/`KEY`/`STALENESS` blocks into stream shapes.
pub fn parse_stream_shapes(
    text: &str,
    dict: &mut Dictionary,
) -> Result<Vec<StreamShape>, String> {
    let (prefixes, cleaned) = extract_prefixes(text)?;

    let blocks = split_at_depth0(&cleaned, '.');
    let mut shapes = Vec::new();

    for (bi, block) in blocks.iter().enumerate() {
        let block = block.trim();
        if block.is_empty() {
            continue;
        }

        // Find positions of each keyword and sort
        let kws = ["STREAM", "PATTERN", "KEY", "STALENESS", "EXPIRY"];
        let mut kw_positions: Vec<(&str, usize)> = kws
            .iter()
            .filter_map(|&kw| find_whole_word(block, kw).map(|pos| (kw, pos)))
            .collect();
        kw_positions.sort_by_key(|(_, pos)| *pos);

        // Build keyword → content map (text from end-of-keyword to start-of-next-keyword)
        let mut sections: HashMap<&str, String> = HashMap::new();
        for i in 0..kw_positions.len() {
            let (kw, kw_pos) = kw_positions[i];
            let content_start = kw_pos + kw.len();
            let content_end = if i + 1 < kw_positions.len() {
                kw_positions[i + 1].1
            } else {
                block.len()
            };
            sections.insert(kw, block[content_start..content_end].trim().to_string());
        }

        // STREAM: first whitespace-delimited token is the IRI
        let stream_raw = sections
            .get("STREAM")
            .ok_or_else(|| format!("Shape block {}: missing STREAM keyword", bi))?;
        let stream_iri_raw = stream_raw
            .split_whitespace()
            .next()
            .ok_or_else(|| format!("Shape block {}: empty STREAM value", bi))?;
        let stream_iri = expand_iri_text(stream_iri_raw, &prefixes);

        // PATTERN: collect all (…,…,…) groups
        let pattern_text = sections.get("PATTERN").cloned().unwrap_or_default();
        if pattern_text.trim().is_empty() {
            return Err(format!("Shape '{}': missing PATTERN section", stream_iri));
        }
        let event_pattern = parse_pattern_group(&pattern_text, dict, &prefixes)?;
        if event_pattern.is_empty() {
            return Err(format!(
                "Shape '{}': no triple patterns found in PATTERN",
                stream_iri
            ));
        }

        // KEY: collect ?var tokens
        let key_text = sections.get("KEY").cloned().unwrap_or_default();
        let channel_key: Vec<String> = key_text
            .split_whitespace()
            .filter(|s| s.starts_with('?'))
            .map(|s| s[1..].to_string())
            .collect();

        // EXPIRY: facts transmitted while the channel is stale. Only key
        // variables can be instantiated once a reading has expired, so reject
        // anything else here rather than silently emitting nothing.
        let expiry_text = sections.get("EXPIRY").cloned().unwrap_or_default();
        let on_expiry = parse_pattern_group(&expiry_text, dict, &prefixes)?;
        for pattern in &on_expiry {
            for term in [&pattern.0, &pattern.1, &pattern.2] {
                if let Term::Variable(v) = term {
                    if !channel_key.contains(v) {
                        return Err(format!(
                            "Shape '{}': EXPIRY variable '?{}' is not a KEY variable — \
                             only the channel key is known once a reading has expired",
                            stream_iri, v
                        ));
                    }
                }
            }
        }

        // STALENESS: first token parsed as u64
        let staleness_text = sections.get("STALENESS").cloned().unwrap_or_default();
        let max_gap_ms = staleness_text
            .split_whitespace()
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        shapes.push(StreamShape {
            stream_iri,
            event_pattern,
            channel_key,
            staleness: StalenessPolicy { max_gap_ms },
            on_expiry,
        });
    }

    Ok(shapes)
}

/// Parse ground facts written as **N-Triples**:
///
/// ```text
/// <http://utm.example.org/zone/hospital> <http://example.org/dront/status> <http://example.org/dront/Restricted> .
/// ```
///
/// Terms are whitespace-separated and the statement is `.`-terminated, so this
/// accepts N-Triples as produced by any RDF tool. `PREFIX` lines and the
/// resulting prefixed names are also accepted as a convenience, though using
/// them means the text is no longer strictly N-Triples.
///
/// Variables are rejected — a fact has nothing to bind them from.
pub fn parse_facts(text: &str, dict: &mut Dictionary) -> Result<Vec<Triple>, String> {
    let (prefixes, cleaned) = extract_prefixes(text)?;
    let mut facts = Vec::new();

    for (i, stmt) in split_at_depth0(&cleaned, '.').iter().enumerate() {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        let tokens = split_ntriples_terms(stmt);
        if tokens.len() != 3 {
            return Err(format!(
                "Fact {}: expected `<subject> <predicate> <object> .`, found {} term(s) in '{}'",
                i,
                tokens.len(),
                stmt
            ));
        }
        let s = parse_term(&tokens[0], dict, &prefixes)?;
        let p = parse_term(&tokens[1], dict, &prefixes)?;
        let o = parse_term(&tokens[2], dict, &prefixes)?;
        facts.push(Triple {
            subject: ground_term(&s, i, "subject")?,
            predicate: ground_term(&p, i, "predicate")?,
            object: ground_term(&o, i, "object")?,
        });
    }
    Ok(facts)
}

/// Split one N-Triples statement into its terms.
///
/// Not a plain whitespace split: `<…>` and `"…"` are read as single units, so an
/// IRI or a literal containing spaces stays intact. Anything trailing a closing
/// quote (`@en`, `^^<…>`) is kept with the literal.
fn split_ntriples_terms(stmt: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let chars: Vec<char> = stmt.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        match chars[i] {
            '<' => {
                while i < chars.len() && chars[i] != '>' {
                    i += 1;
                }
                i = (i + 1).min(chars.len()); // include '>'
            }
            '"' => {
                i += 1;
                while i < chars.len() {
                    match chars[i] {
                        '\\' => i += 2, // escaped char, including \"
                        '"' => break,
                        _ => i += 1,
                    }
                }
                i = (i + 1).min(chars.len()); // include closing quote
                // Keep a language tag or datatype suffix with the literal.
                while i < chars.len() && !chars[i].is_whitespace() {
                    if chars[i] == '<' {
                        while i < chars.len() && chars[i] != '>' {
                            i += 1;
                        }
                    }
                    i += 1;
                }
            }
            _ => {
                while i < chars.len() && !chars[i].is_whitespace() {
                    i += 1;
                }
            }
        }
        terms.push(chars[start..i.min(chars.len())].iter().collect());
    }
    terms
}

fn ground_term(term: &Term, index: usize, position: &str) -> Result<u32, String> {
    match term {
        Term::Constant(c) => Ok(*c),
        Term::Variable(v) => Err(format!(
            "Fact {}: variable '?{}' in {} position — facts must be ground",
            index, v, position
        )),
        other => Err(format!("Fact {}: unsupported {} term {:?}", index, position, other)),
    }
}

/// [`parse_facts`] for callers holding a shared dictionary.
///
/// Takes the write lock for the whole parse — never call it while already
/// holding a lock on the same dictionary.
pub fn parse_facts_shared(
    text: &str,
    dict: &Arc<RwLock<Dictionary>>,
) -> Result<Vec<Triple>, String> {
    let mut guard = dict
        .write()
        .map_err(|_| "dictionary lock poisoned".to_string())?;
    parse_facts(text, &mut guard)
}

/// Parse a bare `start,end` interval body (the text between `[` and `]`).
pub fn parse_interval(s: &str) -> Result<Interval, String> {
    let parts: Vec<&str> = s.splitn(2, ',').collect();
    if parts.len() != 2 {
        return Err(format!("Expected 'start,end' interval, got: '{}'", s));
    }
    let start = parts[0]
        .trim()
        .parse::<u64>()
        .map_err(|e| format!("Invalid interval start '{}': {}", parts[0].trim(), e))?;
    let end = parts[1]
        .trim()
        .parse::<u64>()
        .map_err(|e| format!("Invalid interval end '{}': {}", parts[1].trim(), e))?;
    Ok(Interval { start, end })
}

/// [`parse_rules`] for callers holding a shared dictionary.
///
/// Takes the write lock for the whole parse — never call it while already
/// holding a lock on the same dictionary.
pub fn parse_rules_shared(
    text: &str,
    dict: &Arc<RwLock<Dictionary>>,
    mode: Mode,
) -> Result<Vec<DatalogMTLRule>, String> {
    let mut guard = dict
        .write()
        .map_err(|_| "dictionary lock poisoned".to_string())?;
    parse_rules(text, &mut guard, mode)
}

/// [`parse_stream_shapes`] for callers holding a shared dictionary.
///
/// Takes the write lock for the whole parse — never call it while already
/// holding a lock on the same dictionary.
pub fn parse_stream_shapes_shared(
    text: &str,
    dict: &Arc<RwLock<Dictionary>>,
) -> Result<Vec<StreamShape>, String> {
    let mut guard = dict
        .write()
        .map_err(|_| "dictionary lock poisoned".to_string())?;
    parse_stream_shapes(text, &mut guard)
}

// ── Comment stripping and PREFIX extraction ───────────────────────────────────

/// Strip a `#` line comment. `#` inside `<…>` or `"…"` is content, not a
/// comment — without this, `<http://…/22-rdf-syntax-ns#type>` loses its fragment.
fn strip_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut in_iri = false;
    let mut in_str = false;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'<' if !in_str => in_iri = true,
            b'>' if !in_str => in_iri = false,
            b'"' if !in_iri => in_str = !in_str,
            b'#' if !in_iri && !in_str => return &line[..i],
            _ => {}
        }
    }
    line
}

/// Pull `PREFIX name: <iri>` declarations off the top of a blob, returning the
/// map and the remaining text joined into one line for depth-0 splitting.
///
/// A declaration must sit on its own line; everything else is passed through.
fn extract_prefixes(text: &str) -> Result<(PrefixMap, String), String> {
    let mut prefixes = PrefixMap::new();
    let mut body = Vec::new();

    for raw in text.lines() {
        let line = strip_comment(raw);
        let trimmed = line.trim();
        if trimmed.is_empty() {
            body.push("");
            continue;
        }
        if let Some(rest) = strip_keyword(trimmed, "PREFIX") {
            let (name, iri) = parse_prefix_decl(rest)?;
            prefixes.insert(name, iri);
        } else {
            body.push(line);
        }
    }

    Ok((prefixes, body.join(" ")))
}

/// `rest` is everything after the `PREFIX` keyword: `name: <iri>`.
fn parse_prefix_decl(rest: &str) -> Result<(String, String), String> {
    let rest = rest.trim();
    let colon = rest
        .find(':')
        .ok_or_else(|| format!("PREFIX declaration needs 'name: <iri>', got: '{}'", rest))?;
    let name = rest[..colon].trim().to_string();
    let iri_part = rest[colon + 1..].trim();
    if !iri_part.starts_with('<') || !iri_part.ends_with('>') {
        return Err(format!(
            "PREFIX '{}' must be bound to <iri>, got: '{}'",
            name, iri_part
        ));
    }
    Ok((name, iri_part[1..iri_part.len() - 1].to_string()))
}

/// If `s` starts with `word` as a whole word, return the remainder.
fn strip_keyword<'a>(s: &'a str, word: &str) -> Option<&'a str> {
    let rest = s.strip_prefix(word)?;
    match rest.chars().next() {
        None => Some(rest),
        Some(c) if !c.is_ascii_alphanumeric() && c != '_' => Some(rest),
        Some(_) => None,
    }
}

/// Split a leading `[label]` off a rule, returning `(label, remainder)`.
fn split_label(rule_str: &str) -> Result<(Option<String>, &str), String> {
    if !rule_str.starts_with('[') {
        return Ok((None, rule_str));
    }
    let close = rule_str
        .find(']')
        .ok_or_else(|| format!("Rule label: missing ']' in '{}'", rule_str))?;
    let label = rule_str[1..close].trim().to_string();
    if label.is_empty() {
        return Err(format!("Rule label: empty '[]' in '{}'", rule_str));
    }
    Ok((Some(label), &rule_str[close + 1..]))
}

// ── Scanning helpers (aware of `<…>` and `"…"`) ───────────────────────────────

/// Split on `sep` at paren/bracket depth 0, treating `<…>` and `"…"` as opaque.
///
/// The opacity matters: a dotted host name in `<http://utm.example.org/…>` must
/// not terminate a block when splitting on `.`.
fn split_at_depth0(s: &str, sep: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth: i32 = 0;
    let mut in_iri = false;
    let mut in_str = false;

    for ch in s.chars() {
        if in_iri {
            current.push(ch);
            if ch == '>' {
                in_iri = false;
            }
            continue;
        }
        if in_str {
            current.push(ch);
            if ch == '"' {
                in_str = false;
            }
            continue;
        }
        match ch {
            '<' => {
                in_iri = true;
                current.push(ch);
            }
            '"' => {
                in_str = true;
                current.push(ch);
            }
            '(' | '[' => {
                depth += 1;
                current.push(ch);
            }
            ')' | ']' => {
                if depth > 0 {
                    depth -= 1;
                }
                current.push(ch);
            }
            c if c == sep && depth == 0 => {
                parts.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current);
    }
    parts
}

/// Find the byte offset of `:-` at depth 0, outside `<…>` and `"…"`.
fn find_rule_sep(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth: i32 = 0;
    let mut in_iri = false;
    let mut in_str = false;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if in_iri {
            if b == b'>' {
                in_iri = false;
            }
        } else if in_str {
            if b == b'"' {
                in_str = false;
            }
        } else {
            match b {
                b'<' => in_iri = true,
                b'"' => in_str = true,
                b'(' | b'[' => depth += 1,
                b')' | b']' => {
                    if depth > 0 {
                        depth -= 1;
                    }
                }
                b':' if depth == 0 && i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                    return Some(i);
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Find `word` as a whole word, ignoring occurrences inside `<…>` or `"…"`.
fn find_whole_word(text: &str, word: &str) -> Option<usize> {
    let wlen = word.len();
    let tbytes = text.as_bytes();
    let wbytes = word.as_bytes();
    let mut in_iri = false;
    let mut in_str = false;
    let mut i = 0;

    while i < text.len() {
        let b = tbytes[i];
        if in_iri {
            if b == b'>' {
                in_iri = false;
            }
            i += 1;
            continue;
        }
        if in_str {
            if b == b'"' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'<' => {
                in_iri = true;
                i += 1;
                continue;
            }
            b'"' => {
                in_str = true;
                i += 1;
                continue;
            }
            _ => {}
        }
        if i + wlen <= text.len() && &tbytes[i..i + wlen] == wbytes {
            let before_ok =
                i == 0 || (!tbytes[i - 1].is_ascii_alphanumeric() && tbytes[i - 1] != b'_');
            let after_pos = i + wlen;
            let after_ok = after_pos >= text.len()
                || (!tbytes[after_pos].is_ascii_alphanumeric() && tbytes[after_pos] != b'_');
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

// ── Terms and atoms ───────────────────────────────────────────────────────────

/// Resolve an IRI written as `<iri>`, `pfx:local`, or a bare token.
fn expand_iri_text(raw: &str, prefixes: &PrefixMap) -> String {
    if raw.starts_with('<') && raw.ends_with('>') && raw.len() >= 2 {
        return raw[1..raw.len() - 1].to_string();
    }
    if let Some(colon) = raw.find(':') {
        if let Some(base) = prefixes.get(&raw[..colon]) {
            return format!("{}{}", base, &raw[colon + 1..]);
        }
    }
    raw.to_string()
}

fn parse_term(s: &str, dict: &mut Dictionary, prefixes: &PrefixMap) -> Result<Term, String> {
    let s = s.trim();
    if let Some(name) = s.strip_prefix('?') {
        Ok(Term::Variable(name.to_string()))
    } else if s.starts_with('<') && s.ends_with('>') && s.len() >= 2 {
        Ok(Term::Constant(dict.encode(&s[1..s.len() - 1])))
    } else if s.starts_with('"') {
        // literal kept with quotes
        Ok(Term::Constant(dict.encode(s)))
    } else if s.starts_with("_:") {
        // N-Triples blank node, interned verbatim — `_` is not a prefix.
        Ok(Term::Constant(dict.encode(s)))
    } else if let Some(colon) = s.find(':') {
        let prefix = &s[..colon];
        match prefixes.get(prefix) {
            Some(base) => Ok(Term::Constant(
                dict.encode(&format!("{}{}", base, &s[colon + 1..])),
            )),
            // ":name" with no empty prefix declared is interned verbatim.
            None if prefix.is_empty() => Ok(Term::Constant(dict.encode(s))),
            None => Err(format!(
                "Unknown prefix '{}:' in term '{}' — declare it with `PREFIX {}: <...>`",
                prefix, s, prefix
            )),
        }
    } else {
        Err(format!("Cannot parse term: '{}'", s))
    }
}

/// Every operator keyword that can introduce an atom.
const OP_KEYWORDS: [&str; 9] = [
    "Diamondminus", "Diamond", "Boxminus", "Box", "Prev",
    "Since", "Diamondplus", "Boxplus", "Until",
];

/// Does this depth-0 fragment look like a nested ATOM rather than a bare term?
///
/// Used to tell `((a,b,c), (d,e,f))` — a conjunction — from `(a,b,c)`, a single
/// triple pattern whose parts are terms. Matching operator keywords exactly
/// (rather than sniffing for brackets) keeps a literal like `"a[b]"` from being
/// mistaken for an operator.
fn looks_like_atom(part: &str) -> bool {
    let p = part.trim();
    p.starts_with('(')
        || OP_KEYWORDS
            .iter()
            .any(|k| p.strip_prefix(k).is_some_and(|rest| rest.starts_with('[')))
}

/// Parse every balanced `(…)` group in a section into triple patterns.
/// Shared by the `PATTERN` and `EXPIRY` sections of a shape block.
fn parse_pattern_group(
    text: &str,
    dict: &mut Dictionary,
    prefixes: &PrefixMap,
) -> Result<Vec<TriplePattern>, String> {
    let mut patterns = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'(' {
            let start = i;
            let mut depth = 1usize;
            i += 1;
            while i < bytes.len() && depth > 0 {
                match bytes[i] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
            patterns.push(parse_triple_pattern(&text[start..i], dict, prefixes)?);
        } else {
            i += 1;
        }
    }
    Ok(patterns)
}

fn parse_triple_pattern(
    s: &str,
    dict: &mut Dictionary,
    prefixes: &PrefixMap,
) -> Result<TriplePattern, String> {
    let s = s.trim();
    let inner = if s.starts_with('(') && s.ends_with(')') {
        &s[1..s.len() - 1]
    } else {
        s
    };
    let parts = split_at_depth0(inner, ',');
    if parts.len() != 3 {
        return Err(format!(
            "Expected 3 terms in triple pattern, got {}: '{}'",
            parts.len(),
            s
        ));
    }
    Ok((
        parse_term(parts[0].trim(), dict, prefixes)?,
        parse_term(parts[1].trim(), dict, prefixes)?,
        parse_term(parts[2].trim(), dict, prefixes)?,
    ))
}

fn parse_temporal_atom(
    s: &str,
    dict: &mut Dictionary,
    prefixes: &PrefixMap,
    mode: Mode,
) -> Result<TemporalAtom, String> {
    let s = s.trim();
    if s.starts_with('(') {
        if s.ends_with(')') {
            let parts = split_at_depth0(&s[1..s.len() - 1], ',');
            // `((a,b,c), (d,e,f))` is a conjunction under an operator; `(a,b,c)`
            // is a single pattern, whose depth-0 parts are bare terms.
            if parts.len() >= 2 && parts.iter().all(|p| looks_like_atom(p)) {
                let mut atoms = Vec::new();
                for part in &parts {
                    atoms.push(parse_temporal_atom(part.trim(), dict, prefixes, mode)?);
                }
                return Ok(TemporalAtom::Conj(atoms));
            }
            // Redundant grouping parens around one atom, as in
            // `Diamond[0,10](Box[1,11](...))`. Unwrap and recurse.
            if parts.len() == 1 && looks_like_atom(&parts[0]) {
                return parse_temporal_atom(parts[0].trim(), dict, prefixes, mode);
            }
        }
        return Ok(TemporalAtom::Base(parse_triple_pattern(s, dict, prefixes)?));
    }
    // Past operators (`Diamond`≡`Diamondminus`, `Box`≡`Boxminus`).
    for (kw, kind) in [
        ("Diamondminus[", "Diamond"),
        ("Diamond[", "Diamond"),
        ("Boxminus[", "Box"),
        ("Box[", "Box"),
        ("Prev[", "Prev"),
    ] {
        if let Some(rest) = s.strip_prefix(kw) {
            return parse_interval_atom(rest, dict, prefixes, kind, mode);
        }
    }
    if let Some(rest) = s.strip_prefix("Since[") {
        return parse_binary_atom(rest, dict, prefixes, mode, "Since");
    }
    // Future operators — static data only.
    for (kw, kind) in [
        ("Diamondplus[", "Diamondplus"),
        ("Boxplus[", "Boxplus"),
    ] {
        if let Some(rest) = s.strip_prefix(kw) {
            require_static(mode, kind)?;
            return parse_interval_atom(rest, dict, prefixes, kind, mode);
        }
    }
    if let Some(rest) = s.strip_prefix("Until[") {
        require_static(mode, "Until")?;
        return parse_binary_atom(rest, dict, prefixes, mode, "Until");
    }
    Err(format!("Cannot parse temporal atom: '{}'", s))
}

fn require_static(mode: Mode, op: &str) -> Result<(), String> {
    if mode == Mode::Static {
        Ok(())
    } else {
        Err(format!(
            "future operator '{}' requires static data (streaming is past-only)",
            op
        ))
    }
}

fn parse_interval_atom(
    rest: &str,
    dict: &mut Dictionary,
    prefixes: &PrefixMap,
    kind: &str,
    mode: Mode,
) -> Result<TemporalAtom, String> {
    let close = rest
        .find(']')
        .ok_or_else(|| format!("Missing ']' in {} interval", kind))?;
    let interval = parse_interval(&rest[..close])?;
    let after = rest[close + 1..].trim();
    let inner = Box::new(parse_temporal_atom(after, dict, prefixes, mode)?);
    match kind {
        "Diamond" => Ok(TemporalAtom::Diamond { interval, inner }),
        "Box" => Ok(TemporalAtom::Box_ { interval, inner }),
        "Prev" => Ok(TemporalAtom::Prev { interval, inner }),
        "Diamondplus" => Ok(TemporalAtom::DiamondPlus { interval, inner }),
        "Boxplus" => Ok(TemporalAtom::BoxPlus { interval, inner }),
        _ => Err(format!("Unknown operator: {}", kind)),
    }
}

/// Binary operator `Kind[a,b](phi, psi)` — `Since` (past) or `Until` (future).
fn parse_binary_atom(
    rest: &str,
    dict: &mut Dictionary,
    prefixes: &PrefixMap,
    mode: Mode,
    kind: &str,
) -> Result<TemporalAtom, String> {
    let close = rest
        .find(']')
        .ok_or_else(|| format!("Missing ']' in {} interval", kind))?;
    let interval = parse_interval(&rest[..close])?;
    let after = rest[close + 1..].trim();

    if !after.starts_with('(') || !after.ends_with(')') {
        return Err(format!(
            "{} expects (phi, psi) after interval, got: '{}'",
            kind, after
        ));
    }
    let inner = &after[1..after.len() - 1];
    let parts = split_at_depth0(inner, ',');
    if parts.len() < 2 {
        return Err(format!("{} needs 2 atoms in (phi, psi)", kind));
    }
    let phi = Box::new(parse_temporal_atom(parts[0].trim(), dict, prefixes, mode)?);
    let psi_str = if parts.len() == 2 {
        parts[1].trim().to_string()
    } else {
        parts[1..]
            .iter()
            .map(|s| s.trim())
            .collect::<Vec<_>>()
            .join(",")
    };
    let psi = Box::new(parse_temporal_atom(&psi_str, dict, prefixes, mode)?);
    match kind {
        "Since" => Ok(TemporalAtom::Since { interval, phi, psi }),
        "Until" => Ok(TemporalAtom::Until { interval, phi, psi }),
        _ => Err(format!("Unknown binary operator: {}", kind)),
    }
}
