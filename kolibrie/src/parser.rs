/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::neural_relations::{
    execute_train_decl, materialize_neural_relations_for_patterns, register_neural_declarations,
};
use crate::sparql_database::SparqlDatabase;
use datalog::reasoning::Reasoner;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{char, multispace0, multispace1, space0, space1},
    combinator::{opt, recognize},
    multi::{many0, many1},
    sequence::{delimited, preceded, terminated},
    IResult, Parser,
};
use rayon::str;
use shared::dictionary::Dictionary;
use shared::hybrid::{
    encode_hybrid_results_as_rdf_star, HybridConfig, HybridMetrics, HybridProbabilityResult,
    HybridReason, SeedSnapshot, ThresholdPolicyKind,
};
use shared::provenance::Provenance;
use shared::query::*;
use shared::rule::FilterCondition;
use shared::rule::Rule;
use shared::terms::*;
use shared::triple::Triple;
// Add RSP imports
use crate::rsp::r2s::{Relation2StreamOperator, StreamOperator};
use crate::rsp::s2r::{
    CSPARQLWindow, ContentContainer, Report, ReportStrategy, Tick, WindowTriple,
};
use std::collections::HashMap;
use std::time::Duration;

// Helper function to recognize identifiers
pub fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '-').parse(input)
}

// Parser for a prefixed identifier like ex:worksAt
pub fn prefixed_identifier(input: &str) -> IResult<&str, &str> {
    sparql_prefixed_name(input)
}

// Parser for a predicate (either prefixed or unprefixed)
pub fn predicate(input: &str) -> IResult<&str, &str> {
    alt((
        parse_uri,
        variable,
        recognize((char(':'), identifier)),
        prefixed_identifier,
        tag("a"),
    ))
    .parse(input)
}

// Parser for SPARQL variables (e.g., ?person or $person)
pub fn variable(input: &str) -> IResult<&str, &str> {
    sparql_variable(input)
}

// Parser for a literal value within double quotes
pub fn parse_literal(input: &str) -> IResult<&str, &str> {
    let (remaining, literal) = sparql_quoted_literal(input)?;
    if !literal.starts_with('"') || literal.starts_with("\"\"\"") {
        return sparql_error(literal, nom::error::ErrorKind::Char);
    }
    let mut escaped = false;
    for (offset, character) in literal[1..].char_indices() {
        if character == '"' && !escaped {
            return Ok((remaining, &literal[1..1 + offset]));
        }
        escaped = character == '\\' && !escaped;
        if character != '\\' {
            escaped = false;
        }
    }
    sparql_error(literal, nom::error::ErrorKind::Escaped)
}

// Parser for a URI within angle brackets
pub fn parse_uri(input: &str) -> IResult<&str, &str> {
    sparql_iri(input).map(|(remaining, iri)| (remaining, &iri[1..iri.len() - 1]))
}

// Parser for a full URI including angle brackets (e.g., `<http://...>`)
pub fn parse_full_uri(input: &str) -> IResult<&str, &str> {
    sparql_iri(input)
}

// Parser for a full literal including quotes and optional lang/datatype
pub fn parse_full_literal(input: &str) -> IResult<&str, &str> {
    sparql_quoted_literal(input)
}

/// Parse a subject or object that can appear inside a quoted triple.
/// Handles: quoted triples (recursive), full URIs, variables, full literals,
/// prefixed names, and bare identifiers.
pub fn parse_qt_subject_or_object(input: &str) -> IResult<&str, &str> {
    alt((
        parse_quoted_triple,
        parse_full_uri,
        variable,
        parse_full_literal,
        recognize((char(':'), identifier)),
        prefixed_identifier,
        identifier,
    ))
    .parse(input)
}

/// Parse a quoted triple: `<< subject predicate object >>`
/// Returns the entire `<< ... >>` as a single string slice.
pub fn parse_quoted_triple(input: &str) -> IResult<&str, &str> {
    sparql_quoted_triple(input)
}

/// Parse annotation syntax: `{| predicate object ; ... |}`
/// Returns predicate-object pairs for the annotation.
pub fn parse_annotation(input: &str) -> IResult<&str, Vec<(&str, &str)>> {
    let (input, _) = (tag("{|"), multispace0).parse(input)?;
    let (input, first) = parse_predicate_object(input)?;
    let (input, rest) = many0(preceded(
        (multispace0, char(';'), multispace0),
        parse_predicate_object,
    ))
    .parse(input)?;
    let (input, _) = (multispace0, tag("|}")).parse(input)?;
    let mut pairs = vec![first];
    pairs.extend(rest);
    Ok((input, pairs))
}

// Helper parser to parse a single predicate-object pair.
pub fn parse_predicate_object(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, p) = predicate(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, o) = alt((
        parse_quoted_triple,                // << s p o >> (RDF-star)
        parse_uri,                          // <http://...>
        variable,                           // ?variable
        parse_literal,                      // "literal"
        recognize((char(':'), identifier)), // :localname (like :Stream)
        prefixed_identifier,                // prefix:localname
        identifier,                         // simple identifier
    ))
    .parse(input)?;
    Ok((input, (p, o)))
}

pub fn parse_triple_block(input: &str) -> IResult<&str, Vec<(&str, &str, &str)>> {
    let (input, subject) = alt((
        parse_quoted_triple,                // << s p o >> (RDF-star)
        parse_uri,                          // <http://...>
        variable,                           // ?variable
        recognize((char(':'), identifier)), // :localname
        prefixed_identifier,                // prefix:localname
        identifier,                         // simple identifier
    ))
    .parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // First predicate-object pair
    let (input, first_po) = parse_predicate_object(input)?;

    // Zero or more additional predicate-object pairs separated by semicolon
    let (input, rest_po) = many0(preceded(
        (multispace0, char(';'), multispace0),
        parse_predicate_object,
    ))
    .parse(input)?;

    // Gather all (predicate, object) pairs
    let mut pairs = vec![first_po];
    pairs.extend(rest_po);

    // Convert each pair into a triple by reusing the same subject
    let triples = pairs
        .into_iter()
        .map(|(p, o)| {
            let resolved_p = if p == "a" {
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            } else {
                p
            };
            (subject, resolved_p, o)
        })
        .collect();

    Ok((input, triples))
}

// Parser for values in the VALUES clause
pub fn parse_value_term(input: &str) -> IResult<&str, Value> {
    sparql_value(input)
}

// Parser for the VALUES clause
pub fn parse_values(input: &str) -> IResult<&str, ValuesClause<'_>> {
    sparql_values_clause(input)
}

pub fn parse_aggregate(input: &str) -> IResult<&str, (&str, &str, Option<&str>)> {
    sparql_aggregate(input)
}

pub fn parse_select(input: &str) -> IResult<&str, Vec<(&str, &str, Option<&str>)>> {
    let (mut input, _) = sparql_keyword(input, "SELECT")?;
    if let Ok((remaining, _)) = sparql_keyword(input, "DISTINCT") {
        input = remaining;
    }
    sparql_projection_items(input)
}

pub fn parse_arithmetic_expression(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    sparql_filter_arithmetic(input)
}

// Parse a single comparison expression like ?var > 10
pub fn parse_comparison(input: &str) -> IResult<&str, FilterExpression<'_>> {
    sparql_filter_comparison(input)
}

// Parse a complete FILTER clause
pub fn parse_filter(input: &str) -> IResult<&str, FilterExpression<'_>> {
    sparql_filter_clause(input)
}

// Parser for BIND clauses: BIND(funcName(?var, "literal") AS ?newVar)
pub fn parse_bind(input: &str) -> IResult<&str, (&str, Vec<&str>, &str)> {
    sparql_bind_clause(input)
}

pub fn parse_subquery<'a>(input: &'a str) -> IResult<&'a str, SubQuery<'a>> {
    sparql_subquery(input)
}

// Parser for WINDOW block inside WHERE clause
pub fn parse_window_block(input: &str) -> IResult<&str, WindowBlock<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("WINDOW").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse window name (like :wind)
    let (input, window_name) =
        alt((recognize((char(':'), identifier)), identifier)).parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('{').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse triple patterns inside the window block
    let (input, pattern_blocks) = many0(terminated(
        parse_triple_block,
        (multispace0, opt(char('.')), multispace0),
    ))
    .parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('}').parse(input)?;

    // Flatten all pattern blocks
    let patterns = pattern_blocks.into_iter().flatten().collect();

    Ok((
        input,
        WindowBlock {
            window_name,
            patterns,
        },
    ))
}

/// Parse `NOT triple_block` — negation-as-failure body atom.
/// Returns the list of negated triple patterns.
fn parse_not_triple_block(input: &str) -> IResult<&str, Vec<(&str, &str, &str)>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("NOT").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    parse_triple_block(input)
}

pub fn parse_where(
    input: &str,
) -> IResult<
    &str,
    (
        Vec<(&str, &str, &str)>,
        Vec<FilterExpression<'_>>,
        Option<ValuesClause<'_>>,
        Vec<(&str, Vec<&str>, &str)>,
        Vec<SubQuery<'_>>,
        Vec<WindowBlock<'_>>,
        Vec<(&str, &str, &str)>, // negated triple patterns (NOT X)
    ),
> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("WHERE").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('{').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    let mut patterns = Vec::new();
    let mut neg_patterns = Vec::new();
    let mut filters = Vec::new();
    let mut binds = Vec::new();
    let mut subqueries = Vec::new();
    let mut values_clause = None;
    let mut window_blocks = Vec::new();
    let mut current_input = input;

    // Parse components until we reach the closing brace
    loop {
        let (new_input, _) = multispace0.parse(current_input)?;
        current_input = new_input;

        // Try to match a closing brace
        if let Ok((new_input, _)) = char::<_, nom::error::Error<&str>>('}').parse(current_input) {
            current_input = new_input;
            break;
        }

        // Try to parse each possible component
        current_input = if let Ok((new_input, window_block)) = parse_window_block(current_input) {
            window_blocks.push(window_block);
            new_input
        } else if let Ok((new_input, not_triples)) = parse_not_triple_block(current_input) {
            neg_patterns.extend(not_triples);
            new_input
        } else if let Ok((new_input, triple_block)) = parse_triple_block(current_input) {
            patterns.extend(triple_block);
            new_input
        } else if let Ok((new_input, filter)) = parse_filter(current_input) {
            filters.push(filter);
            new_input
        } else if let Ok((new_input, bind)) = parse_bind(current_input) {
            binds.push(bind);
            new_input
        } else if let Ok((new_input, subquery)) = parse_subquery(current_input) {
            subqueries.push(subquery);
            new_input
        } else if let Ok((new_input, vals)) = parse_values(current_input) {
            values_clause = Some(vals);
            new_input
        } else {
            return Err(nom::Err::Error(nom::error::Error::new(
                current_input,
                nom::error::ErrorKind::Alt,
            )));
        };

        // Consume any trailing dot
        if let Ok((new_input, _)) = (
            space0::<_, nom::error::Error<&str>>,
            char::<_, nom::error::Error<&str>>('.'),
            space0::<_, nom::error::Error<&str>>,
        )
            .parse(current_input)
        {
            current_input = new_input;
        }
    }

    Ok((
        current_input,
        (
            patterns,
            filters,
            values_clause,
            binds,
            subqueries,
            window_blocks,
            neg_patterns,
        ),
    ))
}

// Parser for REGISTER clause
pub fn parse_register_clause(input: &str) -> IResult<&str, RegisterClause<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("REGISTER").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse stream type (RSTREAM, ISTREAM, DSTREAM)
    let (input, stream_type) = parse_stream_type(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse output stream IRI
    let (input, output_iri) = parse_uri(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse AS keyword
    let (input, _) = tag("AS").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse SELECT clause
    let (input, variables) = parse_select(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse optional FROM NAMED WINDOW clause (this comes BEFORE WHERE in your example)
    let (input, window_clause) =
        many1(preceded(multispace0, parse_from_named_window)).parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse WHERE clause with window support
    let (input, (patterns, filters, values_clause, binds, subqueries, window_blocks, _)) =
        parse_where(input)?;

    Ok((
        input,
        RegisterClause {
            stream_type,
            output_stream_iri: output_iri,
            query: RSPQLSelectQuery {
                variables,
                window_clause,
                where_clause: (patterns, filters, values_clause, binds, subqueries),
                window_blocks,
            },
        },
    ))
}

pub fn parse_group_by(input: &str) -> IResult<&str, Vec<&str>> {
    sparql_group_by_clause(input)
}

// Parser for sort direction (ASC/DESC)
pub fn parse_sort_direction(input: &str) -> IResult<&str, SortDirection> {
    if let Ok((remaining, _)) = sparql_keyword(input, "ASC") {
        Ok((remaining, SortDirection::Asc))
    } else if let Ok((remaining, _)) = sparql_keyword(input, "DESC") {
        Ok((remaining, SortDirection::Desc))
    } else {
        Ok((sparql_skip_ws(input), SortDirection::Asc))
    }
}

// Parser for a single ORDER BY condition
pub fn parse_order_condition(input: &str) -> IResult<&str, OrderCondition<'_>> {
    sparql_order_condition(input)
}

// Alternative simpler parser for ORDER BY condition (variable with optional direction)
pub fn parse_simple_order_condition(input: &str) -> IResult<&str, OrderCondition<'_>> {
    sparql_order_condition(input)
}

// Main ORDER BY parser
pub fn parse_order_by(input: &str) -> IResult<&str, Vec<OrderCondition<'_>>> {
    sparql_order_by_clause(input)
}

// Public PREFIX primitive backed by the same token-aware parser as requests.
pub fn parse_prefix(input: &str) -> IResult<&str, (&str, &str)> {
    sparql_prefix_declaration(input)
}

/// Compatibility parser for standalone `INSERT { ... }` data aliases.
///
/// It produces the same `InsertClause` used by the standard update AST; no
/// legacy triple-only representation or executor is retained.
pub fn parse_insert(input: &str) -> IResult<&str, InsertClause<'_>> {
    let (input, _) = sparql_keyword(input, "INSERT")?;
    let input = sparql_keyword(input, "DATA")
        .map(|(remaining, _)| remaining)
        .unwrap_or(input);
    let (remaining, quads) = sparql_quad_block(input)?;
    if let Some(offending) = sparql_quads_first_variable(&quads) {
        return sparql_error(offending, nom::error::ErrorKind::Verify);
    }
    Ok((remaining, InsertClause { quads }))
}

/// Compatibility parser for standalone `DELETE { ... }` data aliases.
pub fn parse_delete(input: &str) -> IResult<&str, DeleteClause<'_>> {
    let (input, _) = sparql_keyword(input, "DELETE")?;
    let input = sparql_keyword(input, "DATA")
        .map(|(remaining, _)| remaining)
        .unwrap_or(input);
    let (remaining, quads) = sparql_quad_block(input)?;
    if let Some(offending) =
        sparql_quads_first_variable(&quads).or_else(|| sparql_quads_first_blank_node(&quads))
    {
        return sparql_error(offending, nom::error::ErrorKind::Verify);
    }
    Ok((remaining, DeleteClause { quads }))
}

pub fn parse_construct_clause(input: &str) -> IResult<&str, Vec<(&str, &str, &str)>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("CONSTRUCT").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse multiple conclusion triples in CONSTRUCT block
    let (input, conclusions) = delimited(
        char('{'),
        preceded(
            multispace0,
            terminated(parse_triple_block, opt((multispace0, char('.')))),
        ),
        preceded(multispace0, char('}')),
    )
    .parse(input)?;

    Ok((input, conclusions))
}

// Public LIMIT primitive backed by the same parser as SELECT.
pub fn parse_limit(input: &str) -> IResult<&str, usize> {
    sparql_limit_clause(input)
}

// ---------------------------------------------------------------------------
// Unified recursive SPARQL parser
// ---------------------------------------------------------------------------

/// Removes SPARQL whitespace and `#` comments. This is intentionally separate
/// from `multispace0`: comments are whitespace in SPARQL and must not leak into
/// keyword or punctuation parsing.
fn sparql_skip_ws(mut input: &str) -> &str {
    loop {
        let before = input.len();
        input = input.trim_start_matches(|character: char| character.is_whitespace());
        if let Some(comment) = input.strip_prefix('#') {
            input = comment
                .find(['\r', '\n'])
                .map_or("", |newline| &comment[newline..]);
            continue;
        }
        if input.len() == before {
            return input;
        }
    }
}

fn sparql_error<'a, T>(input: &'a str, kind: nom::error::ErrorKind) -> IResult<&'a str, T> {
    Err(nom::Err::Error(nom::error::Error::new(input, kind)))
}

fn sparql_name_character(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '-' | ':')
}

/// A case-insensitive keyword parser with a SPARQL token boundary.
fn sparql_keyword<'a>(input: &'a str, keyword: &str) -> IResult<&'a str, &'a str> {
    let input = sparql_skip_ws(input);
    let (remaining, matched) = nom::bytes::complete::tag_no_case(keyword).parse(input)?;
    if remaining.chars().next().is_some_and(sparql_name_character) {
        return sparql_error(input, nom::error::ErrorKind::Tag);
    }
    Ok((remaining, matched))
}

fn sparql_starts_keyword(input: &str, keyword: &str) -> bool {
    sparql_keyword(input, keyword).is_ok()
}

fn sparql_char(input: &str, expected: char) -> IResult<&str, char> {
    char(expected).parse(sparql_skip_ws(input))
}

fn sparql_variable(input: &str) -> IResult<&str, &str> {
    let input = sparql_skip_ws(input);
    let Some(sigil) = input.chars().next() else {
        return sparql_error(input, nom::error::ErrorKind::Eof);
    };
    if !matches!(sigil, '?' | '$') {
        return sparql_error(input, nom::error::ErrorKind::Char);
    }
    let name_start = sigil.len_utf8();
    let mut name_end = name_start;
    for (offset, character) in input[name_start..].char_indices() {
        if character.is_alphanumeric() || character == '_' {
            name_end = name_start + offset + character.len_utf8();
        } else {
            break;
        }
    }
    if name_end == name_start {
        return sparql_error(input, nom::error::ErrorKind::TakeWhile1);
    }
    Ok((&input[name_end..], &input[..name_end]))
}

fn sparql_unicode_escape_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let digits = match bytes.get(1) {
        Some(b'u') => 4,
        Some(b'U') => 8,
        _ => return None,
    };
    let end = 2 + digits;
    let hexadecimal = bytes.get(2..end)?;
    if !hexadecimal.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let value = u32::from_str_radix(&input[2..end], 16).ok()?;
    char::from_u32(value)?;
    Some(end)
}

fn sparql_pn_chars_base(character: char) -> bool {
    character.is_ascii_alphabetic()
        || character.is_alphabetic()
        || matches!(
            character,
            '\u{00C0}'..='\u{00D6}'
                | '\u{00D8}'..='\u{00F6}'
                | '\u{00F8}'..='\u{02FF}'
                | '\u{0370}'..='\u{037D}'
                | '\u{037F}'..='\u{1FFF}'
                | '\u{200C}'..='\u{200D}'
                | '\u{2070}'..='\u{218F}'
                | '\u{2C00}'..='\u{2FEF}'
                | '\u{3001}'..='\u{D7FF}'
                | '\u{F900}'..='\u{FDCF}'
                | '\u{FDF0}'..='\u{FFFD}'
                | '\u{10000}'..='\u{EFFFF}'
        )
}

fn sparql_pn_chars_u(character: char) -> bool {
    character == '_' || sparql_pn_chars_base(character)
}

fn sparql_pn_chars(character: char) -> bool {
    sparql_pn_chars_u(character)
        || character.is_ascii_digit()
        || matches!(
            character,
            '-' | '\u{00B7}' | '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}'
        )
}

fn sparql_invalid_pn_prefix(prefix: &str) -> Option<&str> {
    let Some(first) = prefix.chars().next() else {
        return None;
    };
    if !sparql_pn_chars_base(first) {
        return Some(prefix);
    }

    let mut previous_was_dot = false;
    for (offset, character) in prefix[first.len_utf8()..].char_indices() {
        if character == '.' {
            previous_was_dot = true;
        } else if sparql_pn_chars(character) {
            previous_was_dot = false;
        } else {
            return Some(&prefix[first.len_utf8() + offset..]);
        }
    }
    previous_was_dot.then(|| &prefix[prefix.len() - 1..])
}

fn sparql_iri(input: &str) -> IResult<&str, &str> {
    let input = sparql_skip_ws(input);
    if !input.starts_with('<') {
        return sparql_error(input, nom::error::ErrorKind::Char);
    }

    let mut index = 1;
    while index < input.len() {
        let tail = &input[index..];
        if tail.starts_with('>') {
            let end = index + 1;
            return Ok((&input[end..], &input[..end]));
        }
        if tail.starts_with('\\') {
            let Some(escape_len) = sparql_unicode_escape_len(tail) else {
                return sparql_error(tail, nom::error::ErrorKind::Escaped);
            };
            index += escape_len;
            continue;
        }

        let character = tail.chars().next().expect("valid UTF-8 boundary");
        if character <= '\u{20}' || matches!(character, '<' | '"' | '{' | '}' | '|' | '^' | '`') {
            return sparql_error(tail, nom::error::ErrorKind::Verify);
        }
        index += character.len_utf8();
    }

    sparql_error(input, nom::error::ErrorKind::TakeUntil)
}

fn sparql_blank_node(input: &str) -> IResult<&str, &str> {
    let input = sparql_skip_ws(input);
    let Some(body) = input.strip_prefix("_:") else {
        return sparql_error(input, nom::error::ErrorKind::Tag);
    };
    let Some(first) = body.chars().next() else {
        return sparql_error(input, nom::error::ErrorKind::TakeWhile1);
    };
    if !(sparql_pn_chars_u(first) || first.is_ascii_digit()) {
        return sparql_error(body, nom::error::ErrorKind::Verify);
    }

    let mut index = first.len_utf8();
    let mut token_end = index;
    while index < body.len() {
        let character = body[index..].chars().next().expect("valid UTF-8 boundary");
        if sparql_pn_chars(character) {
            index += character.len_utf8();
            token_end = index;
        } else if character == '.' {
            index += 1;
        } else {
            break;
        }
    }

    Ok((&body[token_end..], &input[..2 + token_end]))
}

fn sparql_prefixed_name(input: &str) -> IResult<&str, &str> {
    let input = sparql_skip_ws(input);
    let Some(colon) = input.find(':') else {
        return sparql_error(input, nom::error::ErrorKind::Verify);
    };
    let prefix = &input[..colon];
    if let Some(offending) = sparql_invalid_pn_prefix(prefix) {
        return sparql_error(offending, nom::error::ErrorKind::Verify);
    }

    let local = &input[colon + 1..];
    let mut index = 0usize;
    let mut token_end = 0usize;
    let mut first = true;
    while index < local.len() {
        let tail = &local[index..];
        let character = tail.chars().next().expect("valid UTF-8 boundary");
        let ordinary = if first {
            sparql_pn_chars_u(character) || character.is_ascii_digit() || character == ':'
        } else {
            sparql_pn_chars(character) || character == ':'
        };
        if ordinary {
            index += character.len_utf8();
            token_end = index;
            first = false;
            continue;
        }
        if character == '.' {
            if first {
                break;
            }
            index += 1;
            continue;
        }
        if character == '%' {
            let escape = tail.as_bytes();
            if escape.len() < 3 || !escape[1].is_ascii_hexdigit() || !escape[2].is_ascii_hexdigit()
            {
                return sparql_error(tail, nom::error::ErrorKind::Escaped);
            }
            index += 3;
            token_end = index;
            first = false;
            continue;
        }
        if character == '\\' {
            let Some(escaped) = tail[1..].chars().next() else {
                return sparql_error(tail, nom::error::ErrorKind::Escaped);
            };
            if !matches!(
                escaped,
                '_' | '~'
                    | '.'
                    | '-'
                    | '!'
                    | '$'
                    | '&'
                    | '\''
                    | '('
                    | ')'
                    | '*'
                    | '+'
                    | ','
                    | ';'
                    | '='
                    | '/'
                    | '?'
                    | '#'
                    | '@'
                    | '%'
            ) {
                return sparql_error(tail, nom::error::ErrorKind::Escaped);
            }
            index += 1 + escaped.len_utf8();
            token_end = index;
            first = false;
            continue;
        }
        break;
    }

    let token_end = colon + 1 + token_end;
    Ok((&input[token_end..], &input[..token_end]))
}

// Kolibrie's established SELECT/ML fragment also accepts bare symbolic RDF
// values (for example a neural class label such as `A`). Keep that lexical
// compatibility in the unified parser without treating reserved keywords as
// part of a longer token.
fn sparql_bare_identifier(input: &str) -> IResult<&str, &str> {
    identifier(sparql_skip_ws(input))
}

fn sparql_numeric_literal(input: &str) -> IResult<&str, &str> {
    let input = sparql_skip_ws(input);
    let bytes = input.as_bytes();
    let mut index = usize::from(matches!(bytes.first(), Some(b'+') | Some(b'-')));
    let integer_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    let integer_digits = index - integer_start;
    let mut fractional_digits = 0;
    if bytes.get(index) == Some(&b'.') && bytes.get(index + 1).is_some_and(u8::is_ascii_digit) {
        index += 1;
        let fraction_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        fractional_digits = index - fraction_start;
    }
    if integer_digits == 0 && fractional_digits == 0 {
        return sparql_error(input, nom::error::ErrorKind::Digit);
    }
    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        let exponent_marker = index;
        index += 1;
        if matches!(bytes.get(index), Some(b'+') | Some(b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if exponent_start == index {
            index = exponent_marker;
        }
    }
    if input[index..]
        .chars()
        .next()
        .is_some_and(|character| character.is_alphabetic() || character == '_')
    {
        return sparql_error(input, nom::error::ErrorKind::Verify);
    }
    Ok((&input[index..], &input[..index]))
}

fn sparql_quoted_literal(input: &str) -> IResult<&str, &str> {
    let input = sparql_skip_ws(input);
    let Some(quote) = input
        .chars()
        .next()
        .filter(|quote| matches!(quote, '\'' | '"'))
    else {
        return sparql_error(input, nom::error::ErrorKind::Char);
    };
    let triple_quoted = input.starts_with(&quote.to_string().repeat(3));
    let delimiter_len = if triple_quoted { 3 } else { 1 };
    let delimiter = if quote == '"' {
        if triple_quoted {
            "\"\"\""
        } else {
            "\""
        }
    } else if triple_quoted {
        "'''"
    } else {
        "'"
    };

    let mut close_end = None;
    let mut index = delimiter_len;
    while index < input.len() {
        if input[index..].starts_with(delimiter) {
            close_end = Some(index + delimiter_len);
            break;
        }
        let character = input[index..].chars().next().expect("valid UTF-8 boundary");
        if !triple_quoted && matches!(character, '\r' | '\n') {
            return sparql_error(&input[index..], nom::error::ErrorKind::Escaped);
        }
        if character == '\\' {
            let escape = &input[index + 1..];
            let Some(escaped) = escape.chars().next() else {
                return sparql_error(&input[index..], nom::error::ErrorKind::Escaped);
            };
            let escape_len = match escaped {
                't' | 'b' | 'n' | 'r' | 'f' | '"' | '\'' | '\\' => escaped.len_utf8(),
                'u' | 'U' => {
                    let digits = if escaped == 'u' { 4 } else { 8 };
                    let hexadecimal = &escape[escaped.len_utf8()..];
                    if hexadecimal.len() < digits
                        || !hexadecimal.as_bytes()[..digits]
                            .iter()
                            .all(u8::is_ascii_hexdigit)
                    {
                        return sparql_error(&input[index..], nom::error::ErrorKind::Escaped);
                    }
                    let value = u32::from_str_radix(&hexadecimal[..digits], 16)
                        .expect("validated hexadecimal escape");
                    if char::from_u32(value).is_none() {
                        return sparql_error(&input[index..], nom::error::ErrorKind::Escaped);
                    }
                    escaped.len_utf8() + digits
                }
                _ => return sparql_error(&input[index..], nom::error::ErrorKind::Escaped),
            };
            index += 1 + escape_len;
            continue;
        }
        index += character.len_utf8();
    }
    let Some(mut literal_end) = close_end else {
        return sparql_error(input, nom::error::ErrorKind::Escaped);
    };

    let suffix = &input[literal_end..];
    if let Some(language) = suffix.strip_prefix('@') {
        let primary_end = language.bytes().take_while(u8::is_ascii_alphabetic).count();
        if primary_end == 0 {
            return sparql_error(suffix, nom::error::ErrorKind::Verify);
        }
        let mut language_end = primary_end;
        while language.as_bytes().get(language_end) == Some(&b'-') {
            let subtag_start = language_end + 1;
            let subtag_len = language[subtag_start..]
                .bytes()
                .take_while(u8::is_ascii_alphanumeric)
                .count();
            if subtag_len == 0 {
                return sparql_error(suffix, nom::error::ErrorKind::Verify);
            }
            language_end = subtag_start + subtag_len;
        }
        if language[language_end..]
            .chars()
            .next()
            .is_some_and(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            return sparql_error(suffix, nom::error::ErrorKind::Verify);
        }
        literal_end += 1 + language_end;
    } else if let Some(datatype) = suffix.strip_prefix("^^") {
        let datatype = sparql_skip_ws(datatype);
        let (remaining, _) = sparql_iri(datatype).or_else(|_| sparql_prefixed_name(datatype))?;
        literal_end = input.len() - remaining.len();
    }

    Ok((&input[literal_end..], &input[..literal_end]))
}

fn sparql_quoted_triple_parts(input: &str) -> IResult<&str, LexicalTriplePattern<'_>> {
    let input = sparql_skip_ws(input);
    let Some(input) = input.strip_prefix("<<") else {
        return sparql_error(input, nom::error::ErrorKind::Tag);
    };
    let (input, subject) = sparql_subject_term(input)?;
    let (input, predicate) = sparql_predicate_term(input)?;
    let (input, object) = sparql_object_term(input)?;
    let input = sparql_skip_ws(input);
    let Some(remaining) = input.strip_prefix(">>") else {
        return sparql_error(input, nom::error::ErrorKind::Tag);
    };
    Ok((remaining, (subject, predicate, object)))
}

/// Parses an RDF-star quoted triple recursively and returns its complete raw
/// source slice. Each nested triple is validated with the same term parsers.
fn sparql_quoted_triple(input: &str) -> IResult<&str, &str> {
    let input = sparql_skip_ws(input);
    let (remaining, _) = sparql_quoted_triple_parts(input)?;
    let consumed = input.len() - remaining.len();
    Ok((remaining, &input[..consumed]))
}

fn sparql_subject_term(input: &str) -> IResult<&str, &str> {
    alt((
        sparql_quoted_triple,
        sparql_variable,
        sparql_iri,
        sparql_blank_node,
        sparql_prefixed_name,
        sparql_bare_identifier,
    ))
    .parse(input)
}

fn sparql_predicate_term(input: &str) -> IResult<&str, &str> {
    if let Ok(parsed) = sparql_variable(input) {
        return Ok(parsed);
    }
    if let Ok(parsed) = sparql_iri(input) {
        return Ok(parsed);
    }
    let input_without_ws = sparql_skip_ws(input);
    if let Some(remaining) = input_without_ws.strip_prefix('a') {
        if !remaining.chars().next().is_some_and(sparql_name_character) {
            return Ok((remaining, &input_without_ws[..1]));
        }
    }
    sparql_prefixed_name(input)
}

fn sparql_object_term(input: &str) -> IResult<&str, &str> {
    alt((
        sparql_quoted_triple,
        sparql_variable,
        sparql_iri,
        sparql_blank_node,
        sparql_quoted_literal,
        sparql_numeric_literal,
        |input| sparql_keyword(input, "true"),
        |input| sparql_keyword(input, "false"),
        sparql_prefixed_name,
        sparql_bare_identifier,
    ))
    .parse(input)
}

fn sparql_graph_name(input: &str) -> IResult<&str, &str> {
    alt((sparql_variable, sparql_iri, sparql_prefixed_name)).parse(input)
}

/// Parses one triples-same-subject statement and expands `;` and `,`
/// abbreviations into ordinary triple patterns.
fn sparql_triples_statement(input: &str) -> IResult<&str, Vec<LexicalTriplePattern<'_>>> {
    let (mut input, subject) = sparql_subject_term(input)?;
    let mut triples = Vec::new();
    loop {
        let (after_predicate, predicate) = sparql_predicate_term(input)?;
        input = after_predicate;
        loop {
            let (after_object, object) = sparql_object_term(input)?;
            triples.push((subject, predicate, object));
            input = after_object;
            let after_ws = sparql_skip_ws(input);
            if let Some(after_comma) = after_ws.strip_prefix(',') {
                input = after_comma;
            } else {
                break;
            }
        }

        let after_ws = sparql_skip_ws(input);
        let Some(after_semicolon) = after_ws.strip_prefix(';') else {
            break;
        };
        let after_semicolon = sparql_skip_ws(after_semicolon);
        if after_semicolon.is_empty()
            || after_semicolon.starts_with(['.', '}'])
            || sparql_starts_keyword(after_semicolon, "GRAPH")
            || sparql_starts_keyword(after_semicolon, "UNION")
        {
            input = after_semicolon;
            break;
        }
        input = after_semicolon;
    }
    Ok((input, triples))
}

fn sparql_filter_operand(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    let input = sparql_skip_ws(input);
    if let Some(after_open) = input.strip_prefix('(') {
        let (after_expression, expression) = sparql_filter_arithmetic(after_open)?;
        let (remaining, _) = sparql_char(after_expression, ')')?;
        return Ok((remaining, expression));
    }

    alt((
        sparql_variable,
        sparql_quoted_literal,
        sparql_numeric_literal,
        sparql_iri,
        |input| sparql_keyword(input, "true"),
        |input| sparql_keyword(input, "false"),
        sparql_prefixed_name,
    ))
    .parse(input)
    .map(|(remaining, operand)| (remaining, ArithmeticExpression::Operand(operand)))
}

fn sparql_filter_product(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    let (mut input, mut expression) = sparql_filter_operand(input)?;
    loop {
        let operator_input = sparql_skip_ws(input);
        let Some(operator) = operator_input
            .chars()
            .next()
            .filter(|operator| matches!(operator, '*' | '/'))
        else {
            break;
        };
        let (remaining, right) = sparql_filter_operand(&operator_input[operator.len_utf8()..])?;
        expression = if operator == '*' {
            ArithmeticExpression::Multiply(Box::new(expression), Box::new(right))
        } else {
            ArithmeticExpression::Divide(Box::new(expression), Box::new(right))
        };
        input = remaining;
    }
    Ok((input, expression))
}

fn sparql_filter_arithmetic(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    let (mut input, mut expression) = sparql_filter_product(input)?;
    loop {
        let operator_input = sparql_skip_ws(input);
        let Some(operator) = operator_input
            .chars()
            .next()
            .filter(|operator| matches!(operator, '+' | '-'))
        else {
            break;
        };
        let (remaining, right) = sparql_filter_product(&operator_input[operator.len_utf8()..])?;
        expression = if operator == '+' {
            ArithmeticExpression::Add(Box::new(expression), Box::new(right))
        } else {
            ArithmeticExpression::Subtract(Box::new(expression), Box::new(right))
        };
        input = remaining;
    }
    Ok((input, expression))
}

fn sparql_filter_operator(input: &str) -> IResult<&str, &str> {
    let input = sparql_skip_ws(input);
    for operator in ["!=", ">=", "<=", "=", ">", "<"] {
        if let Some(remaining) = input.strip_prefix(operator) {
            return Ok((remaining, &input[..operator.len()]));
        }
    }
    sparql_error(input, nom::error::ErrorKind::Tag)
}

fn sparql_filter_comparison(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let left_start = sparql_skip_ws(input);
    let (after_left, _) = sparql_filter_arithmetic(left_start)?;
    let left_end = left_start.len() - after_left.len();
    let left = left_start[..left_end].trim();
    let (after_operator, operator) = sparql_filter_operator(after_left)?;
    let right_start = sparql_skip_ws(after_operator);
    let (remaining, _) = sparql_filter_arithmetic(right_start)?;
    let right_end = right_start.len() - remaining.len();
    let right = right_start[..right_end].trim();
    Ok((
        remaining,
        FilterExpression::Comparison(left, operator, right),
    ))
}

fn sparql_filter_function(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let input = sparql_skip_ws(input);
    let (input, function_name) = if let Ok((remaining, _)) = sparql_keyword(input, "isTRIPLE") {
        (remaining, "isTRIPLE")
    } else if let Ok((remaining, _)) = sparql_keyword(input, "TRIPLE") {
        (remaining, "TRIPLE")
    } else if let Ok((remaining, _)) = sparql_keyword(input, "SUBJECT") {
        (remaining, "SUBJECT")
    } else if let Ok((remaining, _)) = sparql_keyword(input, "PREDICATE") {
        (remaining, "PREDICATE")
    } else if let Ok((remaining, _)) = sparql_keyword(input, "OBJECT") {
        (remaining, "OBJECT")
    } else {
        return sparql_error(input, nom::error::ErrorKind::Alt);
    };
    let (mut input, _) = sparql_char(input, '(')?;
    let mut arguments = Vec::new();
    loop {
        let (remaining, argument) = alt((
            sparql_quoted_triple,
            sparql_variable,
            sparql_quoted_literal,
            sparql_numeric_literal,
            sparql_iri,
            sparql_prefixed_name,
        ))
        .parse(input)?;
        arguments.push(argument);
        input = sparql_skip_ws(remaining);
        if let Some(remaining) = input.strip_prefix(',') {
            input = remaining;
        } else {
            break;
        }
    }
    let (input, _) = sparql_char(input, ')')?;
    Ok((
        input,
        FilterExpression::FunctionCall(function_name, arguments),
    ))
}

fn sparql_filter_atom(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let input = sparql_skip_ws(input);
    if let Some(after_not) = input.strip_prefix('!') {
        if !after_not.starts_with('=') {
            let (remaining, expression) = sparql_filter_atom(after_not)?;
            return Ok((remaining, FilterExpression::Not(Box::new(expression))));
        }
    }
    if let Ok(parsed) = sparql_filter_function(input) {
        return Ok(parsed);
    }
    if let Ok(parsed) = sparql_filter_comparison(input) {
        return Ok(parsed);
    }
    if let Some(after_open) = input.strip_prefix('(') {
        let (after_expression, expression) = sparql_filter_or(after_open)?;
        let (remaining, _) = sparql_char(after_expression, ')')?;
        return Ok((remaining, expression));
    }
    sparql_filter_arithmetic(input).map(|(remaining, expression)| {
        (
            remaining,
            FilterExpression::ArithmeticExpr(Box::new(expression)),
        )
    })
}

fn sparql_filter_and(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (mut input, mut expression) = sparql_filter_atom(input)?;
    loop {
        let operator_input = sparql_skip_ws(input);
        let Some(remaining) = operator_input.strip_prefix("&&") else {
            break;
        };
        let (after_right, right) = sparql_filter_atom(remaining)?;
        expression = FilterExpression::And(Box::new(expression), Box::new(right));
        input = after_right;
    }
    Ok((input, expression))
}

fn sparql_filter_or(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (mut input, mut expression) = sparql_filter_and(input)?;
    loop {
        let operator_input = sparql_skip_ws(input);
        let Some(remaining) = operator_input.strip_prefix("||") else {
            break;
        };
        let (after_right, right) = sparql_filter_and(remaining)?;
        expression = FilterExpression::Or(Box::new(expression), Box::new(right));
        input = after_right;
    }
    Ok((input, expression))
}

fn sparql_filter_clause(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (input, _) = sparql_keyword(input, "FILTER")?;
    let (input, _) = sparql_char(input, '(')?;
    let (input, expression) = sparql_filter_or(input)?;
    let (input, _) = sparql_char(input, ')')?;
    Ok((input, expression))
}

fn sparql_bind_argument(input: &str) -> IResult<&str, &str> {
    if let Ok(parsed) = sparql_variable(input) {
        return Ok(parsed);
    }
    if let Ok((remaining, literal)) = sparql_quoted_literal(input) {
        // Preserve the historical BIND representation: simple quoted
        // arguments omit their surrounding quotes.
        if (literal.starts_with('"') && literal.ends_with('"'))
            || (literal.starts_with('\'') && literal.ends_with('\''))
        {
            return Ok((remaining, &literal[1..literal.len() - 1]));
        }
        return Ok((remaining, literal));
    }
    sparql_numeric_literal(input)
}

fn sparql_bind_clause(input: &str) -> IResult<&str, BindClause<'_>> {
    let (input, _) = sparql_keyword(input, "BIND")?;
    let (input, _) = sparql_char(input, '(')?;
    let input = sparql_skip_ws(input);
    let (input, function_name) = identifier(input)?;
    let function_name = if function_name.eq_ignore_ascii_case("concat") {
        "CONCAT"
    } else {
        function_name
    };
    let (mut input, _) = sparql_char(input, '(')?;
    let mut arguments = Vec::new();
    loop {
        let (remaining, argument) = sparql_bind_argument(input)?;
        arguments.push(argument);
        input = sparql_skip_ws(remaining);
        if let Some(remaining) = input.strip_prefix(',') {
            input = remaining;
        } else {
            break;
        }
    }
    let (input, _) = sparql_char(input, ')')?;
    let (input, _) = sparql_keyword(input, "AS")?;
    let (input, variable) = sparql_variable(input)?;
    let (input, _) = sparql_char(input, ')')?;
    Ok((input, (function_name, arguments, variable)))
}

fn sparql_value(input: &str) -> IResult<&str, Value> {
    if let Ok((remaining, _)) = sparql_keyword(input, "UNDEF") {
        return Ok((remaining, Value::Undef));
    }
    alt((
        sparql_iri,
        sparql_quoted_literal,
        sparql_numeric_literal,
        |input| sparql_keyword(input, "true"),
        |input| sparql_keyword(input, "false"),
        sparql_prefixed_name,
    ))
    .parse(input)
    .map(|(remaining, value)| (remaining, Value::Term(value.to_string())))
}

fn sparql_values_clause(input: &str) -> IResult<&str, ValuesClause<'_>> {
    let (mut input, _) = sparql_keyword(input, "VALUES")?;
    let mut variables = Vec::new();
    if let Ok((after_open, _)) = sparql_char(input, '(') {
        input = after_open;
        loop {
            input = sparql_skip_ws(input);
            if let Some(remaining) = input.strip_prefix(')') {
                input = remaining;
                break;
            }
            let (remaining, variable) = sparql_variable(input)?;
            variables.push(variable);
            input = remaining;
        }
    } else {
        let (remaining, variable) = sparql_variable(input)?;
        variables.push(variable);
        input = remaining;
    }
    if variables.is_empty() {
        return sparql_error(input, nom::error::ErrorKind::Many1);
    }

    let (mut input, _) = sparql_char(input, '{')?;
    let mut values = Vec::new();
    loop {
        input = sparql_skip_ws(input);
        if let Some(remaining) = input.strip_prefix('}') {
            input = remaining;
            break;
        }
        let mut row = Vec::new();
        if variables.len() == 1 {
            let (remaining, value) = sparql_value(input)?;
            row.push(value);
            input = remaining;
        } else {
            let (mut row_input, _) = sparql_char(input, '(')?;
            loop {
                row_input = sparql_skip_ws(row_input);
                if let Some(remaining) = row_input.strip_prefix(')') {
                    row_input = remaining;
                    break;
                }
                let (remaining, value) = sparql_value(row_input)?;
                row.push(value);
                row_input = remaining;
            }
            input = row_input;
        }
        if row.len() != variables.len() {
            return sparql_error(input, nom::error::ErrorKind::Verify);
        }
        values.push(row);
    }
    Ok((input, ValuesClause { variables, values }))
}

fn sparql_subquery(input: &str) -> IResult<&str, SubQuery<'_>> {
    let (input, _) = sparql_char(input, '{')?;
    let (input, query) = sparql_select_core(input, false)?;
    let (input, _) = sparql_char(input, '}')?;
    Ok((input, SubQuery { query }))
}

fn sparql_group_primary(input: &str) -> IResult<&str, GroupGraphPattern<'_>> {
    if let Ok((after_graph, _)) = sparql_keyword(input, "GRAPH") {
        let (after_name, name) = sparql_graph_name(after_graph)?;
        let (remaining, pattern) = parse_group_graph_pattern(after_name)?;
        return Ok((
            remaining,
            GroupGraphPattern::Graph {
                name,
                pattern: Box::new(pattern),
            },
        ));
    }
    if sparql_skip_ws(input).starts_with('{') {
        let after_open = sparql_skip_ws(&sparql_skip_ws(input)[1..]);
        if sparql_starts_keyword(after_open, "SELECT") {
            return sparql_subquery(input).map(|(remaining, subquery)| {
                (remaining, GroupGraphPattern::SubQuery(Box::new(subquery)))
            });
        }
        return parse_group_graph_pattern(input);
    }
    sparql_triples_statement(input)
        .map(|(remaining, triples)| (remaining, GroupGraphPattern::Bgp(triples)))
}

/// Parses a recursive group graph pattern containing BGP, GRAPH, and UNION.
pub fn parse_group_graph_pattern(input: &str) -> IResult<&str, GroupGraphPattern<'_>> {
    let (mut input, _) = sparql_char(input, '{')?;
    let mut joined = Vec::new();
    loop {
        input = sparql_skip_ws(input);
        if let Some(remaining) = input.strip_prefix('}') {
            let pattern = match joined.len() {
                0 => GroupGraphPattern::Unit,
                1 => joined.pop().expect("one graph pattern"),
                _ => GroupGraphPattern::Join(joined),
            };
            return Ok((remaining, pattern));
        }

        if sparql_starts_keyword(input, "FILTER") {
            let (remaining, filter) = sparql_filter_clause(input)?;
            joined.push(GroupGraphPattern::Filter(filter));
            input = remaining;
            continue;
        }
        if sparql_starts_keyword(input, "BIND") {
            let (remaining, bind) = sparql_bind_clause(input)?;
            joined.push(GroupGraphPattern::Bind(bind));
            input = remaining;
            continue;
        }
        if sparql_starts_keyword(input, "VALUES") {
            let (remaining, values) = sparql_values_clause(input)?;
            joined.push(GroupGraphPattern::Values(values));
            input = remaining;
            continue;
        }

        let first_is_braced = sparql_skip_ws(input).starts_with('{');
        let (after_first, first) = sparql_group_primary(input)?;
        input = after_first;
        let mut alternatives = vec![first];
        while let Ok((after_union, _)) = sparql_keyword(input, "UNION") {
            if !first_is_braced || !sparql_skip_ws(after_union).starts_with('{') {
                return sparql_error(after_union, nom::error::ErrorKind::Verify);
            }
            let (after_alternative, alternative) = sparql_group_primary(after_union)?;
            alternatives.push(alternative);
            input = after_alternative;
        }
        joined.push(if alternatives.len() == 1 {
            alternatives.pop().expect("one union branch")
        } else {
            GroupGraphPattern::Union(alternatives)
        });

        input = sparql_skip_ws(input);
        if let Some(remaining) = input.strip_prefix('.') {
            input = remaining;
        }
    }
}

fn sparql_quad_block(input: &str) -> IResult<&str, Vec<LexicalQuadPattern<'_>>> {
    let (mut input, _) = sparql_char(input, '{')?;
    let mut quads = Vec::new();
    loop {
        input = sparql_skip_ws(input);
        if let Some(remaining) = input.strip_prefix('}') {
            return Ok((remaining, quads));
        }

        if let Ok((after_graph, _)) = sparql_keyword(input, "GRAPH") {
            let (after_name, graph) = sparql_graph_name(after_graph)?;
            let (mut graph_input, _) = sparql_char(after_name, '{')?;
            loop {
                graph_input = sparql_skip_ws(graph_input);
                if let Some(remaining) = graph_input.strip_prefix('}') {
                    input = remaining;
                    break;
                }
                let (remaining, triples) = sparql_triples_statement(graph_input)?;
                quads.extend(triples.into_iter().map(|triple| LexicalQuadPattern {
                    graph: Some(graph),
                    triple,
                }));
                graph_input = sparql_skip_ws(remaining);
                if let Some(after_dot) = graph_input.strip_prefix('.') {
                    graph_input = after_dot;
                }
            }
        } else {
            let (remaining, triples) = sparql_triples_statement(input)?;
            quads.extend(triples.into_iter().map(|triple| LexicalQuadPattern {
                graph: None,
                triple,
            }));
            input = remaining;
        }
        input = sparql_skip_ws(input);
        if let Some(remaining) = input.strip_prefix('.') {
            input = remaining;
        }
    }
}

fn sparql_term_first_variable(term: &str) -> Option<&str> {
    if term.starts_with(['?', '$']) {
        return Some(term);
    }
    if !term.starts_with("<<") {
        return None;
    }
    sparql_quoted_triple_parts(term)
        .ok()
        .filter(|(remaining, _)| sparql_skip_ws(remaining).is_empty())
        .and_then(|(_, (subject, predicate, object))| {
            sparql_term_first_variable(subject)
                .or_else(|| sparql_term_first_variable(predicate))
                .or_else(|| sparql_term_first_variable(object))
        })
}

fn sparql_term_first_blank_node(term: &str) -> Option<&str> {
    if term.starts_with("_:") {
        return Some(term);
    }
    if !term.starts_with("<<") {
        return None;
    }
    sparql_quoted_triple_parts(term)
        .ok()
        .filter(|(remaining, _)| sparql_skip_ws(remaining).is_empty())
        .and_then(|(_, (subject, predicate, object))| {
            sparql_term_first_blank_node(subject)
                .or_else(|| sparql_term_first_blank_node(predicate))
                .or_else(|| sparql_term_first_blank_node(object))
        })
}

fn sparql_quads_first_variable<'a>(quads: &[LexicalQuadPattern<'a>]) -> Option<&'a str> {
    quads.iter().find_map(|quad| {
        quad.graph
            .and_then(sparql_term_first_variable)
            .or_else(|| sparql_term_first_variable(quad.triple.0))
            .or_else(|| sparql_term_first_variable(quad.triple.1))
            .or_else(|| sparql_term_first_variable(quad.triple.2))
    })
}

fn sparql_quads_first_blank_node<'a>(quads: &[LexicalQuadPattern<'a>]) -> Option<&'a str> {
    quads.iter().find_map(|quad| {
        sparql_term_first_blank_node(quad.triple.0)
            .or_else(|| sparql_term_first_blank_node(quad.triple.1))
            .or_else(|| sparql_term_first_blank_node(quad.triple.2))
    })
}

fn sparql_quads_to_group<'a>(quads: &[LexicalQuadPattern<'a>]) -> GroupGraphPattern<'a> {
    let mut patterns = Vec::with_capacity(quads.len());
    for quad in quads {
        let bgp = GroupGraphPattern::Bgp(vec![quad.triple]);
        patterns.push(match quad.graph {
            Some(name) => GroupGraphPattern::Graph {
                name,
                pattern: Box::new(bgp),
            },
            None => bgp,
        });
    }
    match patterns.len() {
        0 => GroupGraphPattern::Unit,
        1 => patterns.pop().expect("one quad pattern"),
        _ => GroupGraphPattern::Join(patterns),
    }
}

fn sparql_prefix_declaration(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, _) = sparql_keyword(input, "PREFIX")?;
    let input = sparql_skip_ws(input);
    let Some(colon) = input.find(':') else {
        return sparql_error(input, nom::error::ErrorKind::Char);
    };
    let prefix = &input[..colon];
    if let Some(offending) = sparql_invalid_pn_prefix(prefix) {
        return sparql_error(offending, nom::error::ErrorKind::Verify);
    }
    let (remaining, iri) = sparql_iri(&input[colon + 1..])?;
    Ok((remaining, (prefix, &iri[1..iri.len() - 1])))
}

fn sparql_prefixes(input: &str) -> IResult<&str, HashMap<String, String>> {
    let mut input = input;
    let mut prefixes = HashMap::new();
    while sparql_starts_keyword(input, "PREFIX") {
        let (remaining, (prefix, iri)) = sparql_prefix_declaration(input)?;
        prefixes.insert(prefix.to_string(), iri.to_string());
        input = remaining;
    }
    Ok((input, prefixes))
}

fn sparql_aggregate(input: &str) -> IResult<&str, (&str, &str, Option<&str>)> {
    let input = sparql_skip_ws(input);
    let (input, wrapped) = if let Some(remaining) = input.strip_prefix('(') {
        (remaining, true)
    } else {
        (input, false)
    };
    let (input, aggregate) = if let Ok((remaining, _)) = sparql_keyword(input, "SUM") {
        (remaining, "SUM")
    } else if let Ok((remaining, _)) = sparql_keyword(input, "MIN") {
        (remaining, "MIN")
    } else if let Ok((remaining, _)) = sparql_keyword(input, "MAX") {
        (remaining, "MAX")
    } else if let Ok((remaining, _)) = sparql_keyword(input, "AVG") {
        (remaining, "AVG")
    } else {
        return sparql_error(input, nom::error::ErrorKind::Alt);
    };
    let (input, _) = sparql_char(input, '(')?;
    let (input, variable) = sparql_variable(input)?;
    let (mut input, _) = sparql_char(input, ')')?;
    let alias = if let Ok((remaining, _)) = sparql_keyword(input, "AS") {
        let (remaining, alias) = sparql_variable(remaining)?;
        input = remaining;
        Some(alias)
    } else {
        None
    };
    if wrapped {
        let (remaining, _) = sparql_char(input, ')')?;
        input = remaining;
    }
    Ok((input, (aggregate, variable, alias)))
}

fn sparql_projection_items(input: &str) -> IResult<&str, Vec<(&str, &str, Option<&str>)>> {
    let mut input = sparql_skip_ws(input);
    if let Some(remaining) = input.strip_prefix('*') {
        return Ok((remaining, vec![("*", "*", None)]));
    }

    let mut variables = Vec::new();
    loop {
        if let Ok((remaining, variable)) = sparql_variable(input) {
            variables.push(("VAR", variable, None));
            input = remaining;
        } else if let Ok((remaining, aggregate)) = sparql_aggregate(input) {
            variables.push(aggregate);
            input = remaining;
        } else {
            break;
        }
    }
    if variables.is_empty() {
        return sparql_error(input, nom::error::ErrorKind::Many1);
    }
    Ok((input, variables))
}

fn sparql_group_by_clause(input: &str) -> IResult<&str, Vec<&str>> {
    let (mut input, _) = sparql_keyword(input, "GROUP")?;
    let (remaining, _) = sparql_keyword(input, "BY")?;
    input = remaining;
    let mut variables = Vec::new();
    while let Ok((remaining, variable)) = sparql_variable(input) {
        variables.push(variable);
        input = remaining;
    }
    if variables.is_empty() {
        return sparql_error(input, nom::error::ErrorKind::Many1);
    }
    Ok((input, variables))
}

fn sparql_order_condition(input: &str) -> IResult<&str, OrderCondition<'_>> {
    let input = sparql_skip_ws(input);
    let (remaining, direction, variable) =
        if let Ok((after_direction, _)) = sparql_keyword(input, "ASC") {
            let (after_open, _) = sparql_char(after_direction, '(')?;
            let (after_variable, variable) = sparql_variable(after_open)?;
            let (remaining, _) = sparql_char(after_variable, ')')?;
            (remaining, SortDirection::Asc, variable)
        } else if let Ok((after_direction, _)) = sparql_keyword(input, "DESC") {
            let (after_open, _) = sparql_char(after_direction, '(')?;
            let (after_variable, variable) = sparql_variable(after_open)?;
            let (remaining, _) = sparql_char(after_variable, ')')?;
            (remaining, SortDirection::Desc, variable)
        } else {
            let (remaining, variable) = sparql_variable(input)?;
            (remaining, SortDirection::Asc, variable)
        };
    Ok((
        remaining,
        OrderCondition {
            variable,
            direction,
        },
    ))
}

fn sparql_order_by_clause(input: &str) -> IResult<&str, Vec<OrderCondition<'_>>> {
    let (mut input, _) = sparql_keyword(input, "ORDER")?;
    let (remaining, _) = sparql_keyword(input, "BY")?;
    input = remaining;
    let mut conditions = Vec::new();
    loop {
        input = sparql_skip_ws(input);
        if let Some(remaining) = input.strip_prefix(',') {
            input = remaining;
            continue;
        }
        if input.is_empty()
            || input.starts_with('}')
            || sparql_starts_keyword(input, "LIMIT")
            || sparql_starts_keyword(input, "GROUP")
        {
            break;
        }
        let (remaining, condition) = sparql_order_condition(input)?;
        conditions.push(condition);
        input = remaining;
    }
    if conditions.is_empty() {
        return sparql_error(input, nom::error::ErrorKind::Many1);
    }
    Ok((input, conditions))
}

fn sparql_limit_clause(input: &str) -> IResult<&str, usize> {
    let (input, _) = sparql_keyword(input, "LIMIT")?;
    let input = sparql_skip_ws(input);
    let digit_count = input.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return sparql_error(input, nom::error::ErrorKind::Digit);
    }
    let value = input[..digit_count].parse::<usize>().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    Ok((&input[digit_count..], value))
}

fn sparql_select_core(input: &str, allow_dataset: bool) -> IResult<&str, SelectQuery<'_>> {
    let (mut input, _) = sparql_keyword(input, "SELECT")?;
    let distinct = if let Ok((remaining, _)) = sparql_keyword(input, "DISTINCT") {
        input = remaining;
        true
    } else {
        false
    };

    let (remaining, variables) = sparql_projection_items(input)?;
    input = remaining;

    let mut from = Vec::new();
    let mut from_named = Vec::new();
    if allow_dataset {
        while let Ok((after_from, _)) = sparql_keyword(input, "FROM") {
            if let Ok((after_named, _)) = sparql_keyword(after_from, "NAMED") {
                let (remaining, graph) =
                    alt((sparql_iri, sparql_prefixed_name)).parse(after_named)?;
                from_named.push(graph);
                input = remaining;
            } else {
                let (remaining, graph) =
                    alt((sparql_iri, sparql_prefixed_name)).parse(after_from)?;
                from.push(graph);
                input = remaining;
            }
        }
    }

    if let Ok((remaining, _)) = sparql_keyword(input, "WHERE") {
        input = remaining;
    }
    let (mut input, pattern) = parse_group_graph_pattern(input)?;
    let group_vars = if sparql_starts_keyword(input, "GROUP") {
        let (remaining, variables) = sparql_group_by_clause(input)?;
        input = remaining;
        variables
    } else {
        Vec::new()
    };
    let order_conditions = if sparql_starts_keyword(input, "ORDER") {
        let (remaining, conditions) = sparql_order_by_clause(input)?;
        input = remaining;
        conditions
    } else {
        Vec::new()
    };
    let limit = if sparql_starts_keyword(input, "LIMIT") {
        let (remaining, limit) = sparql_limit_clause(input)?;
        input = remaining;
        Some(limit)
    } else {
        None
    };
    Ok((
        input,
        SelectQuery {
            distinct,
            variables,
            from,
            from_named,
            pattern,
            group_vars,
            order_conditions,
            limit,
        },
    ))
}

fn sparql_update_core(input: &str, allow_data_aliases: bool) -> IResult<&str, UpdateOperation<'_>> {
    if let Ok((after_insert, _)) = sparql_keyword(input, "INSERT") {
        if let Ok((after_data, _)) = sparql_keyword(after_insert, "DATA") {
            let (remaining, quads) = sparql_quad_block(after_data)?;
            if let Some(offending) = sparql_quads_first_variable(&quads) {
                return sparql_error(offending, nom::error::ErrorKind::Verify);
            }
            return Ok((
                remaining,
                UpdateOperation::InsertData(InsertClause { quads }),
            ));
        }

        let (after_template, insert) = sparql_quad_block(after_insert)?;
        if let Ok((after_where, _)) = sparql_keyword(after_template, "WHERE") {
            let (remaining, where_pattern) = parse_group_graph_pattern(after_where)?;
            return Ok((
                remaining,
                UpdateOperation::InsertWhere {
                    insert: InsertClause { quads: insert },
                    where_pattern,
                },
            ));
        }
        if allow_data_aliases && sparql_skip_ws(after_template).is_empty() {
            if let Some(offending) = sparql_quads_first_variable(&insert) {
                return sparql_error(offending, nom::error::ErrorKind::Verify);
            }
            return Ok((
                after_template,
                UpdateOperation::InsertData(InsertClause { quads: insert }),
            ));
        }
        return sparql_error(after_template, nom::error::ErrorKind::Tag);
    }

    let (after_delete, _) = sparql_keyword(input, "DELETE")?;
    if let Ok((after_data, _)) = sparql_keyword(after_delete, "DATA") {
        let (remaining, quads) = sparql_quad_block(after_data)?;
        if let Some(offending) =
            sparql_quads_first_variable(&quads).or_else(|| sparql_quads_first_blank_node(&quads))
        {
            return sparql_error(offending, nom::error::ErrorKind::Verify);
        }
        return Ok((
            remaining,
            UpdateOperation::DeleteData(DeleteClause { quads }),
        ));
    }

    if let Ok((after_where, _)) = sparql_keyword(after_delete, "WHERE") {
        let (remaining, template) = sparql_quad_block(after_where)?;
        if let Some(offending) = sparql_quads_first_blank_node(&template) {
            return sparql_error(offending, nom::error::ErrorKind::Verify);
        }
        let where_pattern = sparql_quads_to_group(&template);
        return Ok((
            remaining,
            UpdateOperation::DeleteWhereShorthand {
                delete: DeleteClause { quads: template },
                where_pattern,
            },
        ));
    }

    let (mut remaining, delete) = sparql_quad_block(after_delete)?;
    if let Some(offending) = sparql_quads_first_blank_node(&delete) {
        return sparql_error(offending, nom::error::ErrorKind::Verify);
    }
    if allow_data_aliases && sparql_skip_ws(remaining).is_empty() {
        if let Some(offending) = sparql_quads_first_variable(&delete) {
            return sparql_error(offending, nom::error::ErrorKind::Verify);
        }
        return Ok((
            remaining,
            UpdateOperation::DeleteData(DeleteClause { quads: delete }),
        ));
    }

    let insert = if let Ok((after_insert, _)) = sparql_keyword(remaining, "INSERT") {
        let (after_template, insert) = sparql_quad_block(after_insert)?;
        remaining = after_template;
        Some(insert)
    } else {
        None
    };
    let (after_where, _) = sparql_keyword(remaining, "WHERE")?;
    let (remaining, where_pattern) = parse_group_graph_pattern(after_where)?;
    let delete = DeleteClause { quads: delete };
    Ok(match insert {
        Some(insert) => (
            remaining,
            UpdateOperation::DeleteInsertWhere {
                delete,
                insert: InsertClause { quads: insert },
                where_pattern,
            },
        ),
        None => (
            remaining,
            UpdateOperation::DeleteWhere {
                delete,
                where_pattern,
            },
        ),
    })
}

/// Parse a standalone SELECT with the same grammar used by `CombinedQuery`.
/// Prefixes are accepted as a request prologue; callers that need the prefix
/// map should use `parse_combined_query`.
pub fn parse_sparql_query(input: &str) -> IResult<&str, SelectQuery<'_>> {
    let (input, _) = sparql_prefixes(input)?;
    let (remaining, query) = sparql_select_core(input, true)?;
    let remaining = sparql_skip_ws(remaining);
    if !remaining.is_empty() {
        return sparql_error(remaining, nom::error::ErrorKind::Eof);
    }
    Ok((remaining, query))
}

pub fn parse_standalone_rule<'a>(
    input: &'a str,
) -> IResult<&'a str, (CombinedRule<'a>, HashMap<String, String>)> {
    // Parse prefixes first
    let (input, prefix_list) = many0(|i| {
        let (i, _) = multispace0.parse(i)?;
        let (i, _) = tag("PREFIX").parse(i)?;
        let (i, _) = space1.parse(i)?;
        let (i, p) = identifier(i)?;
        let (i, _) = char(':').parse(i)?;
        let (i, _) = space0.parse(i)?;
        let (i, uri) = delimited(char('<'), take_while1(|c| c != '>'), char('>')).parse(i)?;
        Ok((i, (p, uri)))
    })
    .parse(input)?;

    let mut prefixes = HashMap::new();
    for (p, uri) in prefix_list {
        prefixes.insert(p.to_string(), uri.to_string());
    }

    let (input, _) = multispace0.parse(input)?;

    // Parse the rule
    let (input, rule) = parse_rule(input)?;

    Ok((input, (rule, prefixes)))
}

pub fn parse_rule_call(input: &str) -> IResult<&str, RuleHead<'_>> {
    let (input, _) = multispace0.parse(input)?;

    // Parse the academic syntax: RULE(:Predicate, ?var1, ?var2, ...)
    let (input, _) = tag("RULE").parse(input)?;
    let (input, _) = preceded(char('('), multispace0).parse(input)?;
    let (input, pred) = predicate(input)?;

    // Parse the first variable
    let (input, _) = (multispace0, char(','), multispace0).parse(input)?;
    let (input, first_var) = variable(input)?;

    // Parse additional variables if they exist
    let (input, additional_vars) =
        many0(preceded((multispace0, char(','), multispace0), variable)).parse(input)?;

    // Combine all variables
    let mut all_vars = vec![first_var];
    all_vars.extend(additional_vars);

    let (input, _) = preceded(multispace0, char(')')).parse(input)?;

    Ok((input, RuleHead { predicate: pred }))
}

pub fn parse_rule_head(input: &str) -> IResult<&str, RuleHead<'_>> {
    let (input, pred) = predicate(input)?;
    Ok((input, RuleHead { predicate: pred }))
}

fn parse_balanced(input: &str) -> IResult<&str, &str> {
    let mut depth = 1;
    for (i, c) in input.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    // Return the content inside the balanced block
                    return Ok((&input[i + 1..], &input[..i]));
                }
            }
            _ => {}
        }
    }
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Tag,
    )))
}

fn split_top_level(input: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut brace_depth = 0i32;
    let mut paren_depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => brace_depth += 1,
            '}' => brace_depth -= 1,
            '(' => paren_depth += 1,
            ')' => paren_depth -= 1,
            _ if ch == delimiter && brace_depth == 0 && paren_depth == 0 => {
                parts.push(input[start..idx].trim());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

fn extract_wrapped_block<'a>(
    input: &'a str,
    open: char,
    close: char,
) -> Option<(&'a str, &'a str)> {
    let trimmed = input.trim();
    if !trimmed.starts_with(open) {
        return None;
    }

    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    for (idx, ch) in trimmed.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            _ if ch == open => depth += 1,
            _ if ch == close => {
                depth -= 1;
                if depth == 0 {
                    return Some((&trimmed[idx + 1..], &trimmed[1..idx]));
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_quoted_value(input: &str) -> Option<&str> {
    let trimmed = input.trim();
    let rest = trimmed.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn parse_loss_fn(value: &str) -> Option<LossFn> {
    match value.trim().to_ascii_lowercase().as_str() {
        "cross_entropy" => Some(LossFn::CrossEntropy),
        "nll" => Some(LossFn::Nll),
        "mse" => Some(LossFn::Mse),
        "binary_cross_entropy" | "bce" => Some(LossFn::BinaryCrossEntropy),
        _ => None,
    }
}

fn parse_optimizer_kind(value: &str) -> Option<OptimizerKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "adam" => Some(OptimizerKind::Adam),
        "sgd" => Some(OptimizerKind::Sgd),
        _ => None,
    }
}

fn into_owned_triple(triple: (&str, &str, &str)) -> (String, String, String) {
    (
        triple.0.to_string(),
        triple.1.to_string(),
        triple.2.to_string(),
    )
}

fn parse_graph_pattern_block_owned(input: &str) -> Result<Vec<(String, String, String)>, String> {
    let wrapped = format!("WHERE {{ {} }}", input.trim());
    let (_, (patterns, _, _, _, _, _, _)) =
        parse_where(&wrapped).map_err(|err| format!("invalid graph-pattern block: {err:?}"))?;
    Ok(patterns.into_iter().map(into_owned_triple).collect())
}

fn parse_usize_list(input: &str) -> Result<Vec<usize>, String> {
    split_top_level(input, ',')
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            part.trim()
                .parse::<usize>()
                .map_err(|_| format!("invalid usize value `{}`", part.trim()))
        })
        .collect()
}

fn parse_output_values(input: &str) -> Vec<String> {
    split_top_level(input, ',')
        .into_iter()
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(
                    parse_quoted_value(trimmed)
                        .unwrap_or(trimmed)
                        .trim()
                        .to_string(),
                )
            }
        })
        .collect()
}

fn infer_anchor_var(patterns: &[(String, String, String)]) -> Result<String, String> {
    if let Some(subject_var) = patterns.iter().find_map(|(s, _, _)| {
        if s.starts_with(['?', '$']) {
            Some(s.clone())
        } else {
            None
        }
    }) {
        return Ok(subject_var);
    }

    for (s, p, o) in patterns {
        for term in [s, p, o] {
            if term.starts_with(['?', '$']) {
                return Ok(term.clone());
            }
        }
    }

    Err("NEURAL RELATION INPUT must contain at least one anchor variable".to_string())
}

pub fn parse_model_decl(input: &str) -> IResult<&str, ModelDecl> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("MODEL").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = char('"').parse(input)?;
    let (input, model_name) = take_until("\"").parse(input)?;
    let (input, _) = char('"').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, body) = preceded(char('{'), parse_balanced).parse(input)?;

    let body = body.trim();
    let arch_tail = body
        .strip_prefix("ARCH")
        .map(str::trim)
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let arch_tail = arch_tail
        .strip_prefix("MLP")
        .map(str::trim)
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let (after_arch, arch_body) = extract_wrapped_block(arch_tail, '{', '}')
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let hidden_body = arch_body
        .trim()
        .strip_prefix("HIDDEN")
        .map(str::trim)
        .and_then(|rest| extract_wrapped_block(rest, '[', ']').map(|(_, hidden)| hidden))
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let hidden_layers = parse_usize_list(hidden_body).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(
            hidden_body,
            nom::error::ErrorKind::Tag,
        ))
    })?;

    let output_tail = after_arch
        .trim()
        .strip_prefix("OUTPUT")
        .map(str::trim)
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let output_kind = if let Some(rest) = output_tail.strip_prefix("EXCLUSIVE") {
        let (_, labels_body) = extract_wrapped_block(rest.trim(), '{', '}').ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
        })?;
        NeuralOutputKind::Exclusive {
            labels: parse_output_values(labels_body),
        }
    } else if let Some(rest) = output_tail.strip_prefix("BINARY") {
        let (_, labels_body) = extract_wrapped_block(rest.trim(), '{', '}').ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
        })?;
        let mut values = parse_output_values(labels_body);
        let positive_literal = values.drain(..).next().ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
        })?;
        NeuralOutputKind::Binary { positive_literal }
    } else {
        return Err(nom::Err::Error(nom::error::Error::new(
            body,
            nom::error::ErrorKind::Tag,
        )));
    };

    Ok((
        input,
        ModelDecl {
            name: model_name.to_string(),
            arch: ModelArch::Mlp { hidden_layers },
            output_kind,
        },
    ))
}

pub fn parse_neural_relation_decl(input: &str) -> IResult<&str, NeuralRelationDecl> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("NEURAL").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("RELATION").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, predicate_name) = predicate(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("USING").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("MODEL").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = char('"').parse(input)?;
    let (input, model_name) = take_until("\"").parse(input)?;
    let (input, _) = char('"').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, body) = preceded(char('{'), parse_balanced).parse(input)?;

    let trimmed = body.trim();
    let input_tail = trimmed
        .strip_prefix("INPUT")
        .map(str::trim)
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let (after_input, input_block) = extract_wrapped_block(input_tail, '{', '}')
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let input_patterns = parse_graph_pattern_block_owned(input_block)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let features_tail = after_input
        .trim()
        .strip_prefix("FEATURES")
        .map(str::trim)
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let (_, features_block) = extract_wrapped_block(features_tail, '{', '}')
        .ok_or_else(|| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;
    let feature_vars = split_top_level(features_block, ',')
        .into_iter()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    let anchor_var = infer_anchor_var(&input_patterns)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag)))?;

    Ok((
        input,
        NeuralRelationDecl {
            predicate: predicate_name.to_string(),
            model_name: model_name.to_string(),
            input_patterns,
            feature_vars,
            anchor_var,
        },
    ))
}

fn parse_top_level_neural_decls(
    mut input: &str,
) -> IResult<
    &str,
    (
        Vec<ModelDecl>,
        Vec<NeuralRelationDecl>,
        Vec<TrainNeuralRelationDecl>,
    ),
> {
    let mut model_decls = Vec::new();
    let mut neural_relation_decls = Vec::new();
    let mut train_neural_relation_decls = Vec::new();

    loop {
        let (after_ws, _) = multispace0.parse(input)?;
        input = after_ws;
        if input.starts_with("MODEL") {
            let (new_input, decl) = parse_model_decl(input)?;
            model_decls.push(decl);
            input = new_input;
        } else if input.starts_with("NEURAL RELATION") {
            let (new_input, decl) = parse_neural_relation_decl(input)?;
            neural_relation_decls.push(decl);
            input = new_input;
        } else if input.starts_with("TRAIN NEURAL RELATION") {
            let (new_input, decl) = parse_train_neural_relation_decl(input)?;
            train_neural_relation_decls.push(decl);
            input = new_input;
        } else {
            break;
        }
    }

    Ok((
        input,
        (
            model_decls,
            neural_relation_decls,
            train_neural_relation_decls,
        ),
    ))
}

pub fn parse_train_neural_relation_decl(input: &str) -> IResult<&str, TrainNeuralRelationDecl> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("TRAIN").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("NEURAL").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("RELATION").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, predicate_name) = predicate(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, body) = preceded(char('{'), parse_balanced).parse(input)?;

    let trimmed = body.trim();
    let (rest, data_source) = if let Some(data_tail) = trimmed.strip_prefix("DATA") {
        let (after_data, data_body) = extract_wrapped_block(data_tail.trim(), '{', '}')
            .ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?;
        let parsed = parse_graph_pattern_block_owned(data_body).map_err(|_| {
            nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
        })?;
        (after_data.trim(), TrainingDataSource::GraphPattern(parsed))
    } else if let Some(query_tail) = trimmed.strip_prefix("QUERY") {
        let (after_query, query_body) = extract_wrapped_block(query_tail.trim(), '{', '}')
            .ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?;
        (
            after_query.trim(),
            TrainingDataSource::Query(query_body.trim().to_string()),
        )
    } else {
        return Err(nom::Err::Error(nom::error::Error::new(
            body,
            nom::error::ErrorKind::Tag,
        )));
    };

    let mut label_var = None;
    let mut target_triple = None;
    let mut loss = None;
    let mut optimizer = None;
    let mut learning_rate = None;
    let mut epochs = None;
    let mut batch_size = None;
    let mut save_path = None;

    for line in rest.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Some(value) = line.strip_prefix("LABEL") {
            label_var = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("TARGET") {
            let (_, block) = extract_wrapped_block(value.trim(), '{', '}').ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?;
            let triple = parse_single_triple_template(block.trim()).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?;
            target_triple = Some(into_owned_triple(triple));
        } else if let Some(value) = line.strip_prefix("LOSS") {
            loss = parse_loss_fn(value.trim());
        } else if let Some(value) = line.strip_prefix("OPTIMIZER") {
            optimizer = parse_optimizer_kind(value.trim());
        } else if let Some(value) = line.strip_prefix("LEARNING_RATE") {
            learning_rate = value.trim().parse::<f64>().ok();
        } else if let Some(value) = line.strip_prefix("EPOCHS") {
            epochs = value.trim().parse::<usize>().ok();
        } else if let Some(value) = line.strip_prefix("BATCH_SIZE") {
            batch_size = value.trim().parse::<usize>().ok();
        } else if let Some(value) = line.strip_prefix("SAVE_TO") {
            save_path = parse_quoted_value(value.trim()).map(str::to_string);
        }
    }

    Ok((
        input,
        TrainNeuralRelationDecl {
            predicate: predicate_name.to_string(),
            data_source,
            label_var: label_var.ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?,
            target_triple: target_triple.ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?,
            loss: loss.ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?,
            optimizer: optimizer.ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?,
            learning_rate: learning_rate.ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?,
            epochs: epochs.ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?,
            batch_size: batch_size.ok_or_else(|| {
                nom::Err::Error(nom::error::Error::new(body, nom::error::ErrorKind::Tag))
            })?,
            save_path,
        },
    ))
}

fn parse_single_triple_template(input: &str) -> Result<(&str, &str, &str), String> {
    let (_, triples) =
        parse_triple_block(input).map_err(|err| format!("invalid triple template: {err:?}"))?;
    if triples.len() != 1 {
        return Err("triple templates must contain exactly one triple".to_string());
    }
    Ok(triples[0])
}

pub fn parse_ml_predict(input: &str) -> IResult<&str, MLPredictClause<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("ML.PREDICT").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    // Parse MODEL clause with quoted name
    let (input, _) = tag("MODEL").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = char('"').parse(input)?; // Expect opening quote
    let (input, model) = take_until("\"").parse(input)?; // Take everything until closing quote
    let (input, _) = char('"').parse(input)?; // Expect closing quote
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(',').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    // Parse INPUT clause using the inclusive balanced parser
    let (input, _) = tag("INPUT").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, input_query) = preceded(char('{'), parse_balanced).parse(input)?;

    // Parse the SELECT statement inside the input query
    let mut select_vars = Vec::new();
    let mut where_patterns = Vec::new();
    let mut filter_conditions = Vec::new();

    // Extract SELECT variables
    if let Some(select_idx) = input_query.find("SELECT") {
        if let Some(where_idx) = input_query.find("WHERE") {
            let select_clause = &input_query[select_idx + 6..where_idx].trim();
            // Parse SELECT variables (simplified version - in real code you would use your actual SELECT parser)
            let vars: Vec<&str> = select_clause.split_whitespace().collect();
            for var in vars {
                if var.starts_with(['?', '$']) {
                    select_vars.push((var, "", None)); // Add proper variable type extraction if needed
                }
            }

            // Parse WHERE patterns and filters (simplified - use your actual WHERE parser)
            let where_clause = &input_query[where_idx + 5..].trim();
            // This is a placeholder - you should use your actual pattern and filter parser here
            let (_rest, (patterns, filters, _values, _binds, _subqueries, _, _)) =
                parse_where(where_clause).unwrap_or_else(|_| {
                    (
                        where_clause,
                        (vec![], vec![], None, vec![], vec![], vec![], vec![]),
                    )
                });

            where_patterns = patterns;
            filter_conditions = filters;
        }
    }

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(',').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    // Parse OUTPUT clause
    let (input, _) = tag("OUTPUT").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, output_var) = variable(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;

    Ok((
        input,
        MLPredictClause {
            model,
            input_raw: input_query,
            input_select: select_vars,
            input_where: where_patterns,
            input_filters: filter_conditions,
            output: output_var,
        },
    ))
}

// Parser for stream type
pub fn parse_stream_type(input: &str) -> IResult<&str, StreamType<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, stream_type) = alt((
        tag("RSTREAM").map(|_| StreamType::RStream),
        tag("ISTREAM").map(|_| StreamType::IStream),
        tag("DSTREAM").map(|_| StreamType::DStream),
        identifier.map(|s| StreamType::Custom(s)),
    ))
    .parse(input)?;
    Ok((input, stream_type))
}

// Parser for window specification
pub fn parse_window_spec(input: &str) -> IResult<&str, WindowSpec<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('[').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse window type and parameters
    let (input, window_type) = alt((
        tag("RANGE").map(|_| WindowType::Range),
        tag("TUMBLING").map(|_| WindowType::Tumbling),
        tag("SLIDING").map(|_| WindowType::Sliding),
    ))
    .parse(input)?;

    let (input, _) = multispace1.parse(input)?;

    // Parse duration (like PT10M) or numeric value
    let (input, width_str) = alt((
        // ISO 8601 duration format (PT10M, PT5S, etc.)
        recognize((
            tag("PT"),
            take_while1(|c: char| c.is_digit(10)),
            alt((char('S'), char('M'), char('H'))),
        )),
        // Simple numeric value
        take_while1(|c: char| c.is_digit(10)),
    ))
    .parse(input)?;

    // Convert duration to numeric value (simplified conversion)
    let width = parse_duration_to_seconds(width_str);

    // Optional STEP parameter for sliding windows
    let (input, slide) = opt(preceded(
        (multispace1, tag("STEP"), multispace1),
        alt((
            // ISO 8601 duration format
            recognize((
                tag("PT"),
                take_while1(|c: char| c.is_digit(10)),
                alt((char('S'), char('M'), char('H'))),
            )),
            // Simple numeric value
            take_while1(|c: char| c.is_digit(10)),
        )),
    ))
    .parse(input)?;

    let slide = slide.map(parse_duration_to_seconds);

    // Optional report strategy
    let (input, report_strategy) = opt(preceded(
        (multispace1, tag("REPORT"), multispace1),
        alt((
            tag("ON_WINDOW_CLOSE"),
            tag("ON_CONTENT_CHANGE"),
            tag("NON_EMPTY_CONTENT"),
            tag("PERIODIC"),
        )),
    ))
    .parse(input)?;

    // Optional tick strategy
    let (input, tick) = opt(preceded(
        (multispace1, tag("TICK"), multispace1),
        alt((tag("TIME_DRIVEN"), tag("TUPLE_DRIVEN"), tag("BATCH_DRIVEN"))),
    ))
    .parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(']').parse(input)?;

    Ok((
        input,
        WindowSpec {
            window_type,
            width,
            slide,
            report_strategy,
            tick,
        },
    ))
}

/// Parse a duration string used in WITH POLICY clauses.
/// Accepts: `5s`, `5000ms`, `PT5S` / `PT5M` / `PT5H`, bare integer (seconds).
fn parse_policy_duration(input: &str) -> IResult<&str, std::time::Duration> {
    alt((parse_policy_duration_iso, parse_policy_duration_numeric)).parse(input)
}

fn parse_policy_duration_iso(input: &str) -> IResult<&str, std::time::Duration> {
    let (input, dur_str) = recognize((
        tag("PT"),
        take_while1(|c: char| c.is_ascii_digit()),
        alt((char('S'), char('M'), char('H'))),
    ))
    .parse(input)?;
    let secs = parse_duration_to_seconds(dur_str) as u64;
    Ok((input, std::time::Duration::from_secs(secs)))
}

fn parse_policy_duration_numeric(input: &str) -> IResult<&str, std::time::Duration> {
    let (input, num_str) = take_while1(|c: char| c.is_ascii_digit()).parse(input)?;
    let (input, suffix) = opt(alt((tag("ms"), tag("s")))).parse(input)?;
    let num: u64 = num_str.parse().unwrap_or(0);
    let dur = match suffix {
        Some("ms") => std::time::Duration::from_millis(num),
        _ => std::time::Duration::from_secs(num),
    };
    Ok((input, dur))
}

/// Parse the policy name / spec after `WITH POLICY`.
/// - `steal`  -> SyncPolicy::Steal
/// - `wait`   -> SyncPolicy::Wait
/// - `(timeout=<dur>, fallback=steal|drop)` -> SyncPolicy::Timeout{...}
fn parse_sync_policy(input: &str) -> IResult<&str, shared::query::SyncPolicy> {
    alt((
        parse_sync_policy_steal,
        parse_sync_policy_wait,
        parse_sync_policy_timeout,
    ))
    .parse(input)
}

fn parse_from_named_window_policy(input: &str) -> IResult<&str, shared::query::SyncPolicy> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("WITH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("POLICY").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    parse_sync_policy(input)
}

fn parse_sync_policy_steal(input: &str) -> IResult<&str, shared::query::SyncPolicy> {
    let (input, _) = tag("steal").parse(input)?;
    Ok((input, shared::query::SyncPolicy::Steal))
}

fn parse_sync_policy_wait(input: &str) -> IResult<&str, shared::query::SyncPolicy> {
    let (input, _) = tag("wait").parse(input)?;
    Ok((input, shared::query::SyncPolicy::Wait))
}

fn parse_sync_policy_timeout(input: &str) -> IResult<&str, shared::query::SyncPolicy> {
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("timeout").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, duration) = parse_policy_duration(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(',').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("fallback").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, fallback) = alt((parse_fallback_steal, parse_fallback_drop)).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    Ok((
        input,
        shared::query::SyncPolicy::Timeout { duration, fallback },
    ))
}

fn parse_fallback_steal(input: &str) -> IResult<&str, shared::query::Fallback> {
    let (input, _) = tag("steal").parse(input)?;
    Ok((input, shared::query::Fallback::Steal))
}

fn parse_fallback_drop(input: &str) -> IResult<&str, shared::query::Fallback> {
    let (input, _) = tag("drop").parse(input)?;
    Ok((input, shared::query::Fallback::Drop))
}

// Helper function to convert duration strings to seconds
fn parse_duration_to_seconds(duration: &str) -> usize {
    if duration.starts_with("PT") && duration.len() > 2 {
        let time_part = &duration[2..];
        if let Some(num_end) = time_part.chars().position(|c| !c.is_digit(10)) {
            if let Ok(num) = time_part[..num_end].parse::<usize>() {
                match time_part.chars().nth(num_end) {
                    Some('S') => num,        // seconds
                    Some('M') => num * 60,   // minutes to seconds
                    Some('H') => num * 3600, // hours to seconds
                    _ => num,
                }
            } else {
                1 // default
            }
        } else {
            1 // default
        }
    } else {
        duration.parse::<usize>().unwrap_or(1)
    }
}

// Parser for FROM NAMED WINDOW clause
pub fn parse_from_named_window(input: &str) -> IResult<&str, WindowClause<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("FROM").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("NAMED").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("WINDOW").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse window identifier (can be :wind, <uri>, or variable)
    let (input, window_iri) = alt((
        delimited(char('<'), take_while1(|c| c != '>'), char('>')), // <uri>
        recognize((char(':'), identifier)),                         // :wind
        variable,                                                   // ?var
        identifier,                                                 // simple name
    ))
    .parse(input)?;

    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("ON").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse stream identifier (can be variable, URI, or namespace reference)
    let (input, stream_iri) = alt((
        delimited(char('<'), take_while1(|c| c != '>'), char('>')), // <uri>
        variable,                                                   // ?s
        recognize((char(':'), identifier)),                         // :stream
        identifier,                                                 // simple name
    ))
    .parse(input)?;

    let (input, _) = multispace1.parse(input)?;

    // Parse window specification with ISO 8601 duration support
    let (input, window_spec) = parse_window_spec(input)?;

    // Optional: WITH POLICY <policy>
    let (input, policy) = opt(parse_from_named_window_policy).parse(input)?;

    Ok((
        input,
        WindowClause {
            window_iri,
            stream_iri,
            window_spec,
            policy,
        },
    ))
}

/// Parse a PROB(...) annotation for provenance rules.
/// Format: PROB(provenance=minmax, threshold=0.3, confidence=0.9)
/// Legacy alias: PROB(combination=independent, threshold=0.3, confidence=0.9)
fn take_prob_body(input: &str) -> IResult<&str, &str> {
    let mut depth = 0usize;
    for (index, character) in input.char_indices() {
        match character {
            '(' => depth = depth.saturating_add(1),
            ')' if depth == 0 => return Ok((&input[index + 1..], &input[..index])),
            ')' => depth -= 1,
            _ => {}
        }
    }
    Err(nom::Err::Failure(nom::error::Error::new(
        input,
        nom::error::ErrorKind::TakeUntil,
    )))
}

fn split_top_level_commas(input: &str) -> Result<Vec<&str>, ()> {
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut values = Vec::new();
    for (index, character) in input.char_indices() {
        match character {
            '(' => depth = depth.saturating_add(1),
            ')' if depth == 0 => return Err(()),
            ')' => depth -= 1,
            ',' if depth == 0 => {
                let value = input[start..index].trim();
                if value.is_empty() {
                    return Err(());
                }
                values.push(value);
                start = index + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(());
    }
    let tail = input[start..].trim();
    if !tail.is_empty() {
        values.push(tail);
    } else if !input.trim().is_empty() {
        return Err(());
    }
    Ok(values)
}

fn parse_hybrid_threshold(value: &str) -> Result<(f64, ThresholdPolicyKind), ()> {
    if let Ok(threshold) = value.parse::<f64>() {
        if threshold.is_finite() && (0.0..=1.0).contains(&threshold) {
            return Ok((threshold, ThresholdPolicyKind::Explicit));
        }
        return Err(());
    }
    let Some(costs) = value
        .strip_prefix("auto:cost(")
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Err(());
    };
    let mut fp = None;
    let mut fn_cost = None;
    for pair in split_top_level_commas(costs)? {
        let (key, raw) = pair.split_once('=').ok_or(())?;
        let parsed = raw.trim().parse::<f64>().map_err(|_| ())?;
        if !parsed.is_finite() || parsed < 0.0 {
            return Err(());
        }
        match key.trim() {
            "fp" if fp.replace(parsed).is_none() => {}
            "fn" if fn_cost.replace(parsed).is_none() => {}
            _ => return Err(()),
        }
    }
    let (fp, fn_cost) = (fp.ok_or(())?, fn_cost.ok_or(())?);
    let total = fp + fn_cost;
    if !total.is_finite() || total <= 0.0 {
        return Err(());
    }
    Ok((fp / total, ThresholdPolicyKind::CostRatio))
}

fn parse_prob_annotation(input: &str) -> IResult<&str, ProbAnnotation<'_>> {
    let (input, _) = tag("PROB").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    let mut combination: &str = "independent";
    let mut threshold: Option<f64> = None;
    let mut threshold_policy = ThresholdPolicyKind::Explicit;
    let mut confidence: Option<f64> = None;
    let mut raw_values: HashMap<&str, &str> = HashMap::new();
    let mut unknown_keys = Vec::new();
    let mut duplicate_key = false;

    // Parse key=value pairs separated by commas
    let (input, kv_str) = take_prob_body(input)?;

    let pairs = split_top_level_commas(kv_str).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(
            kv_str,
            nom::error::ErrorKind::Verify,
        ))
    })?;
    for pair in pairs {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(nom::Err::Failure(nom::error::Error::new(
                pair,
                nom::error::ErrorKind::Verify,
            )));
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || value.is_empty() || raw_values.insert(key, value).is_some() {
            duplicate_key = true;
        }
        match key {
            "combination" | "provenance" => combination = value,
            "threshold" => {}
            "confidence" => confidence = value.parse::<f64>().ok(),
            "band_epsilon" | "marginal_floor" | "k_initial" | "k_max" | "k_growth"
            | "topk_budget_ms" | "sdd_budget_ms" | "node_budget" => {}
            _ => unknown_keys.push(key),
        }
    }

    if let Some(value) = raw_values.get("threshold") {
        if combination == "hybrid" {
            let parsed = parse_hybrid_threshold(value).map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(
                    *value,
                    nom::error::ErrorKind::Verify,
                ))
            })?;
            threshold = Some(parsed.0);
            threshold_policy = parsed.1;
        } else {
            let parsed = value.parse::<f64>().map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(
                    *value,
                    nom::error::ErrorKind::Verify,
                ))
            })?;
            if !parsed.is_finite() {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    *value,
                    nom::error::ErrorKind::Verify,
                )));
            }
            threshold = Some(parsed);
        }
    }

    let hybrid_config = if combination == "hybrid" {
        let allowed = [
            "combination",
            "provenance",
            "threshold",
            "band_epsilon",
            "marginal_floor",
            "k_initial",
            "k_max",
            "k_growth",
            "topk_budget_ms",
            "sdd_budget_ms",
            "node_budget",
        ];
        let has_disallowed =
            !unknown_keys.is_empty() || raw_values.keys().any(|key| !allowed.contains(key));
        if duplicate_key || has_disallowed || confidence.is_some() {
            return Err(nom::Err::Failure(nom::error::Error::new(
                kv_str,
                nom::error::ErrorKind::Verify,
            )));
        }
        let Some(threshold) = threshold else {
            return Err(nom::Err::Failure(nom::error::Error::new(
                kv_str,
                nom::error::ErrorKind::Verify,
            )));
        };
        let mut config = HybridConfig {
            threshold,
            threshold_policy,
            ..HybridConfig::default()
        };
        macro_rules! parse_override {
            ($key:literal, $target:expr, $ty:ty) => {
                if let Some(value) = raw_values.get($key) {
                    match value.parse::<$ty>() {
                        Ok(parsed) => $target = parsed,
                        Err(_) => {
                            return Err(nom::Err::Failure(nom::error::Error::new(
                                kv_str,
                                nom::error::ErrorKind::Verify,
                            )))
                        }
                    }
                }
            };
        }
        parse_override!("band_epsilon", config.band_epsilon, f64);
        parse_override!("marginal_floor", config.marginal_gain_floor, f64);
        parse_override!("k_initial", config.k_initial, usize);
        parse_override!("k_max", config.k_max, usize);
        parse_override!("k_growth", config.k_growth, usize);
        parse_override!("node_budget", config.sdd_node_budget, usize);
        if let Some(value) = raw_values.get("topk_budget_ms") {
            let Ok(ms) = value.parse::<u64>() else {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    kv_str,
                    nom::error::ErrorKind::Verify,
                )));
            };
            config.topk_budget = Duration::from_millis(ms);
        }
        if let Some(value) = raw_values.get("sdd_budget_ms") {
            let Ok(ms) = value.parse::<u64>() else {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    kv_str,
                    nom::error::ErrorKind::Verify,
                )));
            };
            config.sdd_budget = Duration::from_millis(ms);
        }
        if config.validate().is_err() {
            return Err(nom::Err::Failure(nom::error::Error::new(
                kv_str,
                nom::error::ErrorKind::Verify,
            )));
        }
        Some(config)
    } else {
        None
    };

    Ok((
        input,
        ProbAnnotation {
            combination,
            threshold,
            confidence,
            hybrid_config,
        },
    ))
}

/// Parse a complete rule:
///   RULE :OverheatingAlert(?room) :- WHERE { ... } => { ... } .
///   RULE :Name PROB(combination=independent, threshold=0.3) :- CONSTRUCT { ... } WHERE { ... } .
///   RULE :Name PROB(provenance=minmax, threshold=0.3) :- CONSTRUCT { ... } WHERE { ... } .
pub fn parse_rule(input: &str) -> IResult<&str, CombinedRule<'_>> {
    let (input, _) = tag("RULE").parse(input)?;
    let (input, _) = space1.parse(input)?;
    let (input, head) = parse_rule_head(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Optionally parse PROB(...) annotation before :-
    let (input, prob_annotation) =
        opt(terminated(parse_prob_annotation, multispace0)).parse(input)?;

    let (input, _) = tag(":-").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Look ahead to determine parsing path
    let lookahead_input = input;
    let (lookahead_input, _) = multispace0.parse(lookahead_input)?;

    // Check if we have RSP elements or direct CONSTRUCT - with explicit error types
    let has_rsp_elements = matches!(
        alt((
            tag::<_, _, nom::error::Error<&str>>("RSTREAM"),
            tag::<_, _, nom::error::Error<&str>>("ISTREAM"),
            tag::<_, _, nom::error::Error<&str>>("DSTREAM"),
            tag::<_, _, nom::error::Error<&str>>("FROM")
        ))
        .parse(lookahead_input),
        Ok(_)
    );

    let (input, stream_type, window_clause) = if has_rsp_elements {
        // RSP parsing path
        let (input, stream_type) = opt(parse_stream_type).parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        let (input, window_clause) =
            many0(preceded(multispace0, parse_from_named_window)).parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        (input, stream_type, window_clause)
    } else {
        // Basic parsing path - no RSP elements
        (input, None, vec![])
    };

    // Parse CONSTRUCT clause
    let (input, conclusions) = parse_construct_clause(input)?;

    let (input, _) = multispace0.parse(input)?;

    // Parse WHERE clause
    let (input, (patterns, filters, values_clause, binds, subqueries, _, neg_patterns)) =
        parse_where(input)?;
    let body = (patterns, filters, values_clause, binds, subqueries);

    // Optional dot at the end of rule
    let (input, _) = opt(preceded(multispace0, char('.'))).parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Optionally parse ML.PREDICT block if it exists
    let (input, ml_predict) = opt(parse_ml_predict).parse(input)?;

    Ok((
        input,
        CombinedRule {
            head,
            stream_type,
            window_clause,
            model_decls: Vec::new(),
            neural_relation_decls: Vec::new(),
            train_neural_relation_decls: Vec::new(),
            body,
            negated_body: neg_patterns,
            conclusion: conclusions,
            ml_predict,
            prob_annotation,
        },
    ))
}

// Parser for RetrieveMode
pub fn parse_retrieve_mode(input: &str) -> IResult<&str, RetrieveMode> {
    let (input, _) = multispace0.parse(input)?;
    let (input, mode) = alt((
        tag("SOME").map(|_| RetrieveMode::Some),
        tag("EVERY").map(|_| RetrieveMode::Every),
    ))
    .parse(input)?;
    Ok((input, mode))
}

// Parser for StreamState
pub fn parse_stream_state(input: &str) -> IResult<&str, StreamState> {
    let (input, _) = multispace0.parse(input)?;
    let (input, state) = alt((
        tag("LATENT").map(|_| StreamState::Latent),
        tag("ACTIVE").map(|_| StreamState::Active),
    ))
    .parse(input)?;
    Ok((input, state))
}

// Parser for the complete RETRIEVE clause
pub fn parse_retrieve_clause(input: &str) -> IResult<&str, RetrieveClause<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("RETRIEVE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse retrieve mode (SOME | EVERY)
    let (input, mode) = parse_retrieve_mode(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse stream state (LATENT | ACTIVE)
    let (input, state) = parse_stream_state(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse STREAM keyword
    let (input, _) = tag("STREAM").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse variable
    let (input, var) = variable(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse FROM keyword
    let (input, _) = tag("FROM").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse IRI reference
    let (input, iri) = parse_uri(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Parse WITH keyword
    let (input, _) = tag("WITH").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse graph pattern block
    let (input, _) = char('{').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse graph patterns (can be multiple triple blocks)
    let (input, pattern_blocks) = many0(terminated(
        parse_triple_block,
        (multispace0, opt(char('.')), multispace0),
    ))
    .parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('}').parse(input)?;

    // Flatten all pattern blocks into a single vector
    let graph_pattern = pattern_blocks.into_iter().flatten().collect();

    Ok((
        input,
        RetrieveClause {
            mode,
            state,
            variable: var,
            from_iri: iri,
            graph_pattern,
        },
    ))
}

/// The combined query parser parses SPARQL + explicit Kolibrie extensions.
pub fn parse_combined_query(input: &str) -> IResult<&str, CombinedQuery<'_>> {
    parse_combined_query_with_options(input, false)
}

/// Parse through the single combined AST, optionally enabling the historical
/// standalone INSERT/DELETE DATA aliases. Standard APIs pass `false`;
/// compatibility adapters pass `true`.
pub fn parse_combined_query_with_options(
    input: &str,
    allow_data_aliases: bool,
) -> IResult<&str, CombinedQuery<'_>> {
    let (after_prologue, prefixes) = sparql_prefixes(input)?;
    let after_prologue = sparql_skip_ws(after_prologue);
    if after_prologue.is_empty() {
        return sparql_error(after_prologue, nom::error::ErrorKind::Eof);
    }

    if sparql_starts_keyword(after_prologue, "SELECT") {
        let (remaining, query) = sparql_select_core(after_prologue, true)?;
        let remaining = sparql_skip_ws(remaining);
        if !remaining.is_empty() {
            return sparql_error(remaining, nom::error::ErrorKind::Eof);
        }
        return Ok((
            remaining,
            CombinedQuery {
                prefixes,
                retrieve_clause: None,
                register_clause: None,
                model_decls: Vec::new(),
                neural_relation_decls: Vec::new(),
                train_neural_relation_decls: Vec::new(),
                rule: None,
                ml_predict: None,
                sparql: Some(SparqlOperation::Select(query)),
            },
        ));
    }
    if sparql_starts_keyword(after_prologue, "INSERT")
        || sparql_starts_keyword(after_prologue, "DELETE")
    {
        let (remaining, update) = sparql_update_core(after_prologue, allow_data_aliases)?;
        let remaining = sparql_skip_ws(remaining);
        if !remaining.is_empty() {
            return sparql_error(remaining, nom::error::ErrorKind::Eof);
        }
        return Ok((
            remaining,
            CombinedQuery {
                prefixes,
                retrieve_clause: None,
                register_clause: None,
                model_decls: Vec::new(),
                neural_relation_decls: Vec::new(),
                train_neural_relation_decls: Vec::new(),
                rule: None,
                ml_predict: None,
                sparql: Some(SparqlOperation::Update(update)),
            },
        ));
    }

    // Explicit extension path. A recognized standard keyword above never
    // falls through to these restricted grammars after a syntax error.
    let (extension_input, retrieve_clause) = opt(parse_retrieve_clause).parse(after_prologue)?;
    let (extension_input, _) = multispace0.parse(extension_input)?;
    let (extension_input, register_clause) = opt(parse_register_clause).parse(extension_input)?;
    let (extension_input, _) = multispace0.parse(extension_input)?;
    let (extension_input, (model_decls, neural_relation_decls, train_neural_relation_decls)) =
        parse_top_level_neural_decls(extension_input)?;
    let (extension_input, _) = multispace0.parse(extension_input)?;
    let (extension_input, mut rule) = opt(parse_rule).parse(extension_input)?;
    let (extension_input, _) = multispace0.parse(extension_input)?;
    if let Some(rule) = rule.as_mut() {
        rule.model_decls = model_decls.clone();
        rule.neural_relation_decls = neural_relation_decls.clone();
        rule.train_neural_relation_decls = train_neural_relation_decls.clone();
    }
    let (extension_input, ml_predict) = opt(parse_ml_predict).parse(extension_input)?;
    let extension_input = sparql_skip_ws(extension_input);

    let (remaining, sparql) = if extension_input.is_empty() {
        (extension_input, None)
    } else if sparql_starts_keyword(extension_input, "SELECT") {
        let (remaining, query) = sparql_select_core(extension_input, true)?;
        (remaining, Some(SparqlOperation::Select(query)))
    } else if sparql_starts_keyword(extension_input, "INSERT")
        || sparql_starts_keyword(extension_input, "DELETE")
    {
        let (remaining, update) = sparql_update_core(extension_input, allow_data_aliases)?;
        (remaining, Some(SparqlOperation::Update(update)))
    } else {
        return sparql_error(extension_input, nom::error::ErrorKind::Alt);
    };
    let remaining = sparql_skip_ws(remaining);
    if !remaining.is_empty() {
        return sparql_error(remaining, nom::error::ErrorKind::Eof);
    }

    Ok((
        remaining,
        CombinedQuery {
            prefixes,
            retrieve_clause,
            register_clause,
            model_decls,
            neural_relation_decls,
            train_neural_relation_decls,
            rule,
            ml_predict,
            sparql,
        },
    ))
}

fn resolve_term_with_prefix(term: &str, prefixes: &HashMap<String, String>) -> String {
    if let Some(idx) = term.find(':') {
        let prefix = &term[..idx];
        let local = &term[idx + 1..];
        if let Some(expanded) = prefixes.get(prefix) {
            return format!("{}{}", expanded, local);
        }
    }
    term.to_string()
}

fn convert_term(term: &str, dict: &mut Dictionary, prefixes: &HashMap<String, String>) -> Term {
    if term.starts_with(['?', '$']) {
        Term::Variable(term.trim_start_matches(['?', '$']).to_string())
    } else {
        let expanded = resolve_term_with_prefix(term, prefixes);
        Term::Constant(dict.encode(&expanded))
    }
}

/// Convert a triple (subject, predicate, object) from &str into a TriplePattern
pub fn convert_triple_pattern(
    triple: (&str, &str, &str),
    dict: &mut Dictionary,
    prefixes: &HashMap<String, String>,
) -> TriplePattern {
    (
        convert_term(triple.0, dict, prefixes),
        convert_term(triple.1, dict, prefixes),
        convert_term(triple.2, dict, prefixes),
    )
}

pub fn convert_combined_rule<'a>(
    cr: CombinedRule<'a>,
    dict: &mut Dictionary,
    prefixes: &HashMap<String, String>,
) -> Rule {
    let premise_patterns = cr
        .body
        .0
        .into_iter()
        .map(|triple| convert_triple_pattern(triple, dict, prefixes))
        .collect::<Vec<TriplePattern>>();

    let negative_premise_patterns = cr
        .negated_body
        .into_iter()
        .map(|triple| convert_triple_pattern(triple, dict, prefixes))
        .collect::<Vec<TriplePattern>>();

    // Convert filter expressions to filter conditions
    let filter_conditions = cr
        .body
        .1
        .into_iter()
        .flat_map(|filter_expr| {
            match filter_expr {
                FilterExpression::Comparison(var, op, value) => {
                    vec![FilterCondition {
                        variable: var.trim_start_matches(['?', '$']).to_string(),
                        operator: op.to_string(),
                        value: value.to_string(),
                    }]
                }
                FilterExpression::Or(left, right) => {
                    // Handle OR expressions
                    let mut conditions = Vec::new();

                    if let FilterExpression::Comparison(var, op, value) = *left {
                        conditions.push(FilterCondition {
                            variable: var.trim_start_matches(['?', '$']).to_string(),
                            operator: format!("OR:{}", op.to_string()),
                            value: value.to_string(),
                        });
                    }

                    if let FilterExpression::Comparison(var, op, value) = *right {
                        conditions.push(FilterCondition {
                            variable: var.trim_start_matches(['?', '$']).to_string(),
                            operator: format!("OR:{}", op.to_string()),
                            value: value.to_string(),
                        });
                    } else if let FilterExpression::Or(nested_left, nested_right) = *right {
                        // Handle nested OR expressions (common with multiple OR conditions)
                        if let FilterExpression::Comparison(var, op, value) = *nested_left {
                            conditions.push(FilterCondition {
                                variable: var.trim_start_matches(['?', '$']).to_string(),
                                operator: format!("OR:{}", op.to_string()),
                                value: value.to_string(),
                            });
                        }

                        if let FilterExpression::Comparison(var, op, value) = *nested_right {
                            conditions.push(FilterCondition {
                                variable: var.trim_start_matches(['?', '$']).to_string(),
                                operator: format!("OR:{}", op.to_string()),
                                value: value.to_string(),
                            });
                        }
                    }

                    conditions
                }
                FilterExpression::And(left, right) => {
                    // Handle AND expressions
                    let mut conditions = Vec::new();

                    if let FilterExpression::Comparison(var, op, value) = *left {
                        conditions.push(FilterCondition {
                            variable: var.trim_start_matches(['?', '$']).to_string(),
                            operator: op.to_string(),
                            value: value.to_string(),
                        });
                    }

                    if let FilterExpression::Comparison(var, op, value) = *right {
                        conditions.push(FilterCondition {
                            variable: var.trim_start_matches(['?', '$']).to_string(),
                            operator: op.to_string(),
                            value: value.to_string(),
                        });
                    }

                    conditions
                }
                _ => {
                    // Return an empty vector instead of panicking
                    println!("Warning: Unsupported filter expression type - skipping");
                    vec![]
                }
            }
        })
        .collect();

    // Convert all conclusion triples, preserving their structure
    let mut conclusion_triples: Vec<TriplePattern> = cr
        .conclusion
        .into_iter()
        .map(|triple| convert_triple_pattern(triple, dict, prefixes))
        .collect();

    // Handle windowing information if present
    if !cr.window_clause.is_empty() {
        println!("Processing rule with {} windows:", cr.window_clause.len());
        for (idx, window_clause) in cr.window_clause.iter().enumerate() {
            println!("  Window {}: IRI: {}", idx + 1, window_clause.window_iri);
            println!("    Stream IRI: {}", window_clause.stream_iri);
            println!(
                "    Window Type: {:?}",
                window_clause.window_spec.window_type
            );
            println!("    Width: {}", window_clause.window_spec.width);
            if let Some(slide) = window_clause.window_spec.slide {
                println!("    Slide: {}", slide);
            }
            if let Some(report) = window_clause.window_spec.report_strategy {
                println!("    Report Strategy: {}", report);
            }
            if let Some(tick) = window_clause.window_spec.tick {
                println!("    Tick: {}", tick);
            }
        }
    }

    // Handle stream type if present
    if let Some(stream_type) = &cr.stream_type {
        println!("Stream Type: {:?}", stream_type);
    }

    // Handle ML.PREDICT: wire output variable into conclusion triples
    if let Some(ml_predict) = &cr.ml_predict {
        println!("Processing rule with ML.PREDICT");

        let ml_output_var = ml_predict.output.trim_start_matches(['?', '$']);
        println!("ML output variable: ?{}", ml_output_var);

        // Check if the conclusion triples contain the ML output variable
        for (i, conclusion) in conclusion_triples.iter_mut().enumerate() {
            println!("Checking conclusion pattern {}: {:?}", i, conclusion);

            // Check if the conclusion contains variables that need ML output
            match &mut conclusion.2 {
                Term::Variable(var) if var == ml_output_var => {
                    println!(
                        "Found ML output variable ?{} in conclusion object position",
                        ml_output_var
                    );
                }
                Term::Variable(var) if var == "level" => {
                    // Replace generic 'level' variable with ML output variable
                    *var = ml_output_var.to_string();
                    println!("Replaced ?level with ML output variable ?{}", ml_output_var);
                }
                _ => {}
            }

            // Also check subject and predicate positions
            match &mut conclusion.0 {
                Term::Variable(var) if var == ml_output_var => {
                    println!(
                        "Found ML output variable ?{} in conclusion subject position",
                        ml_output_var
                    );
                }
                _ => {}
            }

            match &mut conclusion.1 {
                Term::Variable(var) if var == ml_output_var => {
                    println!(
                        "Found ML output variable ?{} in conclusion predicate position",
                        ml_output_var
                    );
                }
                _ => {}
            }
        }
    }

    Rule {
        premise: premise_patterns,
        negative_premise: negative_premise_patterns,
        filters: filter_conditions,
        conclusion: conclusion_triples,
    }
}

pub fn process_rule_definition(
    rule_input: &str,
    database: &mut SparqlDatabase,
) -> Result<(Rule, Vec<Triple>), String> {
    // First, register any prefixes from the rule with the database
    database.register_prefixes_from_query(rule_input);

    let parse_result = parse_combined_query(rule_input);

    if let Ok((_rest, combined)) = parse_result {
        for (prefix, uri) in &combined.prefixes {
            database.prefixes.insert(prefix.clone(), uri.clone());
        }

        let mut rule_prefixes = combined.prefixes.clone();
        database.share_prefixes_with(&mut rule_prefixes);
        register_neural_declarations(
            database,
            &rule_prefixes,
            &combined.model_decls,
            &combined.neural_relation_decls,
            &combined.train_neural_relation_decls,
        );

        let normalized_trains: Vec<TrainNeuralRelationDecl> = combined
            .train_neural_relation_decls
            .iter()
            .filter_map(|decl| {
                let normalized_pred = database.resolve_query_term(&decl.predicate, &rule_prefixes);
                database
                    .train_neural_relation_decls
                    .get(&normalized_pred)
                    .cloned()
            })
            .collect();
        for train_decl in &normalized_trains {
            execute_train_decl(database, train_decl).map_err(|err| err.to_string())?;
        }

        let mut rule = combined
            .rule
            .ok_or_else(|| "Failed to parse rule definition".to_string())?;

        materialize_neural_relations_for_patterns(database, &rule.body.0, &rule_prefixes)?;

        // Execute ML.PREDICT (if present) before converting the rule: Candle-first
        // dispatch for registered NEURAL RELATION predicates, Python fallback otherwise.
        // Materializes conclusion triples that reference the ML output variable and strips
        // those conclusion templates from the rule so the Datalog pass doesn't try to bind
        // the output variable itself.
        if let Some(ml_predict) = rule.ml_predict.clone() {
            crate::ml_predict_runtime::execute_ml_predict_clause(
                &ml_predict,
                &mut rule,
                database,
                &rule_prefixes,
            )
            .map_err(|err| err.to_string())?;
        }

        let mut kg = Reasoner::new();
        kg.dictionary = database.dictionary.clone();
        for triple in database.query_default_triples(None, None, None) {
            kg.dataset_index.insert(&triple);
        }
        kg.probability_seeds = database.probability_seeds.clone();

        let mut dict = kg.dictionary.write().unwrap();
        let dynamic_rule = convert_combined_rule(rule.clone(), &mut dict, &rule_prefixes);
        drop(dict);
        database.dictionary = kg.dictionary.clone();

        // Check if this rule has windowing - if so, set up RSP processing
        if !rule.window_clause.is_empty() {
            println!(
                "Setting up RSP window processing for rule with {} windows",
                rule.window_clause.len()
            );

            let mut all_stream_results: Vec<Triple> = Vec::new();
            let mut rsp_windows: Vec<CSPARQLWindow<WindowTriple>> = Vec::new();

            // Set up stream operator based on parsed stream type
            let stream_operator = match &rule.stream_type {
                Some(StreamType::RStream) => StreamOperator::RSTREAM,
                Some(StreamType::IStream) => StreamOperator::ISTREAM,
                Some(StreamType::DStream) => StreamOperator::DSTREAM,
                _ => StreamOperator::RSTREAM, // Default
            };

            // Create a window for each window clause
            for window_clause in &rule.window_clause {
                let mut rsp_window = create_rsp_window(&window_clause.window_spec)?;

                // Process existing triples through the window
                let mut current_time = 1;
                let default_triples = database.query_default_triples(None, None, None);
                for triple in default_triples.iter() {
                    let dict = database.dictionary.read().unwrap();
                    let window_triple = WindowTriple {
                        s: dict.decode(triple.subject).unwrap_or("").to_string(),
                        p: dict.decode(triple.predicate).unwrap_or("").to_string(),
                        o: dict.decode(triple.object).unwrap_or("").to_string(),
                    };
                    drop(dict);

                    // Add to window
                    rsp_window.add_to_window(window_triple, current_time);
                    current_time += 1;
                }

                // Register a callback to process windowed results
                let kg_clone = kg.clone();
                let rule_clone = dynamic_rule.clone();
                let _stream_op_clone = stream_operator.clone();

                rsp_window.register_callback(Box::new(
                    move |content: ContentContainer<WindowTriple>| {
                        println!("Processing window content with {} triples", content.len());

                        // Convert window content back to Knowledge Graph format
                        let mut window_kg = kg_clone.clone();
                        for window_triple in content.iter() {
                            window_kg.add_abox_triple(
                                &window_triple.s,
                                &window_triple.p,
                                &window_triple.o,
                            );
                        }

                        // Apply the rule to windowed data
                        window_kg.add_rule(rule_clone.clone());
                        let window_inferred = window_kg.infer_new_facts_semi_naive();

                        println!("Window processing inferred {} facts", window_inferred.len());
                    },
                ));

                rsp_windows.push(rsp_window);
            }

            // Add the rule to the main knowledge graph
            kg.add_rule(dynamic_rule.clone());

            // For immediate processing, also infer from current data
            let inferred_facts = kg.infer_new_facts_semi_naive();

            // Apply stream operator to results
            let eval_time = database
                .query_default_triples(None, None, None)
                .len()
                .saturating_add(1);

            for _window_clause in &rsp_windows {
                let mut r2s_operator = Relation2StreamOperator::new(stream_operator.clone(), 0);
                let stream_results = r2s_operator.eval(inferred_facts.clone(), eval_time);

                println!(
                    "Stream operator ({:?}) produced {} results",
                    stream_operator.clone(),
                    stream_results.len()
                );

                // Add inferred facts to the database
                for triple in stream_results.iter() {
                    database.add_triple(triple.clone());
                    all_stream_results.push(triple.clone());
                }
            }

            // Register rule predicates
            register_rule_predicates(&dynamic_rule, database);

            return Ok((dynamic_rule, all_stream_results));
        }

        // Non-windowed rule processing
        // Check if this is a provenance-annotated rule
        if rule.prob_annotation.is_some() {
            let ann = rule.prob_annotation.as_ref().unwrap();

            kg.add_rule(dynamic_rule.clone());
            register_rule_predicates(&dynamic_rule, database);

            // Choose provenance based on annotation, then materialize tags as RDF-star
            let provenance_type = ann.combination;
            let inferred_facts = match provenance_type {
                "minmax" | "min" => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::MinMaxProbability);
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                "addmult" | "independent" => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::AddMultProbability);
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                "boolean" => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::BooleanProvenance);
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                // Full derivation lineage with certified top-k lower bounds and
                // selective exact SDD escalation.
                "hybrid" => {
                    let config = ann.hybrid_config.as_ref().ok_or_else(|| {
                        "PROB(provenance=hybrid) requires a valid threshold and hybrid configuration"
                            .to_string()
                    })?;
                    let snapshot = SeedSnapshot::from_probability_seeds(&kg.probability_seeds)
                        .map_err(|error| error.to_string())?;
                    let (facts, results, _lineage) = kg
                        .infer_new_facts_with_hybrid(snapshot, config)
                        .map_err(|error| error.to_string())?;
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star =
                        encode_hybrid_results_as_rdf_star(&results, &mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                // Exact proof-formula provenance (WMC via Shannon expansion)
                "wmc" => {
                    let (facts, tag_store) = kg
                        .infer_new_facts_with_provenance(shared::provenance::WmcProvenance::new());
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star =
                        tag_store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                // SDD-based exact proof-formula provenance (WMC via SDD)
                "sdd" => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::sdd::SddProvenance::new());
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star =
                        tag_store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                // Top-K proof-tracking provenance.
                // k is read from the threshold field (default 5).
                // Syntax: PROB(combination=topk) or PROB(combination=topk, threshold=10)
                "topk" => {
                    let k = ann.threshold.map(|t| t as usize).unwrap_or(5);
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::TopKProofs::new(k));
                    let diagnostics: HashMap<Triple, HybridProbabilityResult> = facts
                        .iter()
                        .map(|triple| {
                            let estimate = tag_store
                                .provenance()
                                .recover_probability(&tag_store.get_tag(triple));
                            (
                                triple.clone(),
                                HybridProbabilityResult::UnsafeApproximation {
                                    estimate,
                                    reason: HybridReason::DiagnosticOnly,
                                    metrics: HybridMetrics {
                                        k_used: k,
                                        ..HybridMetrics::default()
                                    },
                                },
                            )
                        })
                        .collect();
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star =
                        encode_hybrid_results_as_rdf_star(&diagnostics, &mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
                _ => {
                    let (facts, tag_store) =
                        kg.infer_new_facts_with_provenance(shared::provenance::MinMaxProbability);
                    let mut dict = kg.dictionary.write().unwrap();
                    let mut qt_store = database.quoted_triple_store.write().unwrap();
                    let rdf_star = tag_store.encode_as_rdf_star(&mut dict, &mut qt_store);
                    drop(qt_store);
                    drop(dict);
                    for triple in rdf_star {
                        database.add_triple(triple);
                    }
                    facts
                }
            };

            for triple in inferred_facts.iter() {
                database.add_triple(triple.clone());
            }

            Ok((dynamic_rule, inferred_facts))
        } else {
            kg.add_rule(dynamic_rule.clone());

            // Register rule predicates
            register_rule_predicates(&dynamic_rule, database);

            // Infer new facts based on the rule
            let inferred_facts = kg.infer_new_facts_semi_naive();

            // Add inferred facts to the database
            for triple in inferred_facts.iter() {
                database.add_triple(triple.clone());
            }

            Ok((dynamic_rule, inferred_facts))
        }
    } else {
        Err("Failed to parse rule definition".to_string())
    }
}

// Add this function to handle RETRIEVE clause processing
pub fn process_retrieve_clause(
    retrieve_clause: &RetrieveClause,
    database: &mut SparqlDatabase,
) -> Result<Vec<Triple>, String> {
    println!("Processing RETRIEVE clause:");
    println!("  Mode: {:?}", retrieve_clause.mode);
    println!("  State: {:?}", retrieve_clause.state);
    println!("  Variable: {}", retrieve_clause.variable);
    println!("  From IRI: {}", retrieve_clause.from_iri);
    println!(
        "  Graph patterns: {} triples",
        retrieve_clause.graph_pattern.len()
    );

    // Convert graph patterns to triple patterns for matching
    let mut retrieved_triples = Vec::new();

    for pattern in &retrieve_clause.graph_pattern {
        println!("  Pattern: {} {} {}", pattern.0, pattern.1, pattern.2);

        // Create a temporary knowledge graph to match patterns
        let mut kg = Reasoner::new();
        let default_triples = database.query_default_triples(None, None, None);
        for triple in default_triples.iter() {
            let dict = database.dictionary.read().unwrap();
            let subject = dict.decode(triple.subject).map(|s| s.to_string());
            let predicate = dict.decode(triple.predicate).map(|p| p.to_string());
            let object = dict.decode(triple.object).map(|o| o.to_string());
            drop(dict);

            if let (Some(s), Some(p), Some(o)) = (subject, predicate, object) {
                kg.add_abox_triple(&s, &p, &o);
            }
        }

        // Match the pattern against the knowledge graph
        let mut dict = database.dictionary.write().unwrap();
        let pattern_converted = convert_triple_pattern(*pattern, &mut dict, &database.prefixes);
        drop(dict);

        // Find matching triples based on the pattern
        for triple in default_triples.iter() {
            if matches_pattern(&pattern_converted, triple) {
                retrieved_triples.push(triple.clone());
            }
        }
    }

    println!("Retrieved {} matching triples", retrieved_triples.len());
    Ok(retrieved_triples)
}

// Helper function to check if a triple matches a pattern
fn matches_pattern(pattern: &TriplePattern, triple: &Triple) -> bool {
    // Check subject match
    let subject_match = match &pattern.0 {
        Term::Variable(_) | Term::QuotedTriple(_) => true,
        Term::Constant(code) => *code == triple.subject,
    };

    // Check predicate match
    let predicate_match = match &pattern.1 {
        Term::Variable(_) | Term::QuotedTriple(_) => true,
        Term::Constant(code) => *code == triple.predicate,
    };

    // Check object match
    let object_match = match &pattern.2 {
        Term::Variable(_) | Term::QuotedTriple(_) => true,
        Term::Constant(code) => *code == triple.object,
    };

    subject_match && predicate_match && object_match
}

// Helper function to create RSP window from parsed specification
fn create_rsp_window(window_spec: &WindowSpec) -> Result<CSPARQLWindow<WindowTriple>, String> {
    // Create report strategy
    let mut report = Report::new();

    let report_strategy = match window_spec.report_strategy {
        Some("NON_EMPTY_CONTENT") => ReportStrategy::NonEmptyContent,
        Some("ON_CONTENT_CHANGE") => ReportStrategy::OnContentChange,
        Some("ON_WINDOW_CLOSE") => ReportStrategy::OnWindowClose,
        Some("PERIODIC") => ReportStrategy::Periodic(5), // Default period
        _ => ReportStrategy::OnWindowClose,              // Default
    };
    report.add(report_strategy);

    // Create tick strategy
    let tick = match window_spec.tick {
        Some("TIME_DRIVEN") => Tick::TimeDriven,
        Some("TUPLE_DRIVEN") => Tick::TupleDriven,
        Some("BATCH_DRIVEN") => Tick::BatchDriven,
        _ => Tick::TimeDriven, // Default
    };

    // Handle different window types
    match window_spec.window_type {
        WindowType::Sliding => {
            let slide = window_spec.slide.unwrap_or(1);
            Ok(CSPARQLWindow::new(
                window_spec.width,
                slide,
                report,
                tick,
                String::default(),
            ))
        }
        WindowType::Tumbling => {
            // Tumbling window: slide = width
            Ok(CSPARQLWindow::new(
                window_spec.width,
                window_spec.width,
                report,
                tick,
                String::default(),
            ))
        }
        WindowType::Range => {
            // Range window: slide = 1 (continuous)
            Ok(CSPARQLWindow::new(
                window_spec.width,
                1,
                report,
                tick,
                String::default(),
            ))
        }
    }
}

// Helper function to register rule predicates
fn register_rule_predicates(rule: &Rule, database: &mut SparqlDatabase) {
    for conclusion in &rule.conclusion {
        if let Term::Constant(code) = conclusion.1 {
            let dict = database.dictionary.read().unwrap();
            let expanded = dict.decode(code).unwrap_or_else(|| "").to_string();
            drop(dict);
            let local = if let Some(idx) = expanded.rfind('#') {
                &expanded[idx + 1..]
            } else if let Some(idx) = expanded.rfind(':') {
                &expanded[idx + 1..]
            } else {
                &expanded
            };
            let rule_key = local.to_lowercase();
            database.rule_map.insert(rule_key, expanded.to_string());
        }
    }
}
