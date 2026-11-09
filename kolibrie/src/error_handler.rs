/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::parse_error::{Expected, ParseError};
use annotate_snippets::{Annotation, AnnotationKind, Group, Level, Renderer, Snippet};

/// Renders a parse failure against the input it came from
pub(crate) fn format_parse_error(input: &str, err: ParseError<'_>) -> String {
    match err.is_incomplete() {
        false => {
            let offset = err.offset_in(input);

            let mut line_no = 1;
            let mut col_no = 1;

            for (i, c) in input.char_indices() {
                if i >= offset {
                    break;
                }
                if c == '\n' {
                    line_no += 1;
                    col_no = 1;
                } else {
                    col_no += 1;
                }
            }

            let lines: Vec<&str> = input.lines().collect();
            let error_line = if line_no <= lines.len() {
                lines[line_no - 1]
            } else {
                "[end of input]"
            };

            let (error_title, error_label) = match err.expected() {
                Expected::Token => (
                    format!(
                        "Expected a specific SPARQL keyword or token at line {}, column {}",
                        line_no, col_no
                    ),
                    "expected a valid SPARQL keyword here",
                ),
                Expected::Character => (
                    format!(
                        "Expected a specific character at line {}, column {}",
                        line_no, col_no
                    ),
                    "expected a specific character here",
                ),
                Expected::Alternative => (
                    format!(
                        "Syntax error in SPARQL query at line {}, column {}",
                        line_no, col_no
                    ),
                    "expected one of several alternatives here",
                ),
                Expected::AtLeastOne => (
                    format!(
                        "Invalid character in SPARQL query at line {}, column {}",
                        line_no, col_no
                    ),
                    "unexpected character or sequence",
                ),
                Expected::Repetition => (
                    format!(
                        "Expected more input or pattern at line {}, column {}",
                        line_no, col_no
                    ),
                    "incomplete pattern or statement",
                ),
                Expected::EndOfInput => (
                    format!(
                        "Unexpected end of query at line {}, column {}",
                        line_no, col_no
                    ),
                    "query ended unexpectedly",
                ),
                _ => (
                    format!("SPARQL syntax error at line {}, column {}", line_no, col_no),
                    "unexpected syntax error",
                ),
            };

            let specific_error = detect_specific_sparql_error(input, offset, error_line);
            let (final_title, final_label, footer) =
                if let Some((title, label, foot)) = specific_error {
                    (title, label, Some(foot))
                } else {
                    (error_title, error_label.to_string(), None)
                };

            let renderer = Renderer::plain();

            let mut report = vec![Level::ERROR.primary_title(&final_title).element(
                Snippet::source(input)
                    .line_start(1)
                    .path("query")
                    .fold(false)
                    .annotation(
                        AnnotationKind::Primary
                            .span(offset..offset.saturating_add(1).min(input.len()))
                            .label(&final_label),
                    ),
            )];

            if let Some(ref footer_text) = footer {
                report.push(Group::with_title(Level::HELP.secondary_title(footer_text)));
            }

            let message = renderer.render(&report);
            message
        }
        true => {
            let renderer = Renderer::plain();
            let report = &[
                Level::ERROR
                    .primary_title("Incomplete SPARQL query")
                    .element(
                        Snippet::<Annotation>::source(input)
                            .line_start(1)
                            .path("query")
                            .fold(false),
                    ),
                Group::with_title(
                    Level::HELP.secondary_title("The parser needs more input to complete parsing"),
                ),
            ];

            renderer.render(report)
        }
    }
}

/// Detect specific SPARQL errors and provide helpful messages
fn detect_specific_sparql_error(
    input: &str,
    offset: usize,
    _error_line: &str,
) -> Option<(String, String, String)> {
    let lower = input.to_lowercase();

    if lower.contains("select") && !lower.contains("where") && !lower.contains("insert") {
        return Some((
            "SELECT query missing WHERE clause".to_string(),
            "SELECT statement found but no WHERE clause".to_string(),
            "SPARQL SELECT queries typically require a WHERE clause.  Example: SELECT ? var WHERE { ?var ?pred ? obj }".to_string()
        ));
    }

    let open_braces = input.matches('{').count();
    let close_braces = input.matches('}').count();
    if open_braces != close_braces {
        return Some((
            "Unclosed brace in SPARQL query".to_string(),
            "missing closing '}'".to_string(),
            format!(
                "Found {} opening '{{' but {} closing '}}' in the query",
                open_braces, close_braces
            ),
        ));
    }

    let before_error = &input[..offset];
    let quote_count = before_error.matches('"').count();
    if quote_count % 2 != 0 {
        return Some((
            "Unterminated string literal".to_string(),
            "string not closed with matching quote".to_string(),
            "Make sure all string literals are properly closed with matching double quotes"
                .to_string(),
        ));
    }

    if let Some(prefix_error) = check_missing_prefix(input, offset) {
        return Some(prefix_error);
    }

    if let Some(separator_error) = check_missing_triple_separator(input, offset) {
        return Some(separator_error);
    }

    None
}

/// Check for usage of undefined prefixes
fn check_missing_prefix(input: &str, offset: usize) -> Option<(String, String, String)> {
    let before_error = &input[..offset];
    let lines: Vec<&str> = input.lines().collect();

    let mut declared_prefixes = vec!["rdf", "rdfs", "owl", "xsd", "foaf", "dc"];
    for line in &lines {
        if line.trim().to_uppercase().starts_with("PREFIX ") {
            if let Some(prefix_name) = extract_prefix_name(line) {
                declared_prefixes.push(prefix_name);
            }
        }
    }

    let words: Vec<&str> = before_error.split_whitespace().collect();
    if let Some(last_word) = words.last() {
        if last_word.contains(':') && !last_word.starts_with('<') {
            let parts: Vec<&str> = last_word.split(':').collect();
            if !parts.is_empty() {
                let potential_prefix = parts[0];
                if !declared_prefixes.iter().any(|p| p == &potential_prefix) {
                    return Some((
                        format!("Undefined prefix '{}'", potential_prefix),
                        format!("prefix '{}' is not declared", potential_prefix),
                        format!(
                            "Add a PREFIX declaration like: PREFIX {}: <http://example.org/>",
                            potential_prefix
                        ),
                    ));
                }
            }
        }
    }

    None
}

/// Check for missing separator (. or ;) between triple patterns
fn check_missing_triple_separator(input: &str, offset: usize) -> Option<(String, String, String)> {
    let before_error = &input[..offset];

    let trimmed = before_error.trim_end();
    if trimmed.contains('?') {
        let chars: Vec<char> = trimmed.chars().collect();
        if let Some(last_char) = chars.last() {
            if last_char.is_alphanumeric() || *last_char == '_' {
                let last_10_chars: String = chars
                    .iter()
                    .rev()
                    .take(10)
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect();

                if last_10_chars.contains('?')
                    && !last_10_chars.contains('.')
                    && !last_10_chars.contains(';')
                    && !last_10_chars.contains('{')
                {
                    return Some((
                        "Missing separator between triple patterns".to_string(),
                        "expected '.' or ';' to separate triple patterns".to_string(),
                        "Triple patterns in SPARQL should be separated by '.' or ';'".to_string(),
                    ));
                }
            }
        }
    }

    None
}

/// Extract prefix name from a PREFIX declaration line
fn extract_prefix_name(line: &str) -> Option<&str> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 {
        let prefix_with_colon = parts[1];
        if let Some(idx) = prefix_with_colon.find(':') {
            return Some(&prefix_with_colon[..idx]);
        }
    }
    None
}
