/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use annotate_snippets::{Level, Renderer, Snippet, AnnotationKind, Group, Annotation};
use nom:: error::Error as NomError;

pub fn format_parse_error(input: &str, err: nom::Err<NomError<&str>>) -> String {
    match err {
        nom::Err::Error(e) | nom::Err::Failure(e) => {
            let error_pos = e.input;
            let offset = input.len() - error_pos.len();
            
            // Calculate line and column numbers
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

            // Get the error line
            let lines: Vec<&str> = input.lines().collect();
            let error_line = if line_no <= lines.len() {
                lines[line_no - 1]
            } else {
                "[end of input]"
            };

            // Determine error message and label based on nom error kind
            let (error_title, error_label) = match e.code {
                nom::error::ErrorKind::Tag => (
                    format!("Expected a specific SPARQL keyword or token at line {}, column {}", line_no, col_no),
                    "expected a valid SPARQL keyword here"
                ),
                nom::error::ErrorKind::Char => (
                    format!("Expected a specific character at line {}, column {}", line_no, col_no),
                    "expected a specific character here"
                ),
                nom::error::ErrorKind::Alt => (
                    format!("Syntax error in SPARQL query at line {}, column {}", line_no, col_no),
                    "expected one of several alternatives here"
                ),
                nom::error::ErrorKind::TakeWhile1 => (
                    format!("Invalid character in SPARQL query at line {}, column {}", line_no, col_no),
                    "unexpected character or sequence"
                ),
                nom::error::ErrorKind::Many0 | nom::error::ErrorKind::Many1 => (
                    format!("Expected more input or pattern at line {}, column {}", line_no, col_no),
                    "incomplete pattern or statement"
                ),
                nom::error::ErrorKind::Eof => (
                    format!("Unexpected end of query at line {}, column {}", line_no, col_no),
                    "query ended unexpectedly"
                ),
                _ => (
                    format!("SPARQL syntax error at line {}, column {}", line_no, col_no),
                    "unexpected syntax error"
                )
            };

            // Check for common SPARQL-specific errors
            let specific_error = detect_specific_sparql_error(input, offset, error_line);
            let (final_title, final_label, footer) = if let Some((title, label, foot)) = specific_error {
                (title, label, Some(foot))
            } else {
                (error_title, error_label.to_string(), None)
            };

            // Create the annotated snippet
            let renderer = Renderer::styled();
            
            let mut report = vec![
                Level::ERROR
                    .primary_title(&final_title)
                    .element(
                        Snippet::source(input)
                            .line_start(1)
                            .path("query")
                            .fold(false)
                            .annotation(
                                AnnotationKind::Primary
                                    .span(offset..offset.saturating_add(1).min(input.len()))
                                    .label(&final_label)
                            )
                    )
            ];

            // Add footer if present
            if let Some(ref footer_text) = footer {
                report.push(
                    Group::with_title(Level::HELP.secondary_title(footer_text))
                );
            }

            let message = renderer.render(&report);
            format!("\n{}", message)
        },
        nom::Err::Incomplete(_) => {
            let renderer = Renderer::styled();
            let report = &[
                Level::ERROR
                    .primary_title("Incomplete SPARQL query")
                    .element(
                        Snippet::<Annotation>::source(input)
                            .line_start(1)
                            .path("query")
                            .fold(false)
                    ),
                Group::with_title(
                    Level::HELP.secondary_title("The parser needs more input to complete parsing")
                )
            ];
            
            format!("\n{}", renderer.render(report))
        }
    }
}

/// Detect specific SPARQL errors and provide helpful messages
fn detect_specific_sparql_error(input: &str, offset: usize, _error_line: &str) -> Option<(String, String, String)> {
    let lower = input.to_lowercase();
    
    // Check for SELECT without WHERE
    if lower.contains("select") && !lower.contains("where") && !lower.contains("insert") {
        return Some((
            "SELECT query missing WHERE clause".to_string(),
            "SELECT statement found but no WHERE clause".to_string(),
            "SPARQL SELECT queries typically require a WHERE clause.  Example: SELECT ? var WHERE { ?var ?pred ? obj }".to_string()
        ));
    }

    // Check for missing closing brace
    let open_braces = input.matches('{').count();
    let close_braces = input.matches('}').count();
    if open_braces != close_braces {
        return Some((
            "Unclosed brace in SPARQL query".to_string(),
            "missing closing '}'".to_string(),
            format!("Found {} opening '{{' but {} closing '}}' in the query", open_braces, close_braces)
        ));
    }

    // Check for unclosed string literal
    let before_error = &input[..offset];
    let quote_count = before_error.matches('"').count();
    if quote_count % 2 != 0 {
        return Some((
            "Unterminated string literal".to_string(),
            "string not closed with matching quote".to_string(),
            "Make sure all string literals are properly closed with matching double quotes".to_string()
        ));
    }

    // Check for missing prefix declaration
    if let Some(prefix_error) = check_missing_prefix(input, offset) {
        return Some(prefix_error);
    }

    // Check for missing separator between triple patterns
    if let Some(separator_error) = check_missing_triple_separator(input, offset) {
        return Some(separator_error);
    }

    None
}

/// Check for usage of undefined prefixes
fn check_missing_prefix(input: &str, offset: usize) -> Option<(String, String, String)> {
    let before_error = &input[..offset];
    let lines: Vec<&str> = input.lines().collect();
    
    // Extract declared prefixes
    let mut declared_prefixes = vec!["rdf", "rdfs", "owl", "xsd", "foaf", "dc"];
    for line in &lines {
        if line.trim().to_uppercase().starts_with("PREFIX ") {
            if let Some(prefix_name) = extract_prefix_name(line) {
                declared_prefixes.push(prefix_name);
            }
        }
    }

    // Check if there's a prefix usage near the error
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
                        format!("Add a PREFIX declaration like: PREFIX {}: <http://example.org/>", potential_prefix)
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
    
    // Look for pattern: variable followed by non-separator followed by variable
    let trimmed = before_error.trim_end();
    if trimmed.contains('?') {
        let chars: Vec<char> = trimmed.chars().collect();
        if let Some(last_char) = chars.last() {
            if last_char.is_alphanumeric() || *last_char == '_' {
                // Check if we're potentially between triple patterns
                let last_10_chars: String = chars.iter().rev().take(10).collect::<String>()
                    .chars().rev().collect();
                
                if last_10_chars.contains('?') && 
                   !last_10_chars.contains('.') && 
                   !last_10_chars.contains(';') &&
                   !last_10_chars.contains('{') {
                    return Some((
                        "Missing separator between triple patterns".to_string(),
                        "expected '.' or ';' to separate triple patterns".to_string(),
                        "Triple patterns in SPARQL should be separated by '.' or ';'".to_string()
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
