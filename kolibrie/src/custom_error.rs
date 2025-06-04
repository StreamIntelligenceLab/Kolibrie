/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;
use nom::error::Error as NomError;

pub fn format_parse_error(input: &str, err: nom::Err<NomError<&str>>) -> String {
    match err {
        nom::Err::Error(e) | nom::Err::Failure(e) => {
            // Run manual SPARQL checks
            if let Some(error_msg) = scan_for_specific_errors(input) {
                return error_msg;
            }

            // Fallback to nom-specific error
            let error_pos = e.input;
            let error_description = match e.code {
                nom::error::ErrorKind::Tag => ". Expected a specific tag or token",
                nom::error::ErrorKind::Char => ". Expected a specific character",
                nom::error::ErrorKind::Alt => ". Expected one of several alternatives",
                _ => ""
            };

            let offset = input.len() - error_pos.len();
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

            format!(
                "\nSyntax error at line {}, column {}{}:\n{}\n{}^ Here\n",
                line_no,
                col_no,
                error_description,
                error_line,
                " ".repeat(col_no - 1)
            )
        },
        nom::Err::Incomplete(_) => {
            "Incomplete input: the parser needs more input to complete parsing".to_string()
        }
    }
}

// Aggregator of custom checks
fn scan_for_specific_errors(input: &str) -> Option<String> {
    let lines: Vec<&str> = input.lines().collect();

    // Naive check for SELECT queries missing WHERE
    if let Some(error_msg) = check_for_select_missing_where(input) {
        return Some(error_msg);
    }

    // Check for prefix errors (including missing colon in prefix declarations)
    if let Some(error_msg) = check_for_prefix_errors(&lines) {
        return Some(error_msg);
    }

    // Check for unterminated string literals
    if let Some(error_msg) = check_for_unterminated_strings(&lines) {
        return Some(error_msg);
    }

    // Check for mismatched braces
    if let Some(error_msg) = check_for_mismatched_braces(&lines) {
        return Some(error_msg);
    }

    // Check for mismatched parentheses
    if let Some(error_msg) = check_for_mismatched_parentheses(&lines) {
        return Some(error_msg);
    }

    // Check for missing separators (e.g., '.' or ';') between triple patterns
    if let Some(error_msg) = check_for_missing_separators(&lines) {
        return Some(error_msg);
    }

    // Check for invalid characters in predicates
    if let Some(error_msg) = check_for_invalid_predicate_char(&lines) {
        return Some(error_msg);
    }

    // Check for incomplete triple patterns
    if let Some(error_msg) = check_for_incomplete_triples(&lines) {
        return Some(error_msg);
    }

    // Check for BIND statements missing 'AS'
    if let Some(error_msg) = check_for_bind_missing_as(&lines) {
        return Some(error_msg);
    }

    // Check for OPTIONAL without braces
    if let Some(error_msg) = check_for_optional_without_braces(&lines) {
        return Some(error_msg);
    }

    None
}

// Check for "missing SELECT" or "missing WHERE"
fn check_for_select_missing_where(input: &str) -> Option<String> {
    let lower = input.to_lowercase();
    let has_select = lower.contains("select");
    let has_where  = lower.contains("where");

    if has_select && !has_where {
        return Some("\nFound 'SELECT' but no corresponding 'WHERE' clause.\n".to_string());
    }
    if has_where && !has_select {
        return Some("\nFound 'WHERE' but no 'SELECT' clause.\n".to_string());
    }
    None
}

// Check for prefix errors (including missing colon in prefix declarations)
fn check_for_prefix_errors(lines: &[&str]) -> Option<String> {
    let mut declared_prefixes = HashSet::new();
    
    // "Built-in" or default prefixes
    let built_in_prefixes = vec!["rdf", "rdfs", "owl", "xsd", "foaf", "dc"];
    for p in built_in_prefixes {
        declared_prefixes.insert(p.to_string());
    }

    // Gather prefixes from lines like "PREFIX ex: <http://example.org/>"
    for (line_idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.to_uppercase().starts_with("PREFIX ") {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() < 2 {
                return Some(format!(
                    "\nMalformed PREFIX declaration at line {}:\n{}\n    Expected 'PREFIX ex: <http://...>'\n",
                    line_idx + 1,
                    line
                ));
            }
            let prefix_candidate = parts[1];
            if !prefix_candidate.contains(':') {
                return Some(format!(
                    "\nMissing colon in prefix declaration at line {}:\n{}\n    Found '{}', expected something like 'ex:'\n",
                    line_idx + 1,
                    line,
                    prefix_candidate
                ));
            }
            if let Some(idx) = prefix_candidate.find(':') {
                let just_prefix = &prefix_candidate[..idx];
                if just_prefix.is_empty() {
                    return Some(format!(
                        "\nEmpty prefix name at line {}:\n{}\n",
                        line_idx + 1,
                        line
                    ));
                }
                declared_prefixes.insert(just_prefix.to_string());
            }
        }
    }

    // Now detect usage that might be missing a colon, e.g. exknows => ex:knows
    for (line_idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.to_uppercase().starts_with("PREFIX ") {
            continue;
        }
        let tokens: Vec<&str> = trimmed
            .split(|c: char| c.is_whitespace() || c == ',' || c == ';' || c == '.' || c == '(' || c == ')')
            .filter(|t| !t.is_empty())
            .collect();
        for token in tokens {
            for prefix in &declared_prefixes {
                if token.starts_with(prefix) {
                    let after_prefix = &token[prefix.len()..];
                    if !after_prefix.is_empty() && !after_prefix.starts_with(':') {
                        return Some(format!(
                            "\nPossible missing colon in predicate usage at line {}:\n{}\n    Found '{}', but it starts with prefix '{}' without a colon.\n    Did you mean '{}:{}'?\n",
                            line_idx + 1,
                            line,
                            token,
                            prefix,
                            prefix,
                            after_prefix
                        ));
                    }
                }
            }
        }
    }
    None
}

// Unterminated string literals
fn check_for_unterminated_strings(lines: &[&str]) -> Option<String> {
    for (line_idx, line) in lines.iter().enumerate() {
        let mut in_string = false;
        let mut string_start = 0;
        for (char_idx, c) in line.char_indices() {
            if c == '"' {
                in_string = !in_string;
                if in_string {
                    string_start = char_idx;
                }
            }
        }
        if in_string {
            return Some(format!(
                "\nUnterminated string literal at line {}:\n{}\n{}^ String not closed\n",
                line_idx + 1,
                line,
                " ".repeat(string_start)
            ));
        }
    }
    None
}

// Mismatched braces
fn check_for_mismatched_braces(lines: &[&str]) -> Option<String> {
    let input = lines.join("\n");
    let total_open_braces = input.matches('{').count();
    let total_close_braces = input.matches('}').count();
    if total_open_braces != total_close_braces {
        let mut open_count = 0;
        let mut close_count = 0;
        for (line_idx, line) in lines.iter().enumerate() {
            let line_open = line.matches('{').count();
            let line_close = line.matches('}').count();
            open_count += line_open;
            close_count += line_close;
            if open_count != close_count && (line_open > 0 || line_close > 0) {
                let brace_pos = if line_open > line_close {
                    line.rfind('{').unwrap_or(0)
                } else {
                    line.find('}').unwrap_or(0)
                };
                return Some(format!(
                    "\nMismatched braces at line {}:\n{}\n{}^ Here\n(Found {} '{{' vs. {} '}}')\n",
                    line_idx + 1,
                    line,
                    " ".repeat(brace_pos),
                    total_open_braces,
                    total_close_braces
                ));
            }
        }
        return Some(format!(
            "\nMismatched braces in document:\n(Found {} '{{' vs. {} '}}')\n",
            total_open_braces,
            total_close_braces
        ));
    }
    None
}

// Mismatched parentheses
fn check_for_mismatched_parentheses(lines: &[&str]) -> Option<String> {
    let mut paren_count = 0;
    for (line_idx, line) in lines.iter().enumerate() {
        for (char_idx, c) in line.char_indices() {
            if c == '(' {
                paren_count += 1;
            } else if c == ')' {
                if paren_count == 0 {
                    return Some(format!(
                        "\nExtra closing parenthesis at line {}:\n{}\n{}^ Here\n",
                        line_idx + 1,
                        line,
                        " ".repeat(char_idx)
                    ));
                }
                paren_count -= 1;
            }
        }
    }
    if paren_count != 0 {
        return Some(format!(
            "\nUnmatched '(' in the query: {} unmatched '(' remaining.\n",
            paren_count
        ));
    }
    None
}

// Missing separators (e.g., '.' or ';') between triple patterns
fn check_for_missing_separators(lines: &[&str]) -> Option<String> {
    let mut in_nested_query = false;
    for (line_idx, line) in lines.iter().enumerate() {
        if line.contains('{') {
            in_nested_query = true;
        }
        if line.contains('}') {
            in_nested_query = false;
        }
        if in_nested_query {
            continue;
        }
        if let Some((pos, expected_sep)) = find_missing_separator(line) {
            let next_line_idx = line_idx + 1;
            if next_line_idx < lines.len() && lines[next_line_idx].trim().starts_with('{') {
                continue;
            }
            return Some(format!(
                "\nMissing separator at line {}:\n{}\n{}^ Expected '{}' between triple patterns\n",
                line_idx + 1,
                line,
                " ".repeat(pos),
                expected_sep
            ));
        }
    }
    None
}

fn find_missing_separator(line: &str) -> Option<(usize, &'static str)> {
    let mut var_positions = Vec::new();
    let mut i = 0;
    while i < line.len() {
        if let Some(pos) = line[i..].find('?') {
            let abs_pos = i + pos;
            var_positions.push(abs_pos);
            i = abs_pos + 1;
        } else {
            break;
        }
    }
    if var_positions.len() < 2 {
        return None;
    }
    for idx in 0..var_positions.len() - 1 {
        let current_var_pos = var_positions[idx];
        let next_var_pos = var_positions[idx + 1];
        if current_var_pos + 1 >= line.len()
            || next_var_pos >= line.len()
            || current_var_pos + 1 >= next_var_pos
        {
            continue;
        }
        let between_vars = &line[current_var_pos + 1..next_var_pos];
        let between_vars_trimmed = between_vars.trim();
        if !between_vars_trimmed.is_empty()
            && !between_vars_trimmed.contains('.')
            && !between_vars_trimmed.contains(';')
            && !between_vars_trimmed.contains("FILTER")
            && !between_vars_trimmed.contains("BIND")
            && !between_vars_trimmed.contains('{')
            && !between_vars_trimmed.contains('}')
        {
            if let Some(word_end_pos) = between_vars.find(char::is_whitespace) {
                return Some((current_var_pos + 1 + word_end_pos, "'.' or ';'"));
            } else {
                return Some((current_var_pos + 1, "'.' or ';'"));
            }
        }
    }
    None
}

// Invalid characters in predicates
fn check_for_invalid_predicate_char(lines: &[&str]) -> Option<String> {
    for (line_idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // Note: We skip lines that are prefix declarations.
        if trimmed.to_uppercase().starts_with("PREFIX ") {
            continue;
        }
        if let Some((pos, invalid_char)) = find_invalid_predicate_character(line) {
            return Some(format!(
                "\nInvalid character '{}' in predicate at line {}:\n{}\n{}^ Invalid\n",
                invalid_char,
                line_idx + 1,
                line,
                " ".repeat(pos)
            ));
        }
    }
    None
}

fn find_invalid_predicate_character(line: &str) -> Option<(usize, char)> {
    let invalid_chars = [
        '!', '@', '#', '$', '%', '^', '&', '*', '(', ')',
        '+', '=', '[', ']', '{', '}', '\\', '|', ',', '<',
        '>', '/', '?'
    ];
    for (i, c) in line.char_indices() {
        if invalid_chars.contains(&c) {
            return Some((i, c));
        }
    }
    None
}

// Check for incomplete triple patterns
fn check_for_incomplete_triples(lines: &[&str]) -> Option<String> {
    let keywords = [
        "PREFIX", "BASE", "SELECT", "ASK", "CONSTRUCT", "DESCRIBE", "FILTER", 
        "BIND", "ORDER", "GROUP", "OPTIONAL", "SERVICE", "VALUES"
    ];
    for (idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.contains('{') || trimmed.contains('}') {
            continue;
        }
        let upper = trimmed.to_uppercase();
        if keywords.iter().any(|kw| upper.contains(kw)) {
            continue;
        }
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }
        if tokens.len() < 3 && !(trimmed.ends_with('.') || trimmed.ends_with(';')) {
            return Some(format!(
                "\nPossibly incomplete triple at line {}:\n{}\n    Expecting at least subject, predicate, and object.\n",
                idx + 1,
                line
            ));
        } else if tokens.len() >= 3 {
            if !trimmed.ends_with('.') && !trimmed.ends_with(';') && !trimmed.ends_with(',') {
                return Some(format!(
                    "\nTriple pattern at line {} may be missing a terminating '.' or ';':\n{}\n",
                    idx + 1,
                    line
                ));
            }
        }
    }
    None
}

// Check for BIND statements missing 'AS'
fn check_for_bind_missing_as(lines: &[&str]) -> Option<String> {
    for (idx, line) in lines.iter().enumerate() {
        let upper = line.to_uppercase();
        if upper.contains("BIND(") && !upper.contains(" AS ") {
            return Some(format!(
                "\nBIND statement missing 'AS' at line {}:\n{}\n",
                idx + 1,
                line
            ));
        }
    }
    None
}

// Check for OPTIONAL without braces
fn check_for_optional_without_braces(lines: &[&str]) -> Option<String> {
    for (idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim().to_uppercase();
        if trimmed.contains("OPTIONAL") && !trimmed.contains('{') {
            return Some(format!(
                "\nPossibly missing '{{' after OPTIONAL at line {}:\n{}\n",
                idx + 1,
                line
            ));
        }
    }
    None
}
