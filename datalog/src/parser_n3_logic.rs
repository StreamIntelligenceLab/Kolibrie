/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::reasoning::*;
use shared::terms::{Term, TriplePattern, UnresolvedTerm, UnresolvedTriple};
use shared::rule::Rule;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_until},
    character::complete::{alphanumeric1, multispace0, multispace1},
    sequence::{delimited, preceded, separated_pair, terminated},
    multi::{separated_list1, many0},
    IResult, Parser,
};
use nom::combinator::{map, opt};
use nom::error::{Error, ErrorKind};
use std::collections::HashMap;

/// Metadata linking parsed rule predicates to their owning SDS windows
#[derive(Debug, Clone)]
pub struct WindowContext {
    /// Maps predicate constant ID → window IRI string (e.g. `"http://sensor/"`)
    pub predicate_to_window: HashMap<u32, String>,
    /// Maps window IRI → window width α (in the same time unit as event timestamps)
    pub window_widths: HashMap<String, u64>,
    /// All component IRIs found in the rules (windows + output-only components),
    /// sorted longest-first for [`strip_window_prefix`]
    pub all_component_iris: Vec<String>,
}

// Parsing prefix statements
fn parse_prefix(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, _) = tag("@prefix").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, prefix) = terminated(alphanumeric1, tag(":")).parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, uri) = delimited(tag("<"), take_until(">"), tag(">")).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag(".").parse(input)?;
    Ok((input, (prefix, uri)))
}

/// Parse something like "test:SubClass" or "?variable"
fn parse_unresolved_term(input: &str) -> IResult<&str, UnresolvedTerm> {
    alt((
        map(preceded(tag("?"), alphanumeric1), |var: &str| {
            UnresolvedTerm::Var(var.to_string())
        }),
        map(delimited(tag("<"), take_until(">"), tag(">")), |iri: &str| {
            UnresolvedTerm::Iri(iri.to_string())
        }),
        map(
            separated_pair(alphanumeric1, tag(":"), alphanumeric1),
            |(prefix, term)| UnresolvedTerm::Prefixed(format!("{}:{}", prefix, term)),
        ),
    )).parse(input)
}

fn parse_unresolved_triple(input: &str) -> IResult<&str, UnresolvedTriple> {
    let (input, _) = multispace0.parse(input)?;
    let (input, subject) = parse_unresolved_term(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, predicate) = parse_unresolved_term(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, object) = parse_unresolved_term(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = opt(tag(".")).parse(input)?;
    Ok((input, (subject, predicate, object)))
}

/// Parsing a single triple block
fn parse_nested_unresolved_rule(input: &str) -> IResult<&str, UnresolvedTriple> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("{").parse(input)?;

    let (input, _) = take_until("}").parse(input)?; 
    let (input, _) = tag("}").parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("=>").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let (input, _) = tag("{").parse(input)?;
    let (input, triple) = parse_unresolved_triple(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("}").parse(input)?;
    
    Ok((input, triple))
}

/// Parse a block that can contain either triple(s) or a nested rule
fn parse_unresolved_clause_block(input: &str) -> IResult<&str, Vec<UnresolvedTriple>> {
    separated_list1(
        multispace0, 
        alt((
            parse_nested_unresolved_rule, // if it's a nested rule
            parse_unresolved_triple,      // if it's just a triple
        ))
    ).parse(input)
}

// Updated to parse multiple conclusion triples
fn parse_unresolved_rule(input: &str) -> IResult<&str, (Vec<UnresolvedTriple>, Vec<UnresolvedTriple>)> {
    let (input, _) = multispace0.parse(input)?;
    
    // Parse premise
    let (input, premise_triples) = delimited(
        tag("{"),
        parse_unresolved_clause_block,
        preceded(multispace0, tag("}")),
    ).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("=>").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Parse multiple conclusions
    let (input, conclusion_triples) = delimited(
        tag("{"),
        parse_unresolved_clause_block,  // Using the same parser for multiple triples
        preceded(multispace0, tag("}")),
    ).parse(input)?;
    
    Ok((input, (premise_triples, conclusion_triples)))
}

/// Parsing into unresolved terms
pub fn parse_n3_rule<'a>(
    input: &'a str,
    graph: &mut Reasoner,
) -> IResult<&'a str, (Vec<(&'a str, &'a str)>, Rule)> {
    let (input, prefixes) = many0(preceded(multispace0, parse_prefix)).parse(input)?;

    // Build a HashMap for prefix expansion
    let prefix_map: HashMap<String, String> = prefixes
        .iter()
        .map(|(prefix, uri)| (prefix.to_string(), uri.to_string()))
        .collect();

    // Parse to an intermediate unresolved form with multiple conclusions
    let (input, (premise_triples, conclusion_triples)) = parse_unresolved_rule(input)?;
    
    // Convert from unresolved string-based data to final `Term` with dictionary encoding
    let premise_parsed: Vec<TriplePattern> = premise_triples
        .into_iter()
        .map(|(s, p, o)| {
            (
                to_term(s, graph, &prefix_map),
                to_term(p, graph, &prefix_map),
                to_term(o, graph, &prefix_map),
            )
        })
        .collect();

    // Convert all conclusions
    let conclusions_parsed: Vec<TriplePattern> = conclusion_triples
        .into_iter()
        .map(|(s, p, o)| {
            (
                to_term(s, graph, &prefix_map),
                to_term(p, graph, &prefix_map),
                to_term(o, graph, &prefix_map),
            )
        })
        .collect();

    let rule = Rule {
        premise: premise_parsed,
        negative_premise: vec![],
        filters: vec![],
        conclusion: conclusions_parsed,
    };

    Ok((input, (prefixes, rule)))
}

/// Helper to convert UnresolvedTerm to Term - NOW WITH PREFIX EXPANSION
fn to_term(ut: UnresolvedTerm, graph: &mut Reasoner, prefix_map: &HashMap<String, String>) -> Term {
    match ut {
        UnresolvedTerm::Var(v) => Term::Variable(v),
        UnresolvedTerm::Prefixed(s) => {
            // Expand the prefix!
            let expanded = expand_prefix(&s, prefix_map);
            let mut dict = graph.dictionary.write().unwrap();
            let id = dict.encode(&expanded);
            drop(dict);
            Term::Constant(id)
        }
        UnresolvedTerm::Iri(iri) => {
            let mut dict = graph.dictionary.write().unwrap();
            let id = dict.encode(&iri);
            drop(dict);
            Term::Constant(id)
        }
    }
}

/// Expand a prefixed name like "ex:hasSensor" to full URI like "http://example.org/hasSensor"
fn expand_prefix(prefixed: &str, prefix_map: &HashMap<String, String>) -> String {
    if let Some(colon_pos) = prefixed.find(':') {
        let prefix = &prefixed[..colon_pos];
        let local = &prefixed[colon_pos + 1..];
        
        if let Some(base_uri) = prefix_map.get(prefix) {
            // Expand: prefix + local part
            format!("{}{}", base_uri, local)
        } else {
            // No prefix found, return as-is
            prefixed.to_string()
        }
    } else {
        // No colon, return as-is
        prefixed.to_string()
    }
}

/// Parse a complete N3 document: one shared prefix block followed by any
/// number of rules.  All rules share the same prefix map, avoiding context
/// loss between rules when the document is parsed in one shot
pub fn parse_n3_document<'a>(
    input: &'a str,
    graph: &mut Reasoner,
) -> IResult<&'a str, (HashMap<String, String>, Vec<Rule>)> {
    let (input, raw_prefixes) =
        many0(preceded(multispace0, parse_prefix)).parse(input)?;

    let prefix_map: HashMap<String, String> = raw_prefixes
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let (input, first_rule) = preceded(multispace0, parse_unresolved_rule).parse(input)?;
    let (input, rest_rules) =
        many0(preceded(multispace0, parse_unresolved_rule)).parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    if !input.is_empty() {
        return Err(nom::Err::Error(Error::new(input, ErrorKind::Eof)));
    }

    let mut rules = Vec::new();
    let rule_pairs = std::iter::once(first_rule).chain(rest_rules);
    for (premise_triples, conclusion_triples) in rule_pairs {
        let premise_parsed: Vec<TriplePattern> = premise_triples
            .into_iter()
            .map(|(s, p, o)| {
                (
                    to_term(s, graph, &prefix_map),
                    to_term(p, graph, &prefix_map),
                    to_term(o, graph, &prefix_map),
                )
            })
            .collect();

        let conclusions_parsed: Vec<TriplePattern> = conclusion_triples
            .into_iter()
            .map(|(s, p, o)| {
                (
                    to_term(s, graph, &prefix_map),
                    to_term(p, graph, &prefix_map),
                    to_term(o, graph, &prefix_map),
                )
            })
            .collect();

        rules.push(Rule {
            premise: premise_parsed,
            negative_premise: vec![],
            filters: vec![],
            conclusion: conclusions_parsed,
        });
    }

    Ok((input, (prefix_map, rules)))
}

/// Parse an N3 rule document while associating predicate constants with their
/// owning SDS windows
pub fn parse_n3_rules_for_sds(
    input: &str,
    graph: &mut Reasoner,
    window_widths: HashMap<String, u64>,
) -> Result<(Vec<Rule>, WindowContext), String> {
    let (_, (prefix_map, rules)) = parse_n3_document(input, graph)
        .map_err(|e| format!("N3 parse error: {:?}", e))?;

    // Sort window IRIs longest-first for correct prefix matching.
    let mut sorted_window_iris: Vec<&String> = window_widths.keys().collect();
    sorted_window_iris.sort_by(|a, b| b.len().cmp(&a.len()));

    let mut predicate_to_window: HashMap<u32, String> = HashMap::new();
    let mut output_iris: Vec<String> = Vec::new();

    {
        let dict = graph.dictionary.read().unwrap();

        // Walk predicate positions only.
        for rule in &rules {
            let preds = rule
                .premise
                .iter()
                .map(|(_, p, _)| p)
                .chain(rule.conclusion.iter().map(|(_, p, _)| p));

            for term in preds {
                if let Term::Constant(id) = term {
                    if let Some(iri) = dict.decode(*id) {
                        // Check against known window IRIs (longest-first)
                        let matched = sorted_window_iris
                            .iter()
                            .find(|w| iri.starts_with(w.as_str()))
                            .map(|w| w.to_string());

                        if let Some(window_iri) = matched {
                            predicate_to_window.insert(*id, window_iri);
                        } else {
                            // Unknown window → look for a matching prefix_map value
                            // (these are output / result component IRIs)
                            for comp_iri in prefix_map.values() {
                                if iri.starts_with(comp_iri.as_str())
                                    && !output_iris.contains(comp_iri)
                                    && !window_widths.contains_key(comp_iri)
                                {
                                    output_iris.push(comp_iri.clone());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Build all_component_iris: windows + output, sorted longest-first, deduped
    let mut all_component_iris: Vec<String> = window_widths
        .keys()
        .cloned()
        .chain(output_iris.iter().cloned())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    all_component_iris.sort_by(|a, b| b.len().cmp(&a.len()));

    Ok((
        rules,
        WindowContext {
            predicate_to_window,
            window_widths,
            all_component_iris,
        },
    ))
}
