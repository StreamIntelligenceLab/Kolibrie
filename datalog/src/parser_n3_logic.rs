/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Parser for N3 logic rules, including the window annotations RSP-QL needs

use crate::reasoning::*;
use nom::combinator::{map, opt};
use nom::error::{Error, ErrorKind};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_until},
    character::complete::{alphanumeric1, multispace0, multispace1},
    multi::{many0, separated_list1},
    sequence::{delimited, preceded, separated_pair, terminated},
    IResult, Parser,
};
use shared::rule::Rule;
use shared::terms::{Term, TriplePattern, UnresolvedTerm, UnresolvedTriple};
use std::collections::HashMap;

/// Metadata linking parsed rule predicates to their owning SDS windows
#[derive(Debug, Clone)]
pub struct WindowContext {
    /// Maps predicate constant ID → window IRI string (e.g. `"http://sensor/"`)
    pub predicate_to_window: HashMap<u32, String>,
    /// Maps window IRI → window width α (in the same time unit as event timestamps)
    pub window_widths: HashMap<String, u64>,
    /// All component IRIs in the rules, longest first, for
    /// [`crate::cross_window_sds::strip_window_prefix`]
    pub all_component_iris: Vec<String>,
}

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
        map(
            delimited(tag("<"), take_until(">"), tag(">")),
            |iri: &str| UnresolvedTerm::Iri(iri.to_string()),
        ),
        map(
            separated_pair(alphanumeric1, tag(":"), alphanumeric1),
            |(prefix, term)| UnresolvedTerm::Prefixed(format!("{}:{}", prefix, term)),
        ),
    ))
    .parse(input)
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
        alt((parse_nested_unresolved_rule, parse_unresolved_triple)),
    )
    .parse(input)
}

fn parse_unresolved_rule(
    input: &str,
) -> IResult<&str, (Vec<UnresolvedTriple>, Vec<UnresolvedTriple>)> {
    let (input, _) = multispace0.parse(input)?;

    let (input, premise_triples) = delimited(
        tag("{"),
        parse_unresolved_clause_block,
        preceded(multispace0, tag("}")),
    )
    .parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("=>").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    let (input, conclusion_triples) = delimited(
        tag("{"),
        parse_unresolved_clause_block,
        preceded(multispace0, tag("}")),
    )
    .parse(input)?;

    Ok((input, (premise_triples, conclusion_triples)))
}

/// Parsing into unresolved terms
pub fn parse_n3_rule<'a>(
    input: &'a str,
    graph: &mut Reasoner,
) -> IResult<&'a str, (Vec<(&'a str, &'a str)>, Rule)> {
    let (input, prefixes) = many0(preceded(multispace0, parse_prefix)).parse(input)?;

    let prefix_map: HashMap<String, String> = prefixes
        .iter()
        .map(|(prefix, uri)| (prefix.to_string(), uri.to_string()))
        .collect();

    let (input, (premise_triples, conclusion_triples)) = parse_unresolved_rule(input)?;

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
            let expanded = expand_prefix(&s, prefix_map);
            Term::Constant(graph.encode_term(&expanded))
        }
        UnresolvedTerm::Iri(iri) => Term::Constant(graph.encode_term(&iri)),
    }
}

/// Expand a prefixed name like "ex:hasSensor" to full URI like "http://example.org/hasSensor"
fn expand_prefix(prefixed: &str, prefix_map: &HashMap<String, String>) -> String {
    if let Some(colon_pos) = prefixed.find(':') {
        let prefix = &prefixed[..colon_pos];
        let local = &prefixed[colon_pos + 1..];

        if let Some(base_uri) = prefix_map.get(prefix) {
            format!("{}{}", base_uri, local)
        } else {
            prefixed.to_string()
        }
    } else {
        prefixed.to_string()
    }
}

/// Parse a complete N3 document: one shared prefix block followed by any number of rules
pub fn parse_n3_document<'a>(
    input: &'a str,
    graph: &mut Reasoner,
) -> IResult<&'a str, (HashMap<String, String>, Vec<Rule>)> {
    let (input, raw_prefixes) = many0(preceded(multispace0, parse_prefix)).parse(input)?;

    let prefix_map: HashMap<String, String> = raw_prefixes
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let terminated = |input| {
        let (input, rule) = preceded(multispace0, parse_unresolved_rule).parse(input)?;
        let (input, _) =
            opt(preceded(multispace0, nom::character::complete::char('.'))).parse(input)?;
        Ok((input, rule))
    };
    let (input, first_rule) = terminated(input)?;
    let (input, rest_rules) = many0(terminated).parse(input)?;

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

/// Parse an N3 rule document while associating predicate constants with their owning SDS windows
pub fn parse_n3_rules_for_sds(
    input: &str,
    graph: &mut Reasoner,
    window_widths: HashMap<String, u64>,
) -> Result<(Vec<Rule>, WindowContext), String> {
    let (_, (prefix_map, rules)) =
        parse_n3_document(input, graph).map_err(|e| format!("N3 parse error: {:?}", e))?;

    let mut sorted_window_iris: Vec<&String> = window_widths.keys().collect();
    sorted_window_iris.sort_by(|a, b| b.len().cmp(&a.len()));

    let mut predicate_to_window: HashMap<u32, String> = HashMap::new();
    let mut output_iris: Vec<String> = Vec::new();

    {
        for rule in &rules {
            let preds = rule
                .premise
                .iter()
                .map(|(_, p, _)| p)
                .chain(rule.conclusion.iter().map(|(_, p, _)| p));

            for term in preds {
                if let Term::Constant(id) = term {
                    if let Some(iri) = graph.decode_term(*id) {
                        let matched = sorted_window_iris
                            .iter()
                            .find(|w| iri.starts_with(w.as_str()))
                            .map(|w| w.to_string());

                        if let Some(window_iri) = matched {
                            predicate_to_window.insert(*id, window_iri);
                        } else {
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
