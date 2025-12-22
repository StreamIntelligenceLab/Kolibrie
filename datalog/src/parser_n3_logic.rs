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
    multi::separated_list1,
    IResult, Parser,
};
use nom::combinator::map;

// Parsing prefix statements
fn parse_prefix(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, _) = tag("@prefix").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, prefix) = terminated(alphanumeric1, tag(":")).parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, uri) = delimited(tag("<"), take_until(">"), tag(">")).parse(input)?;
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
    let (input, _) = tag(".").parse(input)?;
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
    let (input, prefixes) = separated_list1(multispace1, parse_prefix).parse(input)?;

    // Parse to an intermediate unresolved form with multiple conclusions
    let (input, (premise_triples, conclusion_triples)) = parse_unresolved_rule(input)?;
    
    // Convert from unresolved string-based data to final `Term` with dictionary encoding
    let premise_parsed: Vec<TriplePattern> = premise_triples
        .into_iter()
        .map(|(s, p, o)| {
            (
                to_term(s, graph),
                to_term(p, graph),
                to_term(o, graph),
            )
        })
        .collect();

    // Convert all conclusions
    let conclusions_parsed: Vec<TriplePattern> = conclusion_triples
        .into_iter()
        .map(|(s, p, o)| {
            (
                to_term(s, graph),
                to_term(p, graph),
                to_term(o, graph),
            )
        })
        .collect();

    let rule = Rule {
        premise: premise_parsed,
        filters: vec![],
        conclusion: conclusions_parsed,  // Updated to use plural field name
    };

    Ok((input, (prefixes, rule)))
}

/// Helper to convert UnresolvedTerm to Term
fn to_term(ut: UnresolvedTerm, graph: &mut Reasoner) -> Term {
    match ut {
        UnresolvedTerm::Var(v) => Term::Variable(v),
        UnresolvedTerm::Prefixed(s) => Term::Constant(graph.dictionary.encode(&s)),
    }
}
