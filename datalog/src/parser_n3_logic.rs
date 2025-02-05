use crate::knowledge_graph::*;
use shared::terms::{Term, TriplePattern, UnresolvedTerm, UnresolvedTriple};
use shared::rule::Rule;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_until},
    character::complete::{alphanumeric1, multispace0, multispace1},
    sequence::{delimited, preceded, separated_pair, terminated},
    multi::separated_list1,
    IResult,
};
use nom::combinator::map;

// Parsing prefix statements
fn parse_prefix(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, _) = tag("@prefix")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, prefix) = terminated(alphanumeric1, tag(":"))(input)?;
    let (input, _) = multispace1(input)?;
    let (input, uri) = delimited(tag("<"), take_until(">"), tag(">"))(input)?;
    let (input, _) = tag(".")(input)?;
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
    ))(input)
}

fn parse_unresolved_triple(input: &str) -> IResult<&str, UnresolvedTriple> {
    let (input, _) = multispace0(input)?;
    let (input, subject) = parse_unresolved_term(input)?;
    let (input, _) = multispace1(input)?;
    let (input, predicate) = parse_unresolved_term(input)?;
    let (input, _) = multispace1(input)?;
    let (input, object) = parse_unresolved_term(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag(".")(input)?;
    Ok((input, (subject, predicate, object)))
}

/// Parsing a single triple block
fn parse_nested_unresolved_rule(input: &str) -> IResult<&str, UnresolvedTriple> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("{")(input)?;

    let (input, _) = take_until("}")(input)?; 
    let (input, _) = tag("}")(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("=>")(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, _) = tag("{")(input)?;
    let (input, triple) = parse_unresolved_triple(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("}")(input)?;
    
    Ok((input, triple))
}

/// Parse a block that can contain either triple(s) or a nested rule
fn parse_unresolved_clause_block(input: &str) -> IResult<&str, Vec<UnresolvedTriple>> {
    separated_list1(multispace0, |i| {
        alt((
            parse_nested_unresolved_rule, // if it's a nested rule
            parse_unresolved_triple,      // if it's just a triple
        ))(i)
    })(input)
}

fn parse_unresolved_rule(input: &str) -> IResult<&str, (Vec<UnresolvedTriple>, UnresolvedTriple)> {
    let (input, _) = multispace0(input)?;
    
    // Parse premise
    let (input, premise_triples) = delimited(
        tag("{"),
        parse_unresolved_clause_block,
        preceded(multispace0, tag("}")),
    )(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("=>")(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, conclusion) = delimited(
        tag("{"),
        parse_unresolved_triple,
        preceded(multispace0, tag("}")),
    )(input)?;
    
    Ok((input, (premise_triples, conclusion)))
}

/// Parsing into unresolved terms
pub fn parse_n3_rule<'a>(
    input: &'a str,
    graph: &mut KnowledgeGraph,
) -> IResult<&'a str, (Vec<(&'a str, &'a str)>, Rule)> {
    let (input, prefixes) = separated_list1(multispace1, parse_prefix)(input)?;

    // Parse to an intermediate unresolved form
    let (input, (premise_triples, conclusion_triple)) = parse_unresolved_rule(input)?;
    
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

    let conclusion_parsed = (
        to_term(conclusion_triple.0, graph),
        to_term(conclusion_triple.1, graph),
        to_term(conclusion_triple.2, graph),
    );

    let rule = Rule {
        premise: premise_parsed,
        conclusion: conclusion_parsed,
        filters: vec![],
    };

    Ok((input, (prefixes, rule)))
}

/// Helper to convert UnresolvedTerm to Term
fn to_term(ut: UnresolvedTerm, graph: &mut KnowledgeGraph) -> Term {
    match ut {
        UnresolvedTerm::Var(v) => Term::Variable(v),
        UnresolvedTerm::Prefixed(s) => Term::Constant(graph.dictionary.encode(&s)),
    }
}