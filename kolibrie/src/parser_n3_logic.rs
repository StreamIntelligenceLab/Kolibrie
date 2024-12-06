use crate::knowledge_graph::*;
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

// Parsing prefixed names, like rdf:type or test:SubClass
fn parse_prefixed_name(input: &str) -> IResult<&str, String> {
    let res = map(
        separated_pair(alphanumeric1, tag(":"), alphanumeric1),
        |(prefix, term)| format!("{}:{}", prefix, term),
    )(input);
    res
}

// Parsing triples within the rule premises and conclusions
fn parse_triple(input: &str) -> IResult<&str, (String, String, String)> {
    let (input, _) = multispace0(input)?; // Allow optional whitespace at the start of a triple
    let (input, subject) = map(preceded(tag("?"), alphanumeric1), |s: &str| s.to_string())(input)?;
    let (input, _) = multispace1(input)?;
    let (input, predicate) = parse_prefixed_name(input)?;
    let (input, _) = multispace1(input)?;
    let (input, object) = alt((
        map(preceded(tag("?"), alphanumeric1), |o: &str| o.to_string()),
        parse_prefixed_name,
    ))(input)?;
    let (input, _) = multispace0(input)?; // Allow optional whitespace before the period
    let (input, _) = tag(".")(input)?;
    Ok((input, (subject, predicate, object)))
}

// Parsing the rule structure
fn parse_rule(input: &str) -> IResult<&str, (Vec<(String, String, String)>, (String, String, String))> {
    let (input, _) = multispace0(input)?; // Allow optional whitespace
    let (input, premise) = delimited(
        tag("{"),
        separated_list1(multispace0, parse_triple), // Allow flexible spacing between triples
        preceded(multispace0, tag("}"))
    )(input)?;
    let (input, _) = multispace0(input)?; // Allow optional whitespace around =>
    let (input, _) = tag("=>")(input)?;
    let (input, _) = multispace0(input)?; // Allow optional whitespace before the conclusion
    let (input, conclusion) = delimited(tag("{"), parse_triple, preceded(multispace0, tag("}")))(input)?; // Allow whitespace after { in conclusion
    Ok((input, (premise, conclusion)))
}

// Main parsing function that uses the above helpers
pub fn parse_n3_rule<'a>(input: &'a str, graph: &'a mut KnowledgeGraph) -> IResult<&'a str, (Vec<(&'a str, &'a str)>, Rule)> {
    let (input, prefixes) = separated_list1(multispace1, parse_prefix)(input)?;
    let (input, (premises, conclusion)) = parse_rule(input)?;
    // Transform parsed triples into Term and TriplePattern structures
    let premise_patterns: Vec<TriplePattern> = premises
        .iter()
        .map(|(s, p, o)| {
            (
                Term::Variable(s.to_string()),
                Term::Constant(graph.dictionary.encode(p)),
                Term::Constant(graph.dictionary.encode(o)),
            )
        })
        .collect();

    let conclusion_pattern = (
        Term::Variable(conclusion.0.to_string()),
        Term::Constant(graph.dictionary.encode(&conclusion.1)),
        Term::Constant(graph.dictionary.encode(&conclusion.2)),
    );

    let rule = Rule {
        premise: premise_patterns,
        conclusion: conclusion_pattern,
    };

    Ok((input, (prefixes, rule)))
}