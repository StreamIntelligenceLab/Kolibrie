/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{char, multispace0, multispace1, space0, space1},
    combinator::{opt, recognize},
    multi::{many0, many1, separated_list1},
    sequence::{delimited, preceded, terminated},
    IResult,
    Parser
};
use rayon::str;
use crate::sparql_database::SparqlDatabase;
use datalog::reasoning::Reasoner;
use shared::triple::Triple;
use shared::dictionary::Dictionary;
use shared::rule::FilterCondition;
use shared::rule::Rule;
use shared::terms::*;
use shared::query::*;
// Add RSP imports
use crate::rsp::s2r::{CSPARQLWindow, Report, ReportStrategy, Tick, WindowTriple, ContentContainer};
use crate::rsp::r2s::{Relation2StreamOperator, StreamOperator};
use std::collections::HashMap;

// Helper function to recognize identifiers
pub fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '-').parse(input)
}

// Parser for a prefixed identifier like ex:worksAt
pub fn prefixed_identifier(input: &str) -> IResult<&str, &str> {
    recognize((identifier, char(':'), identifier)).parse(input)
}

// Parser for a predicate (either prefixed or unprefixed)
pub fn predicate(input: &str) -> IResult<&str, &str> {
    alt((
        parse_uri,
        recognize((char(':'), identifier)),
        prefixed_identifier,
        tag("a"),
    )).parse(input)
}

// Parser for variables (e.g., ?person)
pub fn variable(input: &str) -> IResult<&str, &str> {
    recognize((char('?'), identifier)).parse(input)
}

// Parser for a literal value within double quotes
pub fn parse_literal(input: &str) -> IResult<&str, &str> {
    delimited(char('"'), take_while1(|c| c != '"'), char('"')).parse(input)
}

// Parser for a URI within angle brackets
pub fn parse_uri(input: &str) -> IResult<&str, &str> {
    delimited(char('<'), take_while1(|c| c != '>'), char('>')).parse(input)
}

// Helper parser to parse a single predicate-object pair.
pub fn parse_predicate_object(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, p) = predicate(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, o) = alt((
        parse_uri,                    // <http://...>
        variable,                     // ?variable
        parse_literal,                // "literal"
        recognize((char(':'), identifier)), // :localname (like :Stream)
        prefixed_identifier,          // prefix:localname
        identifier,                   // simple identifier
    )).parse(input)?;
    Ok((input, (p, o)))
}

pub fn parse_triple_block(input: &str) -> IResult<&str, Vec<(&str, &str, &str)>> {
    let (input, subject) = alt((
        parse_uri,                    // <http://...>
        variable,                     // ?variable
        recognize((char(':'), identifier)), // :localname
        prefixed_identifier,          // prefix:localname
        identifier,                   // simple identifier
    )).parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // First predicate-object pair
    let (input, first_po) = parse_predicate_object(input)?;

    // Zero or more additional predicate-object pairs separated by semicolon
    let (input, rest_po) = many0(preceded(
        (multispace0, char(';'), multispace0),
        parse_predicate_object,
    )).parse(input)?;

    // Gather all (predicate, object) pairs
    let mut pairs = vec![first_po];
    pairs.extend(rest_po);

    // Convert each pair into a triple by reusing the same subject
    let triples = pairs.into_iter().map(|(p, o)| (subject, p, o)).collect();

    Ok((input, triples))
}

// Parser for values in the VALUES clause
pub fn parse_value_term(input: &str) -> IResult<&str, Value> {
    alt((
        // Parse IRI in <>
        delimited(char('<'), take_while1(|c| c != '>'), char('>'))
            .map(|s: &str| Value::Term(s.to_string())),
        // Parse Literal in ""
        delimited(char('"'), take_while1(|c| c != '"'), char('"'))
            .map(|s: &str| Value::Term(s.to_string())),
        // Parse prefixed name
        prefixed_identifier.map(|s| Value::Term(s.to_string())),
        // Parse identifier
        identifier.map(|s: &str| Value::Term(s.to_string())),
    )).parse(input)
}

// Parser for the VALUES clause
pub fn parse_values(input: &str) -> IResult<&str, ValuesClause<'_>> {
    let (input, _) = tag("VALUES").parse(input)?;
    let (input, _) = space1.parse(input)?;

    let (input, vars) = alt((
        // Single variable
        variable.map(|var| vec![var]),
        // Multiple variables in parentheses
        delimited(char('('), separated_list1(space1, variable), char(')')),
    )).parse(input)?;

    let (input, _) = space1.parse(input)?;
    let (input, _) = char('{').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    let (input, values) = many0(preceded(
        multispace0,
        alt((
            // For multiple variables, values are in parentheses
            delimited(
                char('('),
                separated_list1(
                    space1,
                    alt((parse_value_term, tag("UNDEF").map(|_| Value::Undef))),
                ),
                char(')'),
            ),
            // For single variable, values are terms or UNDEF
            alt((parse_value_term, tag("UNDEF").map(|_| Value::Undef))).map(|v| vec![v]),
        )),
    )).parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('}').parse(input)?;

    Ok((
        input,
        ValuesClause {
            variables: vars,
            values,
        },
    ))
}

pub fn parse_aggregate(input: &str) -> IResult<&str, (&str, &str, Option<&str>)> {
    let (input, agg_type) = alt((tag("SUM"), tag("MIN"), tag("MAX"), tag("AVG"))).parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, var) = variable(input)?;
    let (input, _) = char(')').parse(input)?;

    // Optional AS clause to name the aggregated result
    let (input, opt_as) = opt(preceded(
        space1,
        preceded(tag("AS"), preceded(space1, variable)),
    )).parse(input)?;

    Ok((input, (agg_type, var, opt_as)))
}

pub fn parse_select(input: &str) -> IResult<&str, Vec<(&str, &str, Option<&str>)>> {
    let (input, _) = tag("SELECT").parse(input)?;
    let (input, _) = space1.parse(input)?;

    // Check if the next token is '*'
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<&str>>("*").parse(input) {
        return Ok((input, vec![("*", "*", None)]));
    }

    // Parse variables or aggregation functions
    let (input, variables) = separated_list1(
        space1,
        alt((variable.map(|var| ("VAR", var, None)), parse_aggregate)),
    ).parse(input)?;

    Ok((input, variables))
}

// Parse a basic arithmetic operand (variable, literal, or number)
fn parse_operand(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    let (input, _) = multispace0.parse(input)?;
    
    let (input, operand) = alt((
        variable,
        parse_literal,
        take_while1(|c: char| c.is_digit(10) || c == '.'),
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    Ok((input, ArithmeticExpression::Operand(operand)))
}

// Parse a parenthesized arithmetic expression
fn parse_arith_parenthesized(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, expr) = parse_arithmetic_expression(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    Ok((input, expr))
}

// Parse a basic arithmetic term (operand or parenthesized expression)
fn parse_arith_term(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    alt((
        parse_operand,
        parse_arith_parenthesized,
    )).parse(input)
}

// Parse multiplication and division
fn parse_arith_factor(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    let (mut input, mut left) = parse_arith_term(input)?;
    
    // Process all multiplication and division operations in sequence
    loop {
        let (remaining, _) = multispace0.parse(input)?;
        
        // Match a multiplication or division operator with explicit error type
        match alt((
            char::<_, nom::error::Error<&str>>('*'), 
            char::<_, nom::error::Error<&str>>('/')
        )).parse(remaining) {
            Ok((after_op, op)) => {
                // Parse the right-hand term
                let (after_space, _) = multispace0.parse(after_op)?;
                let (new_input, right) = parse_arith_term(after_space)?;
                
                left = match op {
                    '*' => ArithmeticExpression::Multiply(Box::new(left), Box::new(right)),
                    '/' => ArithmeticExpression::Divide(Box::new(left), Box::new(right)),
                    _ => unreachable!(),
                };
                
                // Update input
                input = new_input;
            },
            Err(_) => break,
        }
    }
    
    Ok((input, left))
}

// Parse addition and subtraction
pub fn parse_arithmetic_expression(input: &str) -> IResult<&str, ArithmeticExpression<'_>> {
    let (mut input, mut left) = parse_arith_factor(input)?;
    
    // Process all addition and subtraction operations in sequence
    loop {
        let (remaining, _) = multispace0.parse(input)?;
        
        // Match an addition or subtraction operator with explicit error type
        match alt((
            char::<_, nom::error::Error<&str>>('+'), 
            char::<_, nom::error::Error<&str>>('-')
        )).parse(remaining) {
            Ok((after_op, op)) => {
                // Parse the right-hand factor
                let (after_space, _) = multispace0.parse(after_op)?;
                let (new_input, right) = parse_arith_factor(after_space)?;
                
                left = match op {
                    '+' => ArithmeticExpression::Add(Box::new(left), Box::new(right)),
                    '-' => ArithmeticExpression::Subtract(Box::new(left), Box::new(right)),
                    _ => unreachable!(),
                };
                
                // Update input
                input = new_input;
            },
            Err(_) => break,
        }
    }
    
    Ok((input, left))
}

fn parse_arithmetic_comparison(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (input, _) = multispace0.parse(input)?;

    // Parse left side expression
    let (input, left_str) = alt((
        // Recognize an arithmetic expression (variable followed by operators)
        recognize((
            alt((
                variable,                  // Variable name
                parse_literal,             // String literal
                take_while1(|c: char| c.is_digit(10) || c == '.'), // Number
            )),
            multispace0,
            alt((char('+'), char('-'), char('*'), char('/'))), // Operator
        )),
        // variable/literal/number
        variable,
        parse_literal,
        take_while1(|c: char| c.is_digit(10) || c == '.'),
        // parenthesized expression
        recognize(delimited(
            char('('),
            take_until(")"),
            char(')')
        ))
    )).parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    
    // Parse the comparison operator
    let (input, operator) = alt((
        tag("="), tag("!="), tag(">="),
        tag("<="), tag(">"), tag("<"),
    )).parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    
    // Parse right side expression
    let (input, right_str) = alt((
        // Recognize a parenthesized arithmetic expression
        recognize(delimited(
            char('('),
            take_until(")"),
            char(')')
        )),
        // variable/literal/number
        variable,
        parse_literal,
        take_while1(|c: char| c.is_digit(10) || c == '.'),
        // arithmetic expression
        recognize((
            alt((
                variable,
                parse_literal,
                take_while1(|c: char| c.is_digit(10) || c == '.'),
            )),
            multispace0,
            alt((char('+'), char('-'), char('*'), char('/'))),
        )),
    )).parse(input)?;

    let (input, _) = multispace0.parse(input)?;

    let result = FilterExpression::Comparison(
        left_str,
        operator,
        right_str,
    );

    Ok((input, result))
}

// Parse a single comparison expression like ?var > 10
pub fn parse_comparison(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (input, _) = multispace0.parse(input)?;

    // Parse variable or literal on left side
    let (input, left) = alt((
        variable,
        parse_literal,
        take_while1(|c: char| c.is_digit(10)),
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Parse operator
    let (input, operator) = alt((
        tag("="),
        tag("!="),
        tag(">="),
        tag("<="),
        tag(">"),
        tag("<"),
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Parse variable or literal on right side
    let (input, right) = alt((
        variable,
        parse_literal,
        take_while1(|c: char| c.is_digit(10)),
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    Ok((input, FilterExpression::Comparison(left, operator, right)))
}

// Parse an expression in parentheses
fn parse_parenthesized(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, expr) = parse_filter_expression(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    Ok((input, expr))
}

// Parse a negation (NOT)
fn parse_not(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('!').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let (input, expr) = parse_term(input)?;
    Ok((input, FilterExpression::Not(Box::new(expr))))
}

// Parse a basic term (comparison, parenthesized expression, or negation)
fn parse_term(input: &str) -> IResult<&str, FilterExpression<'_>> {
    alt((
        parse_comparison,
        parse_arithmetic_comparison,
        parse_parenthesized,
        parse_not,
    )).parse(input)
}

// Parse AND expressions
fn parse_and(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (input, left) = parse_term(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<&str>>("&&").parse(input) {
        let (input, _) = multispace0.parse(input)?;
        let (input, right) = parse_and(input)?;
        Ok((input, FilterExpression::And(Box::new(left), Box::new(right))))
    } else {
        Ok((input, left))
    }
}

// Parse OR expressions
fn parse_or(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (input, left) = parse_and(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<&str>>("||").parse(input) {
        let (input, _) = multispace0.parse(input)?;
        let (input, right) = parse_or(input)?;
        Ok((input, FilterExpression::Or(Box::new(left), Box::new(right))))
    } else {
        Ok((input, left))
    }
}

// Main entry point for parsing filter expressions
fn parse_filter_expression(input: &str) -> IResult<&str, FilterExpression<'_>> {
    parse_or(input)
}

// Parse a complete FILTER clause
pub fn parse_filter(input: &str) -> IResult<&str, FilterExpression<'_>> {
    let (input, _) = tag("FILTER").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, expr) = parse_filter_expression(input)?;
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, expr))
}

// Parser for BIND clauses: BIND(funcName(?var, "literal") AS ?newVar)
pub fn parse_bind(input: &str) -> IResult<&str, (&str, Vec<&str>, &str)> {
    let (input, _) = tag("BIND").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, func_name) = identifier(input)?;
    let (input, _) = char('(').parse(input)?;

    // Allow multiple arguments for CONCAT
    let (input, args) = separated_list1(
        (multispace0, char(','), multispace0),
        alt((variable, parse_literal)),
    ).parse(input)?;

    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("AS").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, new_var) = variable(input)?;
    let (input, _) = char(')').parse(input)?;

    Ok((input, (func_name, args, new_var)))
}

pub fn parse_subquery<'a>(input: &'a str) -> IResult<&'a str, SubQuery<'a>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('{').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse SELECT clause
    let (input, variables) = parse_select(input)?;

    // Parse WHERE clause (recursive)
    let (input, (patterns, filters, values_clause, binds, _, _,)) = parse_where(input)?;

    let (input, limit) = opt(preceded(multispace0, parse_limit)).parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('}').parse(input)?;

    Ok((
        input,
        SubQuery {
            variables,
            patterns,
            filters,
            binds,
            _values_clause: values_clause,
            limit,
        },
    ))
}

// Parser for WINDOW block inside WHERE clause
pub fn parse_window_block(input: &str) -> IResult<&str, WindowBlock<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("WINDOW").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // Parse window name (like :wind)
    let (input, window_name) = alt((
        recognize((char(':'), identifier)),
        identifier,
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('{').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Parse triple patterns inside the window block
    let (input, pattern_blocks) = many0(terminated(
        parse_triple_block,
        (multispace0, opt(char('.')), multispace0)
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('}').parse(input)?;
    
    // Flatten all pattern blocks
    let patterns = pattern_blocks.into_iter().flatten().collect();
    
    Ok((input, WindowBlock {
        window_name,
        patterns,
    }))
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
    ),
> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("WHERE").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('{').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    let mut patterns = Vec::new();
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
        ).parse(current_input)
        {
            current_input = new_input;
        }
    }

    Ok((
        current_input,
        (patterns, filters, values_clause, binds, subqueries, window_blocks),
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
    let (input, window_clause) = many1(preceded(multispace0, parse_from_named_window)).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Parse WHERE clause with window support
    let (input, (patterns, filters, values_clause, binds, subqueries, window_blocks)) = parse_where(input)?;
    
    Ok((input, RegisterClause {
        stream_type,
        output_stream_iri: output_iri,
        query: RSPQLSelectQuery {
            variables,
            window_clause,
            where_clause: (patterns, filters, values_clause, binds, subqueries),
            window_blocks,
        },
    }))
}

pub fn parse_group_by(input: &str) -> IResult<&str, Vec<&str>> {
    let (input, _) = tag("GROUPBY").parse(input)?;
    let (input, _) = space1.parse(input)?;

    // Parse the variables to group by
    let (input, group_vars) = separated_list1(space1, variable).parse(input)?;
    Ok((input, group_vars))
}

// Parser for sort direction (ASC/DESC)
pub fn parse_sort_direction(input: &str) -> IResult<&str, SortDirection> {
    let (input, _) = multispace0.parse(input)?;
    let (input, direction) = opt(alt((
        tag("ASC").map(|_| SortDirection::Asc),
        tag("DESC").map(|_| SortDirection::Desc),
    ))).parse(input)?;
    Ok((input, direction.unwrap_or(SortDirection::Asc))) // Default to ASC if not specified
}

// Parser for a single ORDER BY condition
pub fn parse_order_condition(input: &str) -> IResult<&str, OrderCondition<'_>> {
    let (input, _) = multispace0.parse(input)?;
    
    // Try to parse direction first (optional)
    let (input, direction) = opt(alt((
        tag("ASC").map(|_| SortDirection::Asc),
        tag("DESC").map(|_| SortDirection::Desc),
    ))).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Parse opening parenthesis if direction was specified
    let (input, has_parens) = if direction.is_some() {
        let (input, _) = char('(').parse(input)?;
        (input, true)
    } else {
        (input, false)
    };
    
    let (input, _) = multispace0.parse(input)?;
    
    // Parse the variable
    let (input, var) = variable(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Parse closing parenthesis if we had opening one
    let input = if has_parens {
        let (input, _) = char(')').parse(input)?;
        input
    } else {
        input
    };
    
    // If no direction was parsed before variable, try to parse it after
    let (input, final_direction) = if direction.is_none() {
        let (input, post_direction) = opt(preceded(
            multispace1,
            alt((
                tag("ASC").map(|_| SortDirection::Asc),
                tag("DESC").map(|_| SortDirection::Desc),
            ))
        )).parse(input)?;
        (input, post_direction.unwrap_or(SortDirection::Asc))
    } else {
        (input, direction.unwrap())
    };
    
    Ok((input, OrderCondition {
        variable: var,
        direction: final_direction,
    }))
}

// Alternative simpler parser for ORDER BY condition (variable with optional direction)
pub fn parse_simple_order_condition(input: &str) -> IResult<&str, OrderCondition<'_>> {
    let (input, _) = multispace0.parse(input)?;
    
    // Parse variable first
    let (input, var) = variable(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Parse optional direction after variable
    let (input, direction) = opt(alt((
        tag("ASC").map(|_| SortDirection::Asc),
        tag("DESC").map(|_| SortDirection::Desc),
    ))).parse(input)?;
    
    Ok((input, OrderCondition {
        variable: var,
        direction: direction.unwrap_or(SortDirection::Asc),
    }))
}

// Main ORDER BY parser
pub fn parse_order_by(input: &str) -> IResult<&str, Vec<OrderCondition<'_>>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("ORDER").parse(input)?;
    let (input, _) = space1.parse(input)?;
    let (input, _) = tag("BY").parse(input)?;
    let (input, _) = space1.parse(input)?;

    // Parse one or more order conditions separated by commas
    let (input, conditions) = separated_list1(
        (multispace0, char(','), multispace0),
        alt((
            parse_order_condition,      // Try complex form first
            parse_simple_order_condition, // Fall back to simple form
        ))
    ).parse(input)?;

    Ok((input, conditions))
}

// Add a new parser for PREFIX declarations
pub fn parse_prefix(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("PREFIX").parse(input)?;
    let (input, _) = space1.parse(input)?;
    let (input, prefix) = identifier(input)?;
    let (input, _) = char(':').parse(input)?;
    let (input, _) = space0.parse(input)?;
    let (input, uri) = delimited(char('<'), take_while1(|c| c != '>'), char('>')).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    Ok((input, (prefix, uri)))
}

// Modified parse_insert to handle literals and debug output
pub fn parse_insert(input: &str) -> IResult<&str, InsertClause<'_>> {
    let (input, _) = tag("INSERT").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('{').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse one or more triple blocks separated by dots.
    // Each triple block can contain multiple predicate-object pairs separated by semicolons.
    let (input, triple_blocks) =
        separated_list1((space0, char('.'), space0), parse_triple_block).parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('}').parse(input)?;

    // Flatten all the triple blocks into a single Vec
    let triples = triple_blocks.into_iter().flatten().collect();

    Ok((input, InsertClause { triples }))
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
    ).parse(input)?;
    
    Ok((input, conclusions))
}

// Add LIMIT parser
pub fn parse_limit(input: &str) -> IResult<&str, usize> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("LIMIT").parse(input)?;
    let (input, _) = space1.parse(input)?;
    let (input, limit_str) = take_while1(|c: char| c.is_digit(10)).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let limit = limit_str.parse::<usize>().unwrap_or(0);
    Ok((input, limit))
}

pub fn parse_sparql_query(
    input: &str,
) -> IResult<
    &str,
    (
        Option<InsertClause<'_>>,
        Vec<(&str, &str, Option<&str>)>, // variables
        Vec<(&str, &str, &str)>,         // patterns
        Vec<FilterExpression<'_>>,         // filters
        Vec<&str>,                       // group_vars
        HashMap<String, String>,         // prefixes
        Option<ValuesClause<'_>>,
        Vec<(&str, Vec<&str>, &str)>, // BIND clauses
        Vec<SubQuery<'_>>,
        Option<usize>,                  // limit
        Vec<WindowBlock<'_>>,               // Add window blocks
        Vec<OrderCondition<'_>>,             // ORDER BY conditions
    ),
> {
    let mut input = input;
    let mut prefixes = HashMap::new();

    // Parse zero or more PREFIX declarations
    loop {
        let original_input = input;
        if let Ok((new_input, (prefix, uri))) = parse_prefix(input) {
            prefixes.insert(prefix.to_string(), uri.to_string());
            input = new_input;
        } else {
            input = original_input;
            break;
        }
    }

    // Optionally parse the INSERT clause
    let (input, insert_clause) = opt(parse_insert).parse(input)?;
    let (mut input, _) = multispace0.parse(input)?;

    let mut variables = Vec::new();
    if insert_clause.is_none() {
        // Parse SELECT clause only if there is no INSERT clause
        let (new_input, vars) = parse_select(input)?;
        variables = vars;
        input = new_input;
        let (_input, _) = multispace1.parse(input)?;
    }

    // Ensure any spaces are consumed before parsing WHERE clause
    let (input, _) = multispace0.parse(input)?;

    // Parse WHERE clause
    let (input, (patterns, filters, values_clause, binds, subqueries, window_block)) = parse_where(input)?;

    // Optionally parse the GROUP BY clause
    let (input, group_vars) =
        if let Ok((input, group_vars)) = preceded(multispace0, parse_group_by).parse(input) {
            (input, group_vars)
        } else {
            (input, vec![])
        };
    
    // Parse optional ORDER BY clause
    let (input, order_conditions) = opt(preceded(multispace0, parse_order_by)).parse(input)?;
    let order_conditions = order_conditions.unwrap_or_else(Vec::new);

    let (input, limit) = opt(preceded(multispace0, parse_limit)).parse(input)?;

    Ok((
        input,
        (
            insert_clause,
            variables,
            patterns,
            filters,
            group_vars,
            prefixes,
            values_clause,
            binds,
            subqueries,
            limit,
            window_block,
            order_conditions,
        ),
    ))
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
    }).parse(input)?;
    
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
    let (input, additional_vars) = many0(
        preceded(
            (multispace0, char(','), multispace0),
            variable
        )
    ).parse(input)?;
    
    // Combine all variables
    let mut all_vars = vec![first_var];
    all_vars.extend(additional_vars);
    
    let (input, _) = preceded(multispace0, char(')')).parse(input)?;
    
    Ok((
        input,
        RuleHead {
            predicate: pred,
            arguments: all_vars,
        },
    ))
}

pub fn parse_rule_head(input: &str) -> IResult<&str, RuleHead<'_>> {
    let (input, pred) = predicate(input)?;
    let (input, args) = opt(delimited(
        char('('),
        separated_list1((multispace0, char(','), multispace0), variable),
        char(')'),
    )).parse(input)?;
    let arguments = args.unwrap_or_else(|| vec![]);
    Ok((
        input,
        RuleHead {
            predicate: pred,
            arguments,
        },
    ))
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

pub fn parse_ml_predict(input: &str) -> IResult<&str, MLPredictClause<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("ML.PREDICT").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    // Parse MODEL clause with quoted name
    let (input, _) = tag("MODEL").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = char('"').parse(input)?;  // Expect opening quote
    let (input, model) = take_until("\"").parse(input)?;  // Take everything until closing quote
    let (input, _) = char('"').parse(input)?;  // Expect closing quote
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
                if var.starts_with('?') {
                    select_vars.push((var, "", None)); // Add proper variable type extraction if needed
                }
            }
            
            // Parse WHERE patterns and filters (simplified - use your actual WHERE parser)
            let where_clause = &input_query[where_idx + 5..].trim();
            // This is a placeholder - you should use your actual pattern and filter parser here
            let (_rest, (patterns, filters, _values, _binds, _subqueries, _)) = 
                parse_where(where_clause).unwrap_or_else(|_| (where_clause, (vec![], vec![], None, vec![], vec![], vec![])));
            
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
    )).parse(input)?;
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
    )).parse(input)?;
    
    let (input, _) = multispace1.parse(input)?;
    
    // Parse duration (like PT10M) or numeric value
    let (input, width_str) = alt((
        // ISO 8601 duration format (PT10M, PT5S, etc.)
        recognize((
            tag("PT"),
            take_while1(|c: char| c.is_digit(10)),
            alt((char('S'), char('M'), char('H')))
        )),
        // Simple numeric value
        take_while1(|c: char| c.is_digit(10))
    )).parse(input)?;
    
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
                alt((char('S'), char('M'), char('H')))
            )),
            // Simple numeric value
            take_while1(|c: char| c.is_digit(10))
        ))
    )).parse(input)?;
    
    let slide = slide.map(parse_duration_to_seconds);
    
    // Optional report strategy
    let (input, report_strategy) = opt(preceded(
        (multispace1, tag("REPORT"), multispace1),
        alt((
            tag("ON_WINDOW_CLOSE"),
            tag("ON_CONTENT_CHANGE"),
            tag("NON_EMPTY_CONTENT"),
            tag("PERIODIC"),
        ))
    )).parse(input)?;
    
    // Optional tick strategy
    let (input, tick) = opt(preceded(
        (multispace1, tag("TICK"), multispace1),
        alt((
            tag("TIME_DRIVEN"),
            tag("TUPLE_DRIVEN"),
            tag("BATCH_DRIVEN"),
        ))
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(']').parse(input)?;
    
    Ok((input, WindowSpec {
        window_type,
        width,
        slide,
        report_strategy,
        tick,
    }))
}

// Helper function to convert duration strings to seconds
fn parse_duration_to_seconds(duration: &str) -> usize {
    if duration.starts_with("PT") && duration.len() > 2 {
        let time_part = &duration[2..];
        if let Some(num_end) = time_part.chars().position(|c| !c.is_digit(10)) {
            if let Ok(num) = time_part[..num_end].parse::<usize>() {
                match time_part.chars().nth(num_end) {
                    Some('S') => num,      // seconds
                    Some('M') => num * 60, // minutes to seconds
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
        recognize((char(':'), identifier)),                   // :wind
        variable,                                                    // ?var
        identifier,                                                  // simple name
    )).parse(input)?;
    
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag("ON").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // Parse stream identifier (can be variable, URI, or namespace reference)
    let (input, stream_iri) = alt((
        delimited(char('<'), take_while1(|c| c != '>'), char('>')), // <uri>
        variable,                                                    // ?s
        recognize((char(':'), identifier)),                   // :stream
        identifier,                                                  // simple name
    )).parse(input)?;
    
    let (input, _) = multispace1.parse(input)?;
    
    // Parse window specification with ISO 8601 duration support
    let (input, window_spec) = parse_window_spec(input)?;
    
    Ok((input, WindowClause {
        window_iri,
        stream_iri,
        window_spec,
    }))
}

/// Parse a complete rule:
///   RULE :OverheatingAlert(?room) :- WHERE { ... } => { ... } .
pub fn parse_rule(input: &str) -> IResult<&str, CombinedRule<'_>> {
    let (input, _) = tag("RULE").parse(input)?;
    let (input, _) = space1.parse(input)?;
    let (input, head) = parse_rule_head(input)?;
    let (input, _) = multispace0.parse(input)?;

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
        )).parse(lookahead_input),
        Ok(_)
    );
    
    let (input, stream_type, window_clause) = if has_rsp_elements {
        // RSP parsing path
        let (input, stream_type) = opt(parse_stream_type).parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        let (input, window_clause) = many0(preceded(multispace0, parse_from_named_window)).parse(input)?;
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
    let (input, (patterns, filters, values_clause, binds, subqueries, _)) = parse_where(input)?;
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
            body,
            conclusion: conclusions,
            ml_predict,
        },
    ))
}

// Parser for RetrieveMode
pub fn parse_retrieve_mode(input: &str) -> IResult<&str, RetrieveMode> {
    let (input, _) = multispace0.parse(input)?;
    let (input, mode) = alt((
        tag("SOME").map(|_| RetrieveMode::Some),
        tag("EVERY").map(|_| RetrieveMode::Every),
    )).parse(input)?;
    Ok((input, mode))
}

// Parser for StreamState
pub fn parse_stream_state(input: &str) -> IResult<&str, StreamState> {
    let (input, _) = multispace0.parse(input)?;
    let (input, state) = alt((
        tag("LATENT").map(|_| StreamState::Latent),
        tag("ACTIVE").map(|_| StreamState::Active),
    )).parse(input)?;
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
        (multispace0, opt(char('.')), multispace0)
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('}').parse(input)?;
    
    // Flatten all pattern blocks into a single vector
    let graph_pattern = pattern_blocks.into_iter().flatten().collect();
    
    Ok((input, RetrieveClause {
        mode,
        state,
        variable: var,
        from_iri: iri,
        graph_pattern,
    }))
}

/// The combined query parser parses SPARQL + LP
pub fn parse_combined_query(input: &str) -> IResult<&str, CombinedQuery<'_>> {
    let (input, prefix_list) = many0(|i| {
        let (i, _) = multispace0.parse(i)?;
        let (i, _) = tag("PREFIX").parse(i)?;
        let (i, _) = space1.parse(i)?;
        let (i, p) = identifier(i)?;
        let (i, _) = char(':').parse(i)?;
        let (i, _) = space0.parse(i)?;
        let (i, uri) = delimited(char('<'), take_while1(|c| c != '>'), char('>')).parse(i)?;
        Ok((i, (p, uri)))
    }).parse(input)?;
    
    let mut prefixes = HashMap::new();
    for (p, uri) in prefix_list {
        prefixes.insert(p.to_string(), uri.to_string());
    }
    
    let (input, _) = multispace0.parse(input)?;

    // Parse optional RETRIEVE clause
    let (input, retrieve_clause) = opt(parse_retrieve_clause).parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse optional REGISTER clause
    let (input, register_clause) = opt(parse_register_clause).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Parse the rule with ML.PREDICT if present
    let (input, rule_opt) = opt(parse_rule).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Parse the SPARQL query part
    let (input, sparql_parse) = if input.trim().is_empty() {
        // No remaining input - create empty SPARQL parse result
        (input, (None, vec![], vec![], vec![], vec![], HashMap::new(), None, vec![], vec![], None, vec![], vec![]))
    } else {
        // There's remaining input - try to parse it as SPARQL
        parse_sparql_query(input)?
    }; 

    Ok((
        input,
        CombinedQuery {
            prefixes,
            retrieve_clause,
            register_clause,
            rule: rule_opt,
            sparql: sparql_parse,
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
    if term.starts_with('?') {
        Term::Variable(term.trim_start_matches('?').to_string())
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

    // Convert filter expressions to filter conditions
    let filter_conditions = cr
        .body
        .1
        .into_iter()
        .flat_map(|filter_expr| {
            match filter_expr {
                FilterExpression::Comparison(var, op, value) => {
                    vec![FilterCondition {
                        variable: var.trim_start_matches('?').to_string(),
                        operator: op.to_string(),
                        value: value.to_string(),
                    }]
                },
                FilterExpression::Or(left, right) => {
                    // Handle OR expressions
                    let mut conditions = Vec::new();
                    
                    if let FilterExpression::Comparison(var, op, value) = *left {
                        conditions.push(FilterCondition {
                            variable: var.trim_start_matches('?').to_string(),
                            operator: format!("OR:{}", op.to_string()),
                            value: value.to_string(),
                        });
                    }
                    
                    if let FilterExpression::Comparison(var, op, value) = *right {
                        conditions.push(FilterCondition {
                            variable: var.trim_start_matches('?').to_string(),
                            operator: format!("OR:{}", op.to_string()),
                            value: value.to_string(),
                        });
                    } else if let FilterExpression::Or(nested_left, nested_right) = *right {
                        // Handle nested OR expressions (common with multiple OR conditions)
                        if let FilterExpression::Comparison(var, op, value) = *nested_left {
                            conditions.push(FilterCondition {
                                variable: var.trim_start_matches('?').to_string(),
                                operator: format!("OR:{}", op.to_string()),
                                value: value.to_string(),
                            });
                        }
                        
                        if let FilterExpression::Comparison(var, op, value) = *nested_right {
                            conditions.push(FilterCondition {
                                variable: var.trim_start_matches('?').to_string(),
                                operator: format!("OR:{}", op.to_string()),
                                value: value.to_string(),
                            });
                        }
                    }
                    
                    conditions
                },
                FilterExpression::And(left, right) => {
                    // Handle AND expressions
                    let mut conditions = Vec::new();
                    
                    if let FilterExpression::Comparison(var, op, value) = *left {
                        conditions.push(FilterCondition {
                            variable: var.trim_start_matches('?').to_string(),
                            operator: op.to_string(), 
                            value: value.to_string(),
                        });
                    }
                    
                    if let FilterExpression::Comparison(var, op, value) = *right {
                        conditions.push(FilterCondition {
                            variable: var.trim_start_matches('?').to_string(),
                            operator: op.to_string(),
                            value: value.to_string(),
                        });
                    }
                    
                    conditions
                },
                _ => {
                    // Return an empty vector instead of panicking
                    println!("Warning: Unsupported filter expression type - skipping");
                    vec![]
                }
            }
        })
        .collect();

    // Convert all conclusion triples, preserving their structure
    let mut conclusion_triples: Vec<TriplePattern> = cr.conclusion
        .into_iter()
        .map(|triple| convert_triple_pattern(triple, dict, prefixes))
        .collect();

    // Handle windowing information if present
    if !cr.window_clause.is_empty() {
        println!("Processing rule with {} windows:", cr.window_clause.len());
        for (idx, window_clause) in cr.window_clause.iter().enumerate() {
            println!("  Window {}: IRI: {}", idx + 1, window_clause.window_iri);
            println!("    Stream IRI: {}", window_clause.stream_iri);
            println!("    Window Type: {:?}", window_clause.window_spec.window_type);
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

    // Special handling for parameterless rules with ML.PREDICT
    if let Some(ml_predict) = &cr.ml_predict {
        if cr.head.arguments.is_empty() {
            println!("Processing parameterless rule with ML.PREDICT");
            
            let ml_output_var = ml_predict.output.trim_start_matches('?');
            println!("ML output variable: ?{}", ml_output_var);
            
            // Check if the conclusion triples contain the ML output variable
            for (i, conclusion) in conclusion_triples.iter_mut().enumerate() {
                println!("Checking conclusion pattern {}: {:?}", i, conclusion);
                
                // Check if the conclusion contains variables that need ML output
                match &mut conclusion.2 {
                    Term::Variable(var) if var == ml_output_var => {
                        println!("Found ML output variable ?{} in conclusion object position", ml_output_var);
                    },
                    Term::Variable(var) if var == "level" => {
                        // Replace generic 'level' variable with ML output variable
                        *var = ml_output_var.to_string();
                        println!("Replaced ?level with ML output variable ?{}", ml_output_var);
                    },
                    _ => {}
                }
                
                // Also check subject and predicate positions
                match &mut conclusion.0 {
                    Term::Variable(var) if var == ml_output_var => {
                        println!("Found ML output variable ?{} in conclusion subject position", ml_output_var);
                    },
                    _ => {}
                }
                
                match &mut conclusion.1 {
                    Term::Variable(var) if var == ml_output_var => {
                        println!("Found ML output variable ?{} in conclusion predicate position", ml_output_var);
                    },
                    _ => {}
                }
            }
        }
    }

    Rule {
        premise: premise_patterns,
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

    let mut kg = Reasoner::new();
    for triple in database.triples.iter() {
        let subject = database.dictionary.decode(triple.subject);
        let predicate = database.dictionary.decode(triple.predicate);
        let object = database.dictionary.decode(triple.object);
        if let (Some(s), Some(p), Some(o)) = (subject, predicate, object) {
            kg.add_abox_triple(&s, &p, &o);
        }
    }

    // Parse the standalone rule
    let parse_result = parse_standalone_rule(rule_input);

    if let Ok((_rest, (rule, prefixes))) = parse_result {
        // Ensure all prefixes from the rule are in the database
        for (prefix, uri) in &prefixes {
            database.prefixes.insert(prefix.clone(), uri.clone());
        }

        // Convert the rule, ensuring it has access to all prefixes
        let mut rule_prefixes = prefixes.clone();
        database.share_prefixes_with(&mut rule_prefixes);

        let dynamic_rule = convert_combined_rule(rule.clone(), &mut database.dictionary, &rule_prefixes);

        // Check if this rule has windowing - if so, set up RSP processing
        if !rule.window_clause.is_empty() {
            println!("Setting up RSP window processing for rule with {} windows", rule.window_clause.len());

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
                for triple in database.triples.iter() {
                    let window_triple = WindowTriple {
                        s: database.dictionary.decode(triple.subject).unwrap_or("").to_string(),
                        p: database.dictionary.decode(triple.predicate).unwrap_or("").to_string(),
                        o: database.dictionary.decode(triple.object).unwrap_or("").to_string(),
                    };

                    // Add to window
                    rsp_window.add_to_window(window_triple, current_time);
                    current_time += 1;
                }

                // Register a callback to process windowed results
                let kg_clone = kg.clone();
                let rule_clone = dynamic_rule.clone();
                let _stream_op_clone = stream_operator.clone();

                rsp_window.register_callback(Box::new(move |content: ContentContainer<WindowTriple>| {
                    println!("Processing window content with {} triples", content.len());

                    // Convert window content back to Knowledge Graph format
                    let mut window_kg = kg_clone.clone();
                    for window_triple in content.iter() {
                        window_kg.add_abox_triple(&window_triple.s, &window_triple.p, &window_triple.o);
                    }

                    // Apply the rule to windowed data
                    window_kg.add_rule(rule_clone.clone());
                    let window_inferred = window_kg.infer_new_facts_semi_naive();

                    println!("Window processing inferred {} facts", window_inferred.len());
                }));

                rsp_windows.push(rsp_window);
            }

            // Add the rule to the main knowledge graph
            kg.add_rule(dynamic_rule.clone());

            // For immediate processing, also infer from current data
            let inferred_facts = kg.infer_new_facts_semi_naive();

            // Apply stream operator to results
            let eval_time = database.triples.len().saturating_add(1);

            for _window_clause in &rsp_windows {
                let mut r2s_operator = Relation2StreamOperator::new(stream_operator.clone(), 0);
                let stream_results = r2s_operator.eval(inferred_facts.clone(), eval_time);

                println!("Stream operator ({:?}) produced {} results", stream_operator.clone(), stream_results.len());

                // Add inferred facts to the database
                for triple in stream_results.iter() {
                    database.triples.insert(triple.clone());
                    all_stream_results.push(triple.clone());
                }
            }

            // Register rule predicates
            register_rule_predicates(&dynamic_rule, database);

            return Ok((dynamic_rule, all_stream_results));
        }

        // Non-windowed rule processing (existing logic)
        kg.add_rule(dynamic_rule.clone());

        // Register rule predicates
        register_rule_predicates(&dynamic_rule, database);

        // Infer new facts based on the rule
        let inferred_facts = kg.infer_new_facts_semi_naive();

        // Add inferred facts to the database
        for triple in inferred_facts.iter() {
            database.triples.insert(triple.clone());
        }

        Ok((dynamic_rule, inferred_facts))
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
    println!("  Graph patterns: {} triples", retrieve_clause.graph_pattern.len());
    
    // Convert graph patterns to triple patterns for matching
    let mut retrieved_triples = Vec::new();
    
    for pattern in &retrieve_clause.graph_pattern {
        println!("  Pattern: {} {} {}", pattern.0, pattern.1, pattern.2);
        
        // Create a temporary knowledge graph to match patterns
        let mut kg = Reasoner::new();
        for triple in database.triples.iter() {
            let subject = database.dictionary.decode(triple.subject);
            let predicate = database.dictionary.decode(triple.predicate);
            let object = database.dictionary.decode(triple.object);
            if let (Some(s), Some(p), Some(o)) = (subject, predicate, object) {
                kg.add_abox_triple(&s, &p, &o);
            }
        }
        
        // Match the pattern against the knowledge graph
        let pattern_converted = convert_triple_pattern(*pattern, &mut database.dictionary, &database.prefixes);
        
        // Find matching triples based on the pattern
        for triple in database.triples.iter() {
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
        Term::Variable(_) => true, // Variables match anything
        Term::Constant(code) => *code == triple.subject,
    };
    
    // Check predicate match
    let predicate_match = match &pattern.1 {
        Term::Variable(_) => true,
        Term::Constant(code) => *code == triple.predicate,
    };
    
    // Check object match
    let object_match = match &pattern.2 {
        Term::Variable(_) => true,
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
        _ => ReportStrategy::OnWindowClose, // Default
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
            Ok(CSPARQLWindow::new(window_spec.width, slide, report, tick, String::default()))
        },
        WindowType::Tumbling => {
            // Tumbling window: slide = width
            Ok(CSPARQLWindow::new(window_spec.width, window_spec.width, report, tick, String::default()))
        },
        WindowType::Range => {
            // Range window: slide = 1 (continuous)
            Ok(CSPARQLWindow::new(window_spec.width, 1, report, tick, String::default()))
        }
    }
}

// Helper function to register rule predicates
fn register_rule_predicates(rule: &Rule, database: &mut SparqlDatabase) {
    for conclusion in &rule.conclusion {
        if let Term::Constant(code) = conclusion.1 {
            let expanded = database.dictionary.decode(code).unwrap_or_else(|| "");
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

