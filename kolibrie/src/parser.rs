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
    bytes::complete::{tag, take_while1, take_until},
    character::complete::{char, multispace0, multispace1, space0, space1},
    combinator::{opt, recognize},
    multi::{many0, separated_list1},
    sequence::{delimited, preceded, tuple, terminated},
    IResult, Parser,
};
use rayon::str;
use crate::sparql_database::SparqlDatabase;
use datalog::knowledge_graph::KnowledgeGraph;
use shared::triple::Triple;
use shared::dictionary::Dictionary;
use shared::rule::FilterCondition;
use shared::rule::Rule;
use shared::terms::*;
use shared::query::*;
use std::collections::HashMap;

// Helper function to recognize identifiers
pub fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

// Parser for a prefixed identifier like ex:worksAt
pub fn prefixed_identifier(input: &str) -> IResult<&str, &str> {
    recognize(tuple((identifier, char(':'), identifier)))(input)
}

// Parser for a predicate (either prefixed or unprefixed)
pub fn predicate(input: &str) -> IResult<&str, &str> {
    alt((
        recognize(tuple((char(':'), identifier))),
        prefixed_identifier,
        identifier,
    ))(input)
}

// Parser for variables (e.g., ?person)
pub fn variable(input: &str) -> IResult<&str, &str> {
    recognize(tuple((char('?'), identifier)))(input)
}

// Parser for a literal value within double quotes
pub fn parse_literal(input: &str) -> IResult<&str, &str> {
    delimited(char('"'), take_while1(|c| c != '"'), char('"'))(input)
}

// Parser for a URI within angle brackets
pub fn parse_uri(input: &str) -> IResult<&str, &str> {
    delimited(char('<'), take_while1(|c| c != '>'), char('>'))(input)
}

// Helper parser to parse a single predicate-object pair.
pub fn parse_predicate_object(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, p) = predicate(input)?;
    let (input, _) = multispace1(input)?;
    let (input, o) = alt((parse_uri, variable, parse_literal, identifier))(input)?;
    Ok((input, (p, o)))
}

pub fn parse_triple_block(input: &str) -> IResult<&str, Vec<(&str, &str, &str)>> {
    let (input, subject) = alt((parse_uri, variable))(input)?;
    let (input, _) = multispace1(input)?;

    // First predicate-object pair
    let (input, first_po) = parse_predicate_object(input)?;

    // Zero or more additional predicate-object pairs separated by semicolon
    let (input, rest_po) = many0(preceded(
        tuple((multispace0, char(';'), multispace0)),
        parse_predicate_object,
    ))(input)?;

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
    ))(input)
}

// Parser for the VALUES clause
pub fn parse_values(input: &str) -> IResult<&str, ValuesClause> {
    let (input, _) = tag("VALUES")(input)?;
    let (input, _) = space1(input)?;

    let (input, vars) = alt((
        // Single variable
        variable.map(|var| vec![var]),
        // Multiple variables in parentheses
        delimited(char('('), separated_list1(space1, variable), char(')')),
    ))(input)?;

    let (input, _) = space1(input)?;
    let (input, _) = char('{')(input)?;
    let (input, _) = multispace0(input)?;

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
    ))(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char('}')(input)?;

    Ok((
        input,
        ValuesClause {
            variables: vars,
            values,
        },
    ))
}

pub fn parse_aggregate(input: &str) -> IResult<&str, (&str, &str, Option<&str>)> {
    let (input, agg_type) = alt((tag("SUM"), tag("MIN"), tag("MAX"), tag("AVG")))(input)?;
    let (input, _) = char('(')(input)?;
    let (input, var) = variable(input)?;
    let (input, _) = char(')')(input)?;

    // Optional AS clause to name the aggregated result
    let (input, opt_as) = opt(preceded(
        space1,
        preceded(tag("AS"), preceded(space1, variable)),
    ))(input)?;

    Ok((input, (agg_type, var, opt_as)))
}

pub fn parse_select(input: &str) -> IResult<&str, Vec<(&str, &str, Option<&str>)>> {
    let (input, _) = tag("SELECT")(input)?;
    let (input, _) = space1(input)?;

    // Check if the next token is '*'
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<_>>("*")(input) {
        return Ok((input, vec![("*", "*", None)]));
    }

    // Parse variables or aggregation functions
    let (input, variables) = separated_list1(
        space1,
        alt((variable.map(|var| ("VAR", var, None)), parse_aggregate)),
    )(input)?;

    Ok((input, variables))
}

// Parse a basic arithmetic operand (variable, literal, or number)
fn parse_operand(input: &str) -> IResult<&str, ArithmeticExpression> {
    let (input, _) = multispace0(input)?;
    
    let (input, operand) = alt((
        variable,
        parse_literal,
        take_while1(|c: char| c.is_digit(10) || c == '.'),
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    
    Ok((input, ArithmeticExpression::Operand(operand)))
}

// Parse a parenthesized arithmetic expression
fn parse_arith_parenthesized(input: &str) -> IResult<&str, ArithmeticExpression> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, expr) = parse_arithmetic_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = multispace0(input)?;
    
    Ok((input, expr))
}

// Parse a basic arithmetic term (operand or parenthesized expression)
fn parse_arith_term(input: &str) -> IResult<&str, ArithmeticExpression> {
    alt((
        parse_operand,
        parse_arith_parenthesized,
    ))(input)
}

// Parse multiplication and division
fn parse_arith_factor(input: &str) -> IResult<&str, ArithmeticExpression> {
    let (mut input, mut left) = parse_arith_term(input)?;
    
    // Process all multiplication and division operations in sequence
    loop {
        let (remaining, _) = multispace0(input)?;
        
        // Match a multiplication or division operator
        match alt((char::<&str, nom::error::Error<&str>>('*'), char::<&str, nom::error::Error<&str>>('/')))(remaining) {
            Ok((after_op, op)) => {
                // Parse the right-hand term
                let (after_space, _) = multispace0(after_op)?;
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
pub fn parse_arithmetic_expression(input: &str) -> IResult<&str, ArithmeticExpression> {
    let (mut input, mut left) = parse_arith_factor(input)?;
    
    // Process all addition and subtraction operations in sequence
    loop {
        let (remaining, _) = multispace0(input)?;
        
        // Match an addition or subtraction operator
        match alt((char::<&str, nom::error::Error<&str>>('+'), char::<&str, nom::error::Error<&str>>('-')))(remaining) {
            Ok((after_op, op)) => {
                // Parse the right-hand factor
                let (after_space, _) = multispace0(after_op)?;
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

fn parse_arithmetic_comparison(input: &str) -> IResult<&str, FilterExpression> {
    let (input, _) = multispace0(input)?;

    // Parse left side expression
    let (input, left_str) = alt((
        // Recognize an arithmetic expression (variable followed by operators)
        recognize(tuple((
            alt((
                variable,                  // Variable name
                parse_literal,             // String literal
                take_while1(|c: char| c.is_digit(10) || c == '.'), // Number
            )),
            multispace0,
            alt((char('+'), char('-'), char('*'), char('/'))), // Operator
        ))),
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
    ))(input)?;

    let (input, _) = multispace0(input)?;
    
    // Parse the comparison operator
    let (input, operator) = alt((
        tag("="), tag("!="), tag(">="),
        tag("<="), tag(">"), tag("<"),
    ))(input)?;

    let (input, _) = multispace0(input)?;
    
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
        recognize(tuple((
            alt((
                variable,
                parse_literal,
                take_while1(|c: char| c.is_digit(10) || c == '.'),
            )),
            multispace0,
            alt((char('+'), char('-'), char('*'), char('/'))),
        ))),
    ))(input)?;

    let (input, _) = multispace0(input)?;

    let result = FilterExpression::Comparison(
        left_str,
        operator,
        right_str,
    );

    Ok((input, result))
}

// Parse a single comparison expression like ?var > 10
pub fn parse_comparison(input: &str) -> IResult<&str, FilterExpression> {
    let (input, _) = multispace0(input)?;

    // Parse variable or literal on left side
    let (input, left) = alt((
        variable,
        parse_literal,
        take_while1(|c: char| c.is_digit(10)),
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    
    // Parse operator
    let (input, operator) = alt((
        tag("="),
        tag("!="),
        tag(">="),
        tag("<="),
        tag(">"),
        tag("<"),
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    
    // Parse variable or literal on right side
    let (input, right) = alt((
        variable,
        parse_literal,
        take_while1(|c: char| c.is_digit(10)),
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    
    Ok((input, FilterExpression::Comparison(left, operator, right)))
}

// Parse an expression in parentheses
fn parse_parenthesized(input: &str) -> IResult<&str, FilterExpression> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, expr) = parse_filter_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = multispace0(input)?;
    
    Ok((input, expr))
}

// Parse a negation (NOT)
fn parse_not(input: &str) -> IResult<&str, FilterExpression> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('!')(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, expr) = parse_term(input)?;
    Ok((input, FilterExpression::Not(Box::new(expr))))
}

// Parse a basic term (comparison, parenthesized expression, or negation)
fn parse_term(input: &str) -> IResult<&str, FilterExpression> {
    alt((
        parse_comparison,
        parse_arithmetic_comparison,
        parse_parenthesized,
        parse_not,
    ))(input)
}

// Parse AND expressions
fn parse_and(input: &str) -> IResult<&str, FilterExpression> {
    let (input, left) = parse_term(input)?;
    let (input, _) = multispace0(input)?;
    
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<_>>("&&")(input) {
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_and(input)?;
        Ok((input, FilterExpression::And(Box::new(left), Box::new(right))))
    } else {
        Ok((input, left))
    }
}

// Parse OR expressions
fn parse_or(input: &str) -> IResult<&str, FilterExpression> {
    let (input, left) = parse_and(input)?;
    let (input, _) = multispace0(input)?;
    
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<_>>("||")(input) {
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_or(input)?;
        Ok((input, FilterExpression::Or(Box::new(left), Box::new(right))))
    } else {
        Ok((input, left))
    }
}

// Main entry point for parsing filter expressions
fn parse_filter_expression(input: &str) -> IResult<&str, FilterExpression> {
    parse_or(input)
}

// Parse a complete FILTER clause
pub fn parse_filter(input: &str) -> IResult<&str, FilterExpression> {
    let (input, _) = tag("FILTER")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, expr) = parse_filter_expression(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, expr))
}

// Parser for BIND clauses: BIND(funcName(?var, "literal") AS ?newVar)
pub fn parse_bind(input: &str) -> IResult<&str, (&str, Vec<&str>, &str)> {
    let (input, _) = tag("BIND")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, func_name) = identifier(input)?;
    let (input, _) = char('(')(input)?;

    // Allow multiple arguments for CONCAT
    let (input, args) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        alt((variable, parse_literal)),
    )(input)?;

    let (input, _) = char(')')(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("AS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, new_var) = variable(input)?;
    let (input, _) = char(')')(input)?;

    Ok((input, (func_name, args, new_var)))
}

pub fn parse_subquery<'a>(input: &'a str) -> IResult<&'a str, SubQuery<'a>> {
    let (input, _) = multispace0::<&str, nom::error::Error<&str>>(input)?;
    let (input, _) = char('{')(input)?;
    let (input, _) = multispace0::<&str, nom::error::Error<&str>>(input)?;

    // Parse SELECT clause
    let (input, variables) = parse_select(input)?;

    // Parse WHERE clause (recursive)
    let (input, (patterns, filters, values_clause, binds, _)) = parse_where(input)?;

    let (input, _) = multispace0::<&str, nom::error::Error<&str>>(input)?;
    let (input, _) = char('}')(input)?;

    Ok((
        input,
        SubQuery {
            variables,
            patterns,
            filters,
            binds,
            _values_clause: values_clause,
        },
    ))
}

pub fn parse_where(
    input: &str,
) -> IResult<
    &str,
    (
        Vec<(&str, &str, &str)>,
        Vec<FilterExpression>,
        Option<ValuesClause>,
        Vec<(&str, Vec<&str>, &str)>,
        Vec<SubQuery>,
    ),
> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("WHERE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('{')(input)?;
    let (input, _) = multispace0(input)?;

    let mut patterns = Vec::new();
    let mut filters = Vec::new();
    let mut binds = Vec::new();
    let mut subqueries = Vec::new();
    let mut values_clause = None;
    let mut current_input = input;

    // Parse components until we reach the closing brace
    loop {
        let (new_input, _) = multispace0(current_input)?;
        current_input = new_input;

        // Try to match a closing brace
        if let Ok((new_input, _)) = char::<_, nom::error::Error<_>>('}')(current_input) {
            current_input = new_input;
            break;
        }

        // Try to parse each possible component
        current_input = if let Ok((new_input, triple_block)) = parse_triple_block(current_input) {
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
        } else if let Ok((new_input, rule_call)) = parse_rule_call(current_input) {
            let arg_str = rule_call.arguments.join(",");
            patterns.push((
                rule_call.predicate,
                "RULECALL",
                Box::leak(arg_str.into_boxed_str()),
            ));
            new_input
        } else {
            return Err(nom::Err::Error(nom::error::Error::new(
                current_input,
                nom::error::ErrorKind::Alt,
            )));
        };

        // Consume any trailing dot
        if let Ok((new_input, _)) = tuple((
            space0::<&str, nom::error::Error<&str>>,
            char::<&str, nom::error::Error<&str>>('.'),
            space0::<&str, nom::error::Error<&str>>,
        ))(current_input)
        {
            current_input = new_input;
        }
    }

    Ok((
        current_input,
        (patterns, filters, values_clause, binds, subqueries),
    ))
}

pub fn parse_group_by(input: &str) -> IResult<&str, Vec<&str>> {
    let (input, _) = tag("GROUPBY")(input)?;
    let (input, _) = space1(input)?;

    // Parse the variables to group by
    let (input, group_vars) = separated_list1(space1, variable)(input)?;
    Ok((input, group_vars))
}

// Add a new parser for PREFIX declarations
pub fn parse_prefix(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("PREFIX")(input)?;
    let (input, _) = space1(input)?;
    let (input, prefix) = identifier(input)?;
    let (input, _) = char(':')(input)?;
    let (input, _) = space0(input)?;
    let (input, uri) = delimited(char('<'), take_while1(|c| c != '>'), char('>'))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, (prefix, uri)))
}

// Modified parse_insert to handle literals and debug output
pub fn parse_insert(input: &str) -> IResult<&str, InsertClause> {
    let (input, _) = tag("INSERT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('{')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse one or more triple blocks separated by dots.
    // Each triple block can contain multiple predicate-object pairs separated by semicolons.
    let (input, triple_blocks) =
        separated_list1(tuple((space0, char('.'), space0)), parse_triple_block)(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char('}')(input)?;

    // Flatten all the triple blocks into a single Vec
    let triples = triple_blocks.into_iter().flatten().collect();

    Ok((input, InsertClause { triples }))
}

pub fn parse_construct_clause(input: &str) -> IResult<&str, Vec<(&str, &str, &str)>> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("CONSTRUCT")(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse multiple conclusion triples in CONSTRUCT block
    let (input, conclusions) = delimited(
        char('{'),
        preceded(
            multispace0,
            terminated(parse_triple_block, opt(tuple((multispace0, char('.'))))),
        ),
        preceded(multispace0, char('}')),
    )(input)?;
    
    Ok((input, conclusions))
}

pub fn parse_sparql_query(
    input: &str,
) -> IResult<
    &str,
    (
        Option<InsertClause>,
        Vec<(&str, &str, Option<&str>)>, // variables
        Vec<(&str, &str, &str)>,         // patterns
        Vec<FilterExpression>,         // filters
        Vec<&str>,                       // group_vars
        HashMap<String, String>,         // prefixes
        Option<ValuesClause>,
        Vec<(&str, Vec<&str>, &str)>, // BIND clauses
        Vec<SubQuery>,
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
    let (input, insert_clause) = opt(parse_insert)(input)?;
    let (mut input, _) = multispace0(input)?;

    let mut variables = Vec::new();
    if insert_clause.is_none() {
        // Parse SELECT clause only if there is no INSERT clause
        let (new_input, vars) = parse_select(input)?;
        variables = vars;
        input = new_input;
        let (_input, _) = multispace1(input)?;
    }

    // Ensure any spaces are consumed before parsing WHERE clause
    let (input, _) = multispace0(input)?;

    // Parse WHERE clause
    let (input, (patterns, filters, values_clause, binds, subqueries)) = parse_where(input)?;

    // Optionally parse the GROUP BY clause
    let (input, group_vars) =
        if let Ok((input, group_vars)) = preceded(multispace0, parse_group_by)(input) {
            (input, group_vars)
        } else {
            (input, vec![])
        };

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
        ),
    ))
}

pub fn parse_standalone_rule<'a>(
    input: &'a str,
) -> IResult<&'a str, (CombinedRule<'a>, HashMap<String, String>)> {
    // Parse prefixes first
    let (input, prefix_list) = many0(|i| {
        let (i, _) = multispace0(i)?;
        let (i, _) = tag("PREFIX")(i)?;
        let (i, _) = space1(i)?;
        let (i, p) = identifier(i)?;
        let (i, _) = char(':')(i)?;
        let (i, _) = space0(i)?;
        let (i, uri) = delimited(char('<'), take_while1(|c| c != '>'), char('>'))(i)?;
        Ok((i, (p, uri)))
    })(input)?;
    
    let mut prefixes = HashMap::new();
    for (p, uri) in prefix_list {
        prefixes.insert(p.to_string(), uri.to_string());
    }
    
    let (input, _) = multispace0(input)?;
    
    // Parse the rule
    let (input, rule) = parse_rule(input)?;
    
    Ok((input, (rule, prefixes)))
}

pub fn parse_rule_call(input: &str) -> IResult<&str, RuleHead> {
    let (input, _) = multispace0(input)?;
    
    // Parse the academic syntax: RULE(:Predicate, ?var1, ?var2, ...)
    let (input, _) = tag("RULE")(input)?;
    let (input, _) = preceded(char('('), multispace0)(input)?;
    let (input, pred) = predicate(input)?;
    
    // Parse the first variable
    let (input, _) = tuple((multispace0, char(','), multispace0))(input)?;
    let (input, first_var) = variable(input)?;
    
    // Parse additional variables if they exist
    let (input, additional_vars) = many0(
        preceded(
            tuple((multispace0, char(','), multispace0)),
            variable
        )
    )(input)?;
    
    // Combine all variables
    let mut all_vars = vec![first_var];
    all_vars.extend(additional_vars);
    
    let (input, _) = preceded(multispace0, char(')'))(input)?;
    
    Ok((
        input,
        RuleHead {
            predicate: pred,
            arguments: all_vars,
        },
    ))
}

pub fn parse_rule_head(input: &str) -> IResult<&str, RuleHead> {
    let (input, pred) = predicate(input)?;
    let (input, args) = opt(delimited(
        char('('),
        separated_list1(tuple((multispace0, char(','), multispace0)), variable),
        char(')'),
    ))(input)?;
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

pub fn parse_ml_predict(input: &str) -> IResult<&str, MLPredictClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("ML.PREDICT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    // Parse MODEL clause with quoted name
    let (input, _) = tag("MODEL")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = char('"')(input)?;  // Expect opening quote
    let (input, model) = take_until("\"")(input)?;  // Take everything until closing quote
    let (input, _) = char('"')(input)?;  // Expect closing quote
    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;
    // Parse INPUT clause using the inclusive balanced parser
    let (input, _) = tag("INPUT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, input_query) = preceded(char('{'), parse_balanced)(input)?;

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
            let (_rest, (patterns, filters, _values, _binds, _subqueries)) = 
                parse_where(where_clause).unwrap_or_else(|_| (where_clause, (vec![], vec![], None, vec![], vec![])));
            
            where_patterns = patterns;
            filter_conditions = filters;
        }
    }

    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;
    // Parse OUTPUT clause
    let (input, _) = tag("OUTPUT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, output_var) = variable(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

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
pub fn parse_stream_type(input: &str) -> IResult<&str, StreamType> {
    let (input, _) = multispace0(input)?;
    let (input, stream_type) = alt((
        tag("RSTREAM").map(|_| StreamType::RStream),
        tag("ISTREAM").map(|_| StreamType::IStream),
        tag("DSTREAM").map(|_| StreamType::DStream),
        identifier.map(|s| StreamType::Custom(s)),
    ))(input)?;
    Ok((input, stream_type))
}

// Parser for window specification
pub fn parse_window_spec(input: &str) -> IResult<&str, WindowSpec> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('[')(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse window type and parameters
    let (input, window_type) = alt((
        tag("RANGE").map(|_| WindowType::Range),
        tag("TUMBLING").map(|_| WindowType::Tumbling),
        tag("SLIDING").map(|_| WindowType::Sliding),
    ))(input)?;
    
    let (input, _) = multispace1(input)?;
    let (input, width) = take_while1(|c: char| c.is_digit(10))(input)?;
    let width = width.parse::<usize>().unwrap_or(0);
    
    // Optional slide parameter for sliding windows
    let (input, slide) = opt(preceded(
        tuple((multispace1, tag("SLIDE"), multispace1)),
        take_while1(|c: char| c.is_digit(10))
    ))(input)?;
    let slide = slide.and_then(|s| s.parse::<usize>().ok());
    
    // Optional report strategy
    let (input, report_strategy) = opt(preceded(
        tuple((multispace1, tag("REPORT"), multispace1)),
        alt((
            tag("ON_WINDOW_CLOSE"),
            tag("ON_CONTENT_CHANGE"),
            tag("NON_EMPTY_CONTENT"),
            tag("PERIODIC"),
        ))
    ))(input)?;
    
    // Optional tick strategy
    let (input, tick) = opt(preceded(
        tuple((multispace1, tag("TICK"), multispace1)),
        alt((
            tag("TIME_DRIVEN"),
            tag("TUPLE_DRIVEN"),
            tag("BATCH_DRIVEN"),
        ))
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;
    
    Ok((input, WindowSpec {
        window_type,
        width,
        slide,
        report_strategy,
        tick,
    }))
}

// Parser for FROM NAMED WINDOW clause
pub fn parse_from_named_window(input: &str) -> IResult<&str, WindowClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("NAMED")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("WINDOW")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse window IRI in angle brackets
    let (input, window_iri) = delimited(
        char('<'),
        take_while1(|c| c != '>'),
        char('>')
    )(input)?;
    
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("ON")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse stream IRI in angle brackets
    let (input, stream_iri) = delimited(
        char('<'),
        take_while1(|c| c != '>'),
        char('>')
    )(input)?;
    
    let (input, _) = multispace1(input)?;
    
    // Parse window specification
    let (input, window_spec) = parse_window_spec(input)?;
    
    Ok((input, WindowClause {
        window_iri,
        stream_iri,
        window_spec,
    }))
}

/// Parse a complete rule:
///   RULE :OverheatingAlert(?room) :- WHERE { ... } => { ... } .
pub fn parse_rule(input: &str) -> IResult<&str, CombinedRule> {
    let (input, _) = tag("RULE")(input)?;
    let (input, _) = space1(input)?;
    let (input, head) = parse_rule_head(input)?;
    let (input, _) = multispace0(input)?;
    
    // Optional stream type
    let (input, stream_type) = opt(parse_stream_type)(input)?;
    let (input, _) = multispace0(input)?;
    
    // Optional FROM NAMED WINDOW clause
    let (input, window_clause) = opt(parse_from_named_window)(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, _) = tag(":-")(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse CONSTRUCT clause
    let (input, conclusions) = parse_construct_clause(input)?;
    
    let (input, _) = multispace0(input)?;
    
    // Parse WHERE clause
    let (input, body) = parse_where(input)?;
    
    // Optional dot at the end of rule
    let (input, _) = opt(preceded(multispace0, char('.')))(input)?;
    let (input, _) = multispace0(input)?;
    
    // Optionally parse ML.PREDICT block if it exists
    let (input, ml_predict) = opt(parse_ml_predict)(input)?;
    
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

/// The combined query parser parses SPARQL + LP
pub fn parse_combined_query(input: &str) -> IResult<&str, CombinedQuery> {
    let (input, prefix_list) = many0(|i| {
        let (i, _) = multispace0(i)?;
        let (i, _) = tag("PREFIX")(i)?;
        let (i, _) = space1(i)?;
        let (i, p) = identifier(i)?;
        let (i, _) = char(':')(i)?;
        let (i, _) = space0(i)?;
        let (i, uri) = delimited(char('<'), take_while1(|c| c != '>'), char('>'))(i)?;
        Ok((i, (p, uri)))
    })(input)?;
    
    let mut prefixes = HashMap::new();
    for (p, uri) in prefix_list {
        prefixes.insert(p.to_string(), uri.to_string());
    }
    
    let (input, _) = multispace0(input)?;
    
    // Parse the rule with ML.PREDICT if present
    let (input, rule_opt) = opt(parse_rule)(input)?;
    let (input, _) = multispace0(input)?;
    
    // Parse the SPARQL query part
    let (input, sparql_parse) = parse_sparql_query(input)?;
    
    Ok((
        input,
        CombinedQuery {
            prefixes,
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
    if let Some(window_clause) = &cr.window_clause {
        println!("Processing rule with windowing:");
        println!("  Window IRI: {}", window_clause.window_iri);
        println!("  Stream IRI: {}", window_clause.stream_iri);
        println!("  Window Type: {:?}", window_clause.window_spec.window_type);
        println!("  Width: {}", window_clause.window_spec.width);
        if let Some(slide) = window_clause.window_spec.slide {
            println!("  Slide: {}", slide);
        }
        if let Some(report) = window_clause.window_spec.report_strategy {
            println!("  Report Strategy: {}", report);
        }
        if let Some(tick) = window_clause.window_spec.tick {
            println!("  Tick: {}", tick);
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

    let mut kg = KnowledgeGraph::new();
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

        let dynamic_rule = convert_combined_rule(rule, &mut database.dictionary, &rule_prefixes);

        // Add the rule to the knowledge graph
        kg.add_rule(dynamic_rule.clone());

        // Register all predicates from the conclusions for rule resolution
        for conclusion in &dynamic_rule.conclusion {
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
