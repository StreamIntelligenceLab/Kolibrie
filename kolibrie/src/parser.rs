use crate::sparql_database::SparqlDatabase;
use crate::triple::Triple;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0, multispace1, space0, space1},
    combinator::{opt, recognize},
    multi::{many0, separated_list1},
    sequence::{delimited, preceded, tuple},
    IResult, Parser,
};
use rayon::str;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use shared::GPU_MODE_ENABLED;

// Define the Value enum to represent terms or UNDEF in VALUES clause
#[derive(Debug, Clone)]
pub enum Value {
    Term(String),
    Undef,
}

// Define the ValuesClause struct to hold variables and their corresponding values
#[derive(Debug, Clone)]
pub struct ValuesClause<'a> {
    pub variables: Vec<&'a str>,
    pub values: Vec<Vec<Value>>,
}

// Define the InsertClause struct to hold triple patterns for the INSERT clause
#[derive(Debug, Clone)]
pub struct InsertClause<'a> {
    pub triples: Vec<(&'a str, &'a str, &'a str)>,
}

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
    alt((prefixed_identifier, identifier))(input)
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

pub fn parse_filter(input: &str) -> IResult<&str, (&str, &str, &str)> {
    let (input, _) = tag("FILTER")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, var) = variable(input)?;
    let (input, _) = multispace0(input)?;
    let (input, operator) = alt((
        tag("="),
        tag("!="),
        tag(">"),
        tag(">="),
        tag("<"),
        tag("<="),
    ))(input)?;
    let (input, _) = multispace0(input)?;

    // Parse value as either a number or a double-quoted string
    let (input, value) = alt((
        // Parse as a string literal in double quotes
        parse_literal,
        // Parse as a numeric literal
        take_while1(|c: char| c.is_digit(10)),
    ))(input)?;
    let (input, _) = char(')')(input)?;

    Ok((input, (var, operator, value)))
}

// Parser for BIND clauses: BIND(funcName(?var, "literal") AS ?newVar)
pub fn parse_bind(input: &str) -> IResult<&str, (&str, Vec<&str>, &str)> {
    let (input, _) = tag("BIND")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, func_name) = identifier(input)?;
    let (input, _) = char('(')(input)?;

    let (input, args) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        alt((variable, parse_literal))
    )(input)?;

    let (input, _) = char(')')(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("AS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, new_var) = variable(input)?;
    let (input, _) = char(')')(input)?;

    Ok((input, (func_name, args, new_var)))
}

pub fn parse_where(
    input: &str,
) -> IResult<
    &str,
    (
        Vec<(&str, &str, &str)>,
        Vec<(&str, &str, &str)>,
        Option<ValuesClause>,
        Vec<(&str, Vec<&str>, &str)>,
    ),
> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("WHERE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('{')(input)?;
    let (input, _) = multispace0(input)?;

    // Modify this line to accept literals in the object position
    let (input, patterns) = separated_list1(
        tuple((space0, char('.'), multispace1)),
        tuple((
            alt((parse_uri, variable)), // Subject
            multispace1,
            predicate, // Predicate
            multispace1,
            alt((variable, parse_literal)), // Accept variable or literal in object position
        )),
    )(input)?;

    let (input, filters) = many0(preceded(multispace0, parse_filter))(input)?;

    let (input, binds) = many0(preceded(multispace0, parse_bind))(input)?;

    // Try to parse a VALUES clause
    let (input, values_clause) = opt(preceded(multispace0, parse_values))(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char('}')(input)?;

    // Convert patterns from a 5-tuple to a 3-tuple by extracting relevant parts
    let patterns = patterns
        .into_iter()
        .map(|(s, _, p, _, o)| (s, p, o))
        .collect();

    Ok((input, (patterns, filters, values_clause, binds)))
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

    let (input, triples) = separated_list1(
        tuple((space0, char('.'), space0)),
        tuple((
            alt((parse_uri, variable)),
            multispace1,
            predicate,
            multispace1,
            alt((parse_uri, variable, parse_literal)), // Accept either variable or literal
        )),
    )(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char('}')(input)?;

    // Convert triples from a 5-tuple to a 3-tuple by extracting relevant parts
    let triples = triples
        .into_iter()
        .map(|(s, _, p, _, o)| (s, p, o))
        .collect();

    Ok((input, InsertClause { triples }))
}

pub fn parse_sparql_query(
    input: &str,
) -> IResult<
    &str,
    (
        Option<InsertClause>,
        Vec<(&str, &str, Option<&str>)>, // variables
        Vec<(&str, &str, &str)>,         // patterns
        Vec<(&str, &str, &str)>,         // filters
        Vec<&str>,                       // group_vars
        HashMap<String, String>,         // prefixes
        Option<ValuesClause>,
        Vec<(&str, Vec<&str>, &str)>,    // BIND clauses
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
    let (input, (patterns, filters, values_clause, binds)) = parse_where(input)?;

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
        ),
    ))
}

pub fn execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>> {
    let mut final_results: Vec<BTreeMap<&str, String>>;
    let mut selected_variables: Vec<(String, String)> = Vec::new();
    let mut aggregation_vars: Vec<(&str, &str, &str)> = Vec::new();
    let group_by_variables: Vec<&str>;
    let prefixes;

    if let Ok((
        _,
        (
            insert_clause,
            mut variables,
            patterns,
            filters,
            group_vars,
            parsed_prefixes,
            values_clause,
            binds,
        ),
    )) = parse_sparql_query(sparql)
    {
        // Merge the parsed prefixes with the database prefixes
        //let mut all_prefixes = database.prefixes.clone();
        //all_prefixes.extend(parsed_prefixes);
        prefixes = parsed_prefixes;

        // Process the INSERT clause if present
        if let Some(insert_clause) = insert_clause {
            for (subject, predicate, object) in insert_clause.triples {
                let subject_id = database.dictionary.encode(subject);
                let predicate_id = database.dictionary.encode(predicate);
                let object_id = database.dictionary.encode(object);
                let triple = Triple {
                    subject: subject_id,
                    predicate: predicate_id,
                    object: object_id,
                };
                database.triples.insert(triple.clone());
            }
        }

        // If SELECT * is used, gather all variables from patterns
        if variables == vec![("*", "*", None)] {
            let mut all_vars = BTreeSet::new();
            for (subject_var, _, object_var) in &patterns {
                all_vars.insert(*subject_var);
                all_vars.insert(*object_var);
            }
            variables = all_vars.into_iter().map(|var| ("VAR", var, None)).collect();
        }

        for (agg_type, var, opt_output_var) in variables.iter().cloned() {
            if agg_type == "SUM" || agg_type == "MIN" || agg_type == "MAX" || agg_type == "AVG" {
                let output_var = if let Some(name) = opt_output_var {
                    name
                } else {
                    ""
                };
                aggregation_vars.push((agg_type, var, output_var));
                selected_variables.push(("VAR".to_string(), output_var.to_string()));
            } else {
                selected_variables.push((agg_type.to_string(), var.to_string()));
            }
        }

        group_by_variables = group_vars;

        // Convert BTreeSet to a vector of Triple
        let triples_vec: Vec<Triple> = database.triples.iter().cloned().collect();

        // Initialize final_results based on the VALUES clause
        if let Some(values_clause) = values_clause {
            // Initialize final_results based on values_clause
            final_results = Vec::new();
            for value_row in values_clause.values {
                let mut result = BTreeMap::new();
                for (var, value) in values_clause.variables.iter().zip(value_row.iter()) {
                    match value {
                        Value::Term(term) => {
                            result.insert(*var, term.clone());
                        }
                        Value::Undef => {
                            // Do nothing, variable is undefined in this row
                        }
                    }
                }
                final_results.push(result);
            }
        } else {
            // No VALUES clause, start with a single empty result
            final_results = vec![BTreeMap::new()];
        }

        // Process each pattern in the WHERE clause
        for (subject_var, predicate, object_var) in patterns {
            // Resolve the predicate using prefixes
            let resolved_predicate = database.resolve_query_term(predicate, &prefixes);

            // Resolve the object variable using prefixes
            let resolved_object_var = database.resolve_query_term(object_var, &prefixes);

            // Instead of checking double quotes, just use the object_var directly as the filter value
            let literal_filter = if !object_var.starts_with('?') {
                Some(resolved_object_var)
            } else {
                None
            };

            if GPU_MODE_ENABLED.load(Ordering::SeqCst) {
                println!("CUDA");
                final_results = database.perform_hash_join_cuda_wrapper(
                    subject_var,
                    resolved_predicate,
                    object_var,
                    triples_vec.clone(),
                    &database.dictionary,
                    final_results,
                    literal_filter,
                );
            } else {
                println!("NORM");
                final_results = database.perform_join_par_simd_with_strict_filter_1(
                    subject_var,
                    resolved_predicate,
                    object_var,
                    triples_vec.clone(),
                    &database.dictionary,
                    final_results,
                    literal_filter,
                );
            }
        }

        // Apply filters
        final_results = database.apply_filters_simd(final_results, filters);

        // Apply BIND (UDF) clauses
        // Each BIND is (func_name, args, new_var)
        for (func_name, args, new_var) in binds {
            if let Some(func) = database.udfs.get(func_name) {
                // Apply this function to each row
                for row in &mut final_results {
                    let resolved_args: Vec<&str> = args.iter().map(|arg| {
                        if arg.starts_with('?') {
                            row.get(arg).map(|s| s.as_str()).unwrap_or("")
                        } else {
                            // literal
                            arg
                        }
                    }).collect();

                    let result = func.call(resolved_args);
                    row.insert(new_var, result);
                }
            } else {
                eprintln!("UDF {} not found", func_name);
            }
        }

        // Apply GROUP BY and aggregations
        if !group_by_variables.is_empty() {
            final_results =
                group_and_aggregate_results(final_results, &group_by_variables, &aggregation_vars);
        }
    } else {
        eprintln!("Failed to parse the query.");
        return Vec::new();
    }

    // Convert the BTreeMap results into Vec<Vec<String>> format
    final_results
        .into_iter()
        .map(|result| {
            selected_variables
                .iter()
                .map(|(_, var)| result.get(var.as_str()).cloned().unwrap_or_default())
                .collect()
        })
        .collect()
}

pub fn group_and_aggregate_results<'a>(
    results: Vec<BTreeMap<&'a str, String>>,
    group_by_vars: &'a [&'a str],
    aggregation_vars: &'a [(&'a str, &'a str, &'a str)],
) -> Vec<BTreeMap<&'a str, String>> {
    let mut grouped: HashMap<
        Vec<String>,
        (BTreeMap<&'a str, String>, HashMap<&'a str, (f64, usize)>),
    > = HashMap::new();

    for result in results {
        // Create the key based on the group by variables
        let key: Vec<String> = group_by_vars
            .iter()
            .map(|var| result.get(*var).cloned().unwrap_or_default())
            .collect();

        // Extract values for aggregation variables
        let mut agg_values: HashMap<&'a str, f64> = HashMap::new();
        for (_, var, output_var_name) in aggregation_vars {
            if let Some(value_str) = result.get(*var) {
                if let Ok(value) = value_str.parse::<f64>() {
                    agg_values.insert(*output_var_name, value);
                }
            }
        }

        // Insert or update in grouped collection
        grouped
            .entry(key)
            .and_modify(|(_, agg_map)| {
                for (agg_type, _, output_var_name) in aggregation_vars {
                    let value = agg_values.get(*output_var_name).cloned().unwrap_or(0.0);
                    let entry = agg_map.entry(*output_var_name).or_insert((0.0, 0));
                    match *agg_type {
                        "SUM" => entry.0 += value,
                        "MIN" => entry.0 = entry.0.min(value),
                        "MAX" => entry.0 = entry.0.max(value),
                        "AVG" => {
                            entry.0 += value;
                            entry.1 += 1; // Track count for AVG
                        }
                        _ => {}
                    }
                }
            })
            .or_insert_with(|| {
                let mut agg_map = HashMap::new();
                for (_, _, output_var_name) in aggregation_vars {
                    let value = agg_values.get(*output_var_name).cloned().unwrap_or(0.0);
                    agg_map.insert(*output_var_name, (value, 1));
                }
                (result.clone(), agg_map)
            });
    }

    // Convert grouped data back to Vec<BTreeMap> with aggregation results
    grouped
        .into_iter()
        .map(|(_, (mut value, agg_map))| {
            for (output_var_name, (sum, count)) in agg_map {
                let result_value = if let Some((agg_type, _, _)) = aggregation_vars
                    .iter()
                    .find(|(_, _, var)| var == &output_var_name)
                {
                    match *agg_type {
                        "AVG" => sum / count as f64,
                        _ => sum,
                    }
                } else {
                    sum
                };
                value.insert(output_var_name, result_value.to_string());
            }
            value
        })
        .collect()
}