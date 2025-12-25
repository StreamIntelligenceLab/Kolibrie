/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::sparql_database::SparqlDatabase;
use crate::volcano_optimizer::*;
use crate::custom_error::format_parse_error;
use crate::parser::*;
use shared::query::*;
use shared::triple::Triple;
use shared::GPU_MODE_ENABLED;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub fn execute_subquery<'a>(
    subquery: &SubQuery<'a>,
    database: &SparqlDatabase,
    prefixes: &HashMap<String, String>,
    current_results: Vec<BTreeMap<&'a str, String>>,
) -> Vec<BTreeMap<&'a str, String>> {
    // Execute subquery patterns
    let mut results = current_results;

    for (subject_var, predicate, object_var) in &subquery.patterns {
        let triples_vec: Vec<Triple> = database.triples.iter().cloned().collect();

        // IMPORTANT: resolve the prefixed name to its full IRI
        let resolved_predicate = database.resolve_query_term(predicate, prefixes);

        // If object_var is not a variable, also resolve that if needed:
        let literal_filter = if !object_var.starts_with('?') {
            Some(database.resolve_query_term(object_var, prefixes))
        } else {
            None
        };

        results = database.perform_join_par_simd_with_strict_filter_1(
            subject_var,
            resolved_predicate,
            object_var,
            triples_vec,
            &database.dictionary,
            results,
            literal_filter,
        );
    }

    // Apply filters
    results = database.apply_filters_simd(results, subquery.filters.clone());

    // Process BIND clauses
    for (func_name, args, new_var) in subquery.binds.clone() {
        if func_name == "CONCAT" {
            // Process CONCAT function
            for row in &mut results {
                let concatenated = args
                    .iter()
                    .map(|arg| {
                        if arg.starts_with('?') {
                            row.get(arg).map(|s| s.as_str()).unwrap_or("")
                        } else {
                            arg // literal
                        }
                    })
                    .collect::<Vec<&str>>()
                    .join("");
                row.insert(new_var, concatenated);
            }
        } else if let Some(func) = database.udfs.get(func_name) {
            // Process other UDFs
            for row in &mut results {
                let resolved_args: Vec<&str> = args
                    .iter()
                    .map(|arg| {
                        if arg.starts_with('?') {
                            row.get(arg).map(|s| s.as_str()).unwrap_or("")
                        } else {
                            arg
                        }
                    })
                    .collect();
                let result = func.call(resolved_args);
                row.insert(new_var, result);
            }
        } else {
            eprintln!("UDF {} not found", func_name);
        }
    }

    // Return only the variables specified in the SELECT clause
    results
        .into_iter()
        .map(|mut row| {
            let mut new_row = BTreeMap::new();
            for (var_type, var_name, _) in &subquery.variables {
                if *var_type == "VAR" {
                    if let Some(value) = row.remove(var_name) {
                        new_row.insert(*var_name, value);
                    }
                }
            }
            new_row
        })
        .collect()
}

// Add this function to handle ORDER BY sorting
fn apply_order_by<'a>(
    mut results: Vec<BTreeMap<&'a str, String>>,
    order_conditions: Vec<OrderCondition<'a>>, // Take ownership
) -> Vec<BTreeMap<&'a str, String>> {
    if order_conditions.is_empty() {
        return results;
    }

    results.sort_by(|a, b| {
        for condition in &order_conditions {
            // Borrow from owned vector
            let var = condition.variable;

            let val_a = a.get(var).map(|s| s.as_str()).unwrap_or("");
            let val_b = b.get(var).map(|s| s.as_str()).unwrap_or("");

            let comparison = match (val_a.parse::<f64>(), val_b.parse::<f64>()) {
                (Ok(num_a), Ok(num_b)) => num_a
                    .partial_cmp(&num_b)
                    .unwrap_or(std::cmp::Ordering::Equal),
                _ => val_a.cmp(val_b),
            };

            let final_comparison = match condition.direction {
                SortDirection::Asc => comparison,
                SortDirection::Desc => comparison.reverse(),
            };

            if final_comparison != std::cmp::Ordering::Equal {
                return final_comparison;
            }
        }
        std::cmp::Ordering::Equal
    });

    results
}

pub fn execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>> {
    // Register prefixes from the query string first
    database.register_prefixes_from_query(sparql);

    let sparql = normalize_query(sparql);

    // Prepare variables to hold the query processing state.
    let mut final_results: Vec<BTreeMap<&str, String>>;
    let mut selected_variables: Vec<(String, String)> = Vec::new();
    let mut aggregation_vars: Vec<(&str, &str, &str)> = Vec::new();
    let group_by_variables: Vec<&str>;
    let mut prefixes;
    let limit_clause: Option<usize>;

    let parse_result = parse_sparql_query(sparql);

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
            subqueries,
            limit,
            _,
            order_conditions,
        ),
    )) = parse_result
    {
        prefixes = parsed_prefixes;
        limit_clause = limit;

        // Ensure prefixes from the database are also available
        database.share_prefixes_with(&mut prefixes);

        // Process the INSERT clause if present
        process_insert_clause(insert_clause, database);

        // If SELECT * is used, gather all variables from patterns
        if variables == vec![("*", "*", None)] {
            let mut all_vars = BTreeSet::new();
            for (subject_var, _, object_var) in &patterns {
                all_vars.insert(*subject_var);
                all_vars.insert(*object_var);
            }
            variables = all_vars.into_iter().map(|var| ("VAR", var, None)).collect();
        }

        // Process variables for aggregation
        process_variables(&mut selected_variables, &mut aggregation_vars, variables);

        group_by_variables = group_vars;

        // Convert BTreeSet to a vector of Triple
        let triples_vec: Vec<Triple> = database.triples.iter().cloned().collect();

        // Initialize final_results based on the VALUES clause
        final_results = initialize_results(&values_clause);

        let rule_predicates = database
            .rule_map
            .values()
            .cloned()
            .collect::<std::collections::HashSet<String>>();

        // Process each pattern in the WHERE clause
        for (subject_var, predicate, object_var) in patterns {
            if predicate == "RULECALL" {
                final_results = process_rule_call(subject_var, object_var, database, &prefixes);
                continue;
            }

            // Process direct rule conclusion reference: ?room ex:overheatingAlert true
            let resolved_predicate = if predicate.contains(':') {
                let parts: Vec<&str> = predicate.split(':').collect();
                if parts.len() == 2 && prefixes.contains_key(parts[0]) {
                    format!("{}{}", prefixes[parts[0]], parts[1])
                } else {
                    predicate.to_string()
                }
            } else {
                predicate.to_string()
            };

            // Check if this is a direct reference to a rule conclusion
            if rule_predicates.contains(&resolved_predicate) && object_var == "true" {
                // Extract the rule name from the predicate
                let rule_name = if let Some(idx) = resolved_predicate.rfind('#') {
                    &resolved_predicate[idx + 1..]
                } else if let Some(idx) = resolved_predicate.rfind('/') {
                    &resolved_predicate[idx + 1..]
                } else {
                    &resolved_predicate
                };

                // Process this as a rule call
                let var_str = subject_var.to_string();
                final_results = process_rule_call(rule_name, &var_str, database, &prefixes);
                continue;
            }

            // Handle non-RULECALL patterns
            let (join_subject, join_predicate, join_object) =
                resolve_triple_pattern(subject_var, predicate, object_var, database, &prefixes);

            // To satisfy lifetime requirements, leak the computed join_subject and join_object
            let join_subject_static: &'static str = Box::leak(join_subject.into_boxed_str());
            let join_object_static: &'static str = Box::leak(join_object.into_boxed_str());

            if GPU_MODE_ENABLED.load(std::sync::atomic::Ordering::SeqCst) {
                println!("CUDA");
                #[cfg(feature = "cuda")]
                {
                    final_results = database.perform_hash_join_cuda_wrapper(
                        join_subject_static,
                        join_predicate,
                        join_object_static,
                        triples_vec.clone(),
                        &database.dictionary,
                        final_results,
                        if !join_object_static.starts_with('?') {
                            Some(join_object_static.to_string())
                        } else {
                            None
                        },
                    );
                }
            } else {
                // println!("NORM");
                final_results = database.perform_join_par_simd_with_strict_filter_1(
                    join_subject_static,
                    join_predicate,
                    join_object_static,
                    triples_vec.clone(),
                    &database.dictionary,
                    final_results,
                    if !join_object_static.starts_with('?') {
                        Some(join_object_static.to_string())
                    } else {
                        None
                    },
                );
            }
        }

        // Apply filters
        final_results = database.apply_filters_simd(final_results, filters);

        // Process subqueries first
        for subquery in subqueries {
            let subquery_results =
                execute_subquery(&subquery, database, &prefixes, final_results.clone());
            final_results = merge_results(final_results, subquery_results);
        }

        // Apply BIND (UDF) clauses
        process_bind_clauses(&mut final_results, binds, database);

        // Apply GROUP BY and aggregations
        if !group_by_variables.is_empty() {
            final_results =
                group_and_aggregate_results(final_results, &group_by_variables, &aggregation_vars);
        }

        final_results = apply_order_by(final_results, order_conditions);

        if let Some(limit_value) = limit_clause {
            if limit_value > 0 {
                final_results.truncate(limit_value);
            }
        }
    } else {
        // Enhanced error reporting while keeping the same function signature
        if let Err(err) = parse_result {
            let error_message = format_parse_error(sparql, err);
            eprintln!("Failed to parse the query: {}", error_message);
        } else {
            eprintln!("Failed to parse the query with an unknown error.");
        }
        return Vec::new();
    }

    // Convert the final BTreeMap results into Vec<Vec<String>>
    format_results(final_results, &selected_variables)
}

pub fn execute_query_rayon_parallel2_volcano(
    sparql: &str,
    database: &mut SparqlDatabase,
) -> Vec<Vec<String>> {
    let sparql = normalize_query(sparql);

    let limit_clause: Option<usize>;
    // Register prefixes from the query string first
    database.register_prefixes_from_query(&sparql);

    let parse_result = parse_sparql_query(sparql);

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
            subqueries,
            limit,
            _,
            order_conditions,
        ),
    )) = parse_result
    {
        let mut prefixes = parsed_prefixes;
        database.share_prefixes_with(&mut prefixes);

        limit_clause = limit;

        // Process the INSERT clause if present using the existing helper function
        if let Some(insert_clause) = insert_clause {
            process_insert_clause(Some(insert_clause), database);
            database.get_or_build_stats();
            return Vec::new();
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

        // Process variables for aggregation using the existing helper function
        let mut selected_variables: Vec<(String, String)> = Vec::new();
        let mut aggregation_vars: Vec<(&str, &str, &str)> = Vec::new();
        process_variables(&mut selected_variables, &mut aggregation_vars, variables);

        let resolved_patterns: Vec<(&str, &str, &str)> = patterns
            .iter()
            .map(|(subject_var, predicate, object_var)| {
                let (resolved_subject, resolved_predicate, resolved_object) =
                    resolve_triple_pattern(subject_var, predicate, object_var, database, &prefixes);
                
                // Leak strings to get 'static lifetime
                let subject_static: &'static str = Box::leak(resolved_subject.into_boxed_str());
                let predicate_static: &'static str = Box::leak(resolved_predicate.into_boxed_str());
                let object_static: &'static str = Box::leak(resolved_object.into_boxed_str());
                
                (subject_static, predicate_static, object_static)
            })
            .collect();

        // Build indexes before optimization - this is crucial for performance
        // database.build_all_indexes();

        // Check if we should use GPU mode for execution
        if GPU_MODE_ENABLED.load(std::sync::atomic::Ordering::SeqCst) {
            println!("CUDA with Volcano Optimizer");

            // Use Volcano optimizer for plan generation
            let logical_plan = build_logical_plan(
                selected_variables
                    .iter()
                    .map(|(t, v)| (t.as_str(), v.as_str()))
                    .collect(),
                resolved_patterns,
                filters,
                &prefixes,
                database,
                binds.clone(),
                values_clause.as_ref(),
            );

            let stats = database.cached_stats.as_ref().expect("Error");
            let mut optimizer = VolcanoOptimizer::with_cached_stats(stats.clone());
            let _optimized_plan = optimizer.find_best_plan(&logical_plan);

            #[cfg(feature = "cuda")]
            {
                // For now, fall back to the original GPU implementation
                // until execute_gpu is properly implemented for the optimized plan

                // Initialize final_results based on the VALUES clause
                let mut final_results = initialize_results(&values_clause);

                // Convert BTreeSet to a vector of Triple
                let triples_vec: Vec<Triple> = database.triples.iter().cloned().collect();

                // Process each pattern in the WHERE clause
                for (subject_var, predicate, object_var) in patterns {
                    if predicate == "RULECALL" {
                        final_results =
                            process_rule_call(subject_var, object_var, database, &prefixes);
                        continue;
                    }

                    // Handle non-RULECALL patterns
                    let (join_subject, join_predicate, join_object) = resolve_triple_pattern(
                        subject_var,
                        predicate,
                        object_var,
                        database,
                        &prefixes,
                    );

                    // To satisfy lifetime requirements, leak the computed join_subject and join_object
                    let join_subject_static: &'static str =
                        Box::leak(join_subject.into_boxed_str());
                    let join_object_static: &'static str = Box::leak(join_object.into_boxed_str());

                    final_results = database.perform_hash_join_cuda_wrapper(
                        join_subject_static,
                        join_predicate,
                        join_object_static,
                        triples_vec.clone(),
                        &database.dictionary,
                        final_results,
                        if !join_object_static.starts_with('?') {
                            Some(join_object_static.to_string())
                        } else {
                            None
                        },
                    );
                }

                // Apply filters
                final_results = database.apply_filters_simd(final_results, filters);

                // Process subqueries
                for subquery in subqueries {
                    let subquery_results =
                        execute_subquery(&subquery, database, &prefixes, final_results.clone());
                    final_results = merge_results(final_results, subquery_results);
                }

                // Apply BIND (UDF) clauses
                process_bind_clauses(&mut final_results, binds, database);

                // Apply GROUP BY and aggregations
                if !group_vars.is_empty() {
                    final_results =
                        group_and_aggregate_results(final_results, &group_vars, &aggregation_vars);
                }

                final_results = apply_order_by(final_results, &order_conditions);

                // Apply LIMIT clause
                if let Some(limit_value) = limit_clause {
                    if limit_value > 0 {
                        final_results.truncate(limit_value);
                    }
                }

                return format_results(final_results, &selected_variables);
            }

            #[cfg(not(feature = "cuda"))]
            {
                eprintln!("CUDA feature not enabled");
                return Vec::new();
            }
        } else {
            /*let guard = pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                . unwrap();*/

            // Use Volcano optimizer for CPU execution
            let mut logical_plan = build_logical_plan(
                selected_variables
                    .iter()
                    .map(|(t, v)| (t.as_str(), v.as_str()))
                    .collect(),
                resolved_patterns,
                filters.clone(),
                &prefixes,
                database,
                binds.clone(),
                values_clause.as_ref(),
            ); 

            // Integrate subqueries into the logical plan
            for subquery in &subqueries {
                let subquery_plan = build_logical_plan_from_subquery(
                    subquery,
                    &prefixes,
                    database,
                );
            
                // Join the subquery with the main query
                logical_plan = LogicalOperator::join(logical_plan, subquery_plan);
            }

            let stats = database.cached_stats.as_ref().expect("AAA");
            let mut optimizer = VolcanoOptimizer::with_cached_stats(stats.clone());

            let optimized_plan = optimizer.find_best_plan(&logical_plan);
            let results = optimized_plan.execute(database);
            /*if let Ok(report) = guard.report().build() {
                let file = std::fs::File::create("volcano_optimizer_flamegraph.svg").unwrap();
                report.flamegraph(file).unwrap();
                println!("Volcano optimizer flamegraph saved to: volcano_optimizer_flamegraph. svg");
            }*/

            // Convert results to owned strings first to avoid lifetime issues
            let results_owned: Vec<HashMap<String, String>> = results.into_iter().collect();

            // Initialize with VALUES clause for consistency with GPU path
            let mut final_results = initialize_results(&values_clause);

            // Merge optimizer results with VALUES clause results
            let optimizer_results: Vec<BTreeMap<&str, String>> = results_owned
                .iter()
                .map(|result| {
                    result
                        .iter()
                        .map(|(k, v)| {
                        // Add '?' prefix back for consistency with format_results
                        let key_with_prefix = if k.starts_with('?') {
                            k.as_str()
                        } else {
                            Box::leak(format!("?{}", k).into_boxed_str())
                        };
                        (key_with_prefix, v.clone())
                    })
                    .collect()
                })
                .collect();

            // If we have optimizer results, use them; otherwise keep VALUES results
            if !optimizer_results.is_empty() {
                final_results = optimizer_results;
            }

            // Process subqueries if any
            for subquery in subqueries {
                let subquery_results =
                    execute_subquery(&subquery, database, &prefixes, final_results.clone());
                final_results = merge_results(final_results, subquery_results);
            }

            // Apply BIND (UDF) clauses
            // process_bind_clauses(&mut final_results, binds, database);

            // Apply GROUP BY and aggregations
            if !group_vars.is_empty() {
                final_results =
                    group_and_aggregate_results(final_results, &group_vars, &aggregation_vars);
            }

            final_results = apply_order_by(final_results, order_conditions);

            if let Some(limit_value) = limit_clause {
                if limit_value > 0 {
                    final_results.truncate(limit_value);
                }
            }

            return format_results(final_results, &selected_variables);
        }
    } else {
        eprintln!("Failed to parse the query.");
        return Vec::new();
    }
}

// Convert the final BTreeMap results into Vec<Vec<String>>
fn format_results(
    final_results: Vec<BTreeMap<&str, String>>,
    selected_variables: &[(String, String)],
) -> Vec<Vec<String>> {
    final_results
        .into_iter()
        .map(|result| {
            selected_variables
                .iter()
                .map(|(_, var)| {
                    let var_name = if var.starts_with('?') {
                        var
                    } else {
                        &format!("?{}", var)
                    };
                    result.get(var_name.as_str()).cloned().unwrap_or_default()
                })
                .collect()
        })
        .collect()
}

// Helper function to normalize the query by removing any RULE prefix
fn normalize_query(sparql: &str) -> &str {
    if sparql.contains("RULE") {
        if let Some(pos) = sparql.find("SELECT") {
            &sparql[pos..]
        } else {
            sparql
        }
    } else {
        sparql
    }
}

// Helper function to process INSERT clause
fn process_insert_clause(insert_clause: Option<InsertClause>, database: &mut SparqlDatabase) {
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
            database.triples.insert(triple);
        }
    }
}

// Helper function to process variables for aggregation
fn process_variables<'a>(
    selected_variables: &mut Vec<(String, String)>,
    aggregation_vars: &mut Vec<(&'a str, &'a str, &'a str)>,
    variables: Vec<(&'a str, &'a str, Option<&'a str>)>,
) {
    for (agg_type, var, opt_output_var) in variables {
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
}

// Helper function to initialize results based on VALUES clause
fn initialize_results(values_clause: &Option<ValuesClause>) -> Vec<BTreeMap<&'static str, String>> {
    if let Some(values_clause) = values_clause {
        let mut final_results = Vec::new();
        for value_row in &values_clause.values {
            let mut result = BTreeMap::new();
            for (var, value) in values_clause.variables.iter().zip(value_row.iter()) {
                match value {
                    Value::Term(term) => {
                        // Create a static str to avoid lifetime issues - using 'static instead of mut
                        let var_static: &'static str = Box::leak(var.to_string().into_boxed_str());
                        result.insert(var_static, term.clone());
                    }
                    Value::Undef => {}
                }
            }
            final_results.push(result);
        }
        final_results
    } else {
        // No VALUES clause, start with a single empty result
        vec![BTreeMap::new()]
    }
}

// Helper function to process rule calls
fn process_rule_call<'a>(
    subject_var: &'a str,
    object_var: &'a str,
    database: &'a SparqlDatabase,
    prefixes: &'a HashMap<String, String>,
) -> Vec<BTreeMap<&'static str, String>> {
    // Changed return type to use 'static
    let rule_name = if subject_var.starts_with(':') {
        &subject_var[1..]
    } else {
        subject_var
    };
    let rule_key = rule_name.to_lowercase();

    let expanded_rule_predicate = database
        .rule_map
        .get(&rule_key)
        .cloned()
        .unwrap_or_else(|| {
            prefixes
                .get("ex")
                .map(|prefix| format!("{}{}", prefix, rule_name))
                .unwrap_or_else(|| rule_name.to_string())
        });

    // Parse variables from the rule call
    let vars: Vec<&'static str> = if object_var.contains(',') {
        // Multiple variables separated by commas
        object_var
            .split(',')
            .map(|s| {
                let leaked: &'static mut str = Box::leak(s.trim().to_string().into_boxed_str());
                leaked as &'static str
            })
            .collect()
    } else {
        // Single variable
        vec![Box::leak(object_var.trim().to_string().into_boxed_str())]
    };

    // Find all subjects that match the rule predicate
    let mut matched_subjects = Vec::new();
    for triple in database.triples.iter() {
        if let (Some(subj), Some(pred), Some(obj)) = (
            database.dictionary.decode(triple.subject),
            database.dictionary.decode(triple.predicate),
            database.dictionary.decode(triple.object),
        ) {
            if pred == expanded_rule_predicate && obj == "true" {
                // Convert to 'static string to avoid lifetime issues
                let static_subj: &'static str = Box::leak(subj.to_string().into_boxed_str());
                matched_subjects.push(static_subj);
            }
        }
    }

    // Process results for rule-based query
    let mut final_results = Vec::new();
    for subject in matched_subjects {
        let result = process_rule_subject(subject, &vars, database);
        if let Some(result) = result {
            final_results.push(result);
        }
    }

    final_results
}

// Helper function to process a single subject for a rule
fn process_rule_subject(
    subject: &'static str,
    vars: &[&'static str],
    database: &SparqlDatabase,
) -> Option<BTreeMap<&'static str, String>> {
    let mut result = BTreeMap::new();
    let mut found_all_vars = true;

    // Add the first variable (subject/room variable)
    if !vars.is_empty() {
        result.insert(vars[0], subject.to_string());
    }

    // For the second variable (usually value/temperature), find the highest value
    if vars.len() > 1 {
        let mut highest_value: Option<(i64, String)> = None;

        // First, find all sensors/readings for this room
        for triple in database.triples.iter() {
            if let (Some(rel_subj), Some(rel_pred), Some(rel_obj)) = (
                database.dictionary.decode(triple.subject),
                database.dictionary.decode(triple.predicate),
                database.dictionary.decode(triple.object),
            ) {
                // Find sensors that relate to our room
                if rel_pred.ends_with("room") && rel_obj == subject {
                    let sensor_id = rel_subj;
                    highest_value =
                        find_highest_sensor_value(sensor_id, vars[1], database, highest_value);
                }
            }
        }

        // Add the highest value if found
        if let Some((_, value)) = highest_value {
            result.insert(vars[1], value);
        } else {
            found_all_vars = false;
        }
    }

    // Only return the result if we found all variables
    if found_all_vars && result.len() == vars.len() {
        Some(result)
    } else {
        None
    }
}

// Helper function to find the highest sensor value
fn find_highest_sensor_value(
    sensor_id: &str,
    var_name: &str,
    database: &SparqlDatabase,
    mut highest_value: Option<(i64, String)>,
) -> Option<(i64, String)> {
    let var_name = if var_name.starts_with('?') {
        &var_name[1..]
    } else {
        var_name
    };

    for value_triple in database.triples.iter() {
        if let (Some(val_subj), Some(val_pred), Some(val_obj)) = (
            database.dictionary.decode(value_triple.subject),
            database.dictionary.decode(value_triple.predicate),
            database.dictionary.decode(value_triple.object),
        ) {
            if val_subj == sensor_id && val_pred.contains(var_name) {
                if let Ok(num_val) = val_obj.parse::<i64>() {
                    match highest_value {
                        None => highest_value = Some((num_val, val_obj.to_string())),
                        Some((current, _)) if num_val > current => {
                            highest_value = Some((num_val, val_obj.to_string()))
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    highest_value
}

// Helper function to resolve triple pattern terms
fn resolve_triple_pattern(
    subject_var: &str,
    predicate: &str,
    object_var: &str,
    database: &SparqlDatabase,
    prefixes: &HashMap<String, String>,
) -> (String, String, String) {
    if predicate == "RULECALL" {
        let rule_name = if subject_var.starts_with(':') {
            &subject_var[1..]
        } else {
            subject_var
        };
        let rule_key = rule_name.to_lowercase();

        // Look up the expanded predicate in the rule_map
        let expanded_rule_predicate = if let Some(expanded) = database.rule_map.get(&rule_key) {
            expanded.clone()
        } else {
            // Fallback: if the rule name is not already prefixed, prepend default prefix "ex"
            if let Some(default_uri) = prefixes.get("ex") {
                format!("{}{}", default_uri, rule_name)
            } else {
                rule_name.to_string()
            }
        };

        (
            object_var.to_string(),
            expanded_rule_predicate,
            "true".to_string(),
        )
    } else {
        // Resolve subject if it's not a variable
        let resolved_subject = if subject_var.starts_with('?') {
            subject_var.to_string()
        } else {
            database.resolve_query_term(subject_var, prefixes)
        };

        // For a normal triple pattern, resolve predicate and object
        let resolved_predicate = database.resolve_query_term(predicate, prefixes);

        // Resolve object if it's not a variable
        let resolved_object = if object_var.starts_with('?') {
            object_var.to_string()
        } else {
            database.resolve_query_term(object_var, prefixes)
        };

        (resolved_subject, resolved_predicate, resolved_object)
    }
}

// Helper function to process BIND clauses
fn process_bind_clauses<'a>(
    final_results: &mut Vec<BTreeMap<&'a str, String>>,
    binds: Vec<(&'a str, Vec<&'a str>, &'a str)>,
    database: &'a SparqlDatabase,
) {
    for (func_name, args, new_var) in binds {
        if func_name == "CONCAT" {
            // Process CONCAT function
            for row in final_results.iter_mut() {
                let concatenated = args
                    .iter()
                    .map(|arg| {
                        if arg.starts_with('?') {
                            row.get(*arg).map(|s| s.as_str()).unwrap_or("")
                        } else {
                            *arg // literal
                        }
                    })
                    .collect::<Vec<&str>>()
                    .join("");
                row.insert(new_var, concatenated);
            }
        } else if let Some(func) = database.udfs.get(func_name) {
            for row in final_results.iter_mut() {
                let resolved_args: Vec<&str> = args
                    .iter()
                    .map(|arg| {
                        if arg.starts_with('?') {
                            row.get(*arg).map(|s| s.as_str()).unwrap_or("")
                        } else {
                            *arg
                        }
                    })
                    .collect();
                let result = func.call(resolved_args);
                row.insert(new_var, result);
            }
        } else {
            eprintln!("UDF {} not found", func_name);
        }
    }
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

fn merge_results<'a>(
    main_results: Vec<BTreeMap<&'a str, String>>,
    subquery_results: Vec<BTreeMap<&'a str, String>>,
) -> Vec<BTreeMap<&'a str, String>> {
    if main_results.is_empty() {
        return subquery_results;
    }
    if subquery_results.is_empty() {
        return main_results;
    }

    let mut merged = Vec::new();
    for main_row in main_results {
        for sub_row in &subquery_results {
            let mut new_row = main_row.clone();
            new_row.extend(sub_row.iter().map(|(k, v)| (*k, v.clone())));
            merged.push(new_row);
        }
    }
    merged
}
