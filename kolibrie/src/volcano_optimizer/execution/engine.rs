/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::super::operators::PhysicalOperator;

use crate::sparql_database::SparqlDatabase;
use rayon::prelude::*;

use shared::terms::{Term, TriplePattern};

use std::collections::{HashMap, HashSet};

/// Execution engine for physical operators
pub struct ExecutionEngine;

impl ExecutionEngine {
    /// Executes a physical operator and returns string results
    pub fn execute(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<HashMap<String, String>> {
        let id_results = Self::execute_with_ids(operator, database);

        // Convert ID results to string results only at the final step
        id_results
        .into_par_iter()
        .map(|id_result| {
            id_result
            .into_iter()
            .map(|(var, id)| (var, database.dictionary.decode(id).unwrap().to_string()))
            .collect()
        })
        .collect()
    }

    /// Executes a physical operator and returns ID-based results for performance
    pub fn execute_with_ids(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<HashMap<String, u32>> {
        match operator {
            PhysicalOperator::TableScan { pattern } => {
                Self::execute_table_scan_with_ids(database, pattern)
            }
            PhysicalOperator::IndexScan { pattern } => {
                Self::execute_index_scan_with_ids(database, pattern)
            }
            PhysicalOperator::Filter { input, condition } => {
                let input_results = Self::execute_with_ids(input, database);
                // Use parallel filtering
                input_results
                .into_par_iter()
                .filter(|result| condition.evaluate_with_ids(result, &database.dictionary))
                .collect()
            }
            PhysicalOperator::Projection { input, variables } => {
                let input_results = Self::execute_with_ids(input, database);
                
                // Strip '?' prefix from projection variables for matching
                let stripped_vars: Vec<String> = variables
                    .iter()
                    .map(|v| v.strip_prefix('?').unwrap_or(v).to_string())
                    .collect();

                let projected: Vec<HashMap<String, u32>> = input_results
                    .into_par_iter()
                    .map(|mut result| {
                        result.retain(|k, _| stripped_vars.contains(&k.to_string()));
                        result
                    })
                    .collect();
                projected
            }
            PhysicalOperator::OptimizedHashJoin { left, right } => {
                let left_results = Self::execute_with_ids(left, database);
                let right_results = Self::execute_with_ids(right, database);
                Self::execute_optimized_hash_join_with_ids(left_results, right_results)
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_results = Self::execute_with_ids(left, database);
                let right_results = Self::execute_with_ids(right, database);
                Self::execute_hash_join_with_ids(left_results, right_results)
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_results = Self::execute_with_ids(left, database);
                let right_results = Self::execute_with_ids(right, database);
                Self::execute_nested_loop_join_with_ids(left_results, right_results)
            }
            PhysicalOperator::ParallelJoin { left, right } => {
                Self::execute_parallel_join_with_ids(left, right, database)
            }
            PhysicalOperator::StarJoin { join_var, patterns } => {
                Self::execute_star_join_with_ids(database, join_var, patterns)
            }
            PhysicalOperator:: Subquery { inner, projected_vars } => {
                // Execute the inner query with IDs
                let inner_results = Self::execute_with_ids(inner, database);
                
                // Project only the requested variables
                inner_results
                    .into_iter()
                    .map(|mut row| {
                        row.retain(|k, _| projected_vars.contains(&k.to_string()));
                        row
                    })
                    .collect()
            }
            PhysicalOperator::Bind { input, function_name, arguments, output_variable } => {
                // Execute the input operator first
                let mut input_results = Self::execute_with_ids(input, database);
                let output_var = output_variable.strip_prefix('?').unwrap_or(output_variable);
    
                // Process BIND clause
                if function_name == "CONCAT" {
                    // Handle CONCAT function
                    for row in &mut input_results {
                        let concatenated = arguments
                            .iter()
                            .map(|arg| {
                            let arg_stripped = arg.strip_prefix('?').unwrap_or(arg);
                            if arg.starts_with('?') { 
                                if let Some(&id) = row.get(arg_stripped) {
                                    database.dictionary.decode(id).unwrap_or("").to_string()
                                } else {
                                    String::new()
                                }
                            } else {
                                arg.trim_matches('"').to_string()
                            }
                        })
                        .collect::<Vec<String>>()
                        .join("");
            
                        // Encode the concatenated result and store it
                        let result_id = database.dictionary.encode(&concatenated);
                        row.insert(output_var.to_string(), result_id);
                    }
                    input_results
                } else if let Some(func) = database.udfs.get(function_name.as_str()) {
                    // Handle user-defined functions
                    for row in &mut input_results {
                        let resolved_args: Vec<&str> = arguments
                            .iter()
                            .map(|arg| {
                                let arg_stripped = arg.strip_prefix('?').unwrap_or(arg);
                                if arg.starts_with('?') {
                                    if let Some(&id) = row.get(arg_stripped) {
                                        // Need to leak this to get 'static lifetime for UDF call
                                        Box::leak(
                                            database.dictionary.decode(id).unwrap_or("").to_string().into_boxed_str()
                                        )
                                    } else {
                                        ""
                                    }
                                } else {
                                    Box::leak(arg.trim_matches('"').to_string().into_boxed_str())
                                }
                            })
                            .collect();
            
                        let result = func.call(resolved_args);
                        let result_id = database.dictionary.encode(&result);
                        row.insert(output_var.to_string(), result_id);
                    }
                    input_results
                } else {
                    eprintln!("Function {} not found", function_name);
                    input_results
                }
            }
            PhysicalOperator::Values { variables, values } => {
                let stripped_vars: Vec<String> = variables
                    .iter()
                    .map(|v| v.strip_prefix('?').unwrap_or(v).to_string())
                    .collect();

                // Convert VALUES data to result rows
                let mut results = Vec::new();
    
                for value_row in values {
                    let mut row = HashMap::new();
        
                    for (i, var) in stripped_vars.iter().enumerate() {
                        if let Some(Some(value)) = value_row.get(i) {
                            // Encode the value in the dictionary
                            let value_id = database.dictionary.encode(value);
                            row.insert(var.clone(), value_id);
                        }
                    }
        
                    // Only add non-empty rows
                    if !row.is_empty() {
                        results.push(row);
                    }
                }
    
                results
            }
        }
    }

    /// Executes a table scan with ID-based results
    fn execute_table_scan_with_ids(
        database: &SparqlDatabase,
        pattern: &TriplePattern,
    ) -> Vec<HashMap<String, u32>> {
        let mut results = Vec::new();

        // Iterate through all triples in the database
        for triple in &database.triples {
            let mut bindings = HashMap::new();
            let mut matches = true;

            // Check subject
            match &pattern.0 {
                Term::Variable(var) => {
                    bindings.insert(var.clone(), triple.subject);
                }
                Term::Constant(constant) => {
                    if triple.subject != *constant {
                        matches = false;
                    }
                }
            }

            if !matches {
                continue;
            }

            // Check predicate
            match &pattern.1 {
                Term::Variable(var) => {
                    bindings.insert(var.clone(), triple.predicate);
                }
                Term::Constant(constant) => {
                    if triple.predicate != *constant {
                        matches = false;
                    }
                }
            }

            if !matches {
                continue;
            }

            // Check object
            match &pattern.2 {
                Term::Variable(var) => {
                    bindings.insert(var.clone(), triple.object);
                }
                Term::Constant(constant) => {
                    if triple.object != *constant {
                        matches = false;
                    }
                }
            }

            if matches {
                results.push(bindings);
            }
        }

        results
    }

    /// Executes a star join: multiple patterns sharing the same subject
    fn execute_star_join_with_ids(
        database: &SparqlDatabase,
        join_var: &str,
        patterns: &[TriplePattern],
    ) -> Vec<HashMap<String, u32>> {
        if patterns.is_empty() {
            return Vec::new();
        }
        
        let join_var_stripped = join_var.strip_prefix('?').unwrap_or(join_var);

        // Find the most selective pattern
        let mut pattern_estimates: Vec<(usize, u64)> = patterns
        .iter()
        .enumerate()
        .map(|(idx, pattern)| {
            let cardinality = Self::estimate_pattern_cardinality(database, pattern);
            (idx, cardinality)
        })
        .collect();

        pattern_estimates.sort_by_key(|(_, card)| *card);

        // Execute the MOST SELECTIVE pattern first
        let (most_selective_idx, first_card) = pattern_estimates[0];
        let most_selective_pattern = &patterns[most_selective_idx];

        let mut results = Self::execute_index_scan_with_ids(database, most_selective_pattern);

        if results.is_empty() {
            return Vec::new();
        }

        // Adaptive strategy: use sequential for large result sets
        let use_sequential = results.len() > 10_000 || first_card > 50_000;

        // Process remaining patterns
        for (pattern_idx, _) in &pattern_estimates[1..] {
            let pattern = &patterns[*pattern_idx];

            if use_sequential {
                // Process one-by-one with strict memory control
                let mut new_results = Vec::new();

                for binding in results.iter().take(100_000) {  // Hard limit on input size
                    if let Some(&join_value) = binding.get(join_var_stripped) {
                        let mut bound_bindings = HashMap::new();
                        bound_bindings.insert(join_var_stripped.to_string(), join_value);

                        let bound_pattern = Self::bind_pattern(pattern, &bound_bindings);
                        let matches = Self::execute_index_scan_with_ids(database, &bound_pattern);

                        for match_binding in matches {
                            let mut merged = binding.clone();
                            for (var, val) in match_binding {
                                merged.entry(var).or_insert(val);
                            }
                            new_results.push(merged);

                            // Hard stop if we exceed 500K results
                            if new_results.len() >= 500_000 {
                                results = new_results;
                                return results;  // Early exit
                            }
                        }
                    }
                }

                results = new_results;
            } else {
                // Fast path for small result sets
                results = results
                .into_par_iter()
                .flat_map(|binding| {
                    if let Some(&join_value) = binding.get(join_var_stripped) {
                        let mut bound_bindings = HashMap::new();
                        bound_bindings.insert(join_var_stripped.to_string(), join_value);

                        let bound_pattern = Self::bind_pattern(pattern, &bound_bindings);
                        let matches = Self::execute_index_scan_with_ids(database, &bound_pattern);

                        matches
                        .into_iter()
                        .map(|match_binding| {
                            let mut merged = binding.clone();
                            for (var, val) in match_binding {
                                merged.entry(var).or_insert(val);
                            }
                            merged
                        })
                        .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    }
                })
                .collect();
            }

            if results.is_empty() {
                return Vec::new();
            }
        }

        results
    }

    // Helper function to estimate pattern cardinality
    fn estimate_pattern_cardinality(database: &SparqlDatabase, pattern: &TriplePattern) -> u64 {
        let bound_count = [&pattern.0, &pattern.1, &pattern.2]
            .iter()
            .filter(|term| matches!(term, Term::Constant(_)))
            .count();

        match bound_count {
            3 => 1,
            2 => 100,      // Estimate for two-bound patterns
            1 => 10000,    // Estimate for one-bound patterns
            0 => 1000000,  // Estimate for fully unbound
            _ => 1000000,
        }
    }

    /// Executes an optimized hash join with ID-based results
    fn execute_optimized_hash_join_with_ids(
        left_results: Vec<HashMap<String, u32>>,
        right_results: Vec<HashMap<String, u32>>,
    ) -> Vec<HashMap<String, u32>> {
        if left_results.is_empty() || right_results.is_empty() {
            return Vec::new();
        }

        // Find common variables for join condition
        let left_vars: HashSet<String> = left_results[0].keys().cloned().collect();
        let right_vars: HashSet<String> = right_results[0].keys().cloned().collect();
        let common_vars: Vec<String> = left_vars.intersection(&right_vars).cloned().collect();

        if common_vars.is_empty() {
            // Cartesian product if no common variables
            return Self::cartesian_product_join(left_results, right_results);
        }

        // Build hash table from smaller relation
        let (build_side, probe_side) = if left_results.len() <= right_results.len() {
            (left_results, right_results)
        } else {
            (right_results, left_results)
        };

        let mut hash_table: HashMap<Vec<u32>, Vec<HashMap<String, u32>>> = HashMap::with_capacity(build_side.len());

        // Build phase
        for tuple in build_side {
            let key: Vec<u32> = common_vars.iter().map(|var| tuple[var]).collect();
            hash_table.entry(key).or_default().push(tuple);
        }

        // Probe phase
        probe_side
        .par_iter()
        .flat_map(|probe_tuple| {
            let key: Vec<u32> = common_vars.iter().map(|var| probe_tuple[var]).collect();

            if let Some(matching_tuples) = hash_table.get(&key) {
                matching_tuples
                .iter()
                .map(|build_tuple| {
                    let mut result = (*build_tuple).clone();
                    result.extend(probe_tuple.iter().map(|(k, v)| (k.clone(), *v)));
                    result
                })
                .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        })
        .collect()
    }

    /// Executes a regular hash join with ID-based results
    fn execute_hash_join_with_ids(
        left_results: Vec<HashMap<String, u32>>,
        right_results: Vec<HashMap<String, u32>>,
    ) -> Vec<HashMap<String, u32>> {
        if left_results.is_empty() || right_results.is_empty() {
            return Vec::new();
        }

        // Find common variables
        let left_vars: HashSet<String> = left_results[0].keys().cloned().collect();
        let right_vars: HashSet<String> = right_results[0].keys().cloned().collect();
        let common_vars: Vec<String> = left_vars.intersection(&right_vars).cloned().collect();

        if common_vars.is_empty() {
            return Self::cartesian_product_join(left_results, right_results);
        }

        // Simple hash join implementation
        let mut results = Vec::new();
        let mut hash_table: HashMap<Vec<u32>, Vec<HashMap<String, u32>>> = HashMap::new();

        // Build hash table from left results
        for left_tuple in left_results {
            let key: Vec<u32> = common_vars.iter().map(|var| left_tuple[var]).collect();
            hash_table.entry(key).or_default().push(left_tuple);
        }

        // Probe with right results
        for right_tuple in right_results {
            let key: Vec<u32> = common_vars.iter().map(|var| right_tuple[var]).collect();

            if let Some(matching_left_tuples) = hash_table.get(&key) {
                for left_tuple in matching_left_tuples {
                    let mut joined_tuple = left_tuple.clone();
                    for (var, value) in &right_tuple {
                        if !joined_tuple.contains_key(var) {
                            joined_tuple.insert(var.clone(), *value);
                        }
                    }
                    results.push(joined_tuple);
                }
            }
        }

        results
    }

    /// Executes a nested loop join with ID-based results
    fn execute_nested_loop_join_with_ids(
        left_results: Vec<HashMap<String, u32>>,
        right_results: Vec<HashMap<String, u32>>,
    ) -> Vec<HashMap<String, u32>> {
        left_results
        .into_iter()
        .flat_map(|left_tuple| {
            right_results
            .iter()
            .filter_map(|right_tuple| {
                Self::can_join_with_ids(&left_tuple, right_tuple).then(|| {
                    let mut joined_tuple = left_tuple.clone();
                    for (var, value) in right_tuple {
                        if !joined_tuple.contains_key(var) {
                            joined_tuple.insert(var.clone(), *value);
                        }
                    }
                    joined_tuple
                })
            })
            .collect::<Vec<_>>()
        })
        .collect()
    }

    /// Executes a bind join - uses left results to directly probe right index
    fn execute_bind_join_with_ids(
        left_results: Vec<HashMap<String, u32>>,
        right_pattern: &TriplePattern,
        database: &SparqlDatabase,
    ) -> Vec<HashMap<String, u32>> {
        // Safety limits
        let total_results = std::sync::atomic::AtomicUsize::new(0);
        let max_total = 1_000_000;

        let chunk_size = (left_results.len() / rayon::current_num_threads()).max(1).max(100);

        left_results
        .par_chunks(chunk_size)
        .flat_map(|chunk| {
            chunk.iter().flat_map(|left_tuple| {
                // Check global limit
                if total_results.load(std::sync::atomic::Ordering::Relaxed) >= max_total {
                    return Vec::new();
                }

                let bound_pattern = Self::bind_pattern(right_pattern, left_tuple);
                let matches = Self::execute_index_scan_with_ids(database, &bound_pattern);

                // Limit matches per binding
                let match_limit = matches.len().min(10_000);

                matches.into_iter()
                .take(match_limit)  // Apply limit
                .map(|right_tuple| {
                    let mut result = left_tuple.clone();
                    for (k, v) in right_tuple {
                        result.entry(k).or_insert(v);
                    }
                    // Track total
                    total_results.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    result
                })
                .take_while(|_| {
                    // Stop if limit reached
                    total_results.load(std::sync::atomic::Ordering::Relaxed) < max_total
                })
                .collect::<Vec<_>>()
            }).collect::<Vec<_>>()
        })
        .collect()
    }

    // Helper: Bind variables from bindings into pattern
    fn bind_pattern(
        pattern: &TriplePattern,
        bindings: &HashMap<String, u32>,
    ) -> TriplePattern {
        let subject = match &pattern.0 {
            Term::Variable(var) => {
                let lookup_var = var.strip_prefix('?').unwrap_or(var);
                bindings.get(lookup_var)
                .map(|&id| Term::Constant(id))
                .unwrap_or_else(|| pattern.0.clone())
            }
            constant => constant.clone(),
        };

        let predicate = match &pattern.1 {
            Term::Variable(var) => {
                let lookup_var = var.strip_prefix('?').unwrap_or(var);
                bindings.get(lookup_var)
                .map(|&id| Term::Constant(id))
                .unwrap_or_else(|| pattern.1.clone())
            }
            constant => constant.clone(),
        };

        let object = match &pattern.2 {
            Term::Variable(var) => {
                let lookup_var = var.strip_prefix('?').unwrap_or(var);
                bindings.get(lookup_var)
                .map(|&id| Term::Constant(id))
                .unwrap_or_else(|| pattern.2.clone())
            }
            constant => constant.clone(),
        };

        (subject, predicate, object)
    }

    /// Executes a parallel join using SIMD optimization
    fn execute_parallel_join_with_ids(
        left: &PhysicalOperator,
        right: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<HashMap<String, u32>> {
        // Execute left side first
        let left_results = Self::execute_with_ids(left, database);

        // If right side is an index scan, use bind join
        if let Some(right_pattern) = Self::extract_pattern(right) {
            return Self::execute_bind_join_with_ids(left_results, right_pattern, database);
        }

        // Execute right side
        let right_results = Self::execute_with_ids(right, database);

        // If both sides are sorted by join key, use merge join
        if Self::can_use_merge_join(&left_results, &right_results) {
            return Self::execute_merge_join_with_ids(left_results, right_results);
        }

        // Hash join for unsorted data
        Self::execute_hash_join_with_ids(left_results, right_results)
    }

    /// Check if we can use merge join (both sides have same join variables)
    fn can_use_merge_join(
        left_results: &[HashMap<String, u32>],
        right_results: &[HashMap<String, u32>],
    ) -> bool {
        if left_results.is_empty() || right_results.is_empty() {
            return false;
        }

        // Find common variables
        let left_vars: HashSet<String> = left_results[0].keys().cloned().collect();
        let right_vars: HashSet<String> = right_results[0].keys().cloned().collect();
        let common_vars: Vec<String> = left_vars.intersection(&right_vars).cloned().collect();

        // Merge join works well when we have 1-2 common variables
        ! common_vars.is_empty() && common_vars.len() <= 2
    }

    /// Executes a merge join on sorted data
    fn execute_merge_join_with_ids(
        mut left_results: Vec<HashMap<String, u32>>,
        mut right_results: Vec<HashMap<String, u32>>,
    ) -> Vec<HashMap<String, u32>> {
        if left_results.is_empty() || right_results.is_empty() {
            return Vec::new();
        }

        // Find common variables for join
        let left_vars: HashSet<String> = left_results[0].keys().cloned().collect();
        let right_vars: HashSet<String> = right_results[0].keys().cloned().collect();
        let common_vars: Vec<String> = left_vars.intersection(&right_vars).cloned().collect();

        if common_vars.is_empty() {
            return Self::cartesian_product_join(left_results, right_results);
        }

        // Sort both sides by join key
        left_results.par_sort_unstable_by(|a, b| {
            for var in &common_vars {
                match a.get(var).cmp(&b.get(var)) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            std::cmp::Ordering::Equal
        });

        right_results.par_sort_unstable_by(|a, b| {
            for var in &common_vars {
                match a.get(var).cmp(&b.get(var)) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            std::cmp::Ordering::Equal
        });

        // Build index of right side by join key for parallel lookup
        let mut right_index: HashMap<Vec<u32>, Vec<usize>> = HashMap::new();
        for (idx, tuple) in right_results.iter().enumerate() {
            let key: Vec<u32> = common_vars.iter().filter_map(|v| tuple.get(v).copied()).collect();
            right_index.entry(key).or_default().push(idx);
        }

        // Parallel merge using index
        left_results
        .par_iter()
        .flat_map(|left_tuple| {
            let key: Vec<u32> = common_vars.iter().filter_map(|v| left_tuple.get(v).copied()).collect();

            if let Some(right_indices) = right_index.get(&key) {
                right_indices
                .iter()
                .map(|&idx| {
                    let mut joined = left_tuple.clone();
                    for (k, v) in &right_results[idx] {
                        if ! joined.contains_key(k) {
                            joined.insert(k.clone(), *v);
                        }
                    }
                    joined
                })
                .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        })
        .collect()
    }

    /// Checks if two tuples can be joined based on common variables
    fn can_join_with_ids(left: &HashMap<String, u32>, right: &HashMap<String, u32>) -> bool {
        for (var, left_value) in left {
            if let Some(right_value) = right.get(var) {
                if left_value != right_value {
                    return false;
                }
            }
        }
        true
    }

    /// Performs cartesian product join when no common variables exist
    fn cartesian_product_join(
        left_results: Vec<HashMap<String, u32>>,
        right_results: Vec<HashMap<String, u32>>,
    ) -> Vec<HashMap<String, u32>> {
        left_results
        .into_par_iter()
        .flat_map(|left_tuple| {
            right_results
            .iter()
            .map(|right_tuple| {
                let mut joined_tuple = left_tuple.clone();
                joined_tuple.extend(right_tuple.iter().map(|(k, v)| (k.clone(), *v)));
                joined_tuple
            })
            .collect::<Vec<_>>()
        })
        .collect()
    }

    /// Extracts a pattern from a physical operator if it's a scan
    fn extract_pattern(operator: &PhysicalOperator) -> Option<&TriplePattern> {
        match operator {
            PhysicalOperator::TableScan { pattern } => Some(pattern),
            PhysicalOperator::IndexScan { pattern } => Some(pattern),
            _ => None,
        }
    }

    /// Executes an index scan with specialized index-based approach
    fn execute_index_scan_with_ids(
        database: &SparqlDatabase,
        pattern: &TriplePattern,
    ) -> Vec<HashMap<String, u32>> {
        // Determine which index to use based on bound variables
        match pattern {
            // FULLY BOUND (3 constants) - just check if triple exists
            (Term::Constant(s), Term::Constant(p), Term::Constant(o)) => {
                // Use SPO index to check existence
                if let Some(pred_map) = database.index_manager.spo.get(s) {
                    if let Some(objects) = pred_map.get(p) {
                        if objects.contains(o) {
                            // Triple exists - return empty binding (no variables to bind)
                            return vec![HashMap::new()];
                        }
                    }
                }
                // Triple doesn't exist
                Vec::new()
            }

            // TWO BOUNDS (2 constants, 1 variable)
            (Term::Constant(s), Term::Constant(p), Term::Variable(o)) => {
                Self::scan_sp_index_with_ids(database, *s, *p, o.clone())
            }
            (Term::Constant(s), Term::Variable(p), Term::Constant(o)) => {
                Self::scan_so_index_with_ids(database, *s, *o, p.clone())
            }
            (Term::Variable(s), Term::Constant(p), Term::Constant(o)) => {
                Self::scan_po_index_with_ids(database, *p, *o, s.clone())
            }

            // ONE BOUND (1 constant, 2 variables)
            (Term::Constant(s), Term::Variable(p), Term::Variable(o)) => {
                Self::scan_s_index_with_ids(database, *s, p.clone(), o.clone())
            }
            (Term::Variable(s), Term::Constant(p), Term::Variable(o)) => {
                Self::scan_p_index_with_ids(database, *p, s.clone(), o.clone())
            }
            (Term::Variable(s), Term::Variable(p), Term::Constant(o)) => {
                Self::scan_o_index_with_ids(database, *o, s.clone(), p.clone())
            }

            // FULLY UNBOUND (0 constants, 3 variables) - table scan is appropriate
            (Term::Variable(s), Term::Variable(p), Term::Variable(o)) => {
                println!("INFO: Full table scan for fully unbound pattern (? {}, ?{}, ?{})", s, p, o);
                Self::execute_table_scan_with_ids(database, pattern)
            }
        }
    }

    /// Scans SP index (Subject-Predicate -> Object)
    fn scan_sp_index_with_ids(
        database: &SparqlDatabase,
        subject: u32,
        predicate: u32,
        object_var: String,
    ) -> Vec<HashMap<String, u32>> {
        // Strip '?' prefix from variable name
        let object_var = object_var.strip_prefix('?').unwrap_or(&object_var).to_string();

        if let Some(pred_map) = database.index_manager.spo.get(&subject) {
            if let Some(objects) = pred_map.get(&predicate) {
                // Use pre-compute the key
                objects.iter().map(|&object| {
                    let mut result = HashMap::with_capacity(1);  // Pre-size
                    result.insert(object_var.clone(), object);  // Still need clone in closure
                    result
                }).collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    /// Scans SO index (Subject-Object -> Predicate)
    fn scan_so_index_with_ids(
        database: &SparqlDatabase,
        subject: u32,
        object: u32,
        predicate_var: String,
    ) -> Vec<HashMap<String, u32>> {
        // Strip '?' prefix from variable name
        let predicate_var = predicate_var.strip_prefix('?').unwrap_or(&predicate_var).to_string();

        if let Some(obj_map) = database.index_manager.sop.get(&subject) {
            if let Some(predicates) = obj_map.get(&object) {
                // Use iterator with pre-sized HashMap
                predicates.iter().map(|&predicate| {
                    let mut result = HashMap::with_capacity(1);
                    result.insert(predicate_var.clone(), predicate);
                    result
                }).collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    /// Scans PO index (Predicate-Object -> Subject)
    fn scan_po_index_with_ids(
        database: &SparqlDatabase,
        predicate: u32,
        object: u32,
        subject_var: String,
    ) -> Vec<HashMap<String, u32>> {
        // Strip '?' prefix from variable name
        let subject_var = subject_var.strip_prefix('?').unwrap_or(&subject_var).to_string();

        if let Some(obj_map) = database.index_manager.pos.get(&predicate) {
            if let Some(subjects) = obj_map.get(&object) {
                // Use iterator with pre-sized HashMap
                subjects.iter().map(|&subject| {
                    let mut result = HashMap::with_capacity(1);
                    result.insert(subject_var.clone(), subject);
                    result
                }).collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    /// Scans S index (Subject -> (Predicate, Object))
    fn scan_s_index_with_ids(
        database: &SparqlDatabase,
        subject: u32,
        predicate_var: String,
        object_var: String,
    ) -> Vec<HashMap<String, u32>> {
        // Strip '?' prefix from variable names
        let predicate_var = predicate_var.strip_prefix('?').unwrap_or(&predicate_var).to_string();
        let object_var = object_var.strip_prefix('?').unwrap_or(&object_var).to_string();

        if let Some(pred_map) = database.index_manager.spo.get(&subject) {
            // Clone variable names once before flat_map
            pred_map.iter().flat_map(|(&predicate, objects)| {
                let predicate_var = predicate_var.clone();
                let object_var = object_var.clone();
                objects.iter().map(move |&object| {
                    let mut result = HashMap::with_capacity(2);
                    result.insert(predicate_var.clone(), predicate);
                    result.insert(object_var.clone(), object);
                    result
                })
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Scans P index (Predicate -> (Subject, Object))
    fn scan_p_index_with_ids(
        database: &SparqlDatabase,
        predicate: u32,
        subject_var: String,
        object_var: String,
    ) -> Vec<HashMap<String, u32>> {
        // Strip '?' prefix from variable names
        let subject_var = subject_var.strip_prefix('?').unwrap_or(&subject_var).to_string();
        let object_var = object_var.strip_prefix('?').unwrap_or(&object_var).to_string();

        if let Some(obj_map) = database.index_manager.pos.get(&predicate) {
            // Clone variable names once before flat_map
            obj_map.iter().flat_map(|(&object, subjects)| {
                let subject_var = subject_var.clone();
                let object_var = object_var.clone();
                subjects.iter().map(move |&subject| {
                    let mut result = HashMap::with_capacity(2);
                    result.insert(subject_var.clone(), subject);
                    result.insert(object_var.clone(), object);
                    result
                })
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Scans O index (Object -> (Subject, Predicate))
    fn scan_o_index_with_ids(
        database: &SparqlDatabase,
        object: u32,
        subject_var: String,
        predicate_var: String,
    ) -> Vec<HashMap<String, u32>> {
        // Strip '?' prefix from variable names
        let subject_var = subject_var.strip_prefix('?').unwrap_or(&subject_var).to_string();
        let predicate_var = predicate_var.strip_prefix('?').unwrap_or(&predicate_var).to_string();

        if let Some(subj_map) = database.index_manager.osp.get(&object) {
            // Clone variable names once before flat_map
            subj_map.iter().flat_map(|(&subject, predicates)| {
                let subject_var = subject_var.clone();
                let predicate_var = predicate_var.clone();
                predicates.iter().map(move |&predicate| {
                    let mut result = HashMap::with_capacity(2);
                    result.insert(subject_var.clone(), subject);
                    result.insert(predicate_var.clone(), predicate);
                    result
                })
            }).collect()
        } else {
            Vec::new()
        }
    }
}
