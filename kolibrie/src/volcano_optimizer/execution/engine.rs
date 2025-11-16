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

use shared::join_algorithm::perform_join_par_simd_with_strict_filter_4_redesigned_streaming;
use shared::terms::{Term, TriplePattern};

use std::collections::{BTreeMap, HashMap, HashSet};

/// Execution engine for physical operators
pub struct ExecutionEngine;

impl ExecutionEngine {
    /// Executes a physical operator and returns string results
    pub fn execute(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<BTreeMap<String, String>> {
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
    ) -> Vec<BTreeMap<String, u32>> {
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
                input_results
                    .into_par_iter()
                    .map(|mut result| {
                        result.retain(|k, _| variables.contains(k));
                        result
                    })
                    .collect()
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
        }
    }

    /// Executes a table scan with ID-based results
    fn execute_table_scan_with_ids(
        database: &mut SparqlDatabase,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        let mut results = Vec::new();

        // Iterate through all triples in the database
        for triple in &database.triples {
            let mut bindings = BTreeMap::new();
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

    /// Executes an optimized hash join with ID-based results
    fn execute_optimized_hash_join_with_ids(
        left_results: Vec<BTreeMap<String, u32>>,
        right_results: Vec<BTreeMap<String, u32>>,
    ) -> Vec<BTreeMap<String, u32>> {
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
            (&left_results, &right_results)
        } else {
            (&right_results, &left_results)
        };

        let mut hash_table: HashMap<Vec<u32>, Vec<&BTreeMap<String, u32>>> = HashMap::new();

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
        left_results: Vec<BTreeMap<String, u32>>,
        right_results: Vec<BTreeMap<String, u32>>,
    ) -> Vec<BTreeMap<String, u32>> {
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
        let mut hash_table: HashMap<Vec<u32>, Vec<BTreeMap<String, u32>>> = HashMap::new();

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
        left_results: Vec<BTreeMap<String, u32>>,
        right_results: Vec<BTreeMap<String, u32>>,
    ) -> Vec<BTreeMap<String, u32>> {
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

    /// Executes a parallel join using SIMD optimization
    fn execute_parallel_join_with_ids(
        left: &PhysicalOperator,
        right: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<BTreeMap<String, u32>> {
        // Check if we can use the optimized join algorithm
        if let Some(right_pattern) = Self::extract_pattern(right) {
            let left_results = Self::execute_with_ids(left, database);

            if !left_results.is_empty() {
                // Extract pattern variables for the join algorithm
                if let (
                    Term::Variable(subject_var),
                    Term::Constant(predicate),
                    Term::Variable(object_var),
                ) = right_pattern
                {
                    // Convert ID results to string results for the join algorithm
                    let string_results: Vec<BTreeMap<String, String>> = left_results
                        .iter()
                        .map(|id_result| {
                            id_result
                                .iter()
                                .map(|(var, &id)| {
                                    (
                                        var.clone(),
                                        database.dictionary.decode(id).unwrap().to_string(),
                                    )
                                })
                                .collect()
                        })
                        .collect();

                    let triples_vec: Vec<shared::triple::Triple> =
                        database.triples.iter().cloned().collect();

                    // Use the efficient join algorithm
                    let predicate_string =
                        database.dictionary.decode(*predicate).unwrap().to_string();
                    let joined_results =
                        perform_join_par_simd_with_strict_filter_4_redesigned_streaming(
                            subject_var.clone(),
                            predicate_string,
                            object_var.clone(),
                            triples_vec,
                            &database.dictionary,
                            string_results,
                            None,
                        );

                    // Convert string results back to ID results
                    return joined_results
                        .into_iter()
                        .map(|string_result| {
                            string_result
                                .into_iter()
                                .map(|(var, value)| (var, database.dictionary.encode(&value)))
                                .collect()
                        })
                        .collect();
                }
            }
        }

        // Fallback to regular hash join
        let left_results = Self::execute_with_ids(left, database);
        let right_results = Self::execute_with_ids(right, database);
        Self::execute_hash_join_with_ids(left_results, right_results)
    }

    /// Checks if two tuples can be joined based on common variables
    fn can_join_with_ids(left: &BTreeMap<String, u32>, right: &BTreeMap<String, u32>) -> bool {
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
        left_results: Vec<BTreeMap<String, u32>>,
        right_results: Vec<BTreeMap<String, u32>>,
    ) -> Vec<BTreeMap<String, u32>> {
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
        database: &mut SparqlDatabase,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        // Determine which index to use based on bound variables
        match pattern {
            (Term::Constant(s), Term::Constant(p), Term::Variable(o)) => {
                Self::scan_sp_index_with_ids(database, *s, *p, o.clone())
            }
            (Term::Constant(s), Term::Variable(p), Term::Constant(o)) => {
                Self::scan_so_index_with_ids(database, *s, *o, p.clone())
            }
            (Term::Variable(s), Term::Constant(p), Term::Constant(o)) => {
                Self::scan_po_index_with_ids(database, *p, *o, s.clone())
            }
            (Term::Constant(s), Term::Variable(p), Term::Variable(o)) => {
                Self::scan_s_index_with_ids(database, *s, p.clone(), o.clone())
            }
            (Term::Variable(s), Term::Constant(p), Term::Variable(o)) => {
                Self::scan_p_index_with_ids(database, *p, s.clone(), o.clone())
            }
            (Term::Variable(s), Term::Variable(p), Term::Constant(o)) => {
                Self::scan_o_index_with_ids(database, *o, s.clone(), p.clone())
            }
            _ => {
                // Fallback to table scan for fully unbound or fully bound patterns
                Self::execute_table_scan_with_ids(database, pattern)
            }
        }
    }

    /// Scans SP index (Subject-Predicate -> Object)
    fn scan_sp_index_with_ids(
        database: &mut SparqlDatabase,
        subject: u32,
        predicate: u32,
        object_var: String,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(pred_map) = database.index_manager.spo.get(&subject) {
            if let Some(objects) = pred_map.get(&predicate) {
                objects
                    .iter()
                    .map(|&object| {
                        let mut result = BTreeMap::new();
                        result.insert(object_var.clone(), object);
                        result
                    })
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    /// Scans SO index (Subject-Object -> Predicate)
    fn scan_so_index_with_ids(
        database: &mut SparqlDatabase,
        subject: u32,
        object: u32,
        predicate_var: String,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(obj_map) = database.index_manager.sop.get(&subject) {
            if let Some(predicates) = obj_map.get(&object) {
                predicates
                    .iter()
                    .map(|&predicate| {
                        let mut result = BTreeMap::new();
                        result.insert(predicate_var.clone(), predicate);
                        result
                    })
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    /// Scans PO index (Predicate-Object -> Subject)
    fn scan_po_index_with_ids(
        database: &mut SparqlDatabase,
        predicate: u32,
        object: u32,
        subject_var: String,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(obj_map) = database.index_manager.pos.get(&predicate) {
            if let Some(subjects) = obj_map.get(&object) {
                subjects
                    .iter()
                    .map(|&subject| {
                        let mut result = BTreeMap::new();
                        result.insert(subject_var.clone(), subject);
                        result
                    })
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    /// Scans S index (Subject -> (Predicate, Object))
    fn scan_s_index_with_ids(
        database: &mut SparqlDatabase,
        subject: u32,
        predicate_var: String,
        object_var: String,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(pred_map) = database.index_manager.spo.get(&subject) {
            pred_map
                .iter()
                .flat_map(|(&predicate, objects)| {
                    objects.iter().map({
                        let predicate_var = predicate_var.clone();
                        let object_var = object_var.clone();
                        move |&object| {
                            let mut result = BTreeMap::new();
                            result.insert(predicate_var.clone(), predicate);
                            result.insert(object_var.clone(), object);
                            result
                        }
                    })
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Scans P index (Predicate -> (Subject, Object))
    fn scan_p_index_with_ids(
        database: &mut SparqlDatabase,
        predicate: u32,
        subject_var: String,
        object_var: String,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(obj_map) = database.index_manager.pos.get(&predicate) {
            obj_map
                .iter()
                .flat_map({
                    let subject_var = subject_var.clone();
                    let object_var = object_var.clone();
                    move |(&object, subjects)| {
                        subjects
                            .iter()
                            .map({
                                let subject_var = subject_var.clone();
                                let object_var = object_var.clone();
                                move |&subject| {
                                    let mut result = BTreeMap::new();
                                    result.insert(subject_var.clone(), subject);
                                    result.insert(object_var.clone(), object);
                                    result
                                }
                            })
                            .collect::<Vec<_>>()
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Scans O index (Object -> (Subject, Predicate))
    fn scan_o_index_with_ids(
        database: &mut SparqlDatabase,
        object: u32,
        subject_var: String,
        predicate_var: String,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(subj_map) = database.index_manager.osp.get(&object) {
            subj_map
                .iter()
                .flat_map({
                    let subject_var = subject_var.clone();
                    let predicate_var = predicate_var.clone();
                    move |(&subject, predicates)| {
                        predicates
                            .iter()
                            .map({
                                let subject_var = subject_var.clone();
                                let predicate_var = predicate_var.clone();
                                move |&predicate| {
                                    let mut result = BTreeMap::new();
                                    result.insert(subject_var.clone(), subject);
                                    result.insert(predicate_var.clone(), predicate);
                                    result
                                }
                            })
                            .collect::<Vec<_>>()
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}
