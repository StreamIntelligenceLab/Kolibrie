/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::dictionary::Dictionary;
use crate::sparql_database::SparqlDatabase;
use shared::triple::Triple;
use shared::query::FilterExpression;
use std::collections::{BTreeMap, HashMap, HashSet};

// Define logical operators
#[derive(Debug, Clone)]
pub enum LogicalOperator {
    Scan {
        pattern: TriplePattern,
    },
    Selection {
        predicate: Box<LogicalOperator>,
        condition: Condition,
    },
    Projection {
        predicate: Box<LogicalOperator>,
        variables: Vec<String>,
    },
    Join {
        left: Box<LogicalOperator>,
        right: Box<LogicalOperator>,
    },
}

// Define physical operators
#[derive(Debug, Clone)]
pub enum PhysicalOperator {
    TableScan {
        pattern: TriplePattern,
    },
    IndexScan {
        pattern: TriplePattern,
    },
    Filter {
        input: Box<PhysicalOperator>,
        condition: Condition,
    },
    HashJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    NestedLoopJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    ParallelJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    Projection {
        input: Box<PhysicalOperator>,
        variables: Vec<String>,
    },
}

// Triple pattern used in scans
#[derive(Debug, Clone)]
pub struct TriplePattern {
    pub subject: Option<String>,
    pub predicate: Option<String>,
    pub object: Option<String>,
}

// Condition used in selections and filters
#[derive(Debug, Clone)]
pub struct Condition {
    pub variable: String,
    pub operator: String,
    pub value: String,
}

// Define the optimizer
#[derive(Debug, Clone)]
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}

#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub total_triples: u64,
    pub predicate_cardinalities: HashMap<String, u64>,
}

impl VolcanoOptimizer {
    // Constants for cost factors
    const COST_PER_ROW_SCAN: u64 = 1;
    const COST_PER_ROW_INDEX_SCAN: u64 = 1;
    const COST_PER_FILTER: u64 = 1;
    const COST_PER_ROW_JOIN: u64 = 2;
    const COST_PER_ROW_NESTED_LOOP: u64 = 5;
    const COST_PER_PROJECTION: u64 = 1;

    pub fn new(database: &SparqlDatabase) -> Self {
        let stats = DatabaseStats::gather_stats(database);
        Self {
            memo: HashMap::new(),
            selected_variables: Vec::new(),
            stats,
        }
    }

    pub fn find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator {
        self.find_best_plan_recursive(logical_plan)
    }

    fn find_best_plan_recursive(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator {
        let key = format!("{:?}", logical_plan);
        if let Some(plan) = self.memo.get(&key) {
            return plan.clone();
        }

        // Generate possible physical plans
        let mut candidates = Vec::new();

        match logical_plan {
            LogicalOperator::Scan { pattern } => {
                // Implementation rules: Map logical scan to physical scans
                candidates.push(PhysicalOperator::TableScan {
                    pattern: pattern.clone(),
                });
                // If an index is available
                candidates.push(PhysicalOperator::IndexScan {
                    pattern: pattern.clone(),
                });
            }
            LogicalOperator::Selection {
                predicate,
                condition,
            } => {
                // Transformations: Push down selections
                let best_child_plan = self.find_best_plan_recursive(predicate);
                // Implementation rules: Apply selection as a filter
                candidates.push(PhysicalOperator::Filter {
                    input: Box::new(best_child_plan),
                    condition: condition.clone(),
                });
            }
            LogicalOperator::Projection {
                predicate,
                variables,
            } => {
                let best_child_plan = self.find_best_plan_recursive(predicate);
                candidates.push(PhysicalOperator::Projection {
                    input: Box::new(best_child_plan),
                    variables: variables.clone(),
                });
            }
            LogicalOperator::Join { left, right } => {
                let best_left_plan = self.find_best_plan_recursive(left);
                let best_right_plan = self.find_best_plan_recursive(right);

                // Implementation rules: Different join algorithms
                candidates.push(PhysicalOperator::HashJoin {
                    left: Box::new(best_left_plan.clone()),
                    right: Box::new(best_right_plan.clone()),
                });
                candidates.push(PhysicalOperator::NestedLoopJoin {
                    left: Box::new(best_left_plan.clone()),
                    right: Box::new(best_right_plan.clone()),
                });

                // Add parallel join option
                candidates.push(PhysicalOperator::ParallelJoin {
                    left: Box::new(best_left_plan),
                    right: Box::new(best_right_plan),
                });
            }
        }

        // Cost-based optimization: Choose the best candidate
        let best_plan = candidates
            .into_iter()
            .min_by_key(|plan| self.estimate_cost(plan))
            .unwrap();

        // Memoize the best plan
        self.memo.insert(key, best_plan.clone());

        best_plan
    }

    fn estimate_cost(&self, plan: &PhysicalOperator) -> u64 {
        match plan {
            PhysicalOperator::TableScan { pattern } => {
                self.estimate_cardinality(pattern) * Self::COST_PER_ROW_SCAN
            }
            PhysicalOperator::IndexScan { pattern } => {
                let cardinality = self.estimate_cardinality(pattern);

                 // You could discount it more if there's more bound fields
                let bound_count = [
                    &pattern.subject,
                    &pattern.predicate,
                    &pattern.object,
                ]
                    .iter()
                    .filter(|x| {
                        x.as_ref()
                            .map(|v| !v.starts_with('?'))
                            .unwrap_or(false)
                    })
                    .count();
                let discount = match bound_count {
                    0 => 1, // no discount if nothing is bound
                    1 => 1, // minimal discount
                    2 => 2, // bigger discount
                    3 => 5, // everything bound? even bigger discount
                    _ => 1,
                };

                (cardinality * Self::COST_PER_ROW_INDEX_SCAN) / discount
            }
            PhysicalOperator::Filter { input, condition } => {
                let input_cost = self.estimate_cost(input);
                let selectivity = self.estimate_selectivity(condition);
                (input_cost as f64 * selectivity) as u64 + Self::COST_PER_FILTER
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost
                    + right_cost
                    + (left_cardinality + right_cardinality) * Self::COST_PER_ROW_JOIN
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost
                    + right_cost
                    + (left_cardinality * right_cardinality) * Self::COST_PER_ROW_NESTED_LOOP
            }
            PhysicalOperator::ParallelJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost
                    + right_cost
                    + (left_cardinality + right_cardinality) * Self::COST_PER_ROW_JOIN / 2
                // Parallelism reduces cost
            }
            PhysicalOperator::Projection { input, .. } => {
                self.estimate_cost(input) + Self::COST_PER_PROJECTION
            }
        }
    }

    fn estimate_cardinality(&self, pattern: &TriplePattern) -> u64 {
        let mut base_cardinality = self.stats.total_triples;
    
        // Count bound variables for better selectivity estimation
        let bound_count = [
            &pattern.subject,
            &pattern.predicate, 
            &pattern.object,
        ].iter().filter(|var| {
            var.as_ref().map(|v| !v.starts_with('?')).unwrap_or(false)
        }).count();
        
        // More accurate selectivity based on bound variables
        let selectivity = match bound_count {
            0 => 1.0,     // No filtering
            1 => 0.1,     // One bound field
            2 => 0.01,    // Two bound fields  
            3 => 0.001,   // Fully bound
            _ => 1.0,
        };
        
        // Use predicate cardinality if available and more specific
        if let Some(predicate) = &pattern.predicate {
            if !predicate.starts_with('?') {
                let predicate_cardinality = *self.stats.predicate_cardinalities
                    .get(predicate)
                    .unwrap_or(&self.stats.total_triples);
                
                // Use the more restrictive estimate
                base_cardinality = base_cardinality.min(predicate_cardinality);
            }
        }
        
        (base_cardinality as f64 * selectivity) as u64
    }

    fn estimate_selectivity(&self, condition: &Condition) -> f64 {
        // Simplified selectivity estimation
        match condition.operator.as_str() {
            "=" | "!=" => 0.1,              // Assume equality conditions have 10% selectivity
            ">" | "<" | ">=" | "<=" => 0.3, // Range conditions have 30% selectivity
            _ => 1.0,
        }
    }

    fn estimate_output_cardinality(&self, plan: &PhysicalOperator) -> u64 {
        match plan {
            PhysicalOperator::TableScan { pattern } => self.estimate_cardinality(pattern),
            PhysicalOperator::IndexScan { pattern } => self.estimate_cardinality(pattern),
            PhysicalOperator::Filter { input, condition } => {
                let input_cardinality = self.estimate_output_cardinality(input);
                let selectivity = self.estimate_selectivity(condition);
                (input_cardinality as f64 * selectivity) as u64
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                // Assuming the join reduces the output size
                let join_selectivity = 0.5; // Adjust based on expected join selectivity
                (left_cardinality.min(right_cardinality) as f64 * join_selectivity) as u64
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                left_cardinality * right_cardinality
            }
            PhysicalOperator::ParallelJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                // Assuming parallelism reduces the output size similar to hash join
                let join_selectivity = 0.5; // Adjust based on expected join selectivity
                (left_cardinality.min(right_cardinality) as f64 * join_selectivity) as u64
            }
            PhysicalOperator::Projection { input, .. } => self.estimate_output_cardinality(input),
        }
    }
}

impl DatabaseStats {
    pub fn gather_stats(database: &SparqlDatabase) -> Self {
        let total_triples = database.triples.len() as u64;
        let mut predicate_cardinalities: HashMap<String, u64> = HashMap::new();

        for triple in &database.triples {
            let predicate = database.dictionary.decode(triple.predicate).unwrap();
            *predicate_cardinalities
                .entry(predicate.to_string())
                .or_insert(0) += 1;
        }

        Self {
            total_triples,
            predicate_cardinalities,
        }
    }
}

impl PhysicalOperator {
    pub fn execute(&self, database: &mut SparqlDatabase) -> Vec<BTreeMap<String, String>> {
        match self {
            PhysicalOperator::TableScan { pattern } => {
                // Implement table scan execution
                self.execute_table_scan(database, pattern)
            }
            PhysicalOperator::IndexScan { pattern } => {
                // Use the specialized index-based approach
                self.execute_index_scan(database, pattern)
            }
            PhysicalOperator::Filter { input, condition } => {
                // Execute the input operator and apply the filter condition
                let input_results = input.execute(database);
                let filtered_results = input_results
                    .into_iter()
                    .filter(|result| condition.evaluate(result))
                    .collect();

                filtered_results
            }
            PhysicalOperator::Projection { input, variables } => {
                let input_results = input.execute(database);
                input_results
                    .into_iter()
                    .map(|mut result| {
                        result.retain(|k, _| variables.contains(k));
                        result
                    })
                    .collect()
            }
            PhysicalOperator::HashJoin { left, right } => {
                // Implement hash join execution
                let left_results = left.execute(database);
                let right_results = right.execute(database);
                self.execute_hash_join(left_results, right_results)
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                // Implement nested loop join execution
                let left_results = left.execute(database);
                let right_results = right.execute(database);
                self.execute_nested_loop_join(left_results, right_results)
            }

            PhysicalOperator::ParallelJoin { left, right } => {
                // Implement parallel join execution using SIMD
                self.execute_parallel_join(left, right, database)
            }
        }
    }

    fn execute_table_scan(
        &self,
        database: &SparqlDatabase,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, String>> {
        println!("**Executing TableScan** for pattern = {:?}", pattern);
        let mut results = Vec::new();
        for triple in &database.triples {
            if pattern.matches(triple, &database.dictionary) {
                let mut result = BTreeMap::new();
                if let Some(var) = &pattern.subject {
                    if var.starts_with('?') {
                        result.insert(
                            var.clone(),
                            database
                                .dictionary
                                .decode(triple.subject)
                                .unwrap()
                                .to_string(),
                        );
                    }
                }
                if let Some(var) = &pattern.predicate {
                    if var.starts_with('?') {
                        result.insert(
                            var.clone(),
                            database
                                .dictionary
                                .decode(triple.predicate)
                                .unwrap()
                                .to_string(),
                        );
                    }
                }
                if let Some(var) = &pattern.object {
                    if var.starts_with('?') {
                        result.insert(
                            var.clone(),
                            database
                                .dictionary
                                .decode(triple.object)
                                .unwrap()
                                .to_string(),
                        );
                    }
                }
                results.push(result);
            }
        }
        results
    }

    fn execute_hash_join(
        &self,
        left_results: Vec<BTreeMap<String, String>>,
        right_results: Vec<BTreeMap<String, String>>,
    ) -> Vec<BTreeMap<String, String>> {
        if left_results.is_empty() || right_results.is_empty() {
            return Vec::new();
        }

        // Determine join variables dynamically (common variables)
        let left_vars: HashSet<String> = left_results[0].keys().cloned().collect();
        let right_vars: HashSet<String> = right_results[0].keys().cloned().collect();
        let join_vars: HashSet<String> = left_vars.intersection(&right_vars).cloned().collect();

        if join_vars.is_empty() {
            // Perform Cartesian product if no common variables
            let mut results = Vec::new();
            for left_result in &left_results {
                for right_result in &right_results {
                    let mut combined = left_result.clone();
                    combined.extend(right_result.clone());
                    results.push(combined);
                }
            }
            return results;
        }

        // For simplicity, we will use the first common variable as the join key
        let join_var = join_vars.iter().next().unwrap().clone();

        // Create a hash table for the left side, keyed by the join variable
        let mut hash_table: HashMap<String, Vec<BTreeMap<String, String>>> = HashMap::new();

        for left_result in &left_results {
            if let Some(value) = left_result.get(&join_var) {
                hash_table
                    .entry(value.clone())
                    .or_insert_with(Vec::new)
                    .push(left_result.clone());
            }
        }

        let mut results = Vec::new();

        // Iterate over the right results and join them with the left side based on the join variable
        for right_result in &right_results {
            if let Some(value) = right_result.get(&join_var) {
                if let Some(matching_lefts) = hash_table.get(value) {
                    for left_result in matching_lefts {
                        let mut combined = left_result.clone();
                        combined.extend(right_result.clone());
                        results.push(combined);
                    }
                }
            }
        }

        results
    }

    fn execute_nested_loop_join(
        &self,
        left_results: Vec<BTreeMap<String, String>>,
        right_results: Vec<BTreeMap<String, String>>,
    ) -> Vec<BTreeMap<String, String>> {
        let mut results = Vec::new();
        for left_result in &left_results {
            for right_result in &right_results {
                if self.can_join(left_result, right_result) {
                    let mut combined = left_result.clone();
                    combined.extend(right_result.clone());
                    results.push(combined);
                }
            }
        }
        results
    }

    fn execute_parallel_join(
        &self,
        left: &PhysicalOperator,
        right: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<BTreeMap<String, String>> {
        // Execute the left operator to get final_results
        let final_results = left.execute(database);

        // Try to extract the pattern from the right operator
        if let Some(pattern) = extract_pattern(right) {
            println!("WooHoo!!!");
            // Extract variables and predicate from the pattern
            let subject_var = pattern.subject.as_deref().unwrap_or("").to_string();
            let predicate = pattern.predicate.as_deref().unwrap_or("").to_string();
            let object_var = pattern.object.as_deref().unwrap_or("").to_string();

            // Get the triples from the database
            let triples_vec: Vec<Triple> = database.triples.iter().cloned().collect();

            // Convert final_results keys to &str
            let final_results_ref: Vec<BTreeMap<String, String>> = final_results
                .iter()
                .map(|map| {
                    map.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<BTreeMap<String, String>>()
                })
                .collect();

            // Call perform_join_par_simd_with_strict_filter_1
            let results = database.perform_join_par_simd_with_strict_filter_4_redesigned_streaming(
                subject_var,
                predicate.to_string(),
                object_var,
                triples_vec,
                &database.dictionary,
                final_results_ref,
                None,
            );

            // Convert results back to Vec<BTreeMap<String, String>>
            let results_owned: Vec<BTreeMap<String, String>> = results
                .into_iter()
                .map(|map| {
                    map.into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect::<BTreeMap<String, String>>()
                })
                .collect();

            results_owned
        } else {
            println!("Nah!!!");
            // If we cannot extract the pattern, fall back to nested loop join
            self.execute_nested_loop_join(left.execute(database), right.execute(database))
        }
    }

    fn can_join(
        &self,
        left_result: &BTreeMap<String, String>,
        right_result: &BTreeMap<String, String>,
    ) -> bool {
        for (key, left_value) in left_result {
            if let Some(right_value) = right_result.get(key) {
                if left_value != right_value {
                    return false;
                }
            }
        }
        true
    }

    // --- Specialized IndexScan execution ---
    fn execute_index_scan(
        &self,
        database: &mut SparqlDatabase,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, String>> {
        println!("**Executing Parallel IndexScan** for pattern = {:?}", pattern);

        // Convert any non-variable to encoded IDs.
        let subject_id = pattern.subject.as_ref().filter(|s| !s.starts_with('?'))
            .map(|const_s| database.dictionary.encode(const_s));
        let predicate_id = pattern.predicate.as_ref().filter(|p| !p.starts_with('?'))
            .map(|const_p| database.dictionary.encode(const_p));
        let object_id = pattern.object.as_ref().filter(|o| !o.starts_with('?'))
            .map(|const_o| database.dictionary.encode(const_o));

        match (subject_id, predicate_id, object_id) {
            // (S, P, -) => use SubjectPredicate index
            (Some(s), Some(p), None) => {
                println!("**Using Parallel SubjectPredicate index**"); 
                return self.scan_sp_index_parallel(database, s, p, pattern); 
            }

            // (S, -, O) => use SubjectObject index
            (Some(s), None, Some(o)) => {
                println!("**Using Parallel SubjectObject index**"); 
                return self.scan_so_index_parallel(database, s, o, pattern); 
            }

            // (-, P, O) => use PredicateObject index
            (None, Some(p), Some(o)) => {
                println!("**Using Parallel PredicateObject index**");
                return self.scan_po_index_parallel(database, p, o, pattern);
            }

            // (S, P, O) => fully bound; direct membership check
            (Some(s), Some(p), Some(o)) => {
                println!("**Fully bound triple; check membership directly**");
                let triple = Triple { subject: s, predicate: p, object: o };
                if database.triples.contains(&triple) {
                    let mut row = BTreeMap::new();
                    if let Some(subj_str) = &pattern.subject {
                        if subj_str.starts_with('?') {
                            row.insert(subj_str.clone(), database.dictionary.decode(s as u32).unwrap().to_string());
                        }
                    }
                    if let Some(pred_str) = &pattern.predicate {
                        if pred_str.starts_with('?') {
                            row.insert(pred_str.clone(), database.dictionary.decode(p as u32).unwrap().to_string());
                        }
                    }
                    if let Some(obj_str) = &pattern.object {
                        if obj_str.starts_with('?') {
                            row.insert(obj_str.clone(), database.dictionary.decode(o as u32).unwrap().to_string());
                        }
                    }
                    vec![row]
                } else {
                    vec![]
                }
            }

            // Only subject bound
            (Some(s), None, None) => { 
                println!("**Parallel Subject bound scan**");
                return self.scan_s_index_parallel(database, s, pattern); 
            }
            // Only predicate bound
            (None, Some(p), None) => {
                println!("**Parallel Predicate bound scan**");
                return self.scan_p_index_parallel(database, p, pattern);
            }
            // Only object bound
            (None, None, Some(o)) => {
                println!("**Parallel Object bound scan**"); 
                return self.scan_o_index_parallel(database, o, pattern);
            }
            // Nothing bound => fallback to table scan
            _ => {
                println!("**Falling back to TableScan**");
                return self.execute_table_scan(database, pattern);
            }
        }
    }

    /// Parallel SubjectPredicate index scan
    fn scan_sp_index_parallel(
        &self,
        database: &SparqlDatabase,
        s: u32,
        p: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, String>> {
        use rayon::prelude::*;
        
        if let Some(objects) = database.index_manager.scan_sp(s, p) {
            let subject_str = database.dictionary.decode(s).unwrap();
            let predicate_str = database.dictionary.decode(p).unwrap();
            
            // Convert HashSet to Vec for parallel processing
            let objects_vec: Vec<u32> = objects.iter().cloned().collect();
            
            objects_vec.par_iter().map(|&obj| {
                let mut row = BTreeMap::new();
                
                if let Some(subj_var) = &pattern.subject {
                    if subj_var.starts_with('?') {
                        row.insert(subj_var.clone(), subject_str.to_string());
                    }
                }
                if let Some(pred_var) = &pattern.predicate {
                    if pred_var.starts_with('?') {
                        row.insert(pred_var.clone(), predicate_str.to_string());
                    }
                }
                if let Some(obj_var) = &pattern.object {
                    if obj_var.starts_with('?') {
                        row.insert(
                            obj_var.clone(),
                            database.dictionary.decode(obj).unwrap().to_string(),
                        );
                    }
                }
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel SubjectObject index scan
    fn scan_so_index_parallel(
        &self,
        database: &SparqlDatabase,
        s: u32,
        o: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, String>> {
        use rayon::prelude::*;
        
        if let Some(predicates) = database.index_manager.scan_so(s, o) {
            let subject_str = database.dictionary.decode(s).unwrap();
            let object_str = database.dictionary.decode(o).unwrap();
            
            // Convert HashSet to Vec for parallel processing
            let predicates_vec: Vec<u32> = predicates.iter().cloned().collect();
            
            predicates_vec.par_iter().map(|&p| {
                let mut row = BTreeMap::new();
                
                if let Some(subj_var) = &pattern.subject {
                    if subj_var.starts_with('?') {
                        row.insert(subj_var.clone(), subject_str.to_string());
                    }
                }
                if let Some(pred_var) = &pattern.predicate {
                    if pred_var.starts_with('?') {
                        row.insert(
                            pred_var.clone(),
                            database.dictionary.decode(p).unwrap().to_string(),
                        );
                    }
                }
                if let Some(obj_var) = &pattern.object {
                    if obj_var.starts_with('?') {
                        row.insert(obj_var.clone(), object_str.to_string());
                    }
                }
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel PredicateObject index scan
    fn scan_po_index_parallel(
        &self,
        database: &SparqlDatabase,
        p: u32,
        o: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, String>> {
        use rayon::prelude::*;
        
        if let Some(subjects) = database.index_manager.scan_po(p, o) {
            let predicate_str = database.dictionary.decode(p).unwrap();
            let object_str = database.dictionary.decode(o).unwrap();
            
            // Convert HashSet to Vec for parallel processing
            let subjects_vec: Vec<u32> = subjects.iter().cloned().collect();
            
            subjects_vec.par_iter().map(|&s| {
                let mut row = BTreeMap::new();
                
                if let Some(subj_var) = &pattern.subject {
                    if subj_var.starts_with('?') {
                        row.insert(
                            subj_var.clone(),
                            database.dictionary.decode(s).unwrap().to_string(),
                        );
                    }
                }
                if let Some(pred_var) = &pattern.predicate {
                    if pred_var.starts_with('?') {
                        row.insert(pred_var.clone(), predicate_str.to_string());
                    }
                }
                if let Some(obj_var) = &pattern.object {
                    if obj_var.starts_with('?') {
                        row.insert(obj_var.clone(), object_str.to_string());
                    }
                }
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel Subject-only index scan
    fn scan_s_index_parallel(
        &self,
        database: &mut SparqlDatabase,
        s: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, String>> {
        use rayon::prelude::*;
        
        if let Some(pred_map) = database.index_manager.spo.get(&s) {
            let subject_str = database.dictionary.decode(s).unwrap().to_string();
            
            // Collect all (predicate, object) pairs and pre-decode strings
            let pred_obj_pairs: Vec<(String, String)> = pred_map
                .iter()
                .flat_map(|(&pred, objects)| {
                    // Apply predicate filter if specified
                    if let Some(ref p_str) = pattern.predicate {
                        if !p_str.starts_with('?') {
                            let bound_p = database.dictionary.encode(p_str);
                            if pred != bound_p {
                                return Vec::new();
                            }
                        }
                    }
                    
                    let pred_str = database.dictionary.decode(pred).unwrap().to_string();
                    
                    objects.iter().filter_map(|&obj| {
                        // Apply object filter if specified
                        if let Some(ref o_str) = pattern.object {
                            if !o_str.starts_with('?') {
                                let bound_o = database.dictionary.encode(o_str);
                                if obj != bound_o {
                                    return None;
                                }
                            }
                        }
                        
                        let obj_str = database.dictionary.decode(obj).unwrap().to_string();
                        Some((pred_str.clone(), obj_str))
                    }).collect()
                })
                .collect();

            pred_obj_pairs.par_iter().map(|(pred_str, obj_str)| {
                let mut row = BTreeMap::new();
                
                if let Some(s_var) = &pattern.subject {
                    if s_var.starts_with('?') {
                        row.insert(s_var.clone(), subject_str.clone());
                    }
                }
                
                if let Some(p_var) = &pattern.predicate {
                    if p_var.starts_with('?') {
                        row.insert(p_var.clone(), pred_str.clone());
                    }
                }
                
                if let Some(o_var) = &pattern.object {
                    if o_var.starts_with('?') {
                        row.insert(o_var.clone(), obj_str.clone());
                    }
                }
                
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel Predicate-only index scan
    fn scan_p_index_parallel(
        &self,
        database: &mut SparqlDatabase,
        p: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, String>> {
        use rayon::prelude::*;
        
        if let Some(subj_map) = database.index_manager.pso.get(&p) {
            let predicate_str = database.dictionary.decode(p).unwrap().to_string();
            
            // Collect all (subject, object) pairs and pre-decode strings
            let subj_obj_pairs: Vec<(String, String)> = subj_map
                .iter()
                .flat_map(|(&subj, objects)| {
                    // Apply subject filter if specified
                    if let Some(ref s_str) = pattern.subject {
                        if !s_str.starts_with('?') {
                            let bound_s = database.dictionary.encode(s_str);
                            if subj != bound_s {
                                return Vec::new();
                            }
                        }
                    }
                    
                    let subj_str = database.dictionary.decode(subj).unwrap().to_string();
                    
                    objects.iter().filter_map(|&obj| {
                        // Apply object filter if specified
                        if let Some(ref o_str) = pattern.object {
                            if !o_str.starts_with('?') {
                                let bound_o = database.dictionary.encode(o_str);
                                if obj != bound_o {
                                    return None;
                                }
                            }
                        }
                        
                        let obj_str = database.dictionary.decode(obj).unwrap().to_string();
                        Some((subj_str.clone(), obj_str))
                    }).collect()
                })
                .collect();

            subj_obj_pairs.par_iter().map(|(subj_str, obj_str)| {
                let mut row = BTreeMap::new();
                
                if let Some(s_var) = &pattern.subject {
                    if s_var.starts_with('?') {
                        row.insert(s_var.clone(), subj_str.clone());
                    }
                }
                
                if let Some(p_var) = &pattern.predicate {
                    if p_var.starts_with('?') {
                        row.insert(p_var.clone(), predicate_str.clone());
                    }
                }
                
                if let Some(o_var) = &pattern.object {
                    if o_var.starts_with('?') {
                        row.insert(o_var.clone(), obj_str.clone());
                    }
                }
                
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel Object-only index scan
    fn scan_o_index_parallel(
        &self,
        database: &mut SparqlDatabase,
        o: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, String>> {
        use rayon::prelude::*;
        
        if let Some(pred_map) = database.index_manager.ops.get(&o) {
            let object_str = database.dictionary.decode(o).unwrap().to_string();
            
            // Collect all (predicate, subject) pairs and pre-decode strings
            let pred_subj_pairs: Vec<(String, String)> = pred_map
                .iter()
                .flat_map(|(&pred, subjects)| {
                    // Apply predicate filter if specified
                    if let Some(ref p_str) = pattern.predicate {
                        if !p_str.starts_with('?') {
                            let bound_p = database.dictionary.encode(p_str);
                            if pred != bound_p {
                                return Vec::new();
                            }
                        }
                    }
                    
                    let pred_str = database.dictionary.decode(pred).unwrap().to_string();
                    
                    subjects.iter().filter_map(|&subj| {
                        // Apply subject filter if specified
                        if let Some(ref s_str) = pattern.subject {
                            if !s_str.starts_with('?') {
                                let bound_s = database.dictionary.encode(s_str);
                                if subj != bound_s {
                                    return None;
                                }
                            }
                        }
                        
                        let subj_str = database.dictionary.decode(subj).unwrap().to_string();
                        Some((pred_str.clone(), subj_str))
                    }).collect()
                })
                .collect();

            pred_subj_pairs.par_iter().map(|(pred_str, subj_str)| {
                let mut row = BTreeMap::new();
                
                if let Some(s_var) = &pattern.subject {
                    if s_var.starts_with('?') {
                        row.insert(s_var.clone(), subj_str.clone());
                    }
                }
                
                if let Some(p_var) = &pattern.predicate {
                    if p_var.starts_with('?') {
                        row.insert(p_var.clone(), pred_str.clone());
                    }
                }
                
                if let Some(o_var) = &pattern.object {
                    if o_var.starts_with('?') {
                        row.insert(o_var.clone(), object_str.clone());
                    }
                }
                
                row
            }).collect()
        } else {
            Vec::new()
        }
    }
}

impl TriplePattern {
    pub fn matches(&self, triple: &Triple, dict: &Dictionary) -> bool {
        // subject must match if it's a constant
        if let Some(ref s) = self.subject {
            if !s.starts_with('?') {
                let decoded = dict.decode(triple.subject).unwrap();
                if decoded != *s {
                    return false;
                }
            }
        }
        // predicate must match if it's a constant
        if let Some(ref p) = self.predicate {
            if !p.starts_with('?') {
                let decoded = dict.decode(triple.predicate).unwrap();
                if decoded != *p {
                    return false;
                }
            }
        }
        // object must match if it's a constant
        if let Some(ref o) = self.object {
            if !o.starts_with('?') {
                let decoded = dict.decode(triple.object).unwrap();
                if decoded != *o {
                    return false;
                }
            }
        }
        true
    }
}

impl Condition {
    pub fn evaluate(&self, result: &BTreeMap<String, String>) -> bool {
        if let Some(value) = result.get(&self.variable) {
            match self.operator.as_str() {
                "=" => value == &self.value,
                "!=" => value != &self.value,
                ">" => value.parse::<i32>().unwrap_or(0) > self.value.parse::<i32>().unwrap_or(0),
                ">=" => value.parse::<i32>().unwrap_or(0) >= self.value.parse::<i32>().unwrap_or(0),
                "<" => value.parse::<i32>().unwrap_or(0) < self.value.parse::<i32>().unwrap_or(0),
                "<=" => value.parse::<i32>().unwrap_or(0) <= self.value.parse::<i32>().unwrap_or(0),
                _ => false,
            }
        } else {
            false
        }
    }
}

fn extract_pattern(op: &PhysicalOperator) -> Option<&TriplePattern> {
    match op {
        PhysicalOperator::TableScan { pattern } 
        | PhysicalOperator::IndexScan { pattern } => {
            Some(pattern)
        }
        // If it’s a Filter, keep searching in its child
        PhysicalOperator::Filter { input, .. } => {
            extract_pattern(input)
        }

        // Same if it’s a Projection
        PhysicalOperator::Projection { input, .. } => {
            extract_pattern(input)
        }
        _ => None,
    }
}

pub fn build_logical_plan(
    variables: Vec<(&str, &str)>,
    patterns: Vec<(&str, &str, &str)>,
    filters: Vec<FilterExpression>,
    prefixes: &HashMap<String, String>,
    database: &SparqlDatabase,
) -> LogicalOperator {
    // For each pattern, create a scan operator
    let mut scan_operators = Vec::new();
    for (subject_var, predicate, object_var) in patterns {
        
        // Resolve the terms in the pattern
        let resolved_subject = database.resolve_query_term(subject_var, prefixes);
        let resolved_predicate = database.resolve_query_term(predicate, prefixes);
        let resolved_object = database.resolve_query_term(object_var, prefixes);

        let pattern = TriplePattern {
            subject: Some(resolved_subject),
            predicate: Some(resolved_predicate),
            object: Some(resolved_object),
        };
        let scan_op = LogicalOperator::Scan { pattern };
        scan_operators.push(scan_op);
    }

    // Combine scans using joins
    let mut current_op = scan_operators[0].clone();
    for next_op in scan_operators.into_iter().skip(1) {
        current_op = LogicalOperator::Join {
            left: Box::new(current_op),
            right: Box::new(next_op),
        };
    }

    // Apply filters
    for filter in filters {
        match filter {
            FilterExpression::Comparison(var, operator, value) => {
                let condition = Condition {
                    variable: var.to_string(),
                    operator: operator.to_string(),
                    value: value.to_string(),
                };
                current_op = LogicalOperator::Selection {
                    predicate: Box::new(current_op),
                    condition,
                };
            },
            FilterExpression::And(_, _) => {
                // TODO: Handle AND logic
            },
            FilterExpression::Or(_, _) => {
                // TODO: Handle OR logic
            },
            FilterExpression::Not(_) => {
                // TODO: Handle NOT logic
            }

            FilterExpression::ArithmeticExpr(_) => {
                // TODO: Handle arithmetic expressions
            }
        }
    }

    // Extract variable names from variables
    let selected_vars: Vec<String> = variables
        .iter()
        .filter(|(agg_type, _)| *agg_type == "VAR")
        .map(|(_, var)| var.to_string())
        .collect();

    // Wrap with projection
    current_op = LogicalOperator::Projection {
        predicate: Box::new(current_op),
        variables: selected_vars,
    };

    current_op
}
