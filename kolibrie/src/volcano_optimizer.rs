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
use rayon::prelude::*;

// Define logical operators
#[derive(Debug, Clone, PartialEq, Eq)]
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
    OptimizedHashJoin {
        left: Box<PhysicalOperator>,
        right: Box<PhysicalOperator>,
    },
    Projection {
        input: Box<PhysicalOperator>,
        variables: Vec<String>,
    },
}

// Triple pattern used in scans
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriplePattern {
    pub subject: Option<String>,
    pub predicate: Option<String>,
    pub object: Option<String>,
}

// Condition used in selections and filters
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    pub variable: String,
    pub operator: String,
    pub value: String,
}

// ID-based result type for performance
#[derive(Debug, Clone)]
pub struct IdResult {
    pub bindings: BTreeMap<String, u32>, // Variable -> ID mapping
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
    pub subject_cardinalities: HashMap<String, u64>,
    pub object_cardinalities: HashMap<String, u64>,
    pub join_selectivity_cache: HashMap<String, f64>,
    pub predicate_histogram: HashMap<String, Vec<(String, u64)>>, // For better selectivity estimation
}

impl VolcanoOptimizer {
    // Constants for cost factors
    const COST_PER_ROW_SCAN: u64 = 100;
    const COST_PER_ROW_INDEX_SCAN: u64 = 1;
    const COST_PER_FILTER: u64 = 1;
    const COST_PER_ROW_JOIN: u64 = 2;
    const COST_PER_ROW_NESTED_LOOP: u64 = 10;
    const COST_PER_PROJECTION: u64 = 1;
    const COST_PER_ROW_OPTIMIZED_JOIN: u64 = 1; // New optimized join cost

    pub fn new(database: &SparqlDatabase) -> Self {
        let stats = DatabaseStats::gather_stats_fast(database);
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
        let key = self.create_memo_key(logical_plan);
        
        if let Some(plan) = self.memo.get(&key) {
            return plan.clone();
        }

        let mut candidates = Vec::new();

        match logical_plan {
            LogicalOperator::Scan { pattern } => {
                // Implementation rules: Map logical scan to physical scans
                let best_scan = self.choose_best_scan(pattern);
                candidates.push(best_scan);
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
                // Add join reordering based on cost
                let left_cost = self.estimate_logical_cost(left);
                let right_cost = self.estimate_logical_cost(right);
                
                let (cheaper_side, expensive_side) = if left_cost <= right_cost {
                    (left, right)
                } else {
                    (right, left)  // Swap for better order
                };
                
                let best_left_plan = self.find_best_plan_recursive(cheaper_side);
                let best_right_plan = self.find_best_plan_recursive(expensive_side);

                // Implementation rules: Different join algorithms
                candidates.push(PhysicalOperator::OptimizedHashJoin {
                    left: Box::new(best_left_plan.clone()),
                    right: Box::new(best_right_plan.clone()),
                });

                candidates.push(PhysicalOperator::HashJoin {
                    left: Box::new(best_left_plan.clone()),
                    right: Box::new(best_right_plan.clone()),
                });

                // Only use nested loop for small datasets
                let left_cardinality = self.estimate_output_cardinality_from_logical(cheaper_side);
                let right_cardinality = self.estimate_output_cardinality_from_logical(expensive_side);
                
                if left_cardinality < 1000 && right_cardinality < 1000 {
                    candidates.push(PhysicalOperator::NestedLoopJoin {
                        left: Box::new(best_left_plan.clone()),
                        right: Box::new(best_right_plan.clone()),
                    });
                }

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

    // Enhanced scan selection
    fn choose_best_scan(&self, pattern: &TriplePattern) -> PhysicalOperator {
        let bound_vars = self.count_bound_variables(pattern);
        let estimated_size = self.estimate_cardinality(pattern);
        
        match bound_vars {
            3 => PhysicalOperator::IndexScan { pattern: pattern.clone() }, // Fully bound - always use index
            2 => PhysicalOperator::IndexScan { pattern: pattern.clone() }, // Two bounds - index is better
            1 => {
                // Use index if result set is small enough
                if estimated_size < 10000 {
                    PhysicalOperator::IndexScan { pattern: pattern.clone() }
                } else {
                    PhysicalOperator::TableScan { pattern: pattern.clone() }
                }
            }
            0 => PhysicalOperator::TableScan { pattern: pattern.clone() }, // Full scan
            _ => PhysicalOperator::TableScan { pattern: pattern.clone() },
        }
    }

    fn count_bound_variables(&self, pattern: &TriplePattern) -> usize {
        [&pattern.subject, &pattern.predicate, &pattern.object]
            .iter()
            .filter(|x| x.as_ref().map(|v| !v.starts_with('?')).unwrap_or(false))
            .count()
    }

    // Memo key creation
    fn create_memo_key(&self, logical_plan: &LogicalOperator) -> String {
        self.serialize_logical_plan(logical_plan)
    }

    fn serialize_logical_plan(&self, plan: &LogicalOperator) -> String {
        match plan {
            LogicalOperator::Scan { pattern } => {
                format!("Scan({},{},{})", 
                    pattern.subject.as_deref().unwrap_or(""), 
                    pattern.predicate.as_deref().unwrap_or(""), 
                    pattern.object.as_deref().unwrap_or(""))
            }
            LogicalOperator::Selection { predicate, condition } => {
                format!("Selection({},{},{},[{}])", 
                    condition.variable, 
                    condition.operator, 
                    condition.value,
                    self.serialize_logical_plan(predicate))
            }
            LogicalOperator::Projection { predicate, variables } => {
                format!("Projection({:?},[{}])", 
                    variables, 
                    self.serialize_logical_plan(predicate))
            }
            LogicalOperator::Join { left, right } => {
                format!("Join([{}],[{}])", 
                    self.serialize_logical_plan(left), 
                    self.serialize_logical_plan(right))
            }
        }
    }

    fn estimate_logical_cost(&self, logical_plan: &LogicalOperator) -> u64 {
        match logical_plan {
            LogicalOperator::Scan { pattern } => self.estimate_cardinality(pattern),
            LogicalOperator::Join { left, right } => {
                let left_cost = self.estimate_logical_cost(left);
                let right_cost = self.estimate_logical_cost(right);
                let left_card = self.estimate_output_cardinality_from_logical(left);
                let right_card = self.estimate_output_cardinality_from_logical(right);
                
                // More sophisticated join cost estimation
                let join_selectivity = self.estimate_join_selectivity();
                left_cost + right_cost + ((left_card * right_card) as f64 * join_selectivity) as u64
            }
            LogicalOperator::Selection { predicate, condition } => {
                let base_cost = self.estimate_logical_cost(predicate);
                let selectivity = self.estimate_selectivity(condition);
                (base_cost as f64 * selectivity) as u64
            }
            LogicalOperator::Projection { predicate, .. } => {
                self.estimate_logical_cost(predicate)
            }
        }
    }

    fn estimate_join_selectivity(&self) -> f64 {
        0.1
    }

    fn estimate_output_cardinality_from_logical(&self, logical_plan: &LogicalOperator) -> u64 {
        match logical_plan {
            LogicalOperator::Scan { pattern } => self.estimate_cardinality(pattern),
            LogicalOperator::Selection { predicate, condition } => {
                let base_card = self.estimate_output_cardinality_from_logical(predicate);
                let selectivity = self.estimate_selectivity(condition);
                ((base_card as f64 * selectivity) as u64).max(1)
            }
            LogicalOperator::Projection { predicate, .. } => {
                self.estimate_output_cardinality_from_logical(predicate)
            }
            LogicalOperator::Join { left, right } => {
                let left_card = self.estimate_output_cardinality_from_logical(left);
                let right_card = self.estimate_output_cardinality_from_logical(right);
                let join_selectivity = self.estimate_join_selectivity();
                ((left_card.min(right_card) as f64 * join_selectivity) as u64).max(1)
            }
        }
    }

    fn estimate_cost(&self, plan: &PhysicalOperator) -> u64 {
        match plan {
            PhysicalOperator::TableScan { pattern } => {
                self.estimate_cardinality(pattern) * Self::COST_PER_ROW_SCAN
            }
            PhysicalOperator::IndexScan { pattern } => {
                let cardinality = self.estimate_cardinality(pattern);
                let bound_count = self.count_bound_variables(pattern);

                let discount = match bound_count {
                    0 => 1,      // No discount for unbounded scan
                    1 => 10,     // 10x better for one bound field
                    2 => 100,    // 100x better for two bound fields
                    3 => 1000,   // 1000x better for fully bound
                    _ => 1,
                };

                (cardinality * Self::COST_PER_ROW_INDEX_SCAN) / discount
            }
            PhysicalOperator::Filter { input, condition } => {
                let input_cost = self.estimate_cost(input);
                let selectivity = self.estimate_selectivity(condition);
                (input_cost as f64 * selectivity) as u64 + Self::COST_PER_FILTER
            }
            PhysicalOperator::OptimizedHashJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost + right_cost + 
                (left_cardinality + right_cardinality) * Self::COST_PER_ROW_OPTIMIZED_JOIN
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost + right_cost + 
                (left_cardinality + right_cardinality) * Self::COST_PER_ROW_JOIN
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost + right_cost + 
                (left_cardinality * right_cardinality) * Self::COST_PER_ROW_NESTED_LOOP
            }
            PhysicalOperator::ParallelJoin { left, right } => {
                // Check if we can use your efficient join
                if extract_pattern(right).is_some() {
                    let left_cost = self.estimate_cost(left);
                    let left_cardinality = self.estimate_output_cardinality(left);
                    // Massive discount for your efficient join
                    left_cost + (left_cardinality * Self::COST_PER_ROW_JOIN / 20)
                } else {
                    let left_cost = self.estimate_cost(left);
                    let right_cost = self.estimate_cost(right);
                    let left_cardinality = self.estimate_output_cardinality(left);
                    let right_cardinality = self.estimate_output_cardinality(right);

                    left_cost
                        + right_cost
                        + (left_cardinality + right_cardinality) * Self::COST_PER_ROW_JOIN / 2
                }
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
            &pattern.object
        ]
            .iter()
            .filter(|var| var.as_ref().map(|v| !v.starts_with('?')).unwrap_or(false))
            .count();
        
        // More aggressive selectivity based on bound variables
        let selectivity = match bound_count {
            0 => 1.0,       // No filtering
            1 => 0.01,      // More selective for one bound field
            2 => 0.0001,     // Very selective for two bound fields  
            3 => 0.00001,    // Extremely selective for fully bound
            _ => 1.0,
        };
        
        // Use predicate cardinality if available and more specific
        if let Some(predicate) = &pattern.predicate {
            if !predicate.starts_with('?') {
                if let Some(&predicate_cardinality) = self.stats.predicate_cardinalities.get(predicate) {
                    // Use the more restrictive estimate
                    base_cardinality = base_cardinality.min(predicate_cardinality);
                }
            }
        }
        
        if let Some(subject) = &pattern.subject {
            if !subject.starts_with('?') {
                if let Some(&subject_cardinality) = self.stats.subject_cardinalities.get(subject) {
                    base_cardinality = base_cardinality.min(subject_cardinality);
                }
            }
        }
        
        if let Some(object) = &pattern.object {
            if !object.starts_with('?') {
                if let Some(&object_cardinality) = self.stats.object_cardinalities.get(object) {
                    base_cardinality = base_cardinality.min(object_cardinality);
                }
            }
        }
        
        ((base_cardinality as f64 * selectivity) as u64).max(1)
    }

    fn estimate_selectivity(&self, condition: &Condition) -> f64 {
        match condition.operator.as_str() {
            "=" => 0.05,  // More selective
            "!=" => 0.95,
            ">" | "<" | ">=" | "<=" => 0.25,
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
                ((input_cardinality as f64 * selectivity) as u64).max(1)
            }
            PhysicalOperator::OptimizedHashJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                let join_selectivity = 0.05; // More realistic selectivity
                ((left_cardinality.min(right_cardinality) as f64 * join_selectivity) as u64).max(1)
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                let join_selectivity = 0.1;
                ((left_cardinality.min(right_cardinality) as f64 * join_selectivity) as u64).max(1)
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                (left_cardinality * right_cardinality / 1000).max(1)
            }
            PhysicalOperator::ParallelJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                let join_selectivity = 0.1;
                ((left_cardinality.min(right_cardinality) as f64 * join_selectivity) as u64).max(1)
            }
            PhysicalOperator::Projection { input, .. } => self.estimate_output_cardinality(input),
        }
    }
}

impl DatabaseStats {
    pub fn gather_stats_fast(database: &SparqlDatabase) -> Self {
        let total_triples = database.triples.len() as u64;
        
        // Convert BTreeSet to Vec for sampling
        let triples_vec: Vec<_> = database.triples.iter().collect();
        
        // Use sampling for large datasets instead of full scan
        let sample_size = (total_triples as usize).min(100_000);
        let step = if total_triples > sample_size as u64 {
            total_triples as usize / sample_size
        } else {
            1
        };

        // Sample the data by stepping through the vector
        let sampled_triples: Vec<_> = triples_vec
            .iter()
            .step_by(step)
            .take(sample_size)
            .collect();

        // Use parallel processing for stats gathering
        let stats_data: Vec<_> = sampled_triples
            .par_iter()
            .map(|triple| {
                let subject = database.dictionary.decode(triple.subject).unwrap().to_string();
                let predicate = database.dictionary.decode(triple.predicate).unwrap().to_string();
                let object = database.dictionary.decode(triple.object).unwrap().to_string();
                (subject, predicate, object)
            })
            .collect();

        // Build cardinality maps
        let mut predicate_cardinalities: HashMap<String, u64> = HashMap::new();
        let mut subject_cardinalities: HashMap<String, u64> = HashMap::new();
        let mut object_cardinalities: HashMap<String, u64> = HashMap::new();

        for (subject, predicate, object) in stats_data {
            *predicate_cardinalities.entry(predicate).or_insert(0) += 1;
            *subject_cardinalities.entry(subject).or_insert(0) += 1;
            *object_cardinalities.entry(object).or_insert(0) += 1;
        }

        // Scale up sampled statistics
        let scale_factor = if step > 1 { step as u64 } else { 1 };
        predicate_cardinalities.values_mut().for_each(|v| *v *= scale_factor);
        subject_cardinalities.values_mut().for_each(|v| *v *= scale_factor);
        object_cardinalities.values_mut().for_each(|v| *v *= scale_factor);

        Self {
            total_triples,
            predicate_cardinalities,
            subject_cardinalities,
            object_cardinalities,
            join_selectivity_cache: HashMap::new(),
            predicate_histogram: HashMap::new(),
        }
    }
}

impl PhysicalOperator {
    pub fn execute(&self, database: &mut SparqlDatabase) -> Vec<BTreeMap<String, String>> {
        let id_results = self.execute_with_ids(database);
        
        // Convert ID results to string results only at the final step
        id_results
            .into_par_iter()
            .map(|id_result| {
                id_result
                    .into_iter()
                    .map(|(var, id)| {
                        (var, database.dictionary.decode(id).unwrap().to_string())
                    })
                    .collect()
            })
            .collect()
    }

    pub fn execute_with_ids(&self, database: &mut SparqlDatabase) -> Vec<BTreeMap<String, u32>> {
        match self {
            PhysicalOperator::TableScan { pattern } => {
                // Implement table scan execution
                self.execute_table_scan_with_ids(database, pattern)
            }
            PhysicalOperator::IndexScan { pattern } => {
                // Use the specialized index-based approach
                self.execute_index_scan_with_ids(database, pattern)
            }
            PhysicalOperator::Filter { input, condition } => {
                // Execute the input operator and apply the filter condition
                let input_results = input.execute_with_ids(database);
                // Use parallel filtering
                input_results
                    .into_par_iter()
                    .filter(|result| condition.evaluate_with_ids(result, &database.dictionary))
                    .collect()
            }
            PhysicalOperator::Projection { input, variables } => {
                let input_results = input.execute_with_ids(database);
                input_results
                    .into_par_iter()
                    .map(|mut result| {
                        result.retain(|k, _| variables.contains(k));
                        result
                    })
                    .collect()
            }
            PhysicalOperator::OptimizedHashJoin { left, right } => {
                let left_results = left.execute_with_ids(database);
                let right_results = right.execute_with_ids(database);
                self.execute_optimized_hash_join_with_ids(left_results, right_results)
            }
            PhysicalOperator::HashJoin { left, right } => {
                // Implement hash join execution
                let left_results = left.execute_with_ids(database);
                let right_results = right.execute_with_ids(database);
                self.execute_hash_join_with_ids(left_results, right_results)
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                // Implement nested loop join execution
                let left_results = left.execute_with_ids(database);
                let right_results = right.execute_with_ids(database);
                self.execute_nested_loop_join_with_ids(left_results, right_results)
            }
            PhysicalOperator::ParallelJoin { left, right } => {
                // Implement parallel join execution using SIMD
                self.execute_parallel_join_with_ids(left, right, database)
            }
        }
    }

    // Optimized table scan with reduced string operations
    fn execute_table_scan_with_ids(
        &self,
        database: &mut SparqlDatabase,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        // Pre-compute bound IDs for faster matching
        let subject_id = pattern.subject.as_ref()
            .filter(|s| !s.starts_with('?'))
            .map(|s| database.dictionary.encode(s));
        let predicate_id = pattern.predicate.as_ref()
            .filter(|p| !p.starts_with('?'))
            .map(|p| database.dictionary.encode(p));
        let object_id = pattern.object.as_ref()
            .filter(|o| !o.starts_with('?'))
            .map(|o| database.dictionary.encode(o));

        // Parallel processing with reduced allocations
        database.triples
            .par_iter()
            .filter_map(|triple| {
                // Fast ID-based filtering
                if let Some(s_id) = subject_id {
                    if triple.subject != s_id { return None; }
                }
                if let Some(p_id) = predicate_id {
                    if triple.predicate != p_id { return None; }
                }
                if let Some(o_id) = object_id {
                    if triple.object != o_id { return None; }
                }

                // Build result only if match
                let mut result = BTreeMap::new();
                
                if let Some(var) = &pattern.subject {
                    if var.starts_with('?') {
                        result.insert(var.clone(), triple.subject);
                    }
                }
                if let Some(var) = &pattern.predicate {
                    if var.starts_with('?') {
                        result.insert(var.clone(), triple.predicate);
                    }
                }
                if let Some(var) = &pattern.object {
                    if var.starts_with('?') {
                        result.insert(var.clone(), triple.object);
                    }
                }
                
                Some(result)
            })
            .collect()
    }

    // New optimized hash join with proper type annotations
    fn execute_optimized_hash_join_with_ids(
        &self,
        left_results: Vec<BTreeMap<String, u32>>,
        right_results: Vec<BTreeMap<String, u32>>,
    ) -> Vec<BTreeMap<String, u32>> {
        if left_results.is_empty() || right_results.is_empty() {
            return Vec::new();
        }

        // Choose smaller relation as build side
        let (build_side, probe_side, swapped) = if left_results.len() <= right_results.len() {
            (left_results, right_results, false)
        } else {
            (right_results, left_results, true)
        };

        // Determine join variables
        let build_binding = build_side.clone();
        let build_vars: HashSet<&String> = build_binding[0].keys().collect();
        let binding = probe_side.clone();
        let probe_vars: HashSet<&String> = binding[0].keys().collect();
        let join_vars: Vec<&String> = build_vars.intersection(&probe_vars).copied().collect();

        if join_vars.is_empty() {
            // Limited cartesian product
            let max_results = 50_000;
            let mut results = Vec::with_capacity(max_results.min(build_side.len() * probe_side.len()));
            
            'outer: for build_result in &build_side {
                for probe_result in &probe_side {
                    if results.len() >= max_results { break 'outer; }
                    
                    let mut combined = if swapped {
                        probe_result.clone()
                    } else {
                        build_result.clone()
                    };
                    
                    if swapped {
                        combined.extend(build_result.clone());
                    } else {
                        combined.extend(probe_result.clone());
                    }
                    results.push(combined);
                }
            }
            return results;
        }

        // Build hash table in parallel with explicit type annotation
        let hash_table: HashMap<Vec<u32>, Vec<BTreeMap<String, u32>>> = build_side
            .into_par_iter()
            .fold(
                || HashMap::<Vec<u32>, Vec<BTreeMap<String, u32>>>::new(),
                |mut acc: HashMap<Vec<u32>, Vec<BTreeMap<String, u32>>>, result| {
                    let join_key: Vec<u32> = join_vars.iter()
                        .filter_map(|var| result.get(*var).copied())
                        .collect();
                    
                    if join_key.len() == join_vars.len() {
                        acc.entry(join_key).or_default().push(result);
                    }
                    acc
                },
            )
            .reduce(
                || HashMap::<Vec<u32>, Vec<BTreeMap<String, u32>>>::new(),
                |mut acc1, acc2| {
                    for (key, mut values) in acc2 {
                        acc1.entry(key).or_default().append(&mut values);
                    }
                    acc1
                },
            );

        // Probe phase in parallel
        probe_side
            .into_par_iter()
            .flat_map(|probe_result| {
                let probe_key: Vec<u32> = join_vars.iter()
                    .filter_map(|var| probe_result.get(*var).copied())
                    .collect();
                
                if probe_key.len() == join_vars.len() {
                    if let Some(matching_builds) = hash_table.get(&probe_key) {
                        matching_builds
                            .iter()
                            .map(|build_result| {
                                let mut combined = if swapped {
                                    probe_result.clone()
                                } else {
                                    build_result.clone()
                                };
                                
                                if swapped {
                                    combined.extend(build_result.clone());
                                } else {
                                    combined.extend(probe_result.clone());
                                }
                                combined
                            })
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            })
            .collect()
    }

    // Original hash join
    fn execute_hash_join_with_ids(
        &self,
        left_results: Vec<BTreeMap<String, u32>>,
        right_results: Vec<BTreeMap<String, u32>>,
    ) -> Vec<BTreeMap<String, u32>> {
        if left_results.is_empty() || right_results.is_empty() {
            return Vec::new();
        }

        // Determine join variables more efficiently
        let left_vars: HashSet<&String> = left_results[0].keys().collect();
        let right_vars: HashSet<&String> = right_results[0].keys().collect();
        let join_vars: Vec<&String> = left_vars.intersection(&right_vars).copied().collect();

        if join_vars.is_empty() {
            // Cartesian product with size limit
            let max_cartesian = 100_000;
            let mut results = Vec::new();
            
            for (i, left_result) in left_results.iter().enumerate() {
                if i * right_results.len() > max_cartesian { break; }
                
                for right_result in &right_results {
                    let mut combined = left_result.clone();
                    combined.extend(right_result.clone());
                    results.push(combined);
                    
                    if results.len() > max_cartesian { break; }
                }
                if results.len() > max_cartesian { break; }
            }
            return results;
        }

        // Create a hash table for the left side, keyed by the join variable
        let mut hash_table: HashMap<Vec<u32>, Vec<BTreeMap<String, u32>>> = HashMap::new();

        // Build hash table
        for left_result in &left_results {
            let join_key: Vec<u32> = join_vars.iter()
                .filter_map(|var| left_result.get(*var).copied())
                .collect();
            
            if join_key.len() == join_vars.len() {
                hash_table
                    .entry(join_key)
                    .or_insert_with(Vec::new)
                    .push(left_result.clone());
            }
        }

        // Probe phase with parallel processing
        let mut results = Vec::new();

        for right_result in &right_results {
            let join_key: Vec<u32> = join_vars.iter()
                .filter_map(|var| right_result.get(*var).copied())
                .collect();
            
            if join_key.len() == join_vars.len() {
                if let Some(matching_lefts) = hash_table.get(&join_key) {
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

    // Fixed nested loop join
    fn execute_nested_loop_join_with_ids(
        &self,
        left_results: Vec<BTreeMap<String, u32>>,
        right_results: Vec<BTreeMap<String, u32>>,
    ) -> Vec<BTreeMap<String, u32>> {
        let mut results = Vec::new();
        for left_result in &left_results {
            for right_result in &right_results {
                if self.can_join_with_ids(left_result, right_result) {
                    let mut combined = left_result.clone();
                    combined.extend(right_result.clone());
                    results.push(combined);
                }
            }
        }
        results
    }

    fn execute_parallel_join_with_ids(
        &self,
        left: &PhysicalOperator,
        right: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<BTreeMap<String, u32>> {
        // Execute the left operator to get final_results
        let id_results = left.execute_with_ids(database);

        // Try to extract the pattern from the right operator
        if let Some(pattern) = extract_pattern(right) {
            // Extract variables and predicate from the pattern
            let subject_var = pattern.subject.as_deref().unwrap_or("").to_string();
            let predicate = pattern.predicate.as_deref().unwrap_or("").to_string();
            let object_var = pattern.object.as_deref().unwrap_or("").to_string();

            // Convert ID results to string results only for the efficient join method
            let string_results: Vec<BTreeMap<String, String>> = id_results
                .into_par_iter()
                .map(|id_result| {
                    id_result
                        .into_iter()
                        .map(|(var, id)| {
                            (var, database.dictionary.decode(id).unwrap().to_string())
                        })
                        .collect()
                })
                .collect();

            let triples_vec: Vec<Triple> = database.triples.iter().cloned().collect();
            
            // Use efficient join
            let results = database.perform_join_par_simd_with_strict_filter_4_redesigned_streaming(
                subject_var,
                predicate,
                object_var,
                triples_vec,
                &database.dictionary,
                string_results,
                None,
            );

            // Pre-encode all unique values to avoid mutable borrow in closure
            let mut all_values = std::collections::HashSet::new();
            for result in &results {
                for (_, value) in result {
                    all_values.insert(value.clone());
                }
            }
            
            // Create a mapping from string to ID
            let mut value_to_id = HashMap::new();
            for value in all_values {
                let id = database.dictionary.encode(&value);
                value_to_id.insert(value, id);
            }

            // Convert back to ID results using the pre-computed mapping
            results
                .into_par_iter()
                .map(|string_result| {
                    string_result
                        .into_iter()
                        .map(|(var, value)| {
                            (var, *value_to_id.get(&value).unwrap())
                        })
                        .collect()
                })
                .collect()
        } else {
            self.execute_optimized_hash_join_with_ids(
                left.execute_with_ids(database), 
                right.execute_with_ids(database)
            )
        }
    }

    fn can_join_with_ids(
        &self,
        left_result: &BTreeMap<String, u32>,
        right_result: &BTreeMap<String, u32>,
    ) -> bool {
        for (key, left_id) in left_result {
            if let Some(right_id) = right_result.get(key) {
                if left_id != right_id {
                    return false;
                }
            }
        }
        true
    }

    // --- Specialized IndexScan execution ---
    fn execute_index_scan_with_ids(
        &self,
        database: &mut SparqlDatabase,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        // Convert pattern to IDs for faster matching
        let subject_id = pattern.subject.as_ref().filter(|s| !s.starts_with('?'))
            .map(|const_s| database.dictionary.encode(const_s));
        let predicate_id = pattern.predicate.as_ref().filter(|p| !p.starts_with('?'))
            .map(|const_p| database.dictionary.encode(const_p));
        let object_id = pattern.object.as_ref().filter(|o| !o.starts_with('?'))
            .map(|const_o| database.dictionary.encode(const_o));

        match (subject_id, predicate_id, object_id) {
            // (S, P, -) => use SubjectPredicate index
            (Some(s), Some(p), None) => {
                self.scan_sp_index_with_ids(database, s, p, pattern)
            }

            // (S, -, O) => use SubjectObject index
            (Some(s), None, Some(o)) => {
                self.scan_so_index_with_ids(database, s, o, pattern)
            }

            // (-, P, O) => use PredicateObject index
            (None, Some(p), Some(o)) => {
                self.scan_po_index_with_ids(database, p, o, pattern)
            }

            // (S, P, O) => fully bound; direct membership check
            (Some(s), Some(p), Some(o)) => {
                let triple = Triple { subject: s, predicate: p, object: o };
                if database.triples.contains(&triple) {
                    let mut row = BTreeMap::new();
                    if let Some(subj_str) = &pattern.subject {
                        if subj_str.starts_with('?') {
                            row.insert(subj_str.clone(), s);
                        }
                    }
                    if let Some(pred_str) = &pattern.predicate {
                        if pred_str.starts_with('?') {
                            row.insert(pred_str.clone(), p);
                        }
                    }
                    if let Some(obj_str) = &pattern.object {
                        if obj_str.starts_with('?') {
                            row.insert(obj_str.clone(), o);
                        }
                    }
                    vec![row]
                } else {
                    vec![]
                }
            }

            // Only subject bound
            (Some(s), None, None) => {
                self.scan_s_index_with_ids(database, s, pattern)
            }
            // Only predicate bound
            (None, Some(p), None) => {
                self.scan_p_index_with_ids(database, p, pattern)
            }
            // Only object bound
            (None, None, Some(o)) => {
                self.scan_o_index_with_ids(database, o, pattern)
            }
            // Nothing bound => fallback to table scan
            _ => {
                self.execute_table_scan_with_ids(database, pattern)
            }
        }
    }

    /// Parallel SubjectPredicate index scan
    fn scan_sp_index_with_ids(
        &self,
        database: &SparqlDatabase,
        s: u32,
        p: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(objects) = database.index_manager.scan_sp(s, p) {
            let objects_vec: Vec<u32> = objects.iter().cloned().collect();
            
            objects_vec.into_par_iter().map(|obj| {
                let mut row = BTreeMap::new();
                
                if let Some(subj_var) = &pattern.subject {
                    if subj_var.starts_with('?') {
                        row.insert(subj_var.clone(), s);
                    }
                }
                if let Some(pred_var) = &pattern.predicate {
                    if pred_var.starts_with('?') {
                        row.insert(pred_var.clone(), p);
                    }
                }
                if let Some(obj_var) = &pattern.object {
                    if obj_var.starts_with('?') {
                        row.insert(obj_var.clone(), obj);
                    }
                }
                
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel SubjectObject index scan
    fn scan_so_index_with_ids(
        &self,
        database: &SparqlDatabase,
        s: u32,
        o: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(predicates) = database.index_manager.scan_so(s, o) {
            let predicates_vec: Vec<u32> = predicates.iter().cloned().collect();
            
            predicates_vec.into_par_iter().map(|pred| {
                let mut row = BTreeMap::new();
                
                if let Some(subj_var) = &pattern.subject {
                    if subj_var.starts_with('?') {
                        row.insert(subj_var.clone(), s);
                    }
                }
                if let Some(pred_var) = &pattern.predicate {
                    if pred_var.starts_with('?') {
                        row.insert(pred_var.clone(), pred);
                    }
                }
                if let Some(obj_var) = &pattern.object {
                    if obj_var.starts_with('?') {
                        row.insert(obj_var.clone(), o);
                    }
                }
                
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel PredicateObject index scan
    fn scan_po_index_with_ids(
        &self,
        database: &SparqlDatabase,
        p: u32,
        o: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(subjects) = database.index_manager.scan_po(p, o) {
            let subjects_vec: Vec<u32> = subjects.iter().cloned().collect();
            
            subjects_vec.into_par_iter().map(|subj| {
                let mut row = BTreeMap::new();
                
                if let Some(subj_var) = &pattern.subject {
                    if subj_var.starts_with('?') {
                        row.insert(subj_var.clone(), subj);
                    }
                }
                if let Some(pred_var) = &pattern.predicate {
                    if pred_var.starts_with('?') {
                        row.insert(pred_var.clone(), p);
                    }
                }
                if let Some(obj_var) = &pattern.object {
                    if obj_var.starts_with('?') {
                        row.insert(obj_var.clone(), o);
                    }
                }
                
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel Subject-only index scan
    fn scan_s_index_with_ids(
        &self,
        database: &mut SparqlDatabase,
        s: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(pred_map) = database.index_manager.spo.get(&s) {
            let pred_obj_pairs: Vec<(u32, u32)> = pred_map
                .iter()
                .flat_map(|(&pred, objects)| {
                    if let Some(ref p_str) = pattern.predicate {
                        if !p_str.starts_with('?') {
                            let bound_p = database.dictionary.encode(p_str);
                            if pred != bound_p {
                                return Vec::new();
                            }
                        }
                    }
                    
                    objects.iter().filter_map(|&obj| {
                        if let Some(ref o_str) = pattern.object {
                            if !o_str.starts_with('?') {
                                let bound_o = database.dictionary.encode(o_str);
                                if obj != bound_o {
                                    return None;
                                }
                            }
                        }
                        Some((pred, obj))
                    }).collect()
                })
                .collect();

            pred_obj_pairs.into_par_iter().map(|(pred, obj)| {
                let mut row = BTreeMap::new();
                
                if let Some(s_var) = &pattern.subject {
                    if s_var.starts_with('?') {
                        row.insert(s_var.clone(), s);
                    }
                }
                
                if let Some(p_var) = &pattern.predicate {
                    if p_var.starts_with('?') {
                        row.insert(p_var.clone(), pred);
                    }
                }
                
                if let Some(o_var) = &pattern.object {
                    if o_var.starts_with('?') {
                        row.insert(o_var.clone(), obj);
                    }
                }
                
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel Predicate-only index scan
    fn scan_p_index_with_ids(
        &self,
        database: &mut SparqlDatabase,
        p: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(subj_map) = database.index_manager.pso.get(&p) {
            let subj_obj_pairs: Vec<(u32, u32)> = subj_map
                .iter()
                .flat_map(|(&subj, objects)| {
                    if let Some(ref s_str) = pattern.subject {
                        if !s_str.starts_with('?') {
                            let bound_s = database.dictionary.encode(s_str);
                            if subj != bound_s {
                                return Vec::new();
                            }
                        }
                    }
                    
                    objects.iter().filter_map(|&obj| {
                        if let Some(ref o_str) = pattern.object {
                            if !o_str.starts_with('?') {
                                let bound_o = database.dictionary.encode(o_str);
                                if obj != bound_o {
                                    return None;
                                }
                            }
                        }
                        Some((subj, obj))
                    }).collect()
                })
                .collect();

            subj_obj_pairs.into_par_iter().map(|(subj, obj)| {
                let mut row = BTreeMap::new();
                
                if let Some(s_var) = &pattern.subject {
                    if s_var.starts_with('?') {
                        row.insert(s_var.clone(), subj);
                    }
                }
                
                if let Some(p_var) = &pattern.predicate {
                    if p_var.starts_with('?') {
                        row.insert(p_var.clone(), p);
                    }
                }
                
                if let Some(o_var) = &pattern.object {
                    if o_var.starts_with('?') {
                        row.insert(o_var.clone(), obj);
                    }
                }
                
                row
            }).collect()
        } else {
            Vec::new()
        }
    }

    /// Parallel Object-only index scan
    fn scan_o_index_with_ids(
        &self,
        database: &mut SparqlDatabase,
        o: u32,
        pattern: &TriplePattern,
    ) -> Vec<BTreeMap<String, u32>> {
        if let Some(pred_map) = database.index_manager.ops.get(&o) {
            let pred_subj_pairs: Vec<(u32, u32)> = pred_map
                .iter()
                .flat_map(|(&pred, subjects)| {
                    if let Some(ref p_str) = pattern.predicate {
                        if !p_str.starts_with('?') {
                            let bound_p = database.dictionary.encode(p_str);
                            if pred != bound_p {
                                return Vec::new();
                            }
                        }
                    }
                    
                    subjects.iter().filter_map(|&subj| {
                        if let Some(ref s_str) = pattern.subject {
                            if !s_str.starts_with('?') {
                                let bound_s = database.dictionary.encode(s_str);
                                if subj != bound_s {
                                    return None;
                                }
                            }
                        }
                        Some((pred, subj))
                    }).collect()
                })
                .collect();

            pred_subj_pairs.into_par_iter().map(|(pred, subj)| {
                let mut row = BTreeMap::new();
                
                if let Some(s_var) = &pattern.subject {
                    if s_var.starts_with('?') {
                        row.insert(s_var.clone(), subj);
                    }
                }
                
                if let Some(p_var) = &pattern.predicate {
                    if p_var.starts_with('?') {
                        row.insert(p_var.clone(), pred);
                    }
                }
                
                if let Some(o_var) = &pattern.object {
                    if o_var.starts_with('?') {
                        row.insert(o_var.clone(), o);
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

    // New method to evaluate conditions with IDs
    pub fn evaluate_with_ids(&self, result: &BTreeMap<String, u32>, dictionary: &Dictionary) -> bool {
        if let Some(&id) = result.get(&self.variable) {
            // Only decode when necessary for comparison
            let decoded_value = dictionary.decode(id).unwrap();
            match self.operator.as_str() {
                "=" => decoded_value == self.value,
                "!=" => decoded_value != self.value,
                ">" => decoded_value.parse::<i32>().unwrap_or(0) > self.value.parse::<i32>().unwrap_or(0),
                ">=" => decoded_value.parse::<i32>().unwrap_or(0) >= self.value.parse::<i32>().unwrap_or(0),
                "<" => decoded_value.parse::<i32>().unwrap_or(0) < self.value.parse::<i32>().unwrap_or(0),
                "<=" => decoded_value.parse::<i32>().unwrap_or(0) <= self.value.parse::<i32>().unwrap_or(0),
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

// Enhanced logical plan builder with better join ordering
pub fn build_logical_plan_optimized(
    variables: Vec<(&str, &str)>,
    patterns: Vec<(&str, &str, &str)>,
    filters: Vec<FilterExpression>,
    prefixes: &HashMap<String, String>,
    database: &SparqlDatabase,
) -> LogicalOperator {
    // Create scan operators with immediate filter pushdown
    let mut scan_operators = Vec::new();
    
    for (subject_var, predicate, object_var) in patterns {
        let resolved_subject = database.resolve_query_term(subject_var, prefixes);
        let resolved_predicate = database.resolve_query_term(predicate, prefixes);
        let resolved_object = database.resolve_query_term(object_var, prefixes);

        let pattern = TriplePattern {
            subject: Some(resolved_subject),
            predicate: Some(resolved_predicate),
            object: Some(resolved_object),
        };
        
        let mut scan_op = LogicalOperator::Scan { pattern: pattern.clone() };

        // Push applicable filters down to this scan
        for filter in &filters {
            if let FilterExpression::Comparison(var, operator, value) = filter {
                if pattern_contains_variable(&pattern, var) {
                    let condition = Condition {
                        variable: var.to_string(),
                        operator: operator.to_string(),
                        value: value.to_string(),
                    };
                    scan_op = LogicalOperator::Selection {
                        predicate: Box::new(scan_op),
                        condition,
                    };
                }
            }
        }
        
        scan_operators.push(scan_op);
    }

    // Sort by estimated selectivity (most selective first)
    scan_operators.sort_by_key(|op| estimate_operator_selectivity(op, database));

    // Build optimal join tree
    let mut current_op = scan_operators[0].clone();
    for next_op in scan_operators.into_iter().skip(1) {
        current_op = LogicalOperator::Join {
            left: Box::new(current_op),
            right: Box::new(next_op),
        };
    }

    // Final projection
    let selected_vars: Vec<String> = variables
        .iter()
        .filter(|(agg_type, _)| *agg_type == "VAR")
        .map(|(_, var)| var.to_string())
        .collect();

    LogicalOperator::Projection {
        predicate: Box::new(current_op),
        variables: selected_vars,
    }
}

// Helper functions
fn pattern_contains_variable(pattern: &TriplePattern, var: &str) -> bool {
    [&pattern.subject, &pattern.predicate, &pattern.object]
        .iter()
        .any(|field| field.as_ref().map_or(false, |v| v == var))
}

fn estimate_operator_selectivity(op: &LogicalOperator, database: &SparqlDatabase) -> u64 {
    match op {
        LogicalOperator::Scan { pattern } => {
            let bound_count = [&pattern.subject, &pattern.predicate, &pattern.object]
                .iter()
                .filter(|x| x.as_ref().map(|v| !v.starts_with('?')).unwrap_or(false))
                .count();
            
            match bound_count {
                3 => 1,     // Highest priority
                2 => 2,
                1 => 3,
                0 => 4,     // Lowest priority
                _ => 5,
            }
        }
        LogicalOperator::Selection { predicate, .. } => {
            estimate_operator_selectivity(predicate, database) / 2 // Selections are more selective
        }
        _ => 999,
    }
}

// Keep the original build_logical_plan for compatibility
pub fn build_logical_plan(
    variables: Vec<(&str, &str)>,
    patterns: Vec<(&str, &str, &str)>,
    filters: Vec<FilterExpression>,
    prefixes: &HashMap<String, String>,
    database: &SparqlDatabase,
) -> LogicalOperator {
    build_logical_plan_optimized(variables, patterns, filters, prefixes, database)
}
