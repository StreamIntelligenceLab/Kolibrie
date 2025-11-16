/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::cost::CostEstimator;
use super::execution::ExecutionEngine;
use super::operators::{LogicalOperator, PhysicalOperator};
use super::stats::DatabaseStats;

use crate::sparql_database::SparqlDatabase;
use shared::terms::{Term, TriplePattern};
use std::collections::{BTreeMap, HashMap};

/// Volcano-style query optimizer with cost-based optimization
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}

impl VolcanoOptimizer {
    /// Creates a new volcano optimizer
    pub fn new(database: &SparqlDatabase) -> Self {
        let stats = DatabaseStats::gather_stats_fast(database);
        Self {
            memo: HashMap::new(),
            selected_variables: Vec::new(),
            stats,
        }
    }

    /// Finds the best physical plan for a logical plan
    pub fn find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator {
        self.find_best_plan_recursive(logical_plan)
    }

    /// Executes a physical plan and returns results
    pub fn execute_plan(
        &self,
        plan: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<BTreeMap<String, String>> {
        ExecutionEngine::execute(plan, database)
    }

    /// Optimizes and executes a logical plan in one step
    pub fn optimize_and_execute(
        &mut self,
        logical_plan: &LogicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<BTreeMap<String, String>> {
        let physical_plan = self.find_best_plan(logical_plan);
        self.execute_plan(&physical_plan, database)
    }

    /// Recursively finds the best plan using dynamic programming with memoization
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
                candidates.push(PhysicalOperator::filter(best_child_plan, condition.clone()));
            }
            LogicalOperator::Projection {
                predicate,
                variables,
            } => {
                let best_child_plan = self.find_best_plan_recursive(predicate);
                candidates.push(PhysicalOperator::projection(
                    best_child_plan,
                    variables.clone(),
                ));
            }
            LogicalOperator::Join { left, right } => {
                // Add join reordering based on cost
                let left_cost = self.estimate_logical_cost(left);
                let right_cost = self.estimate_logical_cost(right);

                let (cheaper_side, expensive_side) = if left_cost <= right_cost {
                    (left, right)
                } else {
                    (right, left) // Swap for better order
                };

                let best_left_plan = self.find_best_plan_recursive(cheaper_side);
                let best_right_plan = self.find_best_plan_recursive(expensive_side);

                // Implementation rules: Different join algorithms
                candidates.push(PhysicalOperator::optimized_hash_join(
                    best_left_plan.clone(),
                    best_right_plan.clone(),
                ));

                candidates.push(PhysicalOperator::hash_join(
                    best_left_plan.clone(),
                    best_right_plan.clone(),
                ));

                // Only use nested loop for small datasets
                let left_cardinality = self.estimate_output_cardinality_from_logical(cheaper_side);
                let right_cardinality =
                    self.estimate_output_cardinality_from_logical(expensive_side);

                if left_cardinality < 1000 && right_cardinality < 1000 {
                    candidates.push(PhysicalOperator::nested_loop_join(
                        best_left_plan.clone(),
                        best_right_plan.clone(),
                    ));
                }

                // Add parallel join option
                candidates.push(PhysicalOperator::parallel_join(
                    best_left_plan,
                    best_right_plan,
                ));
            }
        }

        // Cost-based optimization: Choose the best candidate
        let cost_estimator = CostEstimator::new(&self.stats);
        let best_plan = candidates
            .into_iter()
            .min_by_key(|plan| cost_estimator.estimate_cost(plan))
            .unwrap();

        // Memoize the best plan
        self.memo.insert(key, best_plan.clone());
        best_plan
    }

    /// Chooses the best scan method based on pattern selectivity
    fn choose_best_scan(&self, pattern: &TriplePattern) -> PhysicalOperator {
        let bound_vars = self.count_bound_variables(pattern);
        let cost_estimator = CostEstimator::new(&self.stats);
        let estimated_size = cost_estimator.estimate_cardinality(pattern);

        match bound_vars {
            3 => PhysicalOperator::index_scan(pattern.clone()), // Fully bound - always use index
            2 => PhysicalOperator::index_scan(pattern.clone()), // Two bounds - index is better
            1 => {
                // Use index if result set is small enough
                if estimated_size < 10000 {
                    PhysicalOperator::index_scan(pattern.clone())
                } else {
                    PhysicalOperator::table_scan(pattern.clone())
                }
            }
            0 => PhysicalOperator::table_scan(pattern.clone()), // Full scan
            _ => PhysicalOperator::table_scan(pattern.clone()),
        }
    }

    /// Counts the number of bound variables in a triple pattern
    fn count_bound_variables(&self, pattern: &TriplePattern) -> usize {
        let mut count = 0;

        match &pattern.0 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) => {}
        }

        match &pattern.1 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) => {}
        }

        match &pattern.2 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) => {}
        }

        count
    }

    /// Creates a memo key for caching optimized plans
    fn create_memo_key(&self, logical_plan: &LogicalOperator) -> String {
        self.serialize_logical_plan(logical_plan)
    }

    /// Serializes a logical plan to a string for memoization
    fn serialize_logical_plan(&self, plan: &LogicalOperator) -> String {
        match plan {
            LogicalOperator::Scan { pattern } => {
                format!("Scan({:?},{:?},{:?})", pattern.0, pattern.1, pattern.2)
            }
            LogicalOperator::Selection {
                predicate,
                condition,
            } => {
                format!(
                    "Selection({},{},{},[{}])",
                    condition.variable,
                    condition.operator,
                    condition.value,
                    self.serialize_logical_plan(predicate)
                )
            }
            LogicalOperator::Projection {
                predicate,
                variables,
            } => {
                format!(
                    "Projection({:?},[{}])",
                    variables,
                    self.serialize_logical_plan(predicate)
                )
            }
            LogicalOperator::Join { left, right } => {
                format!(
                    "Join([{}],[{}])",
                    self.serialize_logical_plan(left),
                    self.serialize_logical_plan(right)
                )
            }
        }
    }

    /// Estimates the cost of a logical plan
    fn estimate_logical_cost(&self, logical_plan: &LogicalOperator) -> u64 {
        let cost_estimator = CostEstimator::new(&self.stats);

        match logical_plan {
            LogicalOperator::Scan { pattern } => cost_estimator.estimate_cardinality(pattern),
            LogicalOperator::Join { left, right } => {
                let left_cost = self.estimate_logical_cost(left);
                let right_cost = self.estimate_logical_cost(right);
                let left_card = self.estimate_output_cardinality_from_logical(left);
                let right_card = self.estimate_output_cardinality_from_logical(right);

                // More sophisticated join cost estimation
                let join_selectivity = self.estimate_join_selectivity();
                left_cost + right_cost + ((left_card * right_card) as f64 * join_selectivity) as u64
            }
            LogicalOperator::Selection {
                predicate,
                condition,
            } => {
                let base_cost = self.estimate_logical_cost(predicate);
                let selectivity = cost_estimator.estimate_selectivity(condition);
                (base_cost as f64 * selectivity) as u64
            }
            LogicalOperator::Projection { predicate, .. } => self.estimate_logical_cost(predicate),
        }
    }

    /// Estimates join selectivity
    fn estimate_join_selectivity(&self) -> f64 {
        0.1
    }

    /// Estimates output cardinality from a logical plan
    fn estimate_output_cardinality_from_logical(&self, logical_plan: &LogicalOperator) -> u64 {
        let cost_estimator = CostEstimator::new(&self.stats);

        match logical_plan {
            LogicalOperator::Scan { pattern } => cost_estimator.estimate_cardinality(pattern),
            LogicalOperator::Selection {
                predicate,
                condition,
            } => {
                let base_card = self.estimate_output_cardinality_from_logical(predicate);
                let selectivity = cost_estimator.estimate_selectivity(condition);
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

    /// Updates the optimizer's statistics
    pub fn update_stats(&mut self, database: &SparqlDatabase) {
        self.stats = DatabaseStats::gather_stats_fast(database);
        self.memo.clear(); // Clear memo as stats have changed
    }

    /// Sets the selected variables for the query
    pub fn set_selected_variables(&mut self, variables: Vec<String>) {
        self.selected_variables = variables;
    }

    /// Gets the current statistics
    pub fn get_stats(&self) -> &DatabaseStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::terms::{Term, TriplePattern};

    fn create_test_optimizer() -> VolcanoOptimizer {
        // Create a mock database for testing
        let database = SparqlDatabase::new();
        VolcanoOptimizer::new(&database)
    }

    #[test]
    fn test_count_bound_variables_all_vars() {
        let optimizer = create_test_optimizer();
        let pattern = (
            Term::Variable("s".to_string()),
            Term::Variable("p".to_string()),
            Term::Variable("o".to_string()),
        );
        assert_eq!(optimizer.count_bound_variables(&pattern), 0);
    }

    #[test]
    fn test_count_bound_variables_some_vars() {
        let optimizer = create_test_optimizer();
        let pattern = (
            Term::Constant(1),
            Term::Variable("p".to_string()),
            Term::Variable("o".to_string()),
        );
        assert_eq!(optimizer.count_bound_variables(&pattern), 1);
    }

    #[test]
    fn test_count_bound_variables_no_vars() {
        let optimizer = create_test_optimizer();
        let pattern = (Term::Constant(1), Term::Constant(2), Term::Constant(3));
        assert_eq!(optimizer.count_bound_variables(&pattern), 3);
    }
}
