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
use shared::query::FilterExpression;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Volcano-style query optimizer with cost-based optimization
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: Arc<DatabaseStats>,
}

impl VolcanoOptimizer {
    /// Creates a new volcano optimizer
    pub fn new(database: &SparqlDatabase) -> Self {
        let stats = Arc::new(DatabaseStats::gather_stats_fast(database));
        Self {
            memo: HashMap::new(),
            selected_variables: Vec::new(),
            stats,
        }
    }

    pub fn with_cached_stats(stats: Arc<DatabaseStats>) -> Self {
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
    ) -> Vec<HashMap<String, String>> {
        ExecutionEngine::execute(plan, database)
    }

    /// Optimizes and executes a logical plan in one step
    pub fn optimize_and_execute(
        &mut self,
        logical_plan: &LogicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<HashMap<String, String>> {
        let physical_plan = self.find_best_plan(logical_plan);
        self.execute_plan(&physical_plan, database)
    }

    /// Detects if a join tree is a star query pattern
    fn is_star_query(&self, plan: &LogicalOperator) -> Option<Vec<(String, Vec<TriplePattern>)>> {
        let mut patterns = Vec::new();
        self.collect_patterns(plan, &mut patterns);

        if patterns.len() < 3 {
            return None;
        }

        // Count how many patterns each variable appears
        let mut var_counts: std::collections::BTreeMap<String, Vec<usize>> = BTreeMap::new();

        for (idx, pattern) in patterns.iter().enumerate() {
            if let Term::Variable(var) = &pattern.0 {
                var_counts.entry(var.clone()).or_default().push(idx);
            }
            if let Term::Variable(var) = &pattern.1 {
                var_counts.entry(var.clone()).or_default().push(idx);
            }
            if let Term::Variable(var) = &pattern.2 {
                var_counts.entry(var.clone()).or_default().push(idx);
            }
        }

        // Find all variables that appear in 2+ patterns
        let mut star_vars: Vec<(&String, &Vec<usize>)> = var_counts
            .iter()
            .filter(|(_, indices)| indices.len() >= 2)  // <- Lowered from 3 to 2
            .collect();

        // Sort by number of occurrences (most frequent first)
        star_vars.sort_by_key(|(_, indices)| std::cmp::Reverse(indices.len()));

        if star_vars.is_empty() {
            return None;
        }

        // Greedily assign patterns to stars
        let mut used_patterns: HashSet<usize> = HashSet::new();
        let mut stars: Vec<(String, Vec<TriplePattern>)> = Vec::new();

        for (var, pattern_indices) in star_vars {
            // Get patterns for this variable that haven't been used yet
            let available: Vec<usize> = pattern_indices
                .iter()
                .filter(|&&idx| !used_patterns.contains(&idx))
                .copied()
                .collect();

            if available.len() >= 2 {  // Need at least 2 patterns for a star
                let star_patterns: Vec<TriplePattern> = available
                    .iter()
                    .map(|&idx| patterns[idx].clone())
                    .collect();

                // Mark these patterns as used
                for &idx in &available {
                    used_patterns.insert(idx);
                }

                stars.push((var.clone(), star_patterns));
            }
        }

        if stars.is_empty() {
            None
        } else {
            Some(stars)
        }
    }

    fn collect_patterns(&self, plan: &LogicalOperator, patterns: &mut Vec<TriplePattern>) {
        match plan {
            LogicalOperator::Scan { pattern } => {
                patterns.push(pattern.clone());
            }
            LogicalOperator::Join { left, right } => {
                self.collect_patterns(left, patterns);
                self.collect_patterns(right, patterns);
            }
            LogicalOperator::Selection { predicate, ..  } => {
                self.collect_patterns(predicate, patterns);
            }
            LogicalOperator::Projection { predicate, .. } => {
                self.collect_patterns(predicate, patterns);
            }
            LogicalOperator::Subquery { inner, .. } => {
                // Subqueries are treated as separate scopes, so we don't collect their patterns
                // for star query detection in the outer query
                self.collect_patterns(inner, patterns);
            }
        }
    }

    /// Recursively finds the best plan using dynamic programming with memoization
    fn find_best_plan_recursive(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator {
        let key = self.create_memo_key(logical_plan);

        if let Some(plan) = self.memo.get(&key) {
            return plan.clone();
        }

        if let LogicalOperator::Projection { predicate: proj_pred, variables } = logical_plan {
            if let LogicalOperator::Selection { predicate: sel_pred, condition } = proj_pred.as_ref() {
                if let Some(stars) = self.is_star_query(sel_pred) {
                    // Build: Projection(Filter(StarJoin))
                    let star_plan = self.build_star_join_from_patterns(stars, sel_pred);
                    let filtered_plan = PhysicalOperator::filter(star_plan, condition.clone());
                    let projected_plan = PhysicalOperator::projection(filtered_plan, variables.clone());
                    self.memo.insert(key, projected_plan.clone());
                    return projected_plan;
                }
            }
        }

        // Handle Selection wrapping star query (no projection)
        if let LogicalOperator::Selection { predicate, condition } = logical_plan {
            if let Some(stars) = self.is_star_query(predicate) {
                let star_plan = self.build_star_join_from_patterns(stars, predicate);
                let filtered_plan = PhysicalOperator::filter(star_plan, condition.clone());
                self.memo.insert(key, filtered_plan.clone());
                return filtered_plan;
            }
        }

        // Handle star query without selection or projection
        if ! matches!(logical_plan, LogicalOperator::Selection { .. } | LogicalOperator::Projection { ..  }) {
            if let Some(stars) = self.is_star_query(logical_plan) {
                let star_plan = self.build_star_join_from_patterns(stars, logical_plan);
                self.memo.insert(key, star_plan.clone());
                return star_plan;
            }
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
            LogicalOperator::Subquery { inner, projected_vars } => {
                // Recursively optimize the inner query
                let optimized_inner = self.find_best_plan_recursive(inner);
                
                // Wrap it in a subquery operator with projection
                let subquery_plan = PhysicalOperator::subquery(
                    optimized_inner,
                    projected_vars.clone()
                );
                
                candidates.push(subquery_plan);
            }
        }

        // Cost-based optimization: Choose the best candidate
        let cost_estimator = CostEstimator::new(&self.stats);
        let best_plan = candidates
            .into_iter()
            .min_by_key(|plan| {
                let cost = cost_estimator.estimate_cost(plan);
                cost
            })
            .unwrap();

        // Memoize the best plan
        self.memo.insert(key, best_plan.clone());
        best_plan
    }

    /// Helper method to build a star join physical plan from detected star patterns
    fn build_star_join_from_patterns(
        &mut self,
        stars: Vec<(String, Vec<TriplePattern>)>,
        logical_plan: &LogicalOperator,
    ) -> PhysicalOperator {
        let mut all_patterns = Vec::new();
        self.collect_patterns(logical_plan, &mut all_patterns);

        let mut used_pattern_indices: HashSet<usize> = HashSet::new();
        for (_, star_patterns) in &stars {
            for star_pattern in star_patterns {
                if let Some(idx) = all_patterns.iter().position(|p| p == star_pattern) {
                    used_pattern_indices.insert(idx);
                }
            }
        }

        if stars.len() > 1 {
            let mut star_operators: Vec<(String, Vec<TriplePattern>)> = stars;

            star_operators.sort_by_key(|(_, patterns)| {
                let bound_count = patterns.iter().filter(|p| {
                    matches!(p.0, Term::Constant(_)) ||
                    matches!(p.1, Term::Constant(_)) ||
                    matches!(p.2, Term::Constant(_))
                }).count();
                std::cmp::Reverse(bound_count)
            });

            let (first_var, first_patterns) = star_operators.remove(0);
            let mut result = PhysicalOperator::StarJoin {
                join_var: first_var.clone(),
                patterns: first_patterns,
            };

            for (_, patterns) in star_operators {
                let star_scans: Vec<PhysicalOperator> = patterns
                    .into_iter()
                    .map(|pattern| PhysicalOperator::index_scan(pattern))
                    .collect();

                for scan in star_scans {
                    result = PhysicalOperator::parallel_join(result, scan);
                }
            }

            for (idx, pattern) in all_patterns.iter().enumerate() {
                if !used_pattern_indices.contains(&idx) {
                    let scan = PhysicalOperator::index_scan(pattern.clone());
                    result = PhysicalOperator::parallel_join(result, scan);
                }
            }

            result
        } else if stars.len() == 1 {
            let (join_var, patterns) = stars.into_iter().next().unwrap();

            if used_pattern_indices.len() < all_patterns.len() {
                let mut result = PhysicalOperator::StarJoin { join_var, patterns };

                for (idx, pattern) in all_patterns.iter().enumerate() {
                    if !used_pattern_indices.contains(&idx) {
                        let scan = PhysicalOperator::index_scan(pattern.clone());
                        result = PhysicalOperator::parallel_join(result, scan);
                    }
                }

                result
            } else {
                PhysicalOperator::StarJoin { join_var, patterns }
            }
        } else {
            // Shouldn't happen, but return a dummy scan as fallback
            PhysicalOperator::table_scan((
                Term::Variable("?s".to_string()),
                Term::Variable("?p".to_string()),
                Term::Variable("?o".to_string()),
            ))
        }
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
                    "Selection([{}], {})",
                    self.serialize_logical_plan(predicate),
                    self.serialize_filter_expression(&condition.expression)
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
            LogicalOperator::Subquery { inner, projected_vars } => {
                format!(
                    "Subquery({:?},[{}])",
                    projected_vars,
                    self.serialize_logical_plan(inner)
                )
            }
        }
    }

    /// Serializes a filter expression to a string
    fn serialize_filter_expression(&self, expr: &FilterExpression) -> String {
        match expr {
            FilterExpression::Comparison(var, op, value) => {
                format!("{}{}'{}'", var, op, value)
            }
            FilterExpression::And(left, right) => {
                format!(
                    "({} AND {})",
                    self.serialize_filter_expression(left),
                    self.serialize_filter_expression(right)
                )
            }
            FilterExpression::Or(left, right) => {
                format!(
                    "({} OR {})",
                    self.serialize_filter_expression(left),
                    self.serialize_filter_expression(right)
                )
            }
            FilterExpression::Not(inner) => {
                format!("NOT({})", self.serialize_filter_expression(inner))
            }
            FilterExpression::ArithmeticExpr(expr) => {
                format!("ARITH({})", expr)
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
                let join_selectivity = self.estimate_join_selectivity(left, right);
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
            LogicalOperator::Subquery { inner, .. } => {
                // Subqueries have materialization cost
                let inner_cost = self.estimate_logical_cost(inner);
                let inner_card = self.estimate_output_cardinality_from_logical(inner);
                // Add materialization overhead (storing results)
                inner_cost + inner_card
            }
        }
    }

    /// Estimates join selectivity
    fn estimate_join_selectivity(&self, left: &LogicalOperator, right: &LogicalOperator) -> f64 {
        // Extract predicates from the join patterns
        let left_predicate = self.extract_predicate_from_plan(left);
        let right_predicate = self.extract_predicate_from_plan(right);

        // Use the actual join selectivity from database stats
        match (left_predicate, right_predicate) {
            (Some(pred), _) => self.stats.get_join_selectivity(pred),
            (None, Some(pred)) => self.stats.get_join_selectivity(pred),
            (None, None) => 0.1, // Fallback to default
        }
    }

    /// Extracts the predicate ID from a logical plan if it's a scan
    fn extract_predicate_from_plan(&self, plan: &LogicalOperator) -> Option<u32> {
        match plan {
            LogicalOperator::Scan { pattern } => {
                if let Term::Constant(pred_id) = pattern.1 {
                    Some(pred_id)
                } else {
                    None
                }
            }
            LogicalOperator::Join { left, ..  } => self.extract_predicate_from_plan(left),
            LogicalOperator::Selection { predicate, .. } => self.extract_predicate_from_plan(predicate),
            LogicalOperator::Projection { predicate, .. } => self.extract_predicate_from_plan(predicate),
            LogicalOperator::Subquery { inner, .. } => self.extract_predicate_from_plan(inner),
        }
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
                let join_selectivity = self.estimate_join_selectivity(left, right);
                ((left_card.min(right_card) as f64 * join_selectivity) as u64).max(1)
            }
            LogicalOperator::Subquery { inner, .. } => {
                self.estimate_output_cardinality_from_logical(inner)
            }
        }
    }

    /// Updates the optimizer's statistics
    pub fn update_stats(&mut self, database: &SparqlDatabase) {
        self.stats = Arc::new(DatabaseStats::gather_stats_fast(database));
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
