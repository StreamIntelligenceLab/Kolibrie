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
use super::super::stats::DatabaseStats;
use super::super::types::Condition;
use shared::terms::{Term, TriplePattern};

/// Cost estimation constants for different operators
pub struct CostConstants;

impl CostConstants {
    pub const COST_PER_ROW_SCAN: u64 = 100;
    pub const COST_PER_ROW_INDEX_SCAN: u64 = 1;
    pub const COST_PER_FILTER: u64 = 1;
    pub const COST_PER_ROW_JOIN: u64 = 2;
    pub const COST_PER_ROW_NESTED_LOOP: u64 = 10;
    pub const COST_PER_PROJECTION: u64 = 1;
    pub const COST_PER_ROW_OPTIMIZED_JOIN: u64 = 1;
}

/// Cost estimator for query optimization
pub struct CostEstimator<'a> {
    stats: &'a DatabaseStats,
}

impl<'a> CostEstimator<'a> {
    /// Creates a new cost estimator with the given statistics
    pub fn new(stats: &'a DatabaseStats) -> Self {
        Self { stats }
    }

    /// Estimates the cost of executing a physical operator
    pub fn estimate_cost(&self, plan: &PhysicalOperator) -> u64 {
        match plan {
            PhysicalOperator::TableScan { pattern } => {
                self.estimate_cardinality(pattern) * CostConstants::COST_PER_ROW_SCAN
            }
            PhysicalOperator::IndexScan { pattern } => {
                let cardinality = self.estimate_cardinality(pattern);
                let bound_count = self.count_bound_variables(pattern);

                let discount = match bound_count {
                    0 => 1,    // No discount for unbounded scan
                    1 => 10,   // 10x better for one bound field
                    2 => 100,  // 100x better for two bound fields
                    3 => 1000, // 1000x better for fully bound
                    _ => 1,
                };

                (cardinality * CostConstants::COST_PER_ROW_INDEX_SCAN) / discount
            }
            PhysicalOperator::Filter { input, condition } => {
                let input_cost = self.estimate_cost(input);
                let selectivity = self.estimate_selectivity(condition);
                (input_cost as f64 * selectivity) as u64 + CostConstants::COST_PER_FILTER
            }
            PhysicalOperator::OptimizedHashJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost
                    + right_cost
                    + (left_cardinality + right_cardinality)
                        * CostConstants::COST_PER_ROW_OPTIMIZED_JOIN
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost
                    + right_cost
                    + (left_cardinality + right_cardinality) * CostConstants::COST_PER_ROW_JOIN
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost
                    + right_cost
                    + (left_cardinality * right_cardinality)
                        * CostConstants::COST_PER_ROW_NESTED_LOOP
            }
            PhysicalOperator::ParallelJoin { left, right } => {
                // Check if we can use efficient join optimization
                if self.can_use_efficient_join(right) {
                    let left_cost = self.estimate_cost(left);
                    let left_cardinality = self.estimate_output_cardinality(left);
                    // Massive discount for efficient join
                    left_cost + (left_cardinality * CostConstants::COST_PER_ROW_JOIN / 20)
                } else {
                    let left_cost = self.estimate_cost(left);
                    let right_cost = self.estimate_cost(right);
                    let left_cardinality = self.estimate_output_cardinality(left);
                    let right_cardinality = self.estimate_output_cardinality(right);

                    left_cost
                        + right_cost
                        + (left_cardinality + right_cardinality) * CostConstants::COST_PER_ROW_JOIN
                            / 2
                }
            }
            PhysicalOperator::Projection { input, .. } => {
                self.estimate_cost(input) + CostConstants::COST_PER_PROJECTION
            }
        }
    }

    /// Estimates the cardinality of a triple pattern
    pub fn estimate_cardinality(&self, pattern: &TriplePattern) -> u64 {
        let mut base_cardinality = self.stats.total_triples;

        // Count bound variables for better selectivity estimation
        let bound_count = self.count_bound_variables(pattern);

        // More aggressive selectivity based on bound variables
        let selectivity = match bound_count {
            0 => 1.0,     // No filtering
            1 => 0.01,    // More selective for one bound field
            2 => 0.0001,  // Very selective for two bound fields
            3 => 0.00001, // Extremely selective for fully bound
            _ => 1.0,
        };

        // Use predicate cardinality if available and more specific
        if let (_, Term::Constant(predicate), _) = pattern {
            if let Some(&predicate_cardinality) = self.stats.predicate_cardinalities.get(predicate)
            {
                // Use the more restrictive estimate
                base_cardinality = base_cardinality.min(predicate_cardinality);
            }
        }
        if let (Term::Constant(subject), _, _) = pattern {
            if let Some(&subject_cardinality) = self.stats.subject_cardinalities.get(subject) {
                base_cardinality = base_cardinality.min(subject_cardinality);
            }
        }
        if let (_, _, Term::Constant(object)) = pattern {
            if let Some(&object_cardinality) = self.stats.object_cardinalities.get(object) {
                base_cardinality = base_cardinality.min(object_cardinality);
            }
        }

        ((base_cardinality as f64 * selectivity) as u64).max(1)
    }

    /// Estimates the selectivity of a condition
    pub fn estimate_selectivity(&self, condition: &Condition) -> f64 {
        match condition.operator.as_str() {
            "=" => 0.05, // More selective
            "!=" => 0.95,
            ">" | "<" | ">=" | "<=" => 0.25,
            _ => 1.0,
        }
    }

    /// Estimates the output cardinality of a physical operator
    pub fn estimate_output_cardinality(&self, plan: &PhysicalOperator) -> u64 {
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

    /// Counts the number of bound variables in a triple pattern
    fn count_bound_variables(&self, pattern: &TriplePattern) -> usize {
        let mut count = 0;

        match pattern.0 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) => {}
        }

        match pattern.1 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) => {}
        }

        match pattern.2 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) => {}
        }

        count
    }

    /// Checks if efficient join optimization can be used
    fn can_use_efficient_join(&self, operator: &PhysicalOperator) -> bool {
        matches!(
            operator,
            PhysicalOperator::TableScan { .. } | PhysicalOperator::IndexScan { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::terms::{Term, TriplePattern};

    fn create_test_stats() -> DatabaseStats {
        let mut stats = DatabaseStats::new();
        stats.total_triples = 1000;
        stats
    }

    #[test]
    fn test_count_bound_variables_all_vars() {
        let stats = create_test_stats();
        let estimator = CostEstimator::new(&stats);
        let pattern = (
            Term::Variable("s".to_string()),
            Term::Variable("p".to_string()),
            Term::Variable("o".to_string()),
        );
        assert_eq!(estimator.count_bound_variables(&pattern), 0);
    }

    #[test]
    fn test_count_bound_variables_some_vars() {
        let stats = create_test_stats();
        let estimator = CostEstimator::new(&stats);
        let pattern = (
            Term::Constant(1),
            Term::Variable("p".to_string()),
            Term::Variable("o".to_string()),
        );
        assert_eq!(estimator.count_bound_variables(&pattern), 1);
    }

    #[test]
    fn test_count_bound_variables_no_vars() {
        let stats = create_test_stats();
        let estimator = CostEstimator::new(&stats);
        let pattern = (Term::Constant(1), Term::Constant(2), Term::Constant(3));
        assert_eq!(estimator.count_bound_variables(&pattern), 3);
    }
}
