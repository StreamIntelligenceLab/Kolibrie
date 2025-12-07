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
use shared::query::FilterExpression;

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
            PhysicalOperator::StarJoin { patterns, .. } => {
                // Cost = scan most selective + filter rest
                let mut costs: Vec<u64> = patterns
                    .iter()
                    .map(|p| self.estimate_cardinality(p))
                    . collect();

                costs.sort();

                // Start with smallest, then check each remaining
                let base_cost = costs[0] * CostConstants::COST_PER_ROW_INDEX_SCAN;
                let filter_cost = costs. iter().skip(1).sum::<u64>() * CostConstants::COST_PER_ROW_INDEX_SCAN / 10;

                base_cost + filter_cost
            }
        }
    }

    /// Estimates the cardinality of a triple pattern
    pub fn estimate_cardinality(&self, pattern: &TriplePattern) -> u64 {
        match pattern {
            // Fully bound - always returns 0 or 1
            (Term::Constant(_), Term::Constant(_), Term::Constant(_)) => 1,

            // Two bounds - use actual index stats
            (Term::Constant(s), Term::Constant(p), Term::Variable(_)) => {
                // Look up actual SPO cardinality
                self.stats.get_subject_cardinality(*s)
                    .min(self.stats.get_predicate_cardinality(*p))
                    .max(1)
            }

            (Term::Constant(s), Term::Variable(_), Term::Constant(o)) => {
                // S*O pattern
                self.stats.get_subject_cardinality(*s)
                    .min(self.stats. get_object_cardinality(*o))
                    .max(1)
            }

            (Term::Variable(_), Term::Constant(p), Term::Constant(o)) => {
                // *PO pattern
                self.stats.get_predicate_cardinality(*p)
                    .min(self. stats.get_object_cardinality(*o))
                    .max(1)
            }

            // One bound - use predicate/subject/object cardinality directly
            (Term::Constant(s), Term::Variable(_), Term::Variable(_)) => {
                self. stats.get_subject_cardinality(*s). max(1)
            }

            (Term::Variable(_), Term::Constant(p), Term::Variable(_)) => {
                // This is the KEY one - should return ACTUAL predicate cardinality!
                self.stats.get_predicate_cardinality(*p).max(1)
            }

            (Term::Variable(_), Term::Variable(_), Term::Constant(o)) => {
                self.stats.get_object_cardinality(*o).max(1)
            }

            // No bounds - full scan
            (Term::Variable(_), Term::Variable(_), Term::Variable(_)) => {
                self. stats.total_triples
            }
        }
    }

    /// Estimates the selectivity of a condition
    pub fn estimate_selectivity(&self, condition: &Condition) -> f64 {
        self.estimate_filter_selectivity(&condition.expression)
    }

    /// Recursively estimates the selectivity of a filter expression
    fn estimate_filter_selectivity(&self, expr: &FilterExpression) -> f64 {
        match expr {
            FilterExpression::Comparison(_, op, _) => {
                match *op {
                    "=" => 0.05,  // Equality is very selective
                    "!=" => 0.95, // Not equal is not very selective
                    ">" | "<" => 0.25,  // Range queries
                    ">=" | "<=" => 0.30,
                    _ => 0.5,  // Unknown operators
                }
            }
            FilterExpression::And(left, right) => {
                // AND is more selective - multiply selectivities
                let left_sel = self.estimate_filter_selectivity(left);
                let right_sel = self.estimate_filter_selectivity(right);
                left_sel * right_sel
            }
            FilterExpression::Or(left, right) => {
                // OR is less selective - use formula: sel(A OR B) = sel(A) + sel(B) - sel(A)*sel(B)
                let left_sel = self.estimate_filter_selectivity(left);
                let right_sel = self.estimate_filter_selectivity(right);
                left_sel + right_sel - (left_sel * right_sel)
            }
            FilterExpression::Not(inner) => {
                // NOT inverts selectivity
                let inner_sel = self.estimate_filter_selectivity(inner);
                1.0 - inner_sel
            }
            FilterExpression::ArithmeticExpr(_) => {
                // Conservative estimate for arithmetic expressions
                0.5
            }
        }
    }

    /// Extracts the predicate ID from a physical operator if it's a scan
    fn extract_predicate_from_physical(&self, plan: &PhysicalOperator) -> Option<u32> {
        match plan {
            PhysicalOperator::TableScan { pattern } | PhysicalOperator::IndexScan { pattern } => {
                if let Term::Constant(pred_id) = pattern.1 {
                    Some(pred_id)
                } else {
                    None
                }
            }
            PhysicalOperator::Filter { input, ..  } => self.extract_predicate_from_physical(input),
            PhysicalOperator::Projection { input, .. } => self.extract_predicate_from_physical(input),
            _ => None,
        }
    }

    /// Computes join selectivity based on actual statistics
    fn compute_join_selectivity(&self, left: &PhysicalOperator, right: &PhysicalOperator) -> f64 {
        let left_predicate = self.extract_predicate_from_physical(left);
        let right_predicate = self.extract_predicate_from_physical(right);

        match (left_predicate, right_predicate) {
            (Some(pred), _) => self.stats.get_join_selectivity(pred),
            (None, Some(pred)) => self. stats.get_join_selectivity(pred),
            (None, None) => 0.1, // Fallback
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
                let join_selectivity = self.compute_join_selectivity(left, right);
                ((left_cardinality.min(right_cardinality) as f64 * join_selectivity) as u64).max(1)
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                let join_selectivity = self.compute_join_selectivity(left, right);
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
                let join_selectivity = self.compute_join_selectivity(left, right);
                ((left_cardinality.min(right_cardinality) as f64 * join_selectivity) as u64).max(1)
            }
            PhysicalOperator::Projection { input, .. } => self.estimate_output_cardinality(input),
            PhysicalOperator::StarJoin { patterns, .. } => {
                // Estimate cardinality of star join:
                // Start with most selective pattern, then apply filtering
                let mut cardinalities: Vec<u64> = patterns
                    .iter()
                    .map(|p| self.estimate_cardinality(p))
                    . collect();

                if cardinalities.is_empty() {
                    return 0;
                }

                cardinalities.sort();

                // Base cardinality is the smallest (most selective) pattern
                let base = cardinalities[0];

                // Each additional pattern acts as a filter
                // Conservative estimate
                let filter_factor = 0.5_f64.powi((patterns.len() - 1) as i32);

                ((base as f64 * filter_factor) as u64).max(1)
            }
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
