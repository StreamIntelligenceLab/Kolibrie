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
use super::super::types::{Condition, ConditionExpression};
use super::super::DatasetView;
use shared::dataset_index::{GraphId, GraphTerm, QuadPattern};
use shared::terms::{Term, TriplePattern};
use std::collections::HashSet;

/// Strips the SPARQL sigil so planner and executor agree on variable identity
pub fn normalize_variable(variable: &str) -> &str {
    variable
        .strip_prefix('?')
        .or_else(|| variable.strip_prefix('$'))
        .unwrap_or(variable)
}

/// Adds every variable a pattern can bind, including inside quoted triples
pub fn collect_pattern_variables(pattern: &QuadPattern, out: &mut HashSet<String>) {
    fn collect_term(term: &Term, out: &mut HashSet<String>) {
        match term {
            Term::Variable(name) => {
                out.insert(normalize_variable(name).to_string());
            }
            Term::QuotedTriple(inner) => {
                collect_term(&inner.0, out);
                collect_term(&inner.1, out);
                collect_term(&inner.2, out);
            }
            Term::Constant(_) => {}
        }
    }

    collect_term(&pattern.subject, out);
    collect_term(&pattern.predicate, out);
    collect_term(&pattern.object, out);
    if let GraphTerm::Variable(graph) = &pattern.graph {
        out.insert(normalize_variable(graph).to_string());
    }
}

/// Cost estimation constants for different operators
pub struct CostConstants;

impl CostConstants {
    pub const COST_PER_ROW_SCAN: u64 = 100;
    pub const COST_PER_ROW_INDEX_SCAN: u64 = 1;
    pub const COST_PER_FILTER: u64 = 1;
    pub const COST_PER_ROW_JOIN: u64 = 2;
    pub const COST_PER_ROW_NESTED_LOOP: u64 = 10;
    pub const COST_PER_PROJECTION: u64 = 1;
    /// One index probe of a dependent plan's right side
    pub const COST_PER_PROBE: u64 = 2;
    pub const TUPLE_COST: u64 = 1;
}

/// Cost estimator for query optimization
pub struct CostEstimator<'a> {
    stats: &'a DatabaseStats,
    dataset: Option<&'a DatasetView>,
}

impl<'a> CostEstimator<'a> {
    /// Creates a new cost estimator with the given statistics
    pub fn new(stats: &'a DatabaseStats) -> Self {
        Self {
            stats,
            dataset: None,
        }
    }

    /// Creates an estimator for a replacement SPARQL dataset.
    ///
    /// The dataset changes both the merged query default graph and the set of
    /// named graphs visible to GRAPH, so it must participate in scan estimates.
    pub fn with_dataset(stats: &'a DatabaseStats, dataset: &'a DatasetView) -> Self {
        Self {
            stats,
            dataset: Some(dataset),
        }
    }

    /// Estimates the cost of executing a physical operator
    pub fn estimate_cost(&self, plan: &PhysicalOperator) -> u64 {
        match plan {
            PhysicalOperator::Unit => 0,
            PhysicalOperator::TableScan { pattern } => {
                self.estimate_quad_cardinality(pattern) * CostConstants::COST_PER_ROW_SCAN
            }
            PhysicalOperator::IndexScan { pattern } => {
                let cardinality = self.estimate_quad_cardinality(pattern);
                let triple = Self::quad_triple(pattern);
                let bound_count = self.count_bound_variables(&triple);

                let discount = match bound_count {
                    0 => 1,    // No discount for unbounded scan
                    1 => 10,   // 10x better for one bound field
                    2 => 100,  // 100x better for two bound fields
                    3 => 1000, // 1000x better for fully bound
                    _ => 1,
                };

                (cardinality * CostConstants::COST_PER_ROW_INDEX_SCAN) / discount
            }
            PhysicalOperator::Union { branches } => branches
                .iter()
                .map(|branch| self.estimate_cost(branch))
                .sum(),
            PhysicalOperator::Graph { input, .. } => self.estimate_cost(input),
            PhysicalOperator::Filter { input, condition } => {
                let input_cost = self.estimate_cost(input);
                let selectivity = self.estimate_selectivity(condition);
                (input_cost as f64 * selectivity) as u64 + CostConstants::COST_PER_FILTER
            }
            PhysicalOperator::BindJoin { left, .. } => {
                // The right side is probed per left row, never executed standalone
                let left_cost = self.estimate_cost(left);
                let left_cardinality = self.estimate_output_cardinality(left);
                let output = self.estimate_output_cardinality(plan);

                left_cost
                    + left_cardinality * CostConstants::COST_PER_PROBE
                    + output * CostConstants::COST_PER_ROW_JOIN
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);
                let output = self.estimate_output_cardinality(plan);

                left_cost
                    + right_cost
                    + (left_cardinality + right_cardinality) * CostConstants::COST_PER_ROW_JOIN
                    + output * CostConstants::COST_PER_ROW_JOIN
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                let left_cardinality = self.estimate_output_cardinality(left);
                let right_cardinality = self.estimate_output_cardinality(right);

                left_cost
                    + right_cost
                    + left_cardinality
                        .saturating_mul(right_cardinality)
                        .saturating_mul(CostConstants::COST_PER_ROW_NESTED_LOOP)
            }
            PhysicalOperator::Projection { input, .. } => {
                self.estimate_cost(input) + CostConstants::COST_PER_PROJECTION
            }
            PhysicalOperator::StarJoin { patterns, .. } => {
                // Cost = scan most selective + filter rest
                let mut costs: Vec<u64> = patterns
                    .iter()
                    .map(|p| self.estimate_cardinality(p))
                    .collect();

                if costs.is_empty() {
                    return 0;
                }

                costs.sort();

                // Start with smallest, then check each remaining
                let base_cost = costs[0] * CostConstants::COST_PER_ROW_INDEX_SCAN;
                let filter_cost =
                    costs.iter().skip(1).sum::<u64>() * CostConstants::COST_PER_ROW_INDEX_SCAN / 10;

                base_cost + filter_cost
            }
            PhysicalOperator::InMemoryBuffer { .. } => 0,
            PhysicalOperator::Subquery { inner, spec } => {
                let inner_cost = self.estimate_cost(inner);
                let inner_card = self.estimate_output_cardinality(inner);

                // Materialization cost:
                // - Cost to execute inner query
                // - Cost to store results (proportional to cardinality)
                // - Small overhead for projection
                let materialization_cost = inner_card * CostConstants::TUPLE_COST;
                let projection_width =
                    spec.projection
                        .as_ref()
                        .map_or(0, |projection| projection.len()) as u64;
                let projection_cost = inner_card * projection_width;

                inner_cost + materialization_cost + projection_cost
            }
            PhysicalOperator::Bind {
                input,
                function_name,
                arguments,
                ..
            } => {
                let input_cost = self.estimate_cost(input);
                let input_cardinality = self.estimate_output_cardinality(input);

                // Cost depends on function complexity
                let function_cost = match function_name.as_str() {
                    "CONCAT" => {
                        // CONCAT cost is proportional to number of arguments
                        arguments.len() as u64 * CostConstants::COST_PER_PROJECTION
                    }
                    _ => {
                        // Generic UDF - assume moderate cost
                        CostConstants::COST_PER_PROJECTION * 2
                    }
                };

                // Total cost = input cost + (cardinality * function cost per row)
                input_cost + (input_cardinality * function_cost)
            }
            PhysicalOperator::Values { values, .. } => {
                // VALUES has minimal cost - just the number of rows
                // No I/O or computation, just materializing the constant values
                (values.len() as u64) * CostConstants::TUPLE_COST
            }
            PhysicalOperator::MLPredict {
                input,
                input_variables,
                ..
            } => {
                let input_cost = self.estimate_cost(input);
                let cardinality = self.estimate_output_cardinality(input);

                // ML prediction is expensive:
                // - Python interop overhead: 1000 per call
                // - Per-row prediction cost: 100 * number of features
                let python_overhead = 1000;
                let per_row_cost = 100 * input_variables.len() as u64;

                input_cost + python_overhead + (cardinality * per_row_cost)
            }
        }
    }

    /// Estimates the cardinality of a triple pattern
    pub fn estimate_cardinality(&self, pattern: &TriplePattern) -> u64 {
        // Treat QuotedTriple terms as variables for cardinality estimation
        match pattern {
            // Fully bound - always returns 0 or 1
            (Term::Constant(_), Term::Constant(_), Term::Constant(_)) => 1,

            // Two bounds - use actual index stats
            (Term::Constant(s), Term::Constant(p), Term::Variable(_)) => {
                // Look up actual SPO cardinality
                self.stats
                    .get_subject_cardinality(*s)
                    .min(self.stats.get_predicate_cardinality(*p))
                    .max(1)
            }

            (Term::Constant(s), Term::Variable(_), Term::Constant(o)) => {
                // S*O pattern
                self.stats
                    .get_subject_cardinality(*s)
                    .min(self.stats.get_object_cardinality(*o))
                    .max(1)
            }

            (Term::Variable(_), Term::Constant(p), Term::Constant(o)) => {
                // *PO pattern
                self.stats
                    .get_predicate_cardinality(*p)
                    .min(self.stats.get_object_cardinality(*o))
                    .max(1)
            }

            // One bound - use predicate/subject/object cardinality directly
            (Term::Constant(s), Term::Variable(_), Term::Variable(_)) => {
                self.stats.get_subject_cardinality(*s).max(1)
            }

            (Term::Variable(_), Term::Constant(p), Term::Variable(_)) => {
                // This is the KEY one - should return ACTUAL predicate cardinality!
                self.stats.get_predicate_cardinality(*p).max(1)
            }

            (Term::Variable(_), Term::Variable(_), Term::Constant(o)) => {
                self.stats.get_object_cardinality(*o).max(1)
            }

            // No bounds - full scan
            (Term::Variable(_), Term::Variable(_), Term::Variable(_)) => self.stats.total_triples,

            // Patterns with QuotedTriple terms — estimate based on bound positions
            _ => {
                let bound = [&pattern.0, &pattern.1, &pattern.2]
                    .iter()
                    .filter(|t| matches!(t, Term::Constant(_)))
                    .count();
                let qt_count = self.stats.quoted_triple_count.max(1);
                match bound {
                    0 => qt_count.min(self.stats.total_triples),
                    1 => (qt_count / 5).max(1),
                    2 => (qt_count / 10).max(1),
                    _ => 1,
                }
            }
        }
    }

    /// Estimates a scan of `pattern` where every position `bound` pins down divides it by that position's domain size
    pub fn estimate_bound_scan_cardinality(
        &self,
        pattern: &QuadPattern,
        bound: &HashSet<String>,
    ) -> u64 {
        let base = self.estimate_quad_cardinality(pattern);
        if base == 0 {
            return 0;
        }

        let predicate = match &pattern.predicate {
            Term::Constant(predicate) => Some(*predicate),
            _ => None,
        };
        let is_bound = |term: &Term| {
            matches!(term, Term::Variable(name) if bound.contains(normalize_variable(name)))
        };

        let mut divisor: u64 = 1;
        if is_bound(&pattern.subject) {
            divisor = divisor.saturating_mul(match predicate {
                Some(predicate) => self.stats.get_predicate_distinct_subjects(predicate).max(1),
                None => self.stats.distinct_subjects.max(1),
            });
        }
        if is_bound(&pattern.object) {
            divisor = divisor.saturating_mul(match predicate {
                Some(predicate) => self.stats.get_predicate_distinct_objects(predicate).max(1),
                None => self.stats.distinct_objects.max(1),
            });
        }
        if is_bound(&pattern.predicate) {
            divisor = divisor.saturating_mul(self.stats.distinct_predicates().max(1));
        }

        (base / divisor.max(1)).max(1)
    }

    /// Estimates a graph-scoped scan. Fixed graphs are capped by their direct
    /// graph cardinality; variable graphs range across all named graphs.
    pub fn estimate_quad_cardinality(&self, pattern: &QuadPattern) -> u64 {
        let graph_cardinality = self.graph_term_cardinality(&pattern.graph);

        if graph_cardinality == 0 {
            return 0;
        }

        let triple = Self::quad_triple(pattern);
        self.estimate_cardinality(&triple).min(graph_cardinality)
    }

    fn graph_term_cardinality(&self, graph: &GraphTerm) -> u64 {
        match graph {
            GraphTerm::Default => self
                .dataset
                .map(|dataset| {
                    dataset
                        .default_graphs
                        .iter()
                        .map(|graph| self.stats.get_graph_cardinality(*graph))
                        .sum()
                })
                .unwrap_or_else(|| self.stats.get_graph_cardinality(GraphId::Default)),
            GraphTerm::Named(graph) => {
                let graph = GraphId::Named(*graph);
                if self.graph_is_visible_and_exists(graph) {
                    self.stats.get_graph_cardinality(graph)
                } else {
                    0
                }
            }
            GraphTerm::Variable(_) => self
                .visible_named_graphs()
                .into_iter()
                .map(|graph| self.stats.get_graph_cardinality(graph))
                .sum(),
        }
    }

    fn graph_is_visible_and_exists(&self, graph: GraphId) -> bool {
        self.stats.graph_cardinalities.contains_key(&graph)
            && self
                .dataset
                .is_none_or(|dataset| dataset.is_named_visible(graph))
    }

    pub(crate) fn visible_named_graphs(&self) -> Vec<GraphId> {
        match self.dataset {
            Some(dataset) => dataset
                .named_graphs
                .iter()
                .copied()
                .filter(|graph| self.stats.graph_cardinalities.contains_key(graph))
                .collect(),
            None => self
                .stats
                .graph_cardinalities
                .keys()
                .copied()
                .filter(|graph| matches!(graph, GraphId::Named(_)))
                .collect(),
        }
    }

    /// Returns the catalogued, visible named-graph count. Empty graphs count.
    pub fn visible_named_graph_count(&self) -> u64 {
        self.visible_named_graphs().len() as u64
    }

    /// Whether a fixed named graph exists and is visible in this query dataset.
    pub fn fixed_graph_is_visible(&self, graph: u32) -> bool {
        self.graph_is_visible_and_exists(GraphId::Named(graph))
    }

    fn quad_triple(pattern: &QuadPattern) -> TriplePattern {
        (
            pattern.subject.clone(),
            pattern.predicate.clone(),
            pattern.object.clone(),
        )
    }

    /// Estimates the selectivity of a condition
    pub fn estimate_selectivity(&self, condition: &Condition) -> f64 {
        self.estimate_filter_selectivity(&condition.expression)
    }

    /// Recursively estimates the selectivity of a filter expression
    fn estimate_filter_selectivity(&self, expr: &ConditionExpression) -> f64 {
        match expr {
            ConditionExpression::Comparison(_, op, _) => {
                match op.as_str() {
                    "=" => 0.05,       // Equality is very selective
                    "!=" => 0.95,      // Not equal is not very selective
                    ">" | "<" => 0.25, // Range queries
                    ">=" | "<=" => 0.30,
                    _ => 0.5, // Unknown operators
                }
            }
            ConditionExpression::ArithmeticComparison(_, op, _) => match op.as_str() {
                "=" => 0.05,
                "!=" => 0.95,
                ">" | "<" => 0.25,
                ">=" | "<=" => 0.30,
                _ => 0.5,
            },
            ConditionExpression::And(left, right) => {
                // AND is more selective - multiply selectivities
                let left_sel = self.estimate_filter_selectivity(left);
                let right_sel = self.estimate_filter_selectivity(right);
                left_sel * right_sel
            }
            ConditionExpression::Or(left, right) => {
                // OR is less selective - use formula: sel(A OR B) = sel(A) + sel(B) - sel(A)*sel(B)
                let left_sel = self.estimate_filter_selectivity(left);
                let right_sel = self.estimate_filter_selectivity(right);
                left_sel + right_sel - (left_sel * right_sel)
            }
            ConditionExpression::Not(inner) => {
                // NOT inverts selectivity
                let inner_sel = self.estimate_filter_selectivity(inner);
                1.0 - inner_sel
            }
            ConditionExpression::ArithmeticExpr(_) => {
                // Conservative estimate for arithmetic expressions
                0.5
            }
            ConditionExpression::FunctionCall(func_name, _) => match func_name.as_str() {
                "isTRIPLE" => 0.1,
                _ => 0.5,
            },
        }
    }

    /// Collects the scan patterns a subtree reads
    pub fn collect_scan_patterns(plan: &PhysicalOperator, out: &mut Vec<QuadPattern>) {
        match plan {
            PhysicalOperator::TableScan { pattern } | PhysicalOperator::IndexScan { pattern } => {
                out.push(pattern.clone())
            }
            PhysicalOperator::Filter { input, .. }
            | PhysicalOperator::Projection { input, .. }
            | PhysicalOperator::Graph { input, .. } => Self::collect_scan_patterns(input, out),
            PhysicalOperator::BindJoin { left, right }
            | PhysicalOperator::HashJoin { left, right }
            | PhysicalOperator::NestedLoopJoin { left, right } => {
                Self::collect_scan_patterns(left, out);
                Self::collect_scan_patterns(right, out);
            }
            PhysicalOperator::StarJoin { patterns, .. } => out.extend(patterns.iter().map(|p| {
                QuadPattern {
                    subject: p.0.clone(),
                    predicate: p.1.clone(),
                    object: p.2.clone(),
                    graph: GraphTerm::Default,
                }
            })),
            _ => {}
        }
    }

    /// Estimates surviving rows as the cross product divided by the domain size of each shared variable
    pub fn estimate_join_cardinality_from_patterns(
        &self,
        left_cardinality: u64,
        right_cardinality: u64,
        left_patterns: &[QuadPattern],
        right_patterns: &[QuadPattern],
    ) -> u64 {
        if left_cardinality == 0 || right_cardinality == 0 {
            return 0;
        }

        let left_variables = Self::pattern_set_variables(left_patterns);
        let right_variables = Self::pattern_set_variables(right_patterns);
        let mut shared: Vec<&String> = left_variables.intersection(&right_variables).collect();
        shared.sort_unstable();

        let cross = left_cardinality.saturating_mul(right_cardinality);
        if shared.is_empty() {
            return cross.max(1);
        }

        let mut divisor: u64 = 1;
        for variable in shared {
            let left_domain = self.domain_size(left_patterns, variable, left_cardinality);
            let right_domain = self.domain_size(right_patterns, variable, right_cardinality);
            if let Some(domain) = match (left_domain, right_domain) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (Some(only), None) | (None, Some(only)) => Some(only),
                (None, None) => None,
            } {
                divisor = divisor.saturating_mul(domain.max(1));
            }
        }

        if divisor <= 1 {
            // No statistic for the join variable: assume one match per row on the larger side
            return left_cardinality.max(right_cardinality);
        }

        (cross / divisor).max(1)
    }

    /// Distinct values a variable takes across patterns, capped by the side's row count
    fn domain_size(&self, patterns: &[QuadPattern], variable: &str, cardinality: u64) -> Option<u64> {
        patterns
            .iter()
            .filter_map(|pattern| self.distinct_estimate_for_var(pattern, variable))
            .min()
            .map(|domain| domain.clamp(1, cardinality.max(1)))
    }

    /// Distinct values a variable takes at its position in one pattern
    fn distinct_estimate_for_var(&self, pattern: &QuadPattern, variable: &str) -> Option<u64> {
        let predicate = match &pattern.predicate {
            Term::Constant(predicate) => Some(*predicate),
            _ => None,
        };

        if Self::term_is_variable(&pattern.subject, variable) {
            return Some(match predicate {
                Some(predicate) => self.stats.get_predicate_distinct_subjects(predicate),
                None => self.stats.distinct_subjects,
            });
        }
        if Self::term_is_variable(&pattern.object, variable) {
            return Some(match predicate {
                Some(predicate) => self.stats.get_predicate_distinct_objects(predicate),
                None => self.stats.distinct_objects,
            });
        }
        if Self::term_is_variable(&pattern.predicate, variable) {
            return Some(self.stats.distinct_predicates());
        }
        if let GraphTerm::Variable(graph) = &pattern.graph {
            if normalize_variable(graph) == variable {
                return Some(self.stats.named_graph_count);
            }
        }
        None
    }

    fn term_is_variable(term: &Term, variable: &str) -> bool {
        matches!(term, Term::Variable(name) if normalize_variable(name) == variable)
    }

    fn pattern_set_variables(patterns: &[QuadPattern]) -> HashSet<String> {
        let mut variables = HashSet::new();
        for pattern in patterns {
            collect_pattern_variables(pattern, &mut variables);
        }
        variables
    }

    /// Estimates the output cardinality of a physical operator
    pub fn estimate_output_cardinality(&self, plan: &PhysicalOperator) -> u64 {
        self.estimate_output_cardinality_in_context(plan, None)
    }

    fn estimate_output_cardinality_in_context(
        &self,
        plan: &PhysicalOperator,
        active_graph: Option<GraphId>,
    ) -> u64 {
        match plan {
            PhysicalOperator::Unit => 1,
            PhysicalOperator::TableScan { pattern } | PhysicalOperator::IndexScan { pattern } => {
                if let Some(GraphId::Named(graph)) = active_graph {
                    let mut scoped = pattern.clone();
                    if matches!(scoped.graph, GraphTerm::Default | GraphTerm::Variable(_)) {
                        scoped.graph = GraphTerm::Named(graph);
                    }
                    self.estimate_quad_cardinality(&scoped)
                } else {
                    self.estimate_quad_cardinality(pattern)
                }
            }
            PhysicalOperator::Union { branches } => branches
                .iter()
                .map(|branch| self.estimate_output_cardinality_in_context(branch, active_graph))
                .sum(),
            PhysicalOperator::Graph { input, graph } => match graph {
                GraphTerm::Default => self.estimate_output_cardinality_in_context(input, None),
                GraphTerm::Named(graph) if self.fixed_graph_is_visible(*graph) => {
                    self.estimate_output_cardinality_in_context(input, Some(GraphId::Named(*graph)))
                }
                GraphTerm::Named(_) => 0,
                GraphTerm::Variable(_) => self
                    .visible_named_graphs()
                    .into_iter()
                    .map(|graph| self.estimate_output_cardinality_in_context(input, Some(graph)))
                    .sum(),
            },
            PhysicalOperator::Filter { input, condition } => {
                let input_cardinality =
                    self.estimate_output_cardinality_in_context(input, active_graph);
                if input_cardinality == 0 {
                    return 0;
                }
                let selectivity = self.estimate_selectivity(condition);
                ((input_cardinality as f64 * selectivity) as u64).max(1)
            }
            PhysicalOperator::BindJoin { left, right }
            | PhysicalOperator::HashJoin { left, right }
            | PhysicalOperator::NestedLoopJoin { left, right } => {
                // The join algorithm does not change how many rows survive
                let left_cardinality =
                    self.estimate_output_cardinality_in_context(left, active_graph);
                let right_cardinality =
                    self.estimate_output_cardinality_in_context(right, active_graph);
                let mut left_patterns = Vec::new();
                let mut right_patterns = Vec::new();
                Self::collect_scan_patterns(left, &mut left_patterns);
                Self::collect_scan_patterns(right, &mut right_patterns);
                self.estimate_join_cardinality_from_patterns(
                    left_cardinality,
                    right_cardinality,
                    &left_patterns,
                    &right_patterns,
                )
            }
            PhysicalOperator::Projection { input, .. } => {
                self.estimate_output_cardinality_in_context(input, active_graph)
            }
            PhysicalOperator::StarJoin { patterns, .. } => {
                // Estimate cardinality of star join:
                // Start with most selective pattern, then apply filtering
                let mut cardinalities: Vec<u64> = patterns
                    .iter()
                    .map(|p| self.estimate_cardinality(p))
                    .collect();

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
            PhysicalOperator::InMemoryBuffer { .. } => 0,
            PhysicalOperator::Subquery { inner, .. } => {
                // Subquery cardinality is the same as inner query
                self.estimate_output_cardinality_in_context(inner, active_graph)
            }
            PhysicalOperator::Bind { input, .. } => {
                // BIND doesn't change cardinality, just adds a column
                self.estimate_output_cardinality_in_context(input, active_graph)
            }
            PhysicalOperator::Values { values, .. } => {
                // Cardinality is simply the number of value rows
                values.len() as u64
            }
            PhysicalOperator::MLPredict { input, .. } => {
                // ML.PREDICT doesn't change cardinality, just adds a column
                self.estimate_output_cardinality_in_context(input, active_graph)
            }
        }
    }

    /// Counts the number of bound variables in a triple pattern
    fn count_bound_variables(&self, pattern: &TriplePattern) -> usize {
        let mut count = 0;

        match pattern.0 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) | Term::QuotedTriple(_) => {}
        }

        match pattern.1 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) | Term::QuotedTriple(_) => {}
        }

        match pattern.2 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) | Term::QuotedTriple(_) => {}
        }

        count
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::terms::Term;

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

    fn unbound_scan(graph: GraphTerm) -> PhysicalOperator {
        PhysicalOperator::quad_table_scan(QuadPattern {
            subject: Term::Variable("?s".to_string()),
            predicate: Term::Variable("?p".to_string()),
            object: Term::Variable("?o".to_string()),
            graph,
        })
    }

    const EDGE: u32 = 1;
    const TAG: u32 = 2;

    fn join_stats() -> DatabaseStats {
        let mut stats = DatabaseStats::new();
        stats.total_triples = 10_000;
        stats.distinct_subjects = 5_000;
        stats.distinct_objects = 5_000;
        stats.predicate_cardinalities.insert(EDGE, 8_000);
        stats.predicate_cardinalities.insert(TAG, 20);
        stats.predicate_distinct_subjects.insert(EDGE, 4_000);
        stats.predicate_distinct_objects.insert(EDGE, 4_000);
        stats.predicate_distinct_subjects.insert(TAG, 20);
        stats.predicate_distinct_objects.insert(TAG, 1);
        stats.graph_cardinalities.insert(GraphId::Default, 10_000);
        stats
    }

    fn pattern(subject: Term, predicate: Term, object: Term) -> QuadPattern {
        QuadPattern {
            subject,
            predicate,
            object,
            graph: GraphTerm::Default,
        }
    }

    fn variable(name: &str) -> Term {
        Term::Variable(name.to_string())
    }

    #[test]
    fn distinct_counts_are_gathered_per_predicate() {
        use crate::sparql_database::SparqlDatabase;
        use shared::dataset_index::{GraphId, Quad};

        let mut database = SparqlDatabase::new();
        let predicate = database.dictionary.write().unwrap().encode("urn:p");
        // Three subjects share one object, so subjects and objects differ
        for subject_name in ["urn:a", "urn:b", "urn:c"] {
            let subject = database.dictionary.write().unwrap().encode(subject_name);
            let object = database.dictionary.write().unwrap().encode("urn:shared");
            database.add_quad(Quad {
                subject,
                predicate,
                object,
                graph: GraphId::Default,
            });
        }

        let stats = DatabaseStats::gather_stats_fast(&database);

        assert_eq!(stats.get_predicate_distinct_subjects(predicate), 3);
        assert_eq!(stats.get_predicate_distinct_objects(predicate), 1);
        assert_eq!(stats.distinct_objects, 1);
    }

    #[test]
    fn join_cardinality_divides_by_the_shared_variable_domain() {
        let stats = join_stats();
        let estimator = CostEstimator::new(&stats);
        // ?a EDGE ?b joined with ?b EDGE ?c through ?b
        let left = [pattern(variable("?a"), Term::Constant(EDGE), variable("?b"))];
        let right = [pattern(variable("?b"), Term::Constant(EDGE), variable("?c"))];

        let estimate =
            estimator.estimate_join_cardinality_from_patterns(8_000, 8_000, &left, &right);

        assert_eq!(estimate, 8_000 * 8_000 / 4_000);
    }

    #[test]
    fn join_cardinality_without_a_shared_variable_is_the_cross_product() {
        let stats = join_stats();
        let estimator = CostEstimator::new(&stats);
        let left = [pattern(variable("?a"), Term::Constant(EDGE), variable("?b"))];
        let right = [pattern(variable("?c"), Term::Constant(TAG), variable("?d"))];

        assert_eq!(
            estimator.estimate_join_cardinality_from_patterns(100, 20, &left, &right),
            2_000
        );
    }

    #[test]
    fn bound_positions_make_a_scan_cheaper_than_an_unbound_one() {
        let stats = join_stats();
        let estimator = CostEstimator::new(&stats);
        let step = pattern(variable("?a"), Term::Constant(EDGE), variable("?b"));

        let unbound = estimator.estimate_bound_scan_cardinality(&step, &HashSet::new());
        let bound = estimator
            .estimate_bound_scan_cardinality(&step, &HashSet::from(["a".to_string()]));

        assert_eq!(unbound, 8_000);
        assert_eq!(bound, 2);
    }

    #[test]
    fn cost_model_prefers_bind_join_for_a_selective_left_side() {
        let stats = join_stats();
        let estimator = CostEstimator::new(&stats);
        let selective =
            PhysicalOperator::quad_index_scan(pattern(variable("?x"), Term::Constant(TAG), Term::Constant(7)));
        let wide =
            PhysicalOperator::quad_index_scan(pattern(variable("?x"), Term::Constant(EDGE), variable("?y")));

        let bind = estimator.estimate_cost(&PhysicalOperator::bind_join(
            selective.clone(),
            wide.clone(),
        ));
        let hash =
            estimator.estimate_cost(&PhysicalOperator::hash_join(selective.clone(), wide.clone()));
        let nested =
            estimator.estimate_cost(&PhysicalOperator::nested_loop_join(selective, wide));

        assert!(
            bind < hash && bind < nested,
            "probing a wide right side once per selective left row should win: bind={} hash={} nested={}",
            bind,
            hash,
            nested
        );
    }

    #[test]
    fn cost_model_prefers_hash_join_when_both_sides_are_wide() {
        let stats = join_stats();
        let estimator = CostEstimator::new(&stats);
        let wide_left = PhysicalOperator::quad_table_scan(pattern(
            variable("?x"),
            Term::Constant(EDGE),
            variable("?y"),
        ));
        let wide_right = PhysicalOperator::quad_table_scan(pattern(
            variable("?y"),
            Term::Constant(EDGE),
            variable("?z"),
        ));

        let hash =
            estimator.estimate_cost(&PhysicalOperator::hash_join(wide_left.clone(), wide_right.clone()));
        let nested = estimator
            .estimate_cost(&PhysicalOperator::nested_loop_join(wide_left, wide_right));

        assert!(
            hash < nested,
            "a build/probe pass should beat a pairwise scan of both sides: hash={} nested={}",
            hash,
            nested
        );
    }

    #[test]
    fn replacement_dataset_drives_default_and_named_graph_estimates() {
        let mut stats = DatabaseStats::new();
        stats.total_triples = 12;
        stats.named_graph_count = 2;
        stats.graph_cardinalities.insert(GraphId::Default, 5);
        stats.graph_cardinalities.insert(GraphId::Named(10), 7);
        stats.graph_cardinalities.insert(GraphId::Named(20), 0);

        // FROM <10> creates a seven-row query default. FROM NAMED <20>
        // hides graph 10 from GRAPH but retains empty graph 20's identity.
        let dataset = DatasetView::new([GraphId::Named(10)], [GraphId::Named(20)]);
        let estimator = CostEstimator::with_dataset(&stats, &dataset);

        assert_eq!(
            estimator.estimate_output_cardinality(&unbound_scan(GraphTerm::Default)),
            7
        );
        assert_eq!(
            estimator.estimate_output_cardinality(&unbound_scan(GraphTerm::Named(10))),
            0,
            "FROM graphs are not implicitly visible as named graphs"
        );
        assert_eq!(
            estimator.estimate_output_cardinality(&PhysicalOperator::graph(
                PhysicalOperator::unit(),
                GraphTerm::Variable("?g".to_string()),
            )),
            1,
            "empty visible named graphs participate in GRAPH ?g {{}}"
        );
        assert_eq!(
            estimator.estimate_output_cardinality(&PhysicalOperator::graph(
                PhysicalOperator::unit(),
                GraphTerm::Named(20),
            )),
            1,
            "an existing empty fixed graph makes GRAPH <g> {{}} a unit solution"
        );
    }

    #[test]
    fn variable_graph_scan_estimate_sums_only_visible_graph_data() {
        let mut stats = DatabaseStats::new();
        stats.total_triples = 12;
        stats.named_graph_count = 2;
        stats.graph_cardinalities.insert(GraphId::Default, 5);
        stats.graph_cardinalities.insert(GraphId::Named(10), 7);
        stats.graph_cardinalities.insert(GraphId::Named(20), 0);

        let dataset = DatasetView::new(std::iter::empty(), [GraphId::Named(10)]);
        let estimator = CostEstimator::with_dataset(&stats, &dataset);
        let scan = unbound_scan(GraphTerm::Variable("?g".to_string()));
        let graph = PhysicalOperator::graph(scan, GraphTerm::Variable("?g".to_string()));

        assert_eq!(estimator.estimate_output_cardinality(&graph), 7);
    }
}
