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
use super::execution::{DatasetView, ExecutionEngine};
use super::operators::{LogicalOperator, PhysicalOperator};
use super::stats::DatabaseStats;
use super::types::{ConditionArithmetic, ConditionExpression};

use crate::sparql_database::SparqlDatabase;
use shared::dataset_index::{GraphId, GraphTerm, QuadPattern};
use shared::terms::{Term, TriplePattern};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Volcano-style query optimizer with cost-based optimization
pub struct Streamertail {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: Arc<DatabaseStats>,
    dataset: Option<DatasetView>,
}

fn serialize_arith_expr(expr: &ConditionArithmetic) -> String {
    use ConditionArithmetic as AE;
    match expr {
        AE::Operand(s) => s.to_string(),
        AE::Add(l, r) => format!(
            "({} + {})",
            serialize_arith_expr(l),
            serialize_arith_expr(r)
        ),
        AE::Subtract(l, r) => format!(
            "({} - {})",
            serialize_arith_expr(l),
            serialize_arith_expr(r)
        ),
        AE::Multiply(l, r) => format!(
            "({} * {})",
            serialize_arith_expr(l),
            serialize_arith_expr(r)
        ),
        AE::Divide(l, r) => format!(
            "({} / {})",
            serialize_arith_expr(l),
            serialize_arith_expr(r)
        ),
    }
}

impl Streamertail {
    /// Creates a new volcano optimizer
    pub fn new(database: &SparqlDatabase) -> Self {
        let stats = Arc::new(DatabaseStats::gather_stats_fast(database));
        Self {
            memo: HashMap::new(),
            selected_variables: Vec::new(),
            stats,
            dataset: None,
        }
    }

    pub fn with_cached_stats(stats: Arc<DatabaseStats>) -> Self {
        Self {
            memo: HashMap::new(),
            selected_variables: Vec::new(),
            stats,
            dataset: None,
        }
    }

    /// Creates an optimizer whose estimates reflect a replacement SPARQL
    /// dataset (`FROM`/`FROM NAMED`) rather than the physical dataset.
    pub fn with_cached_stats_and_dataset(stats: Arc<DatabaseStats>, dataset: DatasetView) -> Self {
        Self {
            memo: HashMap::new(),
            selected_variables: Vec::new(),
            stats,
            dataset: Some(dataset),
        }
    }

    fn cost_estimator(&self) -> CostEstimator<'_> {
        match self.dataset.as_ref() {
            Some(dataset) => CostEstimator::with_dataset(&self.stats, dataset),
            None => CostEstimator::new(&self.stats),
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

    /// Executes an optimized plan against a replacement SPARQL dataset.
    pub fn execute_plan_with_dataset(
        &self,
        plan: &PhysicalOperator,
        database: &mut SparqlDatabase,
        dataset: &DatasetView,
    ) -> Vec<HashMap<String, String>> {
        ExecutionEngine::execute_with_dataset(plan, database, dataset)
    }

    /// Executes an optimized plan against a replacement SPARQL dataset and
    /// retains dictionary IDs for update template instantiation.
    pub fn execute_plan_with_ids_and_dataset(
        &self,
        plan: &PhysicalOperator,
        database: &mut SparqlDatabase,
        dataset: &DatasetView,
    ) -> Vec<HashMap<String, u32>> {
        ExecutionEngine::execute_with_ids_and_dataset(plan, database, dataset)
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
        if !self.collect_patterns(plan, &mut patterns) {
            return None;
        }

        if patterns.len() < 3 {
            return None;
        }

        // Count subject-centered stars only. Object-position "stars" can explode
        // path queries by enumerating unrelated combinations before path joins run.
        let mut var_counts: std::collections::BTreeMap<String, Vec<usize>> = BTreeMap::new();

        for (idx, pattern) in patterns.iter().enumerate() {
            if let Term::Variable(var) = &pattern.0 {
                var_counts.entry(var.clone()).or_default().push(idx);
            }
        }

        // Find variables that appear as the subject in at least 3 patterns.
        let mut star_vars: Vec<(&String, &Vec<usize>)> = var_counts
            .iter()
            .filter(|(_, indices)| indices.len() >= 3)
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

            if available.len() >= 3 {
                let star_patterns: Vec<TriplePattern> =
                    available.iter().map(|&idx| patterns[idx].clone()).collect();

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

    /// Collects only one uninterrupted join group. GRAPH, UNION, filters,
    /// projections, binds, values and subqueries are deliberate optimizer
    /// boundaries.
    fn collect_patterns(&self, plan: &LogicalOperator, patterns: &mut Vec<TriplePattern>) -> bool {
        match plan {
            LogicalOperator::Scan { pattern } => {
                if pattern.graph != GraphTerm::Default {
                    return false;
                }
                patterns.push((
                    pattern.subject.clone(),
                    pattern.predicate.clone(),
                    pattern.object.clone(),
                ));
                true
            }
            LogicalOperator::Join { left, right } => {
                self.collect_patterns(left, patterns) && self.collect_patterns(right, patterns)
            }
            _ => false,
        }
    }

    /// Recursively finds the best plan using dynamic programming with memoization
    fn find_best_plan_recursive(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator {
        let key = self.create_memo_key(logical_plan);

        if let Some(plan) = self.memo.get(&key) {
            return plan.clone();
        }

        if let LogicalOperator::Projection {
            predicate: proj_pred,
            variables,
        } = logical_plan
        {
            if let LogicalOperator::Selection {
                predicate: sel_pred,
                condition,
            } = proj_pred.as_ref()
            {
                if let Some(stars) = self.is_star_query(sel_pred) {
                    // Build: Projection(Filter(StarJoin))
                    let star_plan = self.build_star_join_from_patterns(stars, sel_pred);
                    let filtered_plan = PhysicalOperator::filter(star_plan, condition.clone());
                    let projected_plan =
                        PhysicalOperator::projection(filtered_plan, variables.clone());
                    self.memo.insert(key, projected_plan.clone());
                    return projected_plan;
                }
            }
        }

        // Handle Selection wrapping star query (no projection)
        if let LogicalOperator::Selection {
            predicate,
            condition,
        } = logical_plan
        {
            if let Some(stars) = self.is_star_query(predicate) {
                let star_plan = self.build_star_join_from_patterns(stars, predicate);
                let filtered_plan = PhysicalOperator::filter(star_plan, condition.clone());
                self.memo.insert(key, filtered_plan.clone());
                return filtered_plan;
            }
        }

        // Handle star query without selection or projection
        if !matches!(
            logical_plan,
            LogicalOperator::Selection { .. } | LogicalOperator::Projection { .. }
        ) {
            if let Some(stars) = self.is_star_query(logical_plan) {
                let star_plan = self.build_star_join_from_patterns(stars, logical_plan);
                self.memo.insert(key, star_plan.clone());
                return star_plan;
            }
        }

        let mut candidates = Vec::new();

        match logical_plan {
            LogicalOperator::Unit => candidates.push(PhysicalOperator::unit()),
            LogicalOperator::Scan { pattern } => {
                // Implementation rules: Map logical scan to physical scans
                let best_scan = self.choose_best_scan(pattern);
                candidates.push(best_scan);
            }
            LogicalOperator::Union { branches } => {
                let branches = branches
                    .iter()
                    .map(|branch| self.find_best_plan_recursive(branch))
                    .collect();
                candidates.push(PhysicalOperator::union(branches));
            }
            LogicalOperator::Graph { input, graph } => {
                let input = self.find_best_plan_recursive(input);
                candidates.push(PhysicalOperator::graph(input, graph.clone()));
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
                // Reorder only one uninterrupted group of scans with exactly
                // the same graph scope. Every other operator is a semantic
                // boundary and retains source order.
                let can_reorder = self
                    .homogeneous_scan_scope(left)
                    .zip(self.homogeneous_scan_scope(right))
                    .is_some_and(|(left_scope, right_scope)| left_scope == right_scope);
                let (cheaper_side, expensive_side) = if can_reorder
                    && self.estimate_logical_cost(right) < self.estimate_logical_cost(left)
                {
                    (right, left)
                } else {
                    (left, right)
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
            LogicalOperator::Buffer { content, origin } => {
                let best_buffer = PhysicalOperator::InMemoryBuffer {
                    content: content.clone(),
                    origin: origin.clone(),
                };
                candidates.push(best_buffer);
            }
            LogicalOperator::Subquery { inner, spec } => {
                // Recursively optimize the inner query
                let optimized_inner = self.find_best_plan_recursive(inner);

                // Keep every subquery-local SELECT modifier attached until
                // execution, before the materialized rows join the outer query.
                let subquery_plan = PhysicalOperator::subquery(optimized_inner, spec.clone());

                candidates.push(subquery_plan);
            }
            LogicalOperator::Bind {
                input,
                function_name,
                arguments,
                output_variable,
            } => {
                // Recursively optimize the input
                let best_input_plan = self.find_best_plan_recursive(input);

                // Create the physical BIND operator
                let bind_plan = PhysicalOperator::bind(
                    best_input_plan,
                    function_name.clone(),
                    arguments.clone(),
                    output_variable.clone(),
                );

                candidates.push(bind_plan);
            }
            LogicalOperator::Values { variables, values } => {
                // VALUES is a leaf operator
                candidates.push(PhysicalOperator::values(variables.clone(), values.clone()));
            }
            LogicalOperator::MLPredict {
                input,
                model_name,
                input_variables,
                output_variable,
            } => {
                // Recursively optimize the input
                let best_input_plan = self.find_best_plan_recursive(input);

                // Discover model path
                let model_path = self.discover_model_path();

                // Create the physical ML.PREDICT operator
                let ml_predict_plan = PhysicalOperator::ml_predict(
                    best_input_plan,
                    model_name.clone(),
                    model_path,
                    input_variables.clone(),
                    output_variable.clone(),
                );

                candidates.push(ml_predict_plan);
            }
        }

        // Cost-based optimization: Choose the best candidate
        let cost_estimator = self.cost_estimator();
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

    fn homogeneous_scan_scope(&self, plan: &LogicalOperator) -> Option<GraphTerm> {
        match plan {
            LogicalOperator::Scan { pattern } => Some(pattern.graph.clone()),
            LogicalOperator::Join { left, right } => {
                let left = self.homogeneous_scan_scope(left)?;
                let right = self.homogeneous_scan_scope(right)?;
                (left == right).then_some(left)
            }
            _ => None,
        }
    }

    /// Discovers the model path from the model name
    fn discover_model_path(&self) -> String {
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        loop {
            let ml_dir = path.join("ml");
            if ml_dir.exists() && ml_dir.is_dir() {
                let model_dir = ml_dir.join("examples").join("models");

                // Return just the model directory - the ML handler will discover models
                if model_dir.exists() {
                    return model_dir.to_string_lossy().to_string();
                }

                break;
            }

            if !path.pop() {
                eprintln!("Warning: Could not locate 'ml' directory!");
                break;
            }
        }

        // Fallback to relative path
        format!("ml/examples/models")
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
                let bound_count = patterns
                    .iter()
                    .filter(|p| {
                        matches!(p.0, Term::Constant(_))
                            || matches!(p.1, Term::Constant(_))
                            || matches!(p.2, Term::Constant(_))
                    })
                    .count();
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
    fn choose_best_scan(&self, pattern: &QuadPattern) -> PhysicalOperator {
        let triple = (
            pattern.subject.clone(),
            pattern.predicate.clone(),
            pattern.object.clone(),
        );
        let bound_vars = self.count_bound_variables(&triple);
        let cost_estimator = self.cost_estimator();
        let estimated_size = cost_estimator.estimate_quad_cardinality(pattern);

        match bound_vars {
            3 => PhysicalOperator::quad_index_scan(pattern.clone()), // Fully bound - always use index
            2 => PhysicalOperator::quad_index_scan(pattern.clone()), // Two bounds - index is better
            1 => {
                // Use index if result set is small enough
                if estimated_size < 10000 {
                    PhysicalOperator::quad_index_scan(pattern.clone())
                } else {
                    PhysicalOperator::quad_table_scan(pattern.clone())
                }
            }
            0 => PhysicalOperator::quad_table_scan(pattern.clone()), // Full scan
            _ => PhysicalOperator::quad_table_scan(pattern.clone()),
        }
    }

    /// Counts the number of bound variables in a triple pattern
    fn count_bound_variables(&self, pattern: &TriplePattern) -> usize {
        let mut count = 0;

        match &pattern.0 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) | Term::QuotedTriple(_) => {}
        }

        match &pattern.1 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) | Term::QuotedTriple(_) => {}
        }

        match &pattern.2 {
            Term::Constant(_) => count += 1,
            Term::Variable(_) | Term::QuotedTriple(_) => {}
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
            LogicalOperator::Unit => "Unit".to_string(),
            LogicalOperator::Scan { pattern } => {
                format!(
                    "Scan({:?},{:?},{:?},graph={:?})",
                    pattern.subject, pattern.predicate, pattern.object, pattern.graph
                )
            }
            LogicalOperator::Union { branches } => {
                let branches = branches
                    .iter()
                    .map(|branch| self.serialize_logical_plan(branch))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("Union({branches})")
            }
            LogicalOperator::Graph { input, graph } => {
                format!("Graph({graph:?},[{}])", self.serialize_logical_plan(input))
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
            LogicalOperator::Buffer { content, origin } => {
                format!("Buffer({:?},{:?})", origin, content)
            }
            LogicalOperator::Subquery { inner, spec } => {
                format!(
                    "Subquery({:?},[{}])",
                    spec,
                    self.serialize_logical_plan(inner)
                )
            }
            LogicalOperator::Bind {
                input,
                function_name,
                arguments,
                output_variable,
            } => {
                format!(
                    "Bind({}, {}({:?}), {})",
                    self.serialize_logical_plan(input),
                    function_name,
                    arguments,
                    output_variable
                )
            }
            LogicalOperator::Values { variables, values } => {
                // Values are semantic content, not merely a cardinality hint.
                // Omitting them aliases equal-sized VALUES nodes in the memo
                // and can replace one UNION branch with another.
                format!("Values({variables:?}, {values:?})")
            }
            LogicalOperator::MLPredict {
                input,
                model_name,
                input_variables,
                output_variable,
            } => {
                format!(
                    "MLPredict({}, model={}, inputs={:?}, output={})",
                    self.serialize_logical_plan(input),
                    model_name,
                    input_variables,
                    output_variable
                )
            }
        }
    }

    /// Serializes a filter expression to a string
    fn serialize_filter_expression(&self, expr: &ConditionExpression) -> String {
        match expr {
            ConditionExpression::Comparison(var, op, value) => {
                format!("{}{}'{}'", var, op, value)
            }
            ConditionExpression::ArithmeticComparison(left, op, right) => {
                format!(
                    "ARITH({}){}ARITH({})",
                    serialize_arith_expr(left),
                    op,
                    serialize_arith_expr(right)
                )
            }
            ConditionExpression::And(left, right) => {
                format!(
                    "({} AND {})",
                    self.serialize_filter_expression(left),
                    self.serialize_filter_expression(right)
                )
            }
            ConditionExpression::Or(left, right) => {
                format!(
                    "({} OR {})",
                    self.serialize_filter_expression(left),
                    self.serialize_filter_expression(right)
                )
            }
            ConditionExpression::Not(inner) => {
                format!("NOT({})", self.serialize_filter_expression(inner))
            }
            ConditionExpression::ArithmeticExpr(expr) => {
                format!("ARITH({})", serialize_arith_expr(expr))
            }
            ConditionExpression::FunctionCall(name, args) => {
                format!("{}({})", name, args.join(", "))
            }
        }
    }

    /// Estimates the cost of a logical plan
    fn estimate_logical_cost(&self, logical_plan: &LogicalOperator) -> u64 {
        let cost_estimator = self.cost_estimator();

        match logical_plan {
            LogicalOperator::Unit => 0,
            LogicalOperator::Scan { pattern } => cost_estimator.estimate_quad_cardinality(pattern),
            LogicalOperator::Union { branches } => branches
                .iter()
                .map(|branch| self.estimate_logical_cost(branch))
                .sum(),
            LogicalOperator::Graph { input, .. } => self.estimate_logical_cost(input),
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
            LogicalOperator::Buffer { .. } => 0,
            LogicalOperator::Subquery { inner, .. } => {
                // Subqueries have materialization cost
                let inner_cost = self.estimate_logical_cost(inner);
                let inner_card = self.estimate_output_cardinality_from_logical(inner);
                // Add materialization overhead (storing results)
                inner_cost + inner_card
            }
            LogicalOperator::Bind {
                input, arguments, ..
            } => {
                let base_cost = self.estimate_logical_cost(input);
                let cardinality = self.estimate_output_cardinality_from_logical(input);
                // Add cost proportional to number of arguments and cardinality
                base_cost + (cardinality * arguments.len() as u64)
            }
            LogicalOperator::Values { values, .. } => {
                // VALUES has very low cost
                values.len() as u64
            }
            LogicalOperator::MLPredict {
                input,
                input_variables,
                ..
            } => {
                let base_cost = self.estimate_logical_cost(input);
                let cardinality = self.estimate_output_cardinality_from_logical(input);

                // ML operations are expensive, so we add significant overhead
                let ml_overhead = 100; // Cost per prediction
                                       // ML prediction cost: base cost + (cardinality * input_features * ML_overhead)
                base_cost + (cardinality * input_variables.len() as u64 * ml_overhead)
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
            LogicalOperator::Unit => None,
            LogicalOperator::Scan { pattern } => {
                if let Term::Constant(pred_id) = &pattern.predicate {
                    Some(*pred_id)
                } else {
                    None
                }
            }
            LogicalOperator::Union { .. } => None,
            LogicalOperator::Graph { input, .. } => self.extract_predicate_from_plan(input),
            LogicalOperator::Join { left, .. } => self.extract_predicate_from_plan(left),
            LogicalOperator::Selection { predicate, .. } => {
                self.extract_predicate_from_plan(predicate)
            }
            LogicalOperator::Projection { predicate, .. } => {
                self.extract_predicate_from_plan(predicate)
            }
            LogicalOperator::Buffer { .. } => None,
            LogicalOperator::Subquery { inner, .. } => self.extract_predicate_from_plan(inner),
            LogicalOperator::Bind { input, .. } => self.extract_predicate_from_plan(input),
            LogicalOperator::Values { .. } => None,
            LogicalOperator::MLPredict { input, .. } => self.extract_predicate_from_plan(input),
        }
    }

    /// Estimates output cardinality from a logical plan
    fn estimate_output_cardinality_from_logical(&self, logical_plan: &LogicalOperator) -> u64 {
        self.estimate_logical_cardinality_in_context(logical_plan, None)
    }

    fn estimate_logical_cardinality_in_context(
        &self,
        logical_plan: &LogicalOperator,
        active_graph: Option<GraphId>,
    ) -> u64 {
        let cost_estimator = self.cost_estimator();

        match logical_plan {
            LogicalOperator::Unit => 1,
            LogicalOperator::Scan { pattern } => {
                if let Some(GraphId::Named(graph)) = active_graph {
                    let mut scoped = pattern.clone();
                    if matches!(scoped.graph, GraphTerm::Default | GraphTerm::Variable(_)) {
                        scoped.graph = GraphTerm::Named(graph);
                    }
                    cost_estimator.estimate_quad_cardinality(&scoped)
                } else {
                    cost_estimator.estimate_quad_cardinality(pattern)
                }
            }
            LogicalOperator::Union { branches } => branches
                .iter()
                .map(|branch| self.estimate_logical_cardinality_in_context(branch, active_graph))
                .sum(),
            LogicalOperator::Graph { input, graph } => match graph {
                GraphTerm::Default => self.estimate_logical_cardinality_in_context(input, None),
                GraphTerm::Named(graph) if cost_estimator.fixed_graph_is_visible(*graph) => self
                    .estimate_logical_cardinality_in_context(input, Some(GraphId::Named(*graph))),
                GraphTerm::Named(_) => 0,
                GraphTerm::Variable(_) => cost_estimator
                    .visible_named_graphs()
                    .into_iter()
                    .map(|graph| self.estimate_logical_cardinality_in_context(input, Some(graph)))
                    .sum(),
            },
            LogicalOperator::Selection {
                predicate,
                condition,
            } => {
                let base_card =
                    self.estimate_logical_cardinality_in_context(predicate, active_graph);
                if base_card == 0 {
                    return 0;
                }
                let selectivity = cost_estimator.estimate_selectivity(condition);
                ((base_card as f64 * selectivity) as u64).max(1)
            }
            LogicalOperator::Projection { predicate, .. } => {
                self.estimate_logical_cardinality_in_context(predicate, active_graph)
            }
            LogicalOperator::Join { left, right } => {
                let left_card = self.estimate_logical_cardinality_in_context(left, active_graph);
                let right_card = self.estimate_logical_cardinality_in_context(right, active_graph);
                if left_card == 0 || right_card == 0 {
                    return 0;
                }
                let join_selectivity = self.estimate_join_selectivity(left, right);
                ((left_card.min(right_card) as f64 * join_selectivity) as u64).max(1)
            }
            LogicalOperator::Buffer { .. } => 0,
            LogicalOperator::Subquery { inner, .. } => {
                self.estimate_logical_cardinality_in_context(inner, active_graph)
            }
            LogicalOperator::Bind { input, .. } => {
                self.estimate_logical_cardinality_in_context(input, active_graph)
            }
            LogicalOperator::Values { values, .. } => values.len() as u64,
            LogicalOperator::MLPredict { input, .. } => {
                // ML.PREDICT doesn't change cardinality, just adds a column
                self.estimate_logical_cardinality_in_context(input, active_graph)
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
    use shared::terms::Term;

    fn create_test_optimizer() -> Streamertail {
        // Create a mock database for testing
        let database = SparqlDatabase::new();
        Streamertail::new(&database)
    }

    fn var(name: &str) -> Term {
        Term::Variable(name.to_string())
    }

    fn constant(id: u32) -> Term {
        Term::Constant(id)
    }

    fn scan(subject: Term, predicate: Term, object: Term) -> LogicalOperator {
        LogicalOperator::scan((subject, predicate, object))
    }

    fn join_all(mut plans: Vec<LogicalOperator>) -> LogicalOperator {
        let first = plans.remove(0);
        plans.into_iter().fold(first, LogicalOperator::join)
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

    #[test]
    fn test_subject_centered_star_is_detected() {
        let optimizer = create_test_optimizer();
        let plan = join_all(vec![
            scan(var("?segment"), constant(1), var("?sensor")),
            scan(var("?segment"), constant(2), var("?length")),
            scan(var("?segment"), constant(3), constant(4)),
        ]);

        let stars = optimizer
            .is_star_query(&plan)
            .expect("subject star should be detected");

        assert_eq!(stars.len(), 1);
        assert_eq!(stars[0].0, "?segment");
        assert_eq!(stars[0].1.len(), 3);
    }

    #[test]
    fn test_object_centered_repeated_variable_is_not_detected() {
        let optimizer = create_test_optimizer();
        let plan = join_all(vec![
            scan(var("?segment1"), constant(1), var("?sensor")),
            scan(var("?segment2"), constant(1), var("?sensor")),
            scan(var("?segment3"), constant(1), var("?sensor")),
            scan(var("?sensor"), constant(2), constant(3)),
        ]);

        let stars = optimizer.is_star_query(&plan).unwrap_or_default();

        assert!(!stars.iter().any(|(var, _)| var == "?sensor"));
    }

    #[test]
    fn named_graph_scans_never_become_a_default_graph_star_join() {
        let optimizer = create_test_optimizer();
        let graph = GraphTerm::Named(99);
        let plan = join_all(vec![
            LogicalOperator::quad_scan(QuadPattern {
                subject: var("?s"),
                predicate: constant(1),
                object: var("?a"),
                graph: graph.clone(),
            }),
            LogicalOperator::quad_scan(QuadPattern {
                subject: var("?s"),
                predicate: constant(2),
                object: var("?b"),
                graph: graph.clone(),
            }),
            LogicalOperator::quad_scan(QuadPattern {
                subject: var("?s"),
                predicate: constant(3),
                object: var("?c"),
                graph,
            }),
        ]);

        assert!(
            optimizer.is_star_query(&plan).is_none(),
            "the TriplePattern-only star operator would discard graph scope"
        );
    }

    #[test]
    fn test_sensor_path_query_does_not_use_sensor_as_star_center() {
        let optimizer = create_test_optimizer();
        let plan = join_all(vec![
            scan(var("?segment1"), constant(1), var("?segment2")),
            scan(var("?segment2"), constant(1), var("?segment3")),
            scan(var("?segment3"), constant(1), var("?segment4")),
            scan(var("?segment4"), constant(1), var("?segment5")),
            scan(var("?segment5"), constant(1), var("?segment6")),
            scan(var("?sensor"), constant(2), constant(3)),
            scan(var("?segment1"), constant(4), var("?sensor")),
            scan(var("?segment2"), constant(4), var("?sensor")),
            scan(var("?segment3"), constant(4), var("?sensor")),
            scan(var("?segment4"), constant(4), var("?sensor")),
            scan(var("?segment5"), constant(4), var("?sensor")),
            scan(var("?segment6"), constant(4), var("?sensor")),
            scan(var("?segment1"), constant(2), constant(5)),
            scan(var("?segment2"), constant(2), constant(5)),
            scan(var("?segment3"), constant(2), constant(5)),
            scan(var("?segment4"), constant(2), constant(5)),
            scan(var("?segment5"), constant(2), constant(5)),
            scan(var("?segment6"), constant(2), constant(5)),
        ]);

        let stars = optimizer.is_star_query(&plan).unwrap_or_default();

        assert!(!stars.iter().any(|(var, _)| var == "?sensor"));
    }
}
