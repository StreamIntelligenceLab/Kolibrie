/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use super::cost::{collect_pattern_variables, CostEstimator};
use super::execution::{DatasetView, ExecutionEngine};
use super::operators::{LogicalOperator, PhysicalOperator};
use super::stats::DatabaseStats;
use super::types::{ConditionArithmetic, ConditionExpression};

use crate::sparql_database::SparqlDatabase;
use shared::dataset_index::{GraphTerm, QuadPattern};
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
        let reordered = self.reorder_logical(logical_plan);
        self.find_best_plan_recursive(&reordered)
    }

    /// Reorders each uninterrupted group of same-scope scans so every scan after the first shares a variable with the ones before it
    fn reorder_logical(&self, plan: &LogicalOperator) -> LogicalOperator {
        match plan {
            LogicalOperator::Join { left, right } => {
                if self.homogeneous_scan_scope(plan).is_some() {
                    let mut patterns = Vec::new();
                    Self::flatten_scan_group(plan, &mut patterns);
                    if patterns.len() > 1 {
                        return Self::rebuild_left_deep(self.greedy_order_scans(patterns));
                    }
                }
                LogicalOperator::join(self.reorder_logical(left), self.reorder_logical(right))
            }
            LogicalOperator::Graph { input, graph } => {
                LogicalOperator::graph(self.reorder_logical(input), graph.clone())
            }
            LogicalOperator::Selection {
                predicate,
                condition,
            } => LogicalOperator::selection(self.reorder_logical(predicate), condition.clone()),
            LogicalOperator::Projection {
                predicate,
                variables,
            } => LogicalOperator::projection(self.reorder_logical(predicate), variables.clone()),
            LogicalOperator::Union { branches } => LogicalOperator::union(
                branches
                    .iter()
                    .map(|branch| self.reorder_logical(branch))
                    .collect(),
            ),
            LogicalOperator::Subquery { inner, spec } => {
                LogicalOperator::subquery(self.reorder_logical(inner), spec.clone())
            }
            LogicalOperator::Bind {
                input,
                function_name,
                arguments,
                output_variable,
            } => LogicalOperator::bind(
                self.reorder_logical(input),
                function_name.clone(),
                arguments.clone(),
                output_variable.clone(),
            ),
            other => other.clone(),
        }
    }

    fn flatten_scan_group(plan: &LogicalOperator, out: &mut Vec<QuadPattern>) {
        match plan {
            LogicalOperator::Scan { pattern } => out.push(pattern.clone()),
            LogicalOperator::Join { left, right } => {
                Self::flatten_scan_group(left, out);
                Self::flatten_scan_group(right, out);
            }
            _ => {}
        }
    }

    fn rebuild_left_deep(patterns: Vec<QuadPattern>) -> LogicalOperator {
        let mut plan: Option<LogicalOperator> = None;
        for pattern in patterns {
            let scan = LogicalOperator::quad_scan(pattern);
            plan = Some(match plan {
                Some(existing) => LogicalOperator::join(existing, scan),
                None => scan,
            });
        }
        plan.unwrap_or_else(LogicalOperator::unit)
    }

    /// Orders scans so the cheapest anchored pattern runs first and every later pattern joins through a variable the prefix already binds
    fn greedy_order_scans(&self, patterns: Vec<QuadPattern>) -> Vec<QuadPattern> {
        let cost_estimator = self.cost_estimator();
        let empty = HashSet::new();

        let variables: Vec<HashSet<String>> = patterns
            .iter()
            .map(|pattern| {
                let mut set = HashSet::new();
                collect_pattern_variables(pattern, &mut set);
                set
            })
            .collect();
        let constants: Vec<usize> = patterns
            .iter()
            .map(|pattern| {
                [&pattern.subject, &pattern.predicate, &pattern.object]
                    .iter()
                    .filter(|term| matches!(term, Term::Constant(_)))
                    .count()
            })
            .collect();

        let mut remaining: Vec<usize> = (0..patterns.len()).collect();
        let mut bound: HashSet<String> = HashSet::new();
        let mut order: Vec<usize> = Vec::with_capacity(patterns.len());

        // Nothing is bound yet, so constants alone rank the seed
        let seed = *remaining
            .iter()
            .min_by_key(|&&index| {
                (
                    cost_estimator.estimate_bound_scan_cardinality(&patterns[index], &empty),
                    std::cmp::Reverse(constants[index]),
                    index,
                )
            })
            .expect("scan group is never empty");
        remaining.retain(|&index| index != seed);
        bound.extend(variables[seed].iter().cloned());
        order.push(seed);

        while !remaining.is_empty() {
            let connected: Vec<usize> = remaining
                .iter()
                .copied()
                .filter(|&index| !variables[index].is_disjoint(&bound))
                .collect();
            let candidates = if connected.is_empty() {
                &remaining
            } else {
                &connected
            };

            let next = *candidates
                .iter()
                .min_by_key(|&&index| {
                    (
                        cost_estimator.estimate_bound_scan_cardinality(&patterns[index], &bound),
                        std::cmp::Reverse(constants[index]),
                        index,
                    )
                })
                .expect("candidate list is never empty");

            remaining.retain(|&index| index != next);
            bound.extend(variables[next].iter().cloned());
            order.push(next);
        }

        let mut ordered: Vec<Option<QuadPattern>> =
            patterns.into_iter().map(Some).collect();
        order
            .into_iter()
            .map(|index| ordered[index].take().expect("each pattern is used once"))
            .collect()
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
                // Join order is already fixed by `reorder_logical`
                let best_left_plan = self.find_best_plan_recursive(left);
                let best_right_plan = self.find_best_plan_recursive(right);

                // Implementation rules: costing decides between the three join algorithms
                candidates.push(PhysicalOperator::bind_join(
                    best_left_plan.clone(),
                    best_right_plan.clone(),
                ));

                candidates.push(PhysicalOperator::hash_join(
                    best_left_plan.clone(),
                    best_right_plan.clone(),
                ));

                candidates.push(PhysicalOperator::nested_loop_join(
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
                    result = PhysicalOperator::bind_join(result, scan);
                }
            }

            for (idx, pattern) in all_patterns.iter().enumerate() {
                if !used_pattern_indices.contains(&idx) {
                    let scan = PhysicalOperator::index_scan(pattern.clone());
                    result = PhysicalOperator::bind_join(result, scan);
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
                        result = PhysicalOperator::bind_join(result, scan);
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
    use shared::dataset_index::GraphId;
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

    const NEXT: u32 = 1;
    const TYPE: u32 = 2;
    const ANCHOR: u32 = 3;

    /// A dataset with one rare anchor predicate and one high-fan-out edge predicate
    fn chain_stats() -> Arc<DatabaseStats> {
        let mut stats = DatabaseStats::new();
        stats.total_triples = 100_000;
        stats.distinct_subjects = 50_000;
        stats.distinct_objects = 50_000;
        stats.predicate_cardinalities.insert(NEXT, 90_000);
        stats.predicate_cardinalities.insert(TYPE, 10);
        stats.predicate_distinct_subjects.insert(NEXT, 45_000);
        stats.predicate_distinct_objects.insert(NEXT, 45_000);
        stats.predicate_distinct_subjects.insert(TYPE, 10);
        stats.predicate_distinct_objects.insert(TYPE, 1);
        stats
            .graph_cardinalities
            .insert(GraphId::Default, 100_000);
        Arc::new(stats)
    }

    fn quad(subject: Term, predicate: Term, object: Term) -> QuadPattern {
        QuadPattern {
            subject,
            predicate,
            object,
            graph: GraphTerm::Default,
        }
    }

    /// The links of `?x0 -> ?x1 -> ... -> ?xn`, without the anchor
    fn chain_links(links: usize) -> Vec<QuadPattern> {
        (0..links)
            .map(|i| {
                quad(
                    var(&format!("?x{}", i)),
                    constant(NEXT),
                    var(&format!("?x{}", i + 1)),
                )
            })
            .collect()
    }

    #[test]
    fn greedy_ordering_starts_from_the_anchor_wherever_it_appears() {
        let optimizer = Streamertail::with_cached_stats(chain_stats());
        let anchor = quad(var("?x0"), constant(TYPE), constant(ANCHOR));

        for anchor_at in 0..=4 {
            let mut patterns = chain_links(4);
            patterns.insert(anchor_at, anchor.clone());

            let ordered = optimizer.greedy_order_scans(patterns);

            assert_eq!(
                ordered[0], anchor,
                "the selective anchor must run first when placed at {}",
                anchor_at
            );

            let mut bound: HashSet<String> = HashSet::new();
            collect_pattern_variables(&ordered[0], &mut bound);
            for pattern in &ordered[1..] {
                let mut variables = HashSet::new();
                collect_pattern_variables(pattern, &mut variables);
                assert!(
                    !variables.is_disjoint(&bound),
                    "every step after the anchor must join through a bound variable"
                );
                bound.extend(variables);
            }
        }
    }

    #[test]
    fn greedy_ordering_keeps_source_order_without_distinguishing_statistics() {
        let optimizer = Streamertail::with_cached_stats(Arc::new(DatabaseStats::new()));
        let patterns = chain_links(4);

        assert_eq!(optimizer.greedy_order_scans(patterns.clone()), patterns);
    }

    #[test]
    fn reordering_never_crosses_a_graph_scope_boundary() {
        let optimizer = Streamertail::with_cached_stats(chain_stats());
        let named = QuadPattern {
            graph: GraphTerm::Named(7),
            ..quad(var("?x0"), constant(TYPE), constant(ANCHOR))
        };
        let plan = LogicalOperator::join(
            LogicalOperator::quad_scan(chain_links(1).remove(0)),
            LogicalOperator::quad_scan(named),
        );

        assert_eq!(
            format!("{:?}", optimizer.reorder_logical(&plan)),
            format!("{:?}", plan),
            "scans in different graph scopes are not one reorderable group"
        );
    }

    #[test]
    fn sensor_path_plan_still_builds_a_star_that_is_not_sensor_centered() {
        let mut optimizer = Streamertail::with_cached_stats(chain_stats());
        let plan = join_all(vec![
            scan(var("?segment1"), constant(NEXT), var("?segment2")),
            scan(var("?segment2"), constant(NEXT), var("?segment3")),
            scan(var("?sensor"), constant(TYPE), constant(ANCHOR)),
            scan(var("?segment1"), constant(4), var("?sensor")),
            scan(var("?segment2"), constant(4), var("?sensor")),
            scan(var("?segment3"), constant(4), var("?sensor")),
        ]);

        let physical = optimizer.find_best_plan(&plan);

        fn star_centers(plan: &PhysicalOperator, out: &mut Vec<String>) {
            match plan {
                PhysicalOperator::StarJoin { join_var, .. } => out.push(join_var.clone()),
                PhysicalOperator::BindJoin { left, right }
                | PhysicalOperator::HashJoin { left, right }
                | PhysicalOperator::NestedLoopJoin { left, right } => {
                    star_centers(left, out);
                    star_centers(right, out);
                }
                PhysicalOperator::Filter { input, .. }
                | PhysicalOperator::Projection { input, .. }
                | PhysicalOperator::Graph { input, .. } => star_centers(input, out),
                _ => {}
            }
        }

        let mut centers = Vec::new();
        star_centers(&physical, &mut centers);
        assert!(
            !centers.iter().any(|center| center == "?sensor"),
            "?sensor is a path endpoint, not a star center: {:?}",
            centers
        );
    }
}
