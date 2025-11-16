/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */
#[cfg(not(test))]
use log::{debug, error}; // Use log crate when building application
use rsp::r2r::{AsAnyMut, R2ROperator};
use rsp::r2s::{Relation2StreamOperator, StreamOperator};
use rsp::s2r::{CSPARQLWindow, ContentContainer, Report, ReportStrategy, Tick};
use shared::query::{StreamType, WindowBlock, WindowClause};
use shared::triple::Triple;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(test)]
use std::{println as debug, println as error};

use crate::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use crate::parser::parse_combined_query;
use crate::sparql_database::SparqlDatabase;
use crate::volcano_optimizer::{
    build_logical_plan, ExecutionEngine, LogicalOperator, PhysicalOperator, VolcanoOptimizer,
};

pub struct Syntax {}

#[derive(Clone, Copy)]
pub enum OperationMode {
    SingleThread,
    MultiThread,
}

#[derive(Clone, Copy, Debug)]
pub enum QueryExecutionMode {
    Standard,
    Volcano,
}

/// Window configuration extracted from parsed RSP-QL query
#[derive(Debug, Clone)]
pub struct RSPWindow {
    pub window_iri: String,
    pub stream_iri: String,
    pub width: usize,
    pub slide: usize,
    pub tick: Tick,
    pub report_strategy: ReportStrategy,
    pub query: String, // The SPARQL query to execute on this window
}

/// RSP Query configuration extracted from parsed RSP-QL
#[derive(Debug)]
pub struct RSPQueryConfig {
    pub windows: Vec<RSPWindow>,
    pub output_stream: String,
    pub stream_type: StreamOperator,
    pub shared_variables: Vec<String>, // Variables that appear across multiple windows
    pub static_patterns: Vec<(String, String, String)>, // Static graph patterns outside windows
    pub static_window_shared_vars: Vec<String>, // Variables shared between static patterns and windows
}

/// RSP-QL Query Plan using Volcano optimizer
#[derive(Debug)]
pub struct RSPQueryPlan {
    pub window_plans: Vec<PhysicalOperator>,
    pub static_data_plan: Option<PhysicalOperator>,
    pub cross_window_join_plan: Option<PhysicalOperator>,
    pub static_window_join_plan: Option<PhysicalOperator>,
    pub shared_variables: Vec<String>,
    pub static_window_shared_vars: Vec<String>,
    pub output_variables: Vec<String>,
}

/// Result from a single window execution
#[derive(Debug, Clone)]
pub struct WindowResult {
    pub window_iri: String,
    pub results: Vec<BTreeMap<String, String>>, // Variable bindings
    pub timestamp: usize,
}

pub struct RSPBuilder<'a, I, O> {
    rsp_ql_query: Option<&'a str>,
    triples: Option<&'a str>,
    rules: Option<&'a str>,
    result_consumer: Option<ResultConsumer<O>>,
    r2r: Option<Box<dyn R2ROperator<I, O, O>>>,
    operation_mode: OperationMode,
    query_execution_mode: QueryExecutionMode,
    syntax: String,
}

impl<'a, I, O> RSPBuilder<'a, I, O>
where
    O: Clone + Hash + Eq + Send + Debug + 'static,
    I: Eq + PartialEq + Clone + Debug + Hash + Send + 'static,
{
    pub fn new() -> RSPBuilder<'a, I, O> {
        RSPBuilder {
            rsp_ql_query: None,
            triples: None,
            rules: None,
            result_consumer: None,
            r2r: None,
            operation_mode: OperationMode::MultiThread,
            query_execution_mode: QueryExecutionMode::Volcano, // Default to Volcano optimizer
            syntax: "ntriples".to_string(),
        }
    }

    /// Add RSP-QL query instead of separate window parameters and SPARQL query
    pub fn add_rsp_ql_query(mut self, query: &'a str) -> RSPBuilder<'a, I, O> {
        self.rsp_ql_query = Some(query);
        self
    }

    pub fn add_triples(mut self, triples: &'a str) -> RSPBuilder<'a, I, O> {
        self.triples = Some(triples);
        self
    }

    pub fn add_rules(mut self, rules: &'a str) -> RSPBuilder<'a, I, O> {
        self.rules = Some(rules);
        self
    }

    pub fn add_consumer(mut self, consumer: ResultConsumer<O>) -> RSPBuilder<'a, I, O> {
        self.result_consumer = Some(consumer);
        self
    }

    pub fn add_r2r(mut self, r2r: Box<dyn R2ROperator<I, O, O>>) -> RSPBuilder<'a, I, O> {
        self.r2r = Some(r2r);
        self
    }

    pub fn set_operation_mode(mut self, operation_mode: OperationMode) -> RSPBuilder<'a, I, O> {
        self.operation_mode = operation_mode;
        self
    }

    pub fn set_query_execution_mode(mut self, mode: QueryExecutionMode) -> RSPBuilder<'a, I, O> {
        self.query_execution_mode = mode;
        self
    }

    /// Parse the RSP-QL query and extract window configurations
    fn parse_rsp_ql_query(&self, query: &str) -> Result<RSPQueryConfig, String> {
        match parse_combined_query(query) {
            Ok((_, parsed_query)) => {
                if let Some(register_clause) = &parsed_query.register_clause {
                    let mut windows = Vec::new();

                    // Extract windows from the register clause
                    for window_clause in &register_clause.query.window_clause {
                        let window = self.create_rsp_window(
                            window_clause,
                            &register_clause.query.window_blocks,
                        )?;
                        windows.push(window);
                    }

                    // Convert stream type
                    let stream_type = match &register_clause.stream_type {
                        StreamType::RStream => StreamOperator::RSTREAM,
                        StreamType::IStream => StreamOperator::ISTREAM,
                        StreamType::DStream => StreamOperator::DSTREAM,
                        StreamType::Custom(_) => StreamOperator::RSTREAM, // Default fallback
                    };

                    // Extract static patterns from WHERE clause (outside window blocks)
                    let static_patterns = register_clause
                        .query
                        .where_clause
                        .0
                        .iter()
                        .map(|(s, p, o)| (s.to_string(), p.to_string(), o.to_string()))
                        .collect();

                    Ok(RSPQueryConfig {
                        windows,
                        output_stream: register_clause.output_stream_iri.to_string(),
                        stream_type,
                        shared_variables: Vec::new(), // Will be populated later
                        static_patterns,
                        static_window_shared_vars: Vec::new(), // Will be populated later
                    })
                } else {
                    Err("No REGISTER clause found in RSP-QL query".to_string())
                }
            }
            Err(e) => Err(format!("Failed to parse RSP-QL query: {:?}", e)),
        }
    }

    /// Create RSP window from parsed window clause
    fn create_rsp_window(
        &self,
        window_clause: &WindowClause,
        window_blocks: &[WindowBlock],
    ) -> Result<RSPWindow, String> {
        // Find the corresponding window block for this window
        let window_query = window_blocks
            .iter()
            .find(|block| block.window_name == window_clause.window_iri)
            .map(|block| {
                // Convert window block patterns to SPARQL query
                let mut query = "SELECT * WHERE {\n".to_string();
                for (s, p, o) in &block.patterns {
                    query.push_str(&format!("    {} {} {} .\n", s, p, o));
                }
                query.push_str("}");
                query
            })
            .unwrap_or_else(|| "SELECT * WHERE { ?s ?p ?o }".to_string());

        // Convert window specification
        let slide = window_clause
            .window_spec
            .slide
            .unwrap_or(window_clause.window_spec.width);

        let tick = match window_clause.window_spec.tick {
            Some("TIME_DRIVEN") => Tick::TimeDriven,
            Some("TUPLE_DRIVEN") => Tick::TupleDriven,
            Some("BATCH_DRIVEN") => Tick::BatchDriven,
            _ => Tick::TimeDriven, // Default
        };

        let report_strategy = match window_clause.window_spec.report_strategy {
            Some("ON_WINDOW_CLOSE") => ReportStrategy::OnWindowClose,
            Some("ON_CONTENT_CHANGE") => ReportStrategy::OnContentChange,
            Some("NON_EMPTY_CONTENT") => ReportStrategy::NonEmptyContent,
            Some("PERIODIC") => ReportStrategy::Periodic(1000), // Default 1 second
            _ => ReportStrategy::OnWindowClose,                 // Default
        };

        Ok(RSPWindow {
            window_iri: window_clause.window_iri.to_string(),
            stream_iri: window_clause.stream_iri.to_string(),
            width: window_clause.window_spec.width,
            slide,
            tick,
            report_strategy,
            query: window_query,
        })
    }

    /// Create RSP-QL query plan using Volcano optimizer for cross-window and static data joins
    fn create_rsp_query_plan(query_config: &RSPQueryConfig) -> Result<RSPQueryPlan, String> {
        let mut window_plans = Vec::new();

        // Create individual window plans
        for window in &query_config.windows {
            // For now, create simple scan plans for each window
            // In a full implementation, this would parse the window query to create proper plans
            let logical_plan = Self::create_window_logical_plan(&window.query)?;
            // Convert to physical plan (simplified - would need proper optimizer integration)
            let physical_plan = Self::convert_to_physical_plan(logical_plan);
            window_plans.push(physical_plan);
        }

        // Create static data plan if there are static patterns
        let static_data_plan = if !query_config.static_patterns.is_empty() {
            Some(Self::create_static_data_plan(
                &query_config.static_patterns,
            )?)
        } else {
            None
        };

        // Create cross-window join plan if there are shared variables between windows
        let cross_window_join_plan = if query_config.shared_variables.len() > 0 {
            Some(Self::create_cross_window_join_plan(
                &window_plans,
                &query_config.shared_variables,
            )?)
        } else {
            None
        };

        // Create static-window join plan if there are shared variables between static and window data
        let static_window_join_plan =
            if !query_config.static_window_shared_vars.is_empty() && static_data_plan.is_some() {
                Some(Self::create_static_window_join_plan(
                    static_data_plan.as_ref().unwrap(),
                    &window_plans,
                    &query_config.static_window_shared_vars,
                )?)
            } else {
                None
            };

        // Extract output variables from the query (simplified)
        let mut output_variables = query_config.shared_variables.clone();
        output_variables.extend(query_config.static_window_shared_vars.clone());

        Ok(RSPQueryPlan {
            window_plans,
            static_data_plan,
            cross_window_join_plan,
            static_window_join_plan,
            shared_variables: query_config.shared_variables.clone(),
            static_window_shared_vars: query_config.static_window_shared_vars.clone(),
            output_variables,
        })
    }

    /// Create logical plan for a single window query
    fn create_window_logical_plan(query: &str) -> Result<LogicalOperator, String> {
        // Simplified - parse basic SELECT * WHERE { pattern } queries
        // In a full implementation, this would use the full SPARQL parser
        use shared::terms::{Term, TriplePattern};

        // Create a dummy pattern for now - would need proper parsing
        let pattern = (
            Term::Variable("s".to_string()),
            Term::Variable("p".to_string()),
            Term::Variable("o".to_string()),
        );

        Ok(LogicalOperator::scan(pattern))
    }

    /// Convert logical plan to physical plan (simplified)
    fn convert_to_physical_plan(logical: LogicalOperator) -> PhysicalOperator {
        match logical {
            LogicalOperator::Scan { pattern } => PhysicalOperator::table_scan(pattern),
            LogicalOperator::Selection {
                predicate,
                condition,
            } => PhysicalOperator::filter(Self::convert_to_physical_plan(*predicate), condition),
            LogicalOperator::Join { left, right } => PhysicalOperator::hash_join(
                Self::convert_to_physical_plan(*left),
                Self::convert_to_physical_plan(*right),
            ),
            LogicalOperator::Projection {
                predicate,
                variables,
            } => {
                PhysicalOperator::projection(Self::convert_to_physical_plan(*predicate), variables)
            }
        }
    }

    /// Create cross-window join plan using Volcano optimizer
    fn create_cross_window_join_plan(
        window_plans: &[PhysicalOperator],
        shared_variables: &[String],
    ) -> Result<PhysicalOperator, String> {
        if window_plans.is_empty() {
            return Err("No window plans to join".to_string());
        }

        // Start with the first window
        let mut joined_plan = window_plans[0].clone();

        // Join with each subsequent window using hash joins
        for window_plan in window_plans.iter().skip(1) {
            joined_plan = PhysicalOperator::optimized_hash_join(joined_plan, window_plan.clone());
        }

        // Add projection for shared variables if needed
        if !shared_variables.is_empty() {
            joined_plan = PhysicalOperator::projection(joined_plan, shared_variables.to_vec());
        }

        Ok(joined_plan)
    }

    /// Create static data plan using Volcano optimizer
    fn create_static_data_plan(
        static_patterns: &[(String, String, String)],
    ) -> Result<PhysicalOperator, String> {
        if static_patterns.is_empty() {
            return Err("No static patterns to process".to_string());
        }

        // For now, create a simplified static plan that represents the static patterns
        // In a full implementation, this would properly encode terms and create actual scan operations
        // Currently we create a placeholder plan structure

        // Start with a dummy pattern for the static data
        // This is simplified - a full implementation would need proper term encoding
        use shared::terms::Term;
        let dummy_pattern = (
            Term::Variable("static_s".to_string()),
            Term::Variable("static_p".to_string()),
            Term::Variable("static_o".to_string()),
        );

        let mut static_plan = PhysicalOperator::table_scan(dummy_pattern);

        // For multiple patterns, create a chain of joins (simplified)
        for i in 1..static_patterns.len() {
            let next_pattern = (
                Term::Variable(format!("static_s_{}", i)),
                Term::Variable(format!("static_p_{}", i)),
                Term::Variable(format!("static_o_{}", i)),
            );
            let pattern_scan = PhysicalOperator::table_scan(next_pattern);
            static_plan = PhysicalOperator::hash_join(static_plan, pattern_scan);
        }

        Ok(static_plan)
    }

    /// Create static-window join plan using Volcano optimizer
    fn create_static_window_join_plan(
        static_plan: &PhysicalOperator,
        window_plans: &[PhysicalOperator],
        shared_variables: &[String],
    ) -> Result<PhysicalOperator, String> {
        if window_plans.is_empty() {
            return Err("No window plans to join with static data".to_string());
        }

        // Start with static data as the base
        let mut joined_plan = static_plan.clone();

        // Join with each window that shares variables with static data
        for window_plan in window_plans {
            joined_plan = PhysicalOperator::optimized_hash_join(joined_plan, window_plan.clone());
        }

        // Add projection for shared variables if needed
        if !shared_variables.is_empty() {
            joined_plan = PhysicalOperator::projection(joined_plan, shared_variables.to_vec());
        }

        Ok(joined_plan)
    }

    pub fn build(mut self) -> Result<RSPEngine<I, O>, String> {
        let rsp_ql_query = self
            .rsp_ql_query
            .take()
            .ok_or("Please provide RSP-QL query")?;
        let r2r = self.r2r.take().ok_or("Please provide R2R operator!")?;
        let triples = self.triples.take().unwrap_or("");
        let syntax = self.syntax.clone();
        let rules = self.rules.take().unwrap_or("");
        let result_consumer = self.result_consumer.take().unwrap_or(ResultConsumer {
            function: Arc::new(Box::new(|r| println!("Bindings: {:?}", r))),
        });
        let operation_mode = self.operation_mode;

        // Parse the RSP-QL query
        let mut query_config = self.parse_rsp_ql_query(rsp_ql_query)?;

        // Analyze shared variables across windows
        query_config.shared_variables =
            RSPEngine::<I, O>::extract_shared_variables_static(&query_config.windows);

        // Analyze shared variables between static patterns and windows
        query_config.static_window_shared_vars =
            RSPEngine::<I, O>::extract_static_window_shared_variables(
                &query_config.static_patterns,
                &query_config.windows,
            );

        // Create RSP-QL query plan using Volcano optimizer
        let rsp_query_plan = Self::create_rsp_query_plan(&query_config)?;

        Ok(RSPEngine::new(
            query_config,
            triples,
            syntax,
            rules,
            result_consumer,
            r2r,
            operation_mode,
            self.query_execution_mode,
            rsp_query_plan,
        ))
    }
}

pub struct RSPEngine<I, O>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    windows: Vec<CSPARQLWindow<I>>,
    r2r: Arc<Mutex<Box<dyn R2ROperator<I, O, O>>>>,
    r2s_consumer: ResultConsumer<O>,
    r2s_operator: Arc<Mutex<Relation2StreamOperator<O>>>,
    window_configs: Vec<RSPWindow>,
    query_execution_mode: QueryExecutionMode,
    shared_variables: Vec<String>,
    // Channel for collecting window results for cross-window joins
    window_result_sender: Arc<Mutex<Sender<WindowResult>>>,
    window_result_receiver: Arc<Mutex<Receiver<WindowResult>>>,
    // RSP-QL Query Plan using Volcano optimizer
    rsp_query_plan: RSPQueryPlan,
}

pub struct ResultConsumer<I> {
    pub function: Arc<dyn Fn(I) -> () + Send + Sync>,
}

impl<I, O> RSPEngine<I, O>
where
    O: Clone + Hash + Eq + Send + 'static,
    I: Eq + PartialEq + Clone + Debug + Hash + Send + 'static,
{
    pub fn new(
        query_config: RSPQueryConfig,
        triples: &str,
        syntax: String,
        rules: &str,
        result_consumer: ResultConsumer<O>,
        r2r: Box<dyn R2ROperator<I, O, O>>,
        operation_mode: OperationMode,
        query_execution_mode: QueryExecutionMode,
        rsp_query_plan: RSPQueryPlan,
    ) -> RSPEngine<I, O> {
        let mut store = r2r;

        // Load initial data
        match store.load_triples(triples, syntax) {
            Err(parsing_error) => error!("Unable to load ABox: {:?}", parsing_error.to_string()),
            _ => (),
        }
        store.load_rules(rules);

        // Create windows based on parsed configuration
        let mut windows = Vec::new();
        for window_config in &query_config.windows {
            let mut report = Report::new();
            report.add(window_config.report_strategy.clone());
            let window = CSPARQLWindow::new(
                window_config.width,
                window_config.slide,
                report,
                window_config.tick.clone(),
            );
            windows.push(window);
        }

        // Create channel for cross-window result coordination
        let (result_sender, result_receiver) = mpsc::channel::<WindowResult>();

        let mut engine = RSPEngine {
            windows,
            r2r: Arc::new(Mutex::new(store)),
            r2s_consumer: result_consumer,
            r2s_operator: Arc::new(Mutex::new(Relation2StreamOperator::new(
                query_config.stream_type,
                0,
            ))),
            window_configs: query_config.windows.clone(),
            query_execution_mode,
            shared_variables: query_config.shared_variables.clone(),
            window_result_sender: Arc::new(Mutex::new(result_sender)),
            window_result_receiver: Arc::new(Mutex::new(result_receiver)),
            rsp_query_plan,
        };

        match operation_mode {
            OperationMode::SingleThread => {
                error!("Single thread mode not yet implemented for multi-window RSP-QL");
            }
            OperationMode::MultiThread => {
                engine.register_multi_window_r2r();
                // Start cross-window join coordinator using Volcano optimizer if there are shared variables
                if !engine.shared_variables.is_empty() {
                    engine.start_volcano_cross_window_join_coordinator();
                }
                // Start static-window join coordinator if there are shared variables between static and window data
                if !engine.rsp_query_plan.static_window_shared_vars.is_empty()
                    && engine.rsp_query_plan.static_data_plan.is_some()
                {
                    engine.start_volcano_static_window_join_coordinator();
                }
            }
        }

        engine
    }

    /// Register R2R processing for multiple windows
    fn register_multi_window_r2r(&mut self) {
        for (window_idx, window) in self.windows.iter_mut().enumerate() {
            let receiver = window.register();
            let consumer_temp = self.r2r.clone();
            let query = self.window_configs[window_idx].query.clone();
            let window_iri = self.window_configs[window_idx].window_iri.clone();
            let query_execution_mode = self.query_execution_mode;
            let has_shared_variables = !self.shared_variables.is_empty();
            let window_result_sender = self.window_result_sender.clone();
            let r2s_consumer = if has_shared_variables {
                // If we have shared variables, collect results for joining
                // Create a no-op consumer for individual window results
                let dummy_consumer: Arc<dyn Fn(O) + Send + Sync> = Arc::new(|_| {
                    debug!("Window result collected for cross-window join");
                });
                dummy_consumer
            } else {
                // If no shared variables, send results directly to consumer
                self.r2s_consumer.function.clone()
            };
            let r2s_operator = self.r2s_operator.clone();

            thread::spawn(move || {
                loop {
                    match receiver.recv() {
                        Ok(content) => {
                            debug!(
                                "Processing window {} with query: {} using {:?} execution",
                                window_iri, query, query_execution_mode
                            );

                            // if has_shared_variables {
                            //     // Process for cross-window joins using Volcano plan
                            //     Self::execute_volcano_plan_and_collect_for_join(
                            //         window_idx,
                            //         &window_iri,
                            //         consumer_temp.clone(),
                            //         window_result_sender.clone(),
                            //         content,
                            //         query_execution_mode,
                            //         &self.rsp_query_plan,
                            //     );
                            // } else {
                            //     // Process independently using Volcano plan
                                Self::execute_volcano_plan_and_call_r2s(
                                    window_idx,
                                    consumer_temp.clone(),
                                    r2s_consumer.clone(),
                                    r2s_operator.clone(),
                                    content,
                                    query_execution_mode,
                                    &self.rsp_query_plan,
                                );
                            // }
                        }
                        Err(_) => {
                            debug!("Shutting down window {}!", window_iri);
                            break;
                        }
                    }
                }
                debug!("Shutdown complete for window {}!", window_iri);
            });
        }
    }

    /// Process window content and collect results for cross-window joins using Volcano plans
    fn execute_volcano_plan_and_collect_for_join(
        window_idx: usize,
        window_iri: &str,
        consumer_temp: Arc<Mutex<Box<dyn R2ROperator<I, O, O>>>>,
        window_result_sender: Arc<Mutex<Sender<WindowResult>>>,
        content: ContentContainer<I>,
        _query_execution_mode: QueryExecutionMode,
        rsp_query_plan: &RSPQueryPlan,
    ) {
        debug!(
            "Executing Volcano plan for cross-window join, window {}: {:?}",
            window_idx, content
        );
        let time_stamp = content.get_last_timestamp_changed();
        let mut store = consumer_temp.lock().unwrap();

        // Add data to store
        content.clone().into_iter().for_each(|t| {
            store.add(t);
        });

        // Materialize inferred data
        let inferred = store.materialize();

        // Execute using Volcano physical operator for this window
        let string_results = if window_idx < rsp_query_plan.window_plans.len() {
            Self::execute_volcano_operator_for_join(
                &rsp_query_plan.window_plans[window_idx],
                &mut store,
            )
        } else {
            debug!(
                "No Volcano plan found for window {}, falling back to empty result",
                window_idx
            );
            Vec::new()
        };

        let window_result = Self::convert_to_window_result(window_iri, string_results, time_stamp);

        // Send result for cross-window joining
        if let Err(e) = window_result_sender.lock().unwrap().send(window_result) {
            debug!("Failed to send window result for joining: {:?}", e);
        }

        // Clean up: remove data from store
        content.iter().for_each(|t| {
            store.remove(t);
        });
        inferred.iter().for_each(|t| {
            store.remove(t);
        });
    }

    /// Process window content and collect results for cross-window joins (legacy string-based)
    fn evaluate_r2r_and_collect_for_join(
        query: &str,
        window_iri: &str,
        consumer_temp: Arc<Mutex<Box<dyn R2ROperator<I, O, O>>>>,
        window_result_sender: Arc<Mutex<Sender<WindowResult>>>,
        content: ContentContainer<I>,
        _query_execution_mode: QueryExecutionMode,
    ) {
        debug!(
            "R2R operator retrieved graph for cross-window join: {:?}",
            content
        );
        let time_stamp = content.get_last_timestamp_changed();
        let mut store = consumer_temp.lock().unwrap();

        // Add data to store
        content.clone().into_iter().for_each(|t| {
            store.add(t);
        });

        // Materialize inferred data
        let inferred = store.materialize();

        // Execute window-specific query
        let r2r_result = store.execute_query(query);

        // Convert results to variable bindings format for joining
        // For now, convert Vec<O> to Vec<Vec<String>> - this is a simplified approach
        let string_results: Vec<Vec<String>> = r2r_result
            .into_iter()
            .map(|_| vec!["placeholder".to_string()]) // Simplified conversion
            .collect();
        let window_result = Self::convert_to_window_result(window_iri, string_results, time_stamp);

        // Send result for cross-window joining
        if let Err(e) = window_result_sender.lock().unwrap().send(window_result) {
            debug!("Failed to send window result for joining: {:?}", e);
        }

        // Clean up: remove data from store
        content.iter().for_each(|t| {
            store.remove(t);
        });
        inferred.iter().for_each(|t| {
            store.remove(t);
        });
    }

    fn evaluate_r2r_and_call_r2s(
        query: &str,
        consumer_temp: Arc<Mutex<Box<dyn R2ROperator<I, O, O>>>>,
        r2s_consumer: Arc<dyn Fn(O) + Send + Sync>,
        r2s_operator: Arc<Mutex<Relation2StreamOperator<O>>>,
        content: ContentContainer<I>,
        _query_execution_mode: QueryExecutionMode,
    ) {
        debug!("R2R operator retrieved graph {:?}", content);
        let time_stamp = content.get_last_timestamp_changed();
        let mut store = consumer_temp.lock().unwrap();

        // Add data to store
        content.clone().into_iter().for_each(|t| {
            store.add(t);
        });

        // Materialize inferred data
        let inferred = store.materialize();

        // Execute window-specific query using selected execution mode
        let r2r_result = store.execute_query(&query);

        // Apply R2S operator using the actual output type
        let r2s_result = r2s_operator.lock().unwrap().eval(r2r_result, time_stamp);

        // Send results to consumer
        for result in r2s_result {
            (r2s_consumer)(result);
        }

        // Clean up: remove data from store
        content.iter().for_each(|t| {
            store.remove(t);
        });
        inferred.iter().for_each(|t| {
            store.remove(t);
        });
    }

    /// Execute Volcano physical operator plan instead of string-based query
    fn execute_volcano_plan_and_call_r2s(
        window_idx: usize,
        consumer_temp: Arc<Mutex<Box<dyn R2ROperator<I, O, O>>>>,
        r2s_consumer: Arc<dyn Fn(O) + Send + Sync>,
        r2s_operator: Arc<Mutex<Relation2StreamOperator<O>>>,
        content: ContentContainer<I>,
        _query_execution_mode: QueryExecutionMode,
        rsp_query_plan: &RSPQueryPlan,
    ) {
        debug!("Executing Volcano plan for window {}", window_idx);
        let time_stamp = content.get_last_timestamp_changed();
        let mut store = consumer_temp.lock().unwrap();

        // Add data to store
        content.clone().into_iter().for_each(|t| {
            store.add(t);
        });

        // Materialize inferred data
        let inferred = store.materialize();

        // Execute using Volcano physical operator instead of string query
        let r2r_result = if window_idx < rsp_query_plan.window_plans.len() {
            Self::execute_volcano_operator(&rsp_query_plan.window_plans[window_idx], &mut store)
        } else {
            debug!(
                "No Volcano plan found for window {}, falling back to empty result",
                window_idx
            );
            Vec::new()
        };

        // Apply R2S operator using the actual output type
        let r2s_result = r2s_operator.lock().unwrap().eval(r2r_result, time_stamp);

        // Send results to consumer
        for result in r2s_result {
            (r2s_consumer)(result);
        }

        // Clean up: remove data from store
        content.iter().for_each(|t| {
            store.remove(t);
        });
        inferred.iter().for_each(|t| {
            store.remove(t);
        });
    }

    /// Execute a Volcano physical operator on the R2R store
    fn execute_volcano_operator(
        physical_operator: &PhysicalOperator,
        store: &mut Box<dyn R2ROperator<I, O, O>>,
    ) -> Vec<O> {
        // Try to downcast to SimpleR2R to access the SparqlDatabase
        let any_store = store.as_any_mut();
        if let Some(simple_r2r) = any_store.downcast_mut::<SimpleR2R>() {
            debug!("Executing Volcano operator on SparqlDatabase");

            // Execute the physical operator using Volcano ExecutionEngine
            let string_results = ExecutionEngine::execute(physical_operator, &mut simple_r2r.item);

            // Convert string results to the expected output format
            // For SimpleR2R, O is Vec<String>, so we need to convert BTreeMap<String, String> to Vec<String>
            string_results
                .into_iter()
                .map(|binding_map| {
                    // Convert binding map to vector of strings in a consistent order
                    let mut values: Vec<String> = binding_map.values().cloned().collect();
                    values.sort(); // Ensure consistent ordering
                    values
                })
                .collect::<Vec<Vec<String>>>()
                .into_iter()
                .map(|vec_result| {
                    // This is a bit of a type dance - we know O is Vec<String> for SimpleR2R
                    // but the trait is generic, so we need to do this conversion
                    unsafe { std::mem::transmute_copy(&vec_result) }
                })
                .collect()
        } else {
            debug!("Store is not SimpleR2R, cannot execute Volcano operator");
            Vec::new()
        }
    }

    /// Execute a Volcano physical operator for cross-window joins, returning string results
    fn execute_volcano_operator_for_join(
        physical_operator: &PhysicalOperator,
        store: &mut Box<dyn R2ROperator<I, O, O>>,
    ) -> Vec<Vec<String>> {
        // Try to downcast to SimpleR2R to access the SparqlDatabase
        let any_store = store.as_any_mut();
        if let Some(simple_r2r) = any_store.downcast_mut::<SimpleR2R>() {
            debug!("Executing Volcano operator for cross-window join");

            // Execute the physical operator using Volcano ExecutionEngine
            let string_results = ExecutionEngine::execute(physical_operator, &mut simple_r2r.item);

            // Convert BTreeMap<String, String> results to Vec<Vec<String>> for joining
            string_results
                .into_iter()
                .map(|binding_map| {
                    // Convert binding map to vector of strings in a consistent order
                    let mut values: Vec<String> = binding_map.values().cloned().collect();
                    values.sort(); // Ensure consistent ordering
                    values
                })
                .collect()
        } else {
            debug!("Store is not SimpleR2R, cannot execute Volcano operator for cross-window join");
            Vec::new()
        }
    }

    /// Add data to appropriate window based on stream IRI
    pub fn add_to_stream(&mut self, stream_iri: &str, event_item: I, ts: usize) {
        // Find windows that match this stream IRI
        for (window_idx, window_config) in self.window_configs.iter().enumerate() {
            if window_config.stream_iri == stream_iri || window_config.stream_iri.starts_with("?") {
                // Add to matching window
                if let Some(window) = self.windows.get_mut(window_idx) {
                    window.add_to_window(event_item.clone(), ts);
                }
            }
        }
    }

    /// Legacy method for backward compatibility
    pub fn add(&mut self, event_item: I, ts: usize) {
        // Add to all windows (for backward compatibility)
        for window in &mut self.windows {
            window.add_to_window(event_item.clone(), ts);
        }
    }

    pub fn stop(&mut self) {
        for window in &mut self.windows {
            window.stop();
        }
    }

    pub fn parse_data(&mut self, data: &str) -> Vec<I> {
        self.r2r.lock().unwrap().parse_data(data)
    }

    /// Get information about configured windows
    pub fn get_window_info(&self) -> Vec<&RSPWindow> {
        self.window_configs.iter().collect()
    }

    /// Get the RSP-QL query plan information
    pub fn get_query_plan(&self) -> &RSPQueryPlan {
        &self.rsp_query_plan
    }

    /// Execute query on window data using the specified execution mode
    fn execute_window_query(
        query: &str,
        store: &mut Box<dyn R2ROperator<I, O, O>>,
        execution_mode: QueryExecutionMode,
    ) -> Vec<O> {
        // Delegate to the store's execute_query method which now handles execution mode
        debug!(
            "Executing window query using {:?} execution mode",
            execution_mode
        );

        // The execution mode is handled by the R2R implementation (e.g., SimpleR2R)
        store.execute_query(query)
    }

    /// Extract variables that appear in multiple window blocks
    fn extract_shared_variables_static(windows: &[RSPWindow]) -> Vec<String> {
        let mut variable_counts: HashMap<String, usize> = HashMap::new();

        // Count occurrences of each variable across all window queries
        for window in windows {
            let variables = Self::extract_variables_from_query(&window.query);
            for var in variables {
                *variable_counts.entry(var).or_insert(0) += 1;
            }
        }

        // Return variables that appear in more than one window
        variable_counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(var, _)| var)
            .collect()
    }

    /// Extract variable names from a SPARQL query (simplified)
    fn extract_variables_from_query(query: &str) -> Vec<String> {
        let mut variables = Vec::new();

        // Simple regex-like extraction of ?variable patterns
        let words: Vec<&str> = query.split_whitespace().collect();
        for word in words {
            if word.starts_with('?') && word.len() > 1 {
                // Remove trailing punctuation
                let var_name = word
                    .trim_end_matches(|c: char| !c.is_alphanumeric())
                    .trim_start_matches('?');
                if !var_name.is_empty() {
                    variables.push(var_name.to_string());
                }
            }
        }

        variables.sort();
        variables.dedup();
        variables
    }

    /// Extract variables that appear in both static patterns and window blocks
    fn extract_static_window_shared_variables(
        static_patterns: &[(String, String, String)],
        windows: &[RSPWindow],
    ) -> Vec<String> {
        // Get variables from static patterns
        let mut static_variables = Vec::new();
        for (s, p, o) in static_patterns {
            if s.starts_with('?') {
                static_variables.push(s.trim_start_matches('?').to_string());
            }
            if p.starts_with('?') {
                static_variables.push(p.trim_start_matches('?').to_string());
            }
            if o.starts_with('?') {
                static_variables.push(o.trim_start_matches('?').to_string());
            }
        }
        static_variables.sort();
        static_variables.dedup();

        // Get variables from window queries
        let mut window_variables = Vec::new();
        for window in windows {
            let vars = Self::extract_variables_from_query(&window.query);
            window_variables.extend(vars);
        }
        window_variables.sort();
        window_variables.dedup();

        // Find intersection - variables that appear in both static and window patterns
        let mut shared = Vec::new();
        for static_var in &static_variables {
            if window_variables.contains(static_var) {
                shared.push(static_var.clone());
            }
        }

        shared
    }

    /// Start coordinator thread for cross-window joins using Volcano optimizer
    fn start_volcano_cross_window_join_coordinator(&self) {
        let receiver = self.window_result_receiver.clone();
        let shared_variables = self.shared_variables.clone();
        let window_count = self.window_configs.len();
        let _r2s_consumer = self.r2s_consumer.function.clone();
        let _r2s_operator = self.r2s_operator.clone();

        let cross_window_plan = self.rsp_query_plan.cross_window_join_plan.clone();

        thread::spawn(move || {
            let mut pending_results: HashMap<usize, Vec<WindowResult>> = HashMap::new();

            loop {
                match receiver.lock().unwrap().recv() {
                    Ok(window_result) => {
                        debug!("Received result from window: {}", window_result.window_iri);

                        // Group results by timestamp
                        let timestamp = window_result.timestamp;
                        pending_results
                            .entry(timestamp)
                            .or_insert_with(Vec::new)
                            .push(window_result);

                        // Check if we have results from all windows for this timestamp
                        if let Some(results) = pending_results.get(&timestamp) {
                            if results.len() == window_count {
                                // Use Volcano optimizer for cross-window joins
                                let joined_results = if let Some(ref join_plan) = cross_window_plan
                                {
                                    Self::execute_volcano_cross_window_join(
                                        results.clone(),
                                        join_plan,
                                        &shared_variables,
                                    )
                                } else {
                                    // Fallback to original join logic
                                    Self::perform_cross_window_join(
                                        results.clone(),
                                        &shared_variables,
                                    )
                                };

                                // Send joined results to consumer
                                if !joined_results.is_empty() {
                                    for binding in joined_results {
                                        let result_str = shared_variables
                                            .iter()
                                            .map(|var| {
                                                binding.get(var).cloned().unwrap_or_default()
                                            })
                                            .collect::<Vec<_>>()
                                            .join(",");

                                        debug!("Volcano-optimized joined result: {}", result_str);
                                        // Send to consumer (simplified implementation)
                                    }
                                }

                                // Remove processed timestamp
                                pending_results.remove(&timestamp);
                            }
                        }
                    }
                    Err(_) => {
                        debug!("Volcano cross-window join coordinator shutting down");
                        break;
                    }
                }
            }
        });
    }

    /// Start coordinator thread for static-window joins using Volcano optimizer
    fn start_volcano_static_window_join_coordinator(&self) {
        let receiver = self.window_result_receiver.clone();
        let static_window_shared_vars = self.rsp_query_plan.static_window_shared_vars.clone();
        let window_count = self.window_configs.len();
        let static_window_join_plan = self.rsp_query_plan.static_window_join_plan.clone();
        let static_data_plan = self.rsp_query_plan.static_data_plan.clone();
        let r2s_consumer = self.r2s_consumer.function.clone();

        thread::spawn(move || {
            let mut pending_results: HashMap<usize, Vec<WindowResult>> = HashMap::new();

            // Execute static data plan once (since it's static data)
            let static_results = if let Some(ref static_plan) = static_data_plan {
                debug!("Executing static data plan: {:?}", static_plan);
                // In a full implementation, this would execute the static plan on the knowledge base
                // For now, we'll simulate static results
                Self::simulate_static_data_execution(&static_window_shared_vars)
            } else {
                Vec::new()
            };

            debug!(
                "Static data execution produced {} results",
                static_results.len()
            );

            loop {
                match receiver.lock().unwrap().recv() {
                    Ok(window_result) => {
                        debug!(
                            "Received window result for static-window join: {}",
                            window_result.window_iri
                        );

                        // Group results by timestamp
                        let timestamp = window_result.timestamp;
                        pending_results
                            .entry(timestamp)
                            .or_insert_with(Vec::new)
                            .push(window_result);

                        // Check if we have results from all windows for this timestamp
                        if let Some(results) = pending_results.get(&timestamp) {
                            if results.len() == window_count {
                                // We have results from all windows, perform static-window join
                                let joined_results =
                                    if let Some(ref join_plan) = static_window_join_plan {
                                        Self::execute_volcano_static_window_join(
                                            &static_results,
                                            results.clone(),
                                            join_plan,
                                            &static_window_shared_vars,
                                        )
                                    } else {
                                        // Fallback to simple join
                                        Self::perform_static_window_join(
                                            &static_results,
                                            results.clone(),
                                            &static_window_shared_vars,
                                        )
                                    };

                                // Send joined results to consumer
                                for binding in joined_results {
                                    let result_str = static_window_shared_vars
                                        .iter()
                                        .map(|var| binding.get(var).cloned().unwrap_or_default())
                                        .collect::<Vec<_>>()
                                        .join(",");

                                    debug!(
                                        "Volcano-optimized static-window joined result: {}",
                                        result_str
                                    );
                                    // Send to consumer (simplified implementation)
                                    // Note: This is a simplified implementation - full version would need proper type conversion
                                    debug!("Sending static-window join result: {}", result_str);
                                }

                                // Remove processed timestamp
                                pending_results.remove(&timestamp);
                            }
                        }
                    }
                    Err(_) => {
                        debug!("Volcano static-window join coordinator shutting down");
                        break;
                    }
                }
            }
        });
    }

    /// Execute static-window join using Volcano optimizer
    fn execute_volcano_static_window_join(
        static_results: &[BTreeMap<String, String>],
        window_results: Vec<WindowResult>,
        join_plan: &PhysicalOperator,
        shared_variables: &[String],
    ) -> Vec<BTreeMap<String, String>> {
        debug!(
            "Executing Volcano-optimized static-window join with plan: {:?}",
            join_plan
        );

        // Convert window results to flat bindings
        let mut window_bindings = Vec::new();
        for window_result in window_results {
            window_bindings.extend(window_result.results);
        }

        // Perform join between static results and window results
        Self::simulate_volcano_static_window_join_execution(
            static_results,
            window_bindings,
            shared_variables,
        )
    }

    /// Simulate Volcano optimizer static-window join execution
    fn simulate_volcano_static_window_join_execution(
        static_results: &[BTreeMap<String, String>],
        window_results: Vec<BTreeMap<String, String>>,
        shared_variables: &[String],
    ) -> Vec<BTreeMap<String, String>> {
        let mut joined_results = Vec::new();

        // For each static result, find matching window results based on shared variables
        for static_binding in static_results {
            for window_binding in &window_results {
                // Check if shared variables match
                let mut can_join = true;
                for var in shared_variables {
                    if let (Some(static_val), Some(window_val)) =
                        (static_binding.get(var), window_binding.get(var))
                    {
                        if static_val != window_val {
                            can_join = false;
                            break;
                        }
                    }
                }

                if can_join {
                    // Merge static and window bindings
                    let mut merged = static_binding.clone();
                    for (var, val) in window_binding {
                        merged.insert(var.clone(), val.clone());
                    }
                    joined_results.push(merged);
                }
            }
        }

        debug!(
            "Volcano static-window join simulation produced {} results",
            joined_results.len()
        );
        joined_results
    }

    /// Simulate static data execution (placeholder)
    fn simulate_static_data_execution(
        static_variables: &[String],
    ) -> Vec<BTreeMap<String, String>> {
        // This is a placeholder - in reality would execute static plan on knowledge base
        let mut static_results = Vec::new();

        // Generate some mock static data
        for i in 0..5 {
            let mut binding = BTreeMap::new();
            for var in static_variables {
                binding.insert(var.clone(), format!("static_{}_{}", var, i));
            }
            static_results.push(binding);
        }

        debug!("Generated {} mock static results", static_results.len());
        static_results
    }

    /// Perform static-window join (fallback method)
    fn perform_static_window_join(
        static_results: &[BTreeMap<String, String>],
        window_results: Vec<WindowResult>,
        shared_variables: &[String],
    ) -> Vec<BTreeMap<String, String>> {
        debug!("Performing fallback static-window join");

        // Convert window results
        let mut window_bindings = Vec::new();
        for window_result in window_results {
            window_bindings.extend(window_result.results);
        }

        // Simple join implementation
        Self::simulate_volcano_static_window_join_execution(
            static_results,
            window_bindings,
            shared_variables,
        )
    }

    /// Execute cross-window join using Volcano optimizer
    fn execute_volcano_cross_window_join(
        window_results: Vec<WindowResult>,
        join_plan: &PhysicalOperator,
        shared_variables: &[String],
    ) -> Vec<BTreeMap<String, String>> {
        debug!(
            "Executing Volcano-optimized cross-window join on {} windows with plan: {:?}",
            window_results.len(),
            join_plan
        );

        // Convert window results to format suitable for Volcano optimizer
        let mut all_bindings = Vec::new();
        for window_result in &window_results {
            all_bindings.extend(window_result.results.clone());
        }

        // In a full implementation, this would execute the physical plan
        // For now, we'll use a simplified approach that mimics Volcano execution
        Self::simulate_volcano_join_execution(all_bindings, shared_variables)
    }

    /// Simulate Volcano optimizer join execution (simplified)
    fn simulate_volcano_join_execution(
        all_bindings: Vec<BTreeMap<String, String>>,
        shared_variables: &[String],
    ) -> Vec<BTreeMap<String, String>> {
        // This is a simplified simulation of what the Volcano optimizer would do
        // In reality, it would execute the physical plan with proper hash joins, etc.

        if all_bindings.is_empty() {
            return Vec::new();
        }

        // Group bindings by shared variable values for hash join simulation
        let mut hash_table: HashMap<String, Vec<BTreeMap<String, String>>> = HashMap::new();

        for binding in all_bindings {
            // Create hash key from shared variables
            let hash_key = shared_variables
                .iter()
                .map(|var| binding.get(var).cloned().unwrap_or_default())
                .collect::<Vec<_>>()
                .join("|");

            hash_table
                .entry(hash_key)
                .or_insert_with(Vec::new)
                .push(binding);
        }

        // Perform join - combine bindings with same hash key
        let mut joined_results = Vec::new();
        for (_, bindings) in hash_table {
            if bindings.len() > 1 {
                // Combine all bindings with same shared variable values
                let mut combined = BTreeMap::new();
                for binding in &bindings {
                    for (var, val) in binding {
                        combined.insert(var.clone(), val.clone());
                    }
                }
                joined_results.push(combined);
            }
        }

        debug!(
            "Volcano simulation produced {} joined results",
            joined_results.len()
        );
        joined_results
    }

    /// Perform cross-window join on shared variables (fallback method)
    fn perform_cross_window_join(
        window_results: Vec<WindowResult>,
        shared_variables: &[String],
    ) -> Vec<BTreeMap<String, String>> {
        if window_results.is_empty() {
            return Vec::new();
        }

        debug!(
            "Performing cross-window join on {} windows with shared variables: {:?}",
            window_results.len(),
            shared_variables
        );

        // Start with results from first window
        let mut joined_results = window_results[0].results.clone();

        // Join with each subsequent window
        for window_result in window_results.iter().skip(1) {
            joined_results = Self::join_result_sets(
                joined_results,
                window_result.results.clone(),
                shared_variables,
            );
        }

        debug!(
            "Cross-window join produced {} results",
            joined_results.len()
        );
        joined_results
    }

    /// Join two result sets on shared variables
    fn join_result_sets(
        left_results: Vec<BTreeMap<String, String>>,
        right_results: Vec<BTreeMap<String, String>>,
        shared_variables: &[String],
    ) -> Vec<BTreeMap<String, String>> {
        let mut joined = Vec::new();

        for left_binding in &left_results {
            for right_binding in &right_results {
                // Check if shared variables match
                let mut can_join = true;
                for var in shared_variables {
                    if let (Some(left_val), Some(right_val)) =
                        (left_binding.get(var), right_binding.get(var))
                    {
                        if left_val != right_val {
                            can_join = false;
                            break;
                        }
                    }
                }

                if can_join {
                    // Merge the bindings
                    let mut merged = left_binding.clone();
                    for (var, val) in right_binding {
                        merged.insert(var.clone(), val.clone());
                    }
                    joined.push(merged);
                }
            }
        }

        joined
    }

    /// Convert query results to WindowResult format
    fn convert_to_window_result(
        window_iri: &str,
        results: Vec<Vec<String>>,
        timestamp: usize,
    ) -> WindowResult {
        // For simplicity, assume results are in [var1_val, var2_val, ...] format
        // In a real implementation, this would need proper variable mapping
        let variable_bindings = results
            .into_iter()
            .enumerate()
            .map(|(_i, row)| {
                let mut binding = BTreeMap::new();
                for (j, value) in row.into_iter().enumerate() {
                    binding.insert(format!("var{}", j), value);
                }
                binding
            })
            .collect();

        WindowResult {
            window_iri: window_iri.to_string(),
            results: variable_bindings,
            timestamp,
        }
    }
}

pub struct SimpleR2R {
    pub item: SparqlDatabase,
    pub execution_mode: QueryExecutionMode,
}

impl SimpleR2R {
    pub fn new() -> Self {
        SimpleR2R {
            item: SparqlDatabase::new(),
            execution_mode: QueryExecutionMode::Standard,
        }
    }

    pub fn with_execution_mode(execution_mode: QueryExecutionMode) -> Self {
        SimpleR2R {
            item: SparqlDatabase::new(),
            execution_mode,
        }
    }
}

impl AsAnyMut for SimpleR2R {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl R2ROperator<Triple, Vec<String>, Vec<String>> for SimpleR2R {
    fn load_triples(&mut self, _data: &str, _syntax: String) -> Result<(), String> {
        error!("Unsupported operation");
        Err("something went wrong".to_string())
    }

    fn load_rules(&mut self, _data: &str) -> Result<(), &'static str> {
        error!("Unsupported operation load rules");
        Err("something went wrong")
    }

    fn add(&mut self, data: Triple) {
        self.item.add_triple(data);
    }

    fn remove(&mut self, data: &Triple) {
        self.item.delete_triple(data);
    }

    fn materialize(&mut self) -> Vec<Triple> {
        error!("Unsupported operation materialize");
        Vec::new()
    }

    fn execute_query(&mut self, query: &str) -> Vec<Vec<String>> {
        match self.execution_mode {
            QueryExecutionMode::Standard => {
                debug!("SimpleR2R executing query with standard mode");
                execute_query(query, &mut self.item)
            }
            QueryExecutionMode::Volcano => {
                debug!("SimpleR2R executing query with Volcano optimizer");
                debug!("Evaluating query {}", query);
                execute_query_rayon_parallel2_volcano(query, &mut self.item)
            }
        }
    }

    fn parse_data(&mut self, data: &str) -> Vec<Triple> {
        self.item.parse_and_encode_ntriples(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn rsp_ql_integration() {
        let result_container = Arc::new(Mutex::new(Vec::new()));
        let result_container_clone = Arc::clone(&result_container);
        let function = Box::new(move |r: Vec<String>| {
            println!("Bindings: {:?}", r);
            result_container_clone.lock().unwrap().push(r);
        });
        let result_consumer = ResultConsumer {
            function: Arc::new(function),
        };
        let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

        // RSP-QL query with single window
        let rsp_ql_query = r#"
            REGISTER RSTREAM <http://out/stream> AS
            SELECT *
            FROM NAMED WINDOW :wind ON ?s [RANGE 10 STEP 2]
            WHERE {
                WINDOW :wind {
                    ?s a <http://www.w3.org/test/SuperType> .
                }
            }
        "#;

        let mut engine: RSPEngine<Triple, Vec<String>> = RSPBuilder::new()
            .add_rsp_ql_query(rsp_ql_query)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .build()
            .expect("Failed to build RSP engine");

        // Add data to the stream
        for i in 0..20 {
            let data = format!(
                "<http://test.be/subject{}> a <http://www.w3.org/test/SuperType> .",
                i
            );
            let triples = engine.parse_data(&data);
            for triple in triples {
                engine.add(triple, i);
            }
        }

        engine.stop();
        thread::sleep(Duration::from_secs(2));

        // Should have results from window processing
        assert!(!result_container.lock().unwrap().is_empty());
    }

    #[test]
    fn rsp_ql_multi_window_integration() {
        let result_container = Arc::new(Mutex::new(Vec::new()));
        let result_container_clone = Arc::clone(&result_container);
        let function = Box::new(move |r: Vec<String>| {
            println!("Multi-window Bindings: {:?}", r);
            result_container_clone.lock().unwrap().push(r);
        });
        let result_consumer = ResultConsumer {
            function: Arc::new(function),
        };
        let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

        // RSP-QL query with multiple windows (similar to the example)
        let rsp_ql_query = r#"
            REGISTER RSTREAM <http://out/stream> AS
            SELECT *
            FROM NAMED WINDOW :wind ON ?s [RANGE 10 STEP 2]
            FROM NAMED WINDOW :wind2 ON ?s2 [RANGE 5 STEP 1]
            WHERE {
                WINDOW :wind {
                    ?s a <http://www.w3.org/test/Temperature> .
                }
                WINDOW :wind2 {
                    ?s2 a <http://www.w3.org/test/CO2> .
                }
            }
        "#;

        let mut engine: RSPEngine<Triple, Vec<String>> = RSPBuilder::new()
            .add_rsp_ql_query(rsp_ql_query)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .build()
            .expect("Failed to build RSP engine");

        // Add temperature data
        for i in 0..10 {
            let data = format!(
                "<http://test.be/temp{}> a <http://www.w3.org/test/Temperature> .",
                i
            );
            let triples = engine.parse_data(&data);
            for triple in triples {
                engine.add_to_stream("?s", triple, i);
            }
        }

        // Add CO2 data
        for i in 0..10 {
            let data = format!("<http://test.be/co2{}> a <http://www.w3.org/test/CO2> .", i);
            let triples = engine.parse_data(&data);
            for triple in triples {
                engine.add_to_stream("?s2", triple, i + 10);
            }
        }

        engine.stop();
        thread::sleep(Duration::from_secs(2));

        // Should have results from both windows
        assert!(!result_container.lock().unwrap().is_empty());
    }
}
