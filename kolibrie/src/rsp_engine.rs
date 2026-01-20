/*
* Copyright © 2025 Volodymyr Kadzhaia
* Copyright © 2025 Pieter Bonte
* KU Leuven — Stream Intelligence Lab, Belgium
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this file,
* you can obtain one at [https://mozilla.org/MPL/2.0/](https://mozilla.org/MPL/2.0/).
*/

use crate::rsp::r2r::{AsAnyMut, R2ROperator};

// Blanket AsAnyMut impl removed. Concrete R2R implementations that need downcasting
// should implement `AsAnyMut` in their own module or the `rsp::r2r` module should
// be updated so that `R2ROperator` extends `AsAnyMut` (preferred).
// This avoids unsafe trait-object vtable casts.
use crate::rsp::r2s::{Relation2StreamOperator, StreamOperator};
use crate::rsp::s2r::{CSPARQLWindow, Report, ReportStrategy, Tick};
use crate::rsp::s2r::ContentContainer;

#[cfg(not(test))]
use log::{debug, error}; // Use log crate when building application
use shared::query::{StreamType, WindowBlock, WindowClause};
use shared::terms::Term;
use shared::triple::Triple;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(test)]
use std::{println as debug, println as error};

use crate::parser::parse_combined_query;
use crate::sparql_database::SparqlDatabase;
use crate::streamertail_optimizer::{
    build_logical_plan, ExecutionEngine, LogicalOperator, PhysicalOperator, Streamertail,
};
/*
Update:

* let each window execute with volcano
* use additional receiver to fetch window contents
* when parsing, compose an RSP query plan:
-- windows have  each their own query plan
-- static data has its own query plan
-- an overarching query plan combines the above.
--- add the idea of a Buffer to the Logical/Physical operators
---- this is need to inject the results of window/static into
---- contains: 1) bindings, 2) a str to details the origin
--- upon the buffers, the joins between shared variables, which is another query plan

Use build_logical_plan for converting parsed query to logical plan

* there are currently many functions that er not really used.
*/
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
    pub query: LogicalOperator, // The SPARQL query to execute on this window
}

/// RSP Query configuration extracted from parsed RSP-QL
#[derive(Debug)]
pub struct RSPQueryConfig<'a> {
    pub windows: Vec<RSPWindow>,
    pub output_stream: String,
    pub stream_type: StreamOperator,
    pub shared_variables: Vec<String>, // Variables that appear across multiple windows
    pub static_patterns: Vec<(&'a str, &'a str, &'a str)>, // Static graph patterns outside windows
    pub static_window_shared_vars: Vec<String>, // Variables shared between static patterns and windows
    pub database: SparqlDatabase,               // usesd prefixes
}

/// RSP-QL Query Plan using Volcano optimizer
#[derive(Debug, Clone)]
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
    pub results: Vec<HashMap<String, String>>, // Variable bindings
    pub timestamp: usize,
}

pub struct RSPBuilder<'a, I, O> {
    rsp_ql_query: Option<&'a str>,
    triples: Option<&'a str>,
    rules: Option<&'a str>,
    result_consumer: Option<ResultConsumer<O>>,
    r2r: Option<Box<dyn R2ROperator<I, Vec<PhysicalOperator>, O>>>,
    operation_mode: OperationMode,
    query_execution_mode: QueryExecutionMode,
    syntax: String,
}

impl<'a, I, O> RSPBuilder<'a, I, O>
where
    O: Clone + Eq + Send + Debug + Hash + 'static + From<Vec<(String, String)>>,
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

    pub fn add_r2r(
        mut self,
        r2r: Box<dyn R2ROperator<I, Vec<PhysicalOperator>, O>>,
    ) -> RSPBuilder<'a, I, O> {
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
    fn parse_rsp_ql_query<'b>(&self, query: &'b str) -> Result<RSPQueryConfig<'b>, String> {
        match parse_combined_query(query) {
            Ok((_, parsed_query)) => {
                if let Some(register_clause) = &parsed_query.register_clause {
                    let mut windows = Vec::new();
                    let mut database = SparqlDatabase::new();
                    database.set_prefixes(parsed_query.prefixes.clone());
                    // Extract windows from the register clause
                    for window_clause in &register_clause.query.window_clause {
                        let window = self.create_rsp_window(
                            window_clause,
                            &register_clause.query.window_blocks,
                            &mut database,
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
                    let static_patterns = register_clause.query.where_clause.0.clone();

                    Ok(RSPQueryConfig {
                        windows,
                        output_stream: register_clause.output_stream_iri.to_string(),
                        stream_type,
                        shared_variables: Vec::new(), // Will be populated later
                        static_patterns,
                        static_window_shared_vars: Vec::new(), // Will be populated later
                        database,
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
        database: &mut SparqlDatabase,
    ) -> Result<RSPWindow, String> {
        // Find the corresponding window block for this window
        let spo_query = LogicalOperator::scan((
            Term::Variable("s".to_string()),
            Term::Variable("p".to_string()),
            Term::Variable("o".to_string()),
        ));
        let window_query = window_blocks
            .iter()
            .find(|block| block.window_name == window_clause.window_iri)
            .map(|block| {
                // Convert window block patterns to query plan
                for (j, (s, p, o)) in block.patterns.iter().enumerate() {
                    println!(" Registering     {}: {} {} {}", j + 1, s, p, o);
                }
                let op = build_logical_plan(
                    Vec::new(),
                    block.patterns.clone(),
                    Vec::new(),
                    &database.prefixes.clone(),
                    database,
                    &[],
                    None,
                );
                println!("\tResults in {:?}", op);
                op
            })
            .unwrap_or_else(|| spo_query);

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
            window_plans.push(window.query.clone());
        }
        let mut database = query_config.database.clone();
        // Create static data plan if there are static patterns
        let static_data_plan = if !query_config.static_patterns.is_empty() {
            let logical_plan = build_logical_plan(
                Vec::new(),
                query_config.static_patterns.clone(),
                Vec::new(),
                &database.prefixes.clone(),
                &mut database,
                &[],
                None,
            );
            Some(logical_plan)
        } else {
            None
        };

        // Create cross-window join plan if there are shared variables between windows
        let cross_window_join_plan = Self::create_cross_window_join_plan(&query_config.windows);

        // Create static-window join plan if there are shared variables between static and window data
        let static_window_join_plan = if static_data_plan.is_some() {
            Some(Self::create_static_window_join_plan())
        } else {
            None
        };

        // Extract output variables from the query (simplified)
        let mut output_variables = query_config.shared_variables.clone();
        output_variables.extend(query_config.static_window_shared_vars.clone());

        //create physical plans from the logical ones
        let mut optimizer = Streamertail::new(&database);

        let static_data_plan = match static_data_plan {
            Some(v) => Some(optimizer.find_best_plan(&v)),
            None => None,
        };
        let cross_window_join_plan = match cross_window_join_plan.as_ref() {
            Ok(v) => Some(optimizer.find_best_plan(v)),
            Err(_) => None,
        };
        let static_window_join_plan = match static_window_join_plan {
            Some(v) => Some(optimizer.find_best_plan(&v)),
            None => None,
        };
        println!("logical window plans {:?}", window_plans);

        let window_plans = window_plans
            .iter()
            .map(|v| optimizer.find_best_plan(v))
            .collect();
        println!("physical window plans {:?}", window_plans);

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

    /// Create cross-window join plan using Volcano optimizer
    fn create_cross_window_join_plan(windows: &[RSPWindow]) -> Result<LogicalOperator, String> {
        if windows.is_empty() {
            return Err("No window plans to join".to_string());
        }
        // Start with the first window
        let mut joined_plan = LogicalOperator::Buffer {
            content: Vec::new(),
            origin: windows[0].window_iri.to_string(),
        };

        for window in windows.iter().skip(1) {
            let new_window_buffer = LogicalOperator::Buffer {
                content: Vec::new(),
                origin: window.window_iri.to_string(),
            };

            joined_plan = LogicalOperator::join(joined_plan, new_window_buffer);
        }
        Ok(joined_plan)
    }

    /// Create static-window join plan using Volcano optimizer
    fn create_static_window_join_plan() -> LogicalOperator {
        let static_buffer = LogicalOperator::Buffer {
            content: Vec::new(),
            origin: "static".to_string(),
        };
        let window_buffer = LogicalOperator::Buffer {
            content: Vec::new(),
            origin: "window".to_string(),
        };

        // Join implementation takes care of overlapping variables
        let joined_plan = LogicalOperator::join(static_buffer, window_buffer);

        joined_plan
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
        let query_config = self.parse_rsp_ql_query(rsp_ql_query)?;

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
    O: Hash,
{
    windows: Vec<CSPARQLWindow<I>>,
    r2r: Arc<Mutex<Box<dyn R2ROperator<I, Vec<PhysicalOperator>, O>>>>,
    r2s_consumer: ResultConsumer<O>,
    #[allow(dead_code)]
    r2s_operator: Arc<Mutex<Relation2StreamOperator<O>>>,
    window_configs: Vec<RSPWindow>,
    query_execution_mode: QueryExecutionMode,
    shared_variables: Vec<String>,
    // Channel for collecting window results for cross-window joins
    window_result_sender: Sender<WindowResult>,
    window_result_receiver: Receiver<WindowResult>,
    // RSP-QL Query Plan using Volcano optimizer
    rsp_query_plan: RSPQueryPlan,
    single_thread_buffer: Arc<Mutex<HashMap<String, Vec<HashMap<String, String>>>>>,
}

pub struct ResultConsumer<I> {
    pub function: Arc<dyn Fn(I) -> () + Send + Sync>,
}

/// Macro to generate the window processing logic
macro_rules! create_window_processor {
    ($window_iri:expr, $query:expr, $query_execution_mode:expr, 
     $r2r_store:expr, $has_joins:expr, $window_result_sender:expr, $r2s_consumer_func:expr) => {
        move |content: ContentContainer<I>| {
            debug!(
                "Processing window {} with query: {:?} using {:?} execution",
                $window_iri, $query, $query_execution_mode
            );

            let ts = content.get_last_timestamp_changed();
            let mut store = $r2r_store.lock().unwrap();
            
            for t in content.into_iter() {
                store.add(t);
            }

            let results = store.execute_query(&$query);
            debug!("Got # results {} for window {}", results.len(), $window_iri);
            
            // Release lock early to reduce contention
            drop(store);

            if $has_joins {
                let mut mapped_results: Vec<HashMap<String, String>> = Vec::new();
                mapped_results.reserve(results.len());

                for res in &results {
                    if let Some(bindings) = (res as &dyn std::any::Any)
                        .downcast_ref::<Vec<(String, String)>>()
                    {
                        let map: HashMap<String, String> = bindings.iter().cloned().collect();
                        mapped_results.push(map);
                    }
                }

                let window_res = WindowResult {
                    window_iri: $window_iri.clone(),
                    results: mapped_results,
                    timestamp: ts,
                };

                if let Err(e) = $window_result_sender.send(window_res) {
                    error!("Failed to send window result to buffer: {:?}", e);
                }
            } else {
                for res in results {
                    ($r2s_consumer_func)(res);
                }
            }
        }
    };
}

/// Macro to register windows based on operation mode
macro_rules! register_window {
    (SingleThread, $window:expr, $processor:expr) => {
        $window.register_callback(Box::new($processor));
    };
    (MultiThread, $window:expr, $processor:expr, $window_iri:expr) => {{
        let receiver = $window.register();
        thread::spawn(move || {
            loop {
                match receiver.recv() {
                    Ok(content) => {
                        $processor(content);
                    }
                    Err(_) => {
                        debug!("Shutting down window {}!", $window_iri);
                        break;
                    }
                }
            }
            debug!("Shutdown complete for window {}!", $window_iri);
        });
    }};
}

impl<I, O> RSPEngine<I, O>
where
    O: Clone + Hash + Eq + Send + 'static + From<Vec<(String, String)>>,
    I: Eq + PartialEq + Clone + Debug + Hash + Send + 'static,
{
    pub fn new(
        query_config: RSPQueryConfig,
        triples: &str,
        syntax: String,
        rules: &str,
        result_consumer: ResultConsumer<O>,
        r2r: Box<dyn R2ROperator<I, Vec<PhysicalOperator>, O>>,
        operation_mode: OperationMode,
        query_execution_mode: QueryExecutionMode,
        rsp_query_plan: RSPQueryPlan,
    ) -> RSPEngine<I, O> {
        let mut store = r2r;

        // The PhysicalOperator plans created in `rsp_query_plan` contain integer IDs (constants)
        // that were generated by the Dictionary in `query_config.database`.
        // The `store` (R2R operator) has its own Dictionary. If we don't sync them,
        // the store will assign different IDs to incoming data, and the execution engine
        // will fail to match them against the plan.
        if let Some(simple_r2r) = store.as_any_mut().downcast_mut::<SimpleR2R>() {
            debug!("Synchronizing R2R dictionary with Query dictionary");
            simple_r2r
                .item
                .dictionary
                .merge(&query_config.database.dictionary);
        }

        // Load initial data
        match store.load_triples(triples, syntax) {
            Err(parsing_error) => error!("Unable to load ABox: {:?}", parsing_error.to_string()),
            _ => (),
        }

        match store.load_rules(rules) {
            Ok(_) => debug!("Rules loaded successfully"),
            Err(e) => error!("Failed to load rules: {:?}", e),
        }

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
                window_config.window_iri.clone(),
            );
            windows.push(window);
        }

        // Create channel for cross-window result coordination
        let (result_sender, result_receiver) = unbounded::<WindowResult>();

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
            window_result_sender: result_sender,
            window_result_receiver: result_receiver,
            rsp_query_plan,
            single_thread_buffer: Arc::new(Mutex::new(HashMap::new())),
        };

        match operation_mode {
            mode @ (OperationMode::SingleThread | OperationMode::MultiThread) => {
                engine.register_windows(mode);
                if matches!(mode, OperationMode::MultiThread) {
                    engine.start_cross_window_coordinator();
                }
            }
        }

        engine
    }

    /// Register windows using macros to eliminate code duplication
    fn register_windows(&mut self, operation_mode: OperationMode) {
        let has_joins = self.windows.len() > 1 || !self.shared_variables.is_empty();

        for (window_idx, window) in self.windows.iter_mut().enumerate() {
            let query = self.rsp_query_plan.window_plans[window_idx].clone();
            let window_iri = self.window_configs[window_idx].window_iri.clone();
            let window_iri_for_thread = window_iri.clone(); // Clone for MultiThread usage
            let query_execution_mode = self.query_execution_mode;
            let window_result_sender = self.window_result_sender.clone();
            let r2r_store = self.r2r.clone();
            
            let r2s_consumer_func = if has_joins {
                Arc::new(|_| {}) as Arc<dyn Fn(O) + Send + Sync>
            } else {
                self.r2s_consumer.function.clone()
            };

            // Create processor using macro
            let processor = create_window_processor!(
                window_iri,
                query,
                query_execution_mode,
                r2r_store,
                has_joins,
                window_result_sender,
                r2s_consumer_func
            );

            // Register based on mode
            match operation_mode {
                OperationMode::SingleThread => {
                    register_window!(SingleThread, window, processor);
                }
                OperationMode::MultiThread => {
                    register_window!(MultiThread, window, processor, window_iri_for_thread);
                }
            }
        }
    }

    /// Start a coordinator thread that collects and joins results from multiple windows
    fn start_cross_window_coordinator(&self)
    where
        O: From<Vec<(String, String)>>,
    {
        if self.windows.len() <= 1 {
            return; // No need for coordinator with single window
        }
        
        let receiver = self.window_result_receiver.clone();
        let consumer = self.r2s_consumer.function.clone();
        let num_windows = self.windows.len();
        
        thread::spawn(move || {
            // Keep the most recent results from each window
            let mut latest_window_results: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
            
            loop {
                match receiver.recv() {
                    Ok(window_result) => {
                        debug!("Coordinator received {} results from window: {}", 
                            window_result.results.len(), window_result.window_iri);
                        
                        // Update the latest results for this window (replace old results)
                        latest_window_results.insert(
                            window_result.window_iri.clone(),
                            window_result.results.clone()
                        );
                        
                        // If we have results from ALL windows, perform join
                        if latest_window_results.len() == num_windows {
                            debug!("Coordinator: All {} windows have results, performing join", num_windows);
                            
                            // Check that all windows have non-empty results
                            let all_have_results = latest_window_results.values().all(|v| !v.is_empty());
                            
                            if all_have_results {
                                let joined = join_window_results(&latest_window_results);
                                
                                debug!("Coordinator: Joined {} results", joined.len());
                                
                                // Output joined results
                                for binding in joined {
                                    let output: Vec<(String, String)> = binding.into_iter().collect();
                                    (consumer)(output.into());
                                }
                                
                                debug!("Coordinator: Join complete, keeping window states");
                            }
                        } else {
                            debug!("Coordinator: Waiting for more windows ({}/{}) - have results from: {:?}", 
                                latest_window_results.len(), 
                                num_windows,
                                latest_window_results.keys().collect::<Vec<_>>());
                        }
                    }
                    Err(_) => {
                        debug!("Coordinator: Channel disconnected, shutting down");
                        break;
                    }
                }
            }
            
            debug!("Coordinator: Shutdown complete");
        });
    }

    /// Add data to appropriate window based on stream IRI
    pub fn add_to_stream(&mut self, stream_iri: &str, event_item: I, ts: usize) {
        if self.windows.len() > 1 {
            self.process_single_thread_window_results();
        }

        fn normalize_stream_iri(s: &str) -> String {
            let s = s.trim();
            // Some callers might pass a full IRI in `<...>` form.
            let s = s.trim_start_matches('<').trim_end_matches('>');
            // Accept prefixed notation with an optional leading colon, e.g. `:stream1`.
            let s = s.strip_prefix(':').unwrap_or(s);
            s.to_string()
        }

        let input_norm = normalize_stream_iri(stream_iri);

        // Find windows that match this stream IRI
        for (window_idx, window_config) in self.window_configs.iter().enumerate() {
            // Variable stream (e.g. `?s`) matches any stream.
            if window_config.stream_iri.starts_with('?') {
                if let Some(window) = self.windows.get_mut(window_idx) {
                    window.add_to_window(event_item.clone(), ts);
                }
                continue;
            }

            let cfg_norm = normalize_stream_iri(&window_config.stream_iri);
            if cfg_norm == input_norm {
                if let Some(window) = self.windows.get_mut(window_idx) {
                    window.add_to_window(event_item.clone(), ts);
                }
            }
        }
    }

    fn process_single_thread_window_results(&mut self)
    where
        O: From<Vec<(String, String)>>,
    {
        let consumer = self.r2s_consumer.function.clone();
        let num_windows = self.windows.len();
        let buffer = self.single_thread_buffer.clone();
        
        // Collect all available results from the channel
        let mut new_results = Vec::new();
        while let Ok(window_result) = self.window_result_receiver.try_recv() {
            new_results.push(window_result);
        }
        
        // If no new results, nothing to do
        if new_results.is_empty() {
            return;
        }
        
        // Accumulate into persistent buffer
        let mut window_buffers = buffer.lock().unwrap();
        for window_result in new_results {
            window_buffers
                .entry(window_result.window_iri.clone())
                .or_insert_with(Vec::new)
                .extend(window_result.results);
        }
        
        // Check if we have results from ALL windows
        if window_buffers.len() == num_windows {
            debug!("SingleThread: All windows have results, performing join");
            
            // Perform join across all windows
            let joined_results = join_window_results(&window_buffers);
            
            debug!("SingleThread: Joined {} results", joined_results.len());
            
            // Output joined results
            for binding in joined_results {
                let output: Vec<(String, String)> = binding.into_iter().collect();
                (consumer)(output.into());
            }
            
            // Clear buffer after outputting
            window_buffers.clear();
        } else {
            debug!("SingleThread: Waiting for more windows ({}/{})", 
                window_buffers.len(), num_windows);
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
}

/// Join results from multiple windows based on shared variables
fn join_window_results(window_buffers: &HashMap<String, Vec<HashMap<String, String>>>) -> Vec<HashMap<String, String>> {
    if window_buffers.is_empty() {
        return Vec::new();
    }
    
    // Get all window results as a vector
    let mut all_windows: Vec<Vec<HashMap<String, String>>> = window_buffers.values().cloned().collect();
    
    if all_windows.len() == 1 {
        return all_windows.into_iter().next().unwrap();
    }
    
    // Start with first window results
    let mut joined = all_windows.remove(0);
    
    // Join with each remaining window
    for window_results in all_windows {
        let mut new_joined = Vec::new();
        
        for left_binding in &joined {
            for right_binding in &window_results {
                // Check if bindings are compatible (shared variables have same values)
                let mut compatible = true;
                
                // Check shared variables
                for (var, val) in left_binding {
                    if let Some(right_val) = right_binding.get(var) {
                        // Same variable exists in both - must have same value
                        if val != right_val {
                            compatible = false;
                            break;
                        }
                    }
                }
                
                if compatible {
                    // Merge bindings (Cartesian product if no shared vars, natural join if shared)
                    let mut merged = left_binding.clone();
                    for (k, v) in right_binding {
                        merged.insert(k.clone(), v.clone());
                    }
                    new_joined.push(merged);
                }
            }
        }
        
        joined = new_joined;
    }
    
    joined
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

/// Allow downcasting from trait objects by exposing Any for mutable references.
/// This helps code that needs to access concrete `SimpleR2R` internals (e.g. the SparqlDatabase).
impl AsAnyMut for SimpleR2R {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Implement the R2R operator trait for SimpleR2R. Note: the `R` generic parameter
/// is `Vec<PhysicalOperator>` (a list of physical plans). The output `O` is
/// a binding map `HashMap<String, String>` per row.
impl R2ROperator<Triple, Vec<PhysicalOperator>, Vec<(String, String)>> for SimpleR2R {
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

    fn execute_query(&mut self, op: &PhysicalOperator) -> Vec<Vec<(String, String)>> {
        debug!("SimpleR2R executing query with PhysicalOperator");

        // Execute the physical operator using the Volcano execution engine.
        // The engine returns Vec<HashMap<String,String>> (bindings per row).
        ExecutionEngine::execute(op, &mut self.item)
            .into_iter()
            .map(|hashmap| hashmap.into_iter().collect())
            .collect()
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
        let function = Box::new(move |r: Vec<(String, String)>| {
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

        let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
            .add_rsp_ql_query(rsp_ql_query)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .build()
            .expect("Failed to build RSP engine");
        //small hack to make sure the encoder is aligned between parsing and query injection
        engine.parse_data("a a <http://www.w3.org/test/SuperType> .");

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
    fn rsp_ql_integration_with_join() {
        let result_container = Arc::new(Mutex::new(Vec::new()));
        let result_container_clone = Arc::clone(&result_container);
        let function = Box::new(move |r: Vec<(String, String)>| {
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
                    ?s a <http://www.w3.org/test/MegaType> .
                }
            }
        "#;

        let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
            .add_rsp_ql_query(rsp_ql_query)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .build()
            .expect("Failed to build RSP engine");

        // Add data to the stream
        for i in 0..20 {
            let data = format!(
                "<http://test.be/subject{}> a <http://www.w3.org/test/SuperType> .\n\
                <http://test.be/subject{}> a <http://www.w3.org/test/MegaType> .",
                i, i
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
        let function = Box::new(move |r: Vec<(String, String)>| {
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
            FROM NAMED WINDOW :wind ON :stream1 [RANGE 10 STEP 2]
            FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 5 STEP 1]
            WHERE {
                WINDOW :wind {
                    ?s a <http://www.w3.org/test/Temperature> .
                }
                WINDOW :wind2 {
                    ?s2 a <http://www.w3.org/test/CO2> .
                }
            }
        "#;

        let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
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
                engine.add_to_stream("stream1", triple, i);
            }
        }

        // Add CO2 data
        for i in 0..10 {
            let data = format!("<http://test.be/co2{}> a <http://www.w3.org/test/CO2> .", i);
            let triples = engine.parse_data(&data);
            for triple in triples {
                engine.add_to_stream("stream2", triple, i + 10);
            }
        }

        engine.stop();
        thread::sleep(Duration::from_secs(2));

        // Should have results from both windows
        assert!(!result_container.lock().unwrap().is_empty());
    }
    
    #[test]
    fn rsp_ql_joining_multi_window_integration() {
        let result_container = Arc::new(Mutex::new(Vec::new()));
        let result_container_clone = Arc::clone(&result_container);
        let function = Box::new(move |r: Vec<(String, String)>| {
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
            FROM NAMED WINDOW :wind ON :stream1 [RANGE 10 STEP 2]
            FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 5 STEP 1]
            WHERE {
                WINDOW :wind {
                    ?s a <http://www.w3.org/test/Temperature> .
                }
                WINDOW :wind2 {
                    ?s a <http://www.w3.org/test/CO2> .
                }
            }
        "#;

        let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
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
                engine.add_to_stream("stream1", triple, i);
            }
        }

        // Add CO2 data
        for i in 0..10 {
            let data = format!("<http://test.be/co2{}> a <http://www.w3.org/test/CO2> .", i);
            let triples = engine.parse_data(&data);
            for triple in triples {
                engine.add_to_stream("stream2", triple, i + 10);
            }
        }

        engine.stop();
        thread::sleep(Duration::from_secs(2));

        // Should have no results if windows are correctly joined together
        assert!(result_container.lock().unwrap().is_empty());
    }

    #[test]
    fn rsp_ql_single_thread_multi_window_integration() {
        let result_container = Arc::new(Mutex::new(Vec::new()));
        let result_container_clone = Arc::clone(&result_container);

        let function = Box::new(move |r: Vec<(String, String)>| {
            println!("SingleThread Multi-Window Bindings: {:?}", r);
            result_container_clone.lock().unwrap().push(r);
        });

        let result_consumer = ResultConsumer {
            function: Arc::new(function),
        };
        
        let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

        // FIXED:  Consistent spacing and proper SPARQL variable syntax
        let rsp_ql_query = r#"
            REGISTER RSTREAM <http://out/stream> AS
            SELECT *
            FROM NAMED WINDOW :wind1 ON :stream1 [RANGE 10 STEP 2]
            FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 5 STEP 1]
            WHERE {
                WINDOW :wind1 {
                    ?s1 a <http://www.w3.org/test/TypeOne> .
                }
                WINDOW :wind2 {
                    ?s2 a <http://www.w3.org/test/TypeTwo> . 
                }
            }
        "#;

        let mut engine: RSPEngine<Triple, Vec<(String, String)>> = RSPBuilder::new()
            .add_rsp_ql_query(rsp_ql_query)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .set_operation_mode(OperationMode::SingleThread)
            .build()
            .expect("Failed to build RSP engine");

        // Feed data to both streams
        for i in 0..5 {
            // Add to stream1
            let data1 = format!(
                "<http://test.be/one_{}> a <http://www.w3.org/test/TypeOne> .",
                i
            );
            let triples1 = engine.parse_data(&data1);
            for triple in triples1 {
                engine.add_to_stream("stream1", triple, i);
            }
            
            // Add to stream2
            let data2 = format!(
                "<http://test.be/two_{}> a <http://www.w3.org/test/TypeTwo> .",
                i
            );
            let triples2 = engine.parse_data(&data2);
            for triple in triples2 {
                engine.add_to_stream("stream2", triple, i + 10);
            }
        }

        engine.stop();

        // Verify we got results
        let results = result_container.lock().unwrap();
        println!("Total results captured:  {}", results.len());
        
        for (i, result) in results.iter().take(3).enumerate() {
            println!("Result {}: {:?}", i, result);
        }

        // Check if there are results with BOTH s1 AND s2 in the same binding
        let has_joined_results = results.iter().any(|binding| {
            let has_s1 = binding. iter().any(|(k, _)| k == "s1");
            let has_s2 = binding.iter().any(|(k, _)| k == "s2");
            has_s1 && has_s2
        });

        assert!(has_joined_results, "Should have joined results with both s1 and s2 in the same binding");
        assert! (!results.is_empty(), "Should have at least some results");
        
        let joined_count = results.iter().filter(|binding| {
            let has_s1 = binding.iter().any(|(k, _)| k == "s1");
            let has_s2 = binding.iter().any(|(k, _)| k == "s2");
            has_s1 && has_s2
        }).count();
        
        println!("Number of properly joined results: {}", joined_count);
        assert!(joined_count > 0, "Should have at least one properly joined result");
    }
}
