/*
* Copyright © 2025 Volodymyr Kadzhaia
* Copyright © 2025 Pieter Bonte
* KU Leuven — Stream Intelligence Lab, Belgium
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this file,
* you can obtain one at [https://mozilla.org/MPL/2.0/](https://mozilla.org/MPL/2.0/).
*/

use crate::rsp::r2r::R2ROperator;
use crate::rsp::r2s::Relation2StreamOperator;
use crate::rsp::s2r::{ContentContainer, ReportStrategy, Tick};
use crate::rsp::window_runner::{WindowRunner, WindowSpec};

#[cfg(not(test))]
use log::{debug, error}; // Use log crate when building application
use shared::query::{Fallback, SyncPolicy};
use shared::rule::Rule;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use crossbeam::channel::{unbounded, RecvTimeoutError, Receiver, Sender};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
#[cfg(test)]
use std::{println as debug, println as error};

use crate::parser::process_rule_definition;
use crate::sparql_database::SparqlDatabase;
use crate::streamertail_optimizer::{ExecutionEngine, LogicalOperator, PhysicalOperator};
use datalog::cross_window_sds::{
    all_component_iris, sds_with_expiry_to_external, Sds, WindowData, WindowedTriple,
};
use datalog::parser_n3_logic::{parse_n3_rules_for_sds, WindowContext};
use datalog::reasoning::materialisation::cross_window_incremental::{
    incremental_sds_plus, SdsWithExpiry,
};
use datalog::reasoning::materialisation::cross_window_naive::naive_sds_plus;
use shared::dictionary::Dictionary;
use shared::triple::Triple;
use std::sync::RwLock;

// Re-exports to preserve the public API used by kolibrie-http-server and examples.
pub use crate::rsp::builder::{RSPBuilder, RSPQueryConfig};
pub use crate::rsp::simple_r2r::SimpleR2R;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrossWindowReasoningMode {
    Incremental,
    Naive,
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

/// RSP-QL Query Plan using Volcano optimizer
#[derive(Debug, Clone)]
pub struct RSPQueryPlan {
    pub window_plans: Vec<PhysicalOperator>,
    pub static_data_plan: Option<PhysicalOperator>,
}

/// Result from a single window execution
#[derive(Debug, Clone)]
pub struct WindowResult {
    pub window_iri: String,
    pub results: Vec<HashMap<String, String>>, // Variable bindings
    pub timestamp: usize,
    pub raw_triples: Vec<(Triple, u64)>,
}

const CROSS_WINDOW_STATIC_IRI: &str = "urn:kolibrie:static:";

pub struct ResultConsumer<I> {
    pub function: Arc<dyn Fn(I) -> () + Send + Sync>,
}

/// Macro to generate the window processing logic
macro_rules! create_window_processor {
    ($window_iri:expr, $query:expr, $query_execution_mode:expr,
     $r2r_store:expr, $has_joins:expr, $cross_window_enabled:expr,
     $window_result_sender:expr, $r2s_consumer_func:expr) => {{
        let mut prev_window_triples: Vec<I> = Vec::new();
        move |content: ContentContainer<I>| {
            debug!(
                "Processing window {} with query: {:?} using {:?} execution",
                $window_iri, $query, $query_execution_mode
            );

            let ts = content.get_last_timestamp_changed();
            if $cross_window_enabled {
                let raw_triples: Vec<(Triple, u64)> = content
                    .iter_with_timestamps()
                    .filter_map(|(item, event_ts)| {
                        (item as &dyn std::any::Any)
                            .downcast_ref::<Triple>()
                            .map(|triple| (triple.clone(), event_ts as u64))
                    })
                    .collect();

                let window_res = WindowResult {
                    window_iri: $window_iri.clone(),
                    results: Vec::new(),
                    timestamp: ts,
                    raw_triples,
                };

                if let Err(e) = $window_result_sender.send(window_res) {
                    error!("Failed to send cross-window raw content: {:?}", e);
                }
                return;
            }

            let mut store = $r2r_store.lock().unwrap();

            // Evict triples from the previous firing of this window
            for t in &prev_window_triples {
                store.remove(t);
            }
            prev_window_triples.clear();

            // Add current window triples and track them for next eviction
            for t in content.into_iter() {
                prev_window_triples.push(t.clone());
                store.add(t);
            }

            // Run forward-chaining inference to materialise derived facts
            store.materialize();

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
                    raw_triples: Vec::new(),
                };

                if let Err(e) = $window_result_sender.send(window_res) {
                    error!("Failed to send window result to buffer: {:?}", e);
                }
            } else {
                ($r2s_consumer_func)(results, ts);
            }
        }
    }};
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

pub struct RSPEngine<I, O>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
    O: Hash,
{
    windows: Vec<WindowRunner<I>>,
    r2r: Arc<Mutex<Box<dyn R2ROperator<I, Vec<PhysicalOperator>, O>>>>,
    r2s_consumer: ResultConsumer<O>,
    window_configs: Vec<RSPWindow>,
    query_execution_mode: QueryExecutionMode,
    operation_mode: OperationMode,
    // Channel for collecting window results for cross-window joins
    window_result_sender: Sender<WindowResult>,
    window_result_receiver: Receiver<WindowResult>,
    // RSP-QL Query Plan using Volcano optimizer
    rsp_query_plan: RSPQueryPlan,
    /// Latest materialized results per window (replace semantics); SingleThread only.
    single_thread_last_materialized: Arc<Mutex<HashMap<String, Vec<HashMap<String, String>>>>>,
    /// Synchronization policy governing multi-window coordination.
    sync_policy: SyncPolicy,
    /// Separate store for static background triples (never touched by window processors).
    static_db: Arc<Mutex<SparqlDatabase>>,
    /// R2S operator for stream-type filtering (RSTREAM/ISTREAM/DSTREAM).
    r2s_operator: Arc<Mutex<Relation2StreamOperator<O>>>,
    /// Opt-in cross-window SDS+ reasoning state.
    cross_window_enabled: bool,
    cross_window_rules: Arc<Vec<Rule>>,
    cross_window_context: Option<WindowContext>,
    cross_window_sds_plus: Arc<Mutex<SdsWithExpiry>>,
    cross_window_latest_contents: Arc<Mutex<HashMap<String, Vec<(Triple, u64)>>>>,
    cross_window_dictionary: Option<Arc<RwLock<Dictionary>>>,
    cross_window_output_iris: Arc<Vec<String>>,
    cross_window_reasoning_mode: CrossWindowReasoningMode,
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
        sync_policy: SyncPolicy,
        reasoning_rules: Vec<Rule>,
        sparql_rules: Vec<String>,
        cross_window_rules: Option<&str>,
        cross_window_reasoning_mode: CrossWindowReasoningMode,
    ) -> Result<RSPEngine<I, O>, String> {
        let mut store = r2r;

        // The PhysicalOperator plans created in `rsp_query_plan` contain integer IDs (constants)
        // that were generated by the Dictionary in `query_config.database`.
        // The `store` (R2R operator) has its own Dictionary. If we don't sync them,
        // the store will assign different IDs to incoming data, and the execution engine
        // will fail to match them against the plan.
        let shared_dict = store
            .as_any_mut()
            .downcast_mut::<SimpleR2R>()
            .map(|s| Arc::clone(&s.item.dictionary));

        if let Some(simple_r2r) = store.as_any_mut().downcast_mut::<SimpleR2R>() {
            debug!("Synchronizing R2R dictionary with Query dictionary");

            // Acquire locks on both dictionaries
            let mut store_dict = simple_r2r.item.dictionary.write().unwrap();
            let query_dict = query_config.database.dictionary.read().unwrap();

            store_dict.merge(&*query_dict);

            drop(store_dict);
            drop(query_dict);
        }

        // Build the static-data store sharing the same dictionary as the R2R store.
        let mut static_sdb = SparqlDatabase::new();
        if let Some(d) = &shared_dict {
            static_sdb.dictionary = Arc::clone(d);
        }
        let static_db = Arc::new(Mutex::new(static_sdb));

        let mut parsed_cross_window_rules = Vec::new();
        let mut cross_window_context = None;
        let mut cross_window_output_iris = Vec::new();
        if let Some(n3_rules) = cross_window_rules {
            let dict = shared_dict
                .as_ref()
                .ok_or_else(|| {
                    "Cross-window SDS+ reasoning requires a SimpleR2R shared dictionary"
                        .to_string()
                })?;
            let mut reasoner = datalog::reasoning::Reasoner::new();
            reasoner.dictionary = Arc::clone(dict);
            let window_widths: HashMap<String, u64> = query_config
                .windows
                .iter()
                .map(|w| (w.window_iri.clone(), w.width as u64))
                .collect();
            let (rules, context) = parse_n3_rules_for_sds(n3_rules, &mut reasoner, window_widths)
                .map_err(|e| format!("Failed to parse cross-window N3 rules: {}", e))?;
            let window_iris: HashSet<String> = query_config
                .windows
                .iter()
                .map(|w| w.window_iri.clone())
                .collect();
            cross_window_output_iris = context
                .all_component_iris
                .iter()
                .filter(|iri| !window_iris.contains(*iri) && iri.as_str() != CROSS_WINDOW_STATIC_IRI)
                .cloned()
                .collect();
            parsed_cross_window_rules = rules;
            cross_window_context = Some(context);
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

        if !reasoning_rules.is_empty() {
            if let Some(simple_r2r) = store.as_any_mut().downcast_mut::<SimpleR2R>() {
                simple_r2r.add_reasoning_rules(reasoning_rules);
            }
        }

        if !sparql_rules.is_empty() {
            if let Some(dict) = store
                .as_any_mut()
                .downcast_mut::<SimpleR2R>()
                .map(|s| Arc::clone(&s.item.dictionary))
            {
                for rule_str in &sparql_rules {
                    let mut temp_db = SparqlDatabase::new();
                    temp_db.dictionary = Arc::clone(&dict);
                    match process_rule_definition(rule_str, &mut temp_db) {
                        Ok((rule, _)) => {
                            if let Some(simple_r2r) = store.as_any_mut().downcast_mut::<SimpleR2R>() {
                                simple_r2r.rules.push(rule);
                            }
                        }
                        Err(e) => error!("Failed to parse SPARQL rule: {:?}", e),
                    }
                }
            }
        }

        // Create windows based on parsed configuration
        let mut windows = Vec::new();
        for window_config in &query_config.windows {
            let spec = WindowSpec {
                width: window_config.width,
                slide: window_config.slide,
                report_strategies: vec![window_config.report_strategy.clone()],
                tick: window_config.tick.clone(),
            };
            windows.push(WindowRunner::new(spec, window_config.window_iri.clone()));
        }

        // Create channel for cross-window result coordination
        let (result_sender, result_receiver) = unbounded::<WindowResult>();

        let stream_type = query_config.stream_type.clone();
        let r2s_operator = Arc::new(Mutex::new(Relation2StreamOperator::new(stream_type, 0)));
        let cross_window_enabled = !parsed_cross_window_rules.is_empty();

        let mut engine = RSPEngine {
            windows,
            r2r: Arc::new(Mutex::new(store)),
            r2s_consumer: result_consumer,
            window_configs: query_config.windows.clone(),
            query_execution_mode,
            operation_mode,
            window_result_sender: result_sender,
            window_result_receiver: result_receiver,
            rsp_query_plan,
            single_thread_last_materialized: Arc::new(Mutex::new(HashMap::new())),
            sync_policy,
            static_db,
            r2s_operator,
            cross_window_enabled,
            cross_window_rules: Arc::new(parsed_cross_window_rules),
            cross_window_context,
            cross_window_sds_plus: Arc::new(Mutex::new(HashMap::new())),
            cross_window_latest_contents: Arc::new(Mutex::new(HashMap::new())),
            cross_window_dictionary: shared_dict,
            cross_window_output_iris: Arc::new(cross_window_output_iris),
            cross_window_reasoning_mode,
        };

        match operation_mode {
            mode @ (OperationMode::SingleThread | OperationMode::MultiThread) => {
                engine.register_windows(mode);
                if matches!(mode, OperationMode::MultiThread) {
                    let has_joins = engine.cross_window_enabled
                        || engine.windows.len() > 1
                        || engine.rsp_query_plan.static_data_plan.is_some();
                    if has_joins {
                        engine.start_cross_window_coordinator();
                    }
                }
            }
        }

        Ok(engine)
    }

    /// Register windows using macros to eliminate code duplication
    fn register_windows(&mut self, operation_mode: OperationMode) {
        let has_joins = self.cross_window_enabled
            || self.windows.len() > 1
            || self.rsp_query_plan.static_data_plan.is_some();

        for (window_idx, window) in self.windows.iter_mut().enumerate() {
            let query = self.rsp_query_plan.window_plans[window_idx].clone();
            let window_iri = self.window_configs[window_idx].window_iri.clone();
            let window_iri_for_thread = window_iri.clone(); // Clone for MultiThread usage
            let query_execution_mode = self.query_execution_mode;
            let window_result_sender = self.window_result_sender.clone();
            let r2r_store = self.r2r.clone();
            let cross_window_enabled = self.cross_window_enabled;

            let r2s_consumer_func: Arc<dyn Fn(Vec<O>, usize) + Send + Sync> = if has_joins {
                Arc::new(|_, _| {})
            } else {
                let r2s_op = Arc::clone(&self.r2s_operator);
                let consumer_fn = self.r2s_consumer.function.clone();
                Arc::new(move |results: Vec<O>, ts: usize| {
                    let filtered = r2s_op.lock().unwrap().eval(results, ts);
                    for r in filtered {
                        (consumer_fn)(r);
                    }
                })
            };

            // Create processor using macro
            let mut processor = create_window_processor!(
                window_iri,
                query,
                query_execution_mode,
                r2r_store,
                has_joins,
                cross_window_enabled,
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
    /// (and optionally joins with static background data), respecting `sync_policy`.
    fn start_cross_window_coordinator(&self)
    where
        O: From<Vec<(String, String)>>,
    {
        let receiver = self.window_result_receiver.clone();
        let consumer = self.r2s_consumer.function.clone();
        let num_windows = self.windows.len();
        let static_data_plan = self.rsp_query_plan.static_data_plan.clone();
        let static_db = self.static_db.clone();
        let sync_policy = self.sync_policy.clone();
        let r2s_operator = Arc::clone(&self.r2s_operator);
        let cross_window_enabled = self.cross_window_enabled;
        let cross_window_rules = Arc::clone(&self.cross_window_rules);
        let cross_window_sds_plus = Arc::clone(&self.cross_window_sds_plus);
        let cross_window_latest_contents = Arc::clone(&self.cross_window_latest_contents);
        let cross_window_dictionary = self.cross_window_dictionary.clone();
        let cross_window_output_iris = Arc::clone(&self.cross_window_output_iris);
        let cross_window_reasoning_mode = self.cross_window_reasoning_mode;
        let window_configs = self.window_configs.clone();
        let window_plans = self.rsp_query_plan.window_plans.clone();

        thread::spawn(move || {
            // Latest results per window (replace semantics)
            let mut last_materialized: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
            // Windows that have fired since the last reset
            let mut cycle_triggered: HashSet<String> = HashSet::new();
            // When the first window fired in the current cycle
            let mut cycle_start: Option<Instant> = None;
            let mut max_ts: usize = 0;

            loop {
                // Compute recv timeout when policy has a finite deadline
                let timeout_remaining = match &sync_policy {
                    SyncPolicy::Timeout { duration, .. } => {
                        cycle_start.map(|start| duration.saturating_sub(start.elapsed()))
                    }
                    _ => None,
                };

                // Receive next window result (or timeout/disconnect)
                let maybe_result: Option<WindowResult> = if let Some(remaining) = timeout_remaining {
                    match receiver.recv_timeout(remaining) {
                        Ok(r) => Some(r),
                        Err(RecvTimeoutError::Timeout) => {
                            // Deadline elapsed
                            if !cycle_triggered.is_empty() {
                                match &sync_policy {
                                    SyncPolicy::Timeout { fallback: Fallback::Steal, .. } => {
                                        if last_materialized.len() == num_windows {
                                            if cross_window_enabled {
                                                if let Some(dict) = &cross_window_dictionary {
                                                    emit_cross_window_results(
                                                        &window_configs,
                                                        &window_plans,
                                                        &static_data_plan,
                                                        &static_db,
                                                        &r2s_operator,
                                                        max_ts,
                                                        &consumer,
                                                        &cross_window_rules,
                                                        &cross_window_sds_plus,
                                                        &cross_window_latest_contents,
                                                        dict,
                                                        &cross_window_output_iris,
                                                        cross_window_reasoning_mode,
                                                    );
                                                }
                                            } else {
                                                emit_results(&last_materialized, &static_data_plan, &static_db, &r2s_operator, max_ts, &consumer);
                                            }
                                        }
                                    }
                                    SyncPolicy::Timeout { fallback: Fallback::Drop, .. } => {
                                        // discard this cycle
                                    }
                                    _ => {}
                                }
                                cycle_triggered.clear();
                                cycle_start = None;
                                max_ts = 0;
                            }
                            continue;
                        }
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                } else {
                    match receiver.recv() {
                        Ok(r) => Some(r),
                        Err(_) => break,
                    }
                };

                if let Some(window_result) = maybe_result {
                    debug!(
                        "Coordinator received {} results from window: {}",
                        window_result.results.len(),
                        window_result.window_iri
                    );

                    max_ts = max_ts.max(window_result.timestamp);
                    if cross_window_enabled {
                        cross_window_latest_contents.lock().unwrap().insert(
                            window_result.window_iri.clone(),
                            window_result.raw_triples.clone(),
                        );
                    }
                    // Update last_materialized (replace)
                    last_materialized.insert(
                        window_result.window_iri.clone(),
                        window_result.results.clone(),
                    );
                    if cycle_triggered.is_empty() {
                        cycle_start = Some(Instant::now());
                    }
                    cycle_triggered.insert(window_result.window_iri.clone());

                    // Drain any additional pending results
                    while let Ok(wr) = receiver.try_recv() {
                        max_ts = max_ts.max(wr.timestamp);
                        if cross_window_enabled {
                            cross_window_latest_contents
                                .lock()
                                .unwrap()
                                .insert(wr.window_iri.clone(), wr.raw_triples.clone());
                        }
                        last_materialized.insert(wr.window_iri.clone(), wr.results.clone());
                        cycle_triggered.insert(wr.window_iri.clone());
                    }

                    if cycle_triggered.len() == num_windows {
                        // All windows fired this cycle
                        if cross_window_enabled {
                            if let Some(dict) = &cross_window_dictionary {
                                emit_cross_window_results(
                                    &window_configs,
                                    &window_plans,
                                    &static_data_plan,
                                    &static_db,
                                    &r2s_operator,
                                    max_ts,
                                    &consumer,
                                    &cross_window_rules,
                                    &cross_window_sds_plus,
                                    &cross_window_latest_contents,
                                    dict,
                                    &cross_window_output_iris,
                                    cross_window_reasoning_mode,
                                );
                            }
                        } else {
                            emit_results(&last_materialized, &static_data_plan, &static_db, &r2s_operator, max_ts, &consumer);
                        }
                        cycle_triggered.clear();
                        cycle_start = None;
                        max_ts = 0;
                    } else {
                        match &sync_policy {
                            SyncPolicy::Steal => {
                                // Emit immediately using stale data from non-firing windows
                                if last_materialized.len() == num_windows {
                                    if cross_window_enabled {
                                        if let Some(dict) = &cross_window_dictionary {
                                            emit_cross_window_results(
                                                &window_configs,
                                                &window_plans,
                                                &static_data_plan,
                                                &static_db,
                                                &r2s_operator,
                                                max_ts,
                                                &consumer,
                                                &cross_window_rules,
                                                &cross_window_sds_plus,
                                                &cross_window_latest_contents,
                                                dict,
                                                &cross_window_output_iris,
                                                cross_window_reasoning_mode,
                                            );
                                        }
                                    } else {
                                        emit_results(&last_materialized, &static_data_plan, &static_db, &r2s_operator, max_ts, &consumer);
                                    }
                                }
                                cycle_triggered.clear();
                                cycle_start = None;
                                max_ts = 0;
                            }
                            SyncPolicy::Wait | SyncPolicy::Timeout { .. } => {
                                // Keep waiting for remaining windows
                                debug!(
                                    "Coordinator: waiting for more windows ({}/{}) — have: {:?}",
                                    cycle_triggered.len(),
                                    num_windows,
                                    cycle_triggered.iter().collect::<Vec<_>>()
                                );
                            }
                        }
                    }
                }
            }

            debug!("Coordinator: shutdown complete");
        });
    }

    /// Add data to appropriate window based on stream IRI
    pub fn add_to_stream(&mut self, stream_iri: &str, event_item: I, ts: usize) {
        if matches!(self.operation_mode, OperationMode::SingleThread)
            && (self.cross_window_enabled
                || self.windows.len() > 1
                || self.rsp_query_plan.static_data_plan.is_some())
        {
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

    pub fn process_single_thread_window_results(&mut self)
    where
        O: From<Vec<(String, String)>>,
    {
        let consumer = self.r2s_consumer.function.clone();
        let num_windows = self.windows.len();
        let sync_policy = self.sync_policy.clone();

        // Drain all pending channel results; update last_materialized with replace semantics.
        let mut last_mat = self.single_thread_last_materialized.lock().unwrap();
        let mut had_new_results = false;
        let mut max_ts: usize = 0;
        while let Ok(window_result) = self.window_result_receiver.try_recv() {
            max_ts = max_ts.max(window_result.timestamp);
            if self.cross_window_enabled {
                self.cross_window_latest_contents.lock().unwrap().insert(
                    window_result.window_iri.clone(),
                    window_result.raw_triples.clone(),
                );
            }
            let rows = last_mat
                .entry(window_result.window_iri.clone())
                .or_default();
            rows.extend(window_result.results);
            had_new_results = true;
        }

        if !had_new_results {
            return;
        }

        // Check whether to emit based on policy.
        if last_mat.len() == num_windows {
            debug!("SingleThread: all {} windows materialized, emitting", num_windows);
            let static_data_plan = self.rsp_query_plan.static_data_plan.clone();
            if self.cross_window_enabled {
                if let Some(dict) = &self.cross_window_dictionary {
                    emit_cross_window_results(
                        &self.window_configs,
                        &self.rsp_query_plan.window_plans,
                        &static_data_plan,
                        &self.static_db,
                        &self.r2s_operator,
                        max_ts,
                        &consumer,
                        &self.cross_window_rules,
                        &self.cross_window_sds_plus,
                        &self.cross_window_latest_contents,
                        dict,
                        &self.cross_window_output_iris,
                        self.cross_window_reasoning_mode,
                    );
                }
            } else {
                emit_results(&*last_mat, &static_data_plan, &self.static_db, &self.r2s_operator, max_ts, &consumer);
            }

            match sync_policy {
                // Wait: require all windows to fire again before next emission.
                // Timeout: no wall-clock timer in single-threaded context; treat as Wait.
                SyncPolicy::Wait | SyncPolicy::Timeout { .. } => {
                    last_mat.clear();
                }
                // Steal: keep last_mat so stale data from non-firing windows is reused.
                SyncPolicy::Steal => {}
            }
        } else {
            debug!(
                "SingleThread: waiting for more windows ({}/{})",
                last_mat.len(),
                num_windows
            );
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
            window.flush();
            window.stop();
        }
        if matches!(self.operation_mode, OperationMode::SingleThread) {
            self.process_single_thread_window_results();
        }
    }

    pub fn parse_data(&mut self, data: &str) -> Vec<I> {
        self.r2r.lock().unwrap().parse_data(data)
    }

    /// Pre-populate the static background store with N-Triples data.
    /// These triples are never placed in the window R2R store, so they cannot
    /// leak into window query results.  They are only visible when `emit_results`
    /// joins the window output with the static-data plan.
    pub fn add_static_ntriples(&mut self, data: &str) {
        let mut db = self.static_db.lock().unwrap();
        db.parse_ntriples_and_add(data);
        db.get_or_build_stats();
        db.build_all_indexes();
    }

    /// Get information about configured windows
    pub fn get_window_info(&self) -> Vec<&RSPWindow> {
        self.window_configs.iter().collect()
    }

    /// Get the RSP-QL query plan information
    pub fn get_query_plan(&self) -> &RSPQueryPlan {
        &self.rsp_query_plan
    }

    /// Return parsed cross-window SDS+ metadata when cross-window reasoning is enabled.
    pub fn get_cross_window_context(&self) -> Option<&WindowContext> {
        self.cross_window_context.as_ref()
    }

    /// Return the stream IRIs registered across all configured windows.
    pub fn stream_iris(&self) -> Vec<String> {
        self.window_configs.iter().map(|w| w.stream_iri.clone()).collect()
    }
}

/// Join all window results, optionally apply the static-data join, apply the R2S operator,
/// and call `consumer` for each output binding.
/// Called from both the coordinator thread and the SingleThread processor.
fn emit_results<O>(
    last_materialized: &HashMap<String, Vec<HashMap<String, String>>>,
    static_data_plan: &Option<PhysicalOperator>,
    static_db: &Arc<Mutex<SparqlDatabase>>,
    r2s: &Arc<Mutex<Relation2StreamOperator<O>>>,
    ts: usize,
    consumer: &Arc<dyn Fn(O) -> () + Send + Sync>,
) where
    O: 'static + Clone + Hash + Eq + From<Vec<(String, String)>>,
{
    let joined = join_window_results(last_materialized);

    let final_results = if let Some(ref plan) = static_data_plan {
        let static_bindings = execute_plan_as_bindings(static_db, plan);
        debug!("emit_results: static bindings = {}", static_bindings.len());
        natural_join(&joined, &static_bindings)
    } else {
        joined
    };

    debug!("emit_results: emitting {} bindings before R2S filter", final_results.len());
    let outputs: Vec<O> = final_results
        .into_iter()
        .map(|b| {
            let mut kv: Vec<(String, String)> = b.into_iter().collect();
            kv.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            kv.into()
        })
        .collect();
    let filtered = r2s.lock().unwrap().eval(outputs, ts);
    for result in filtered {
        (consumer)(result);
    }
}

/// Natural join of two binding sets: compatible bindings are merged, incompatible ones are dropped.
/// Produces the Cartesian product when the two sets share no variables.
fn natural_join(
    left: &[HashMap<String, String>],
    right: &[HashMap<String, String>],
) -> Vec<HashMap<String, String>> {
    if left.is_empty() || right.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();

    for left_binding in left {
        for right_binding in right {
            // Check compatibility: shared variables must agree on value
            let mut compatible = true;
            for (var, val) in left_binding {
                if let Some(right_val) = right_binding.get(var) {
                    if val != right_val {
                        compatible = false;
                        break;
                    }
                }
            }

            if compatible {
                let mut merged = left_binding.clone();
                for (k, v) in right_binding {
                    merged.insert(k.clone(), v.clone());
                }
                result.push(merged);
            }
        }
    }

    result
}

/// Join results from multiple windows using natural join semantics.
fn join_window_results(window_buffers: &HashMap<String, Vec<HashMap<String, String>>>) -> Vec<HashMap<String, String>> {
    if window_buffers.is_empty() {
        return Vec::new();
    }

    let mut all_windows: Vec<Vec<HashMap<String, String>>> = window_buffers.values().cloned().collect();

    if all_windows.len() == 1 {
        return all_windows.into_iter().next().unwrap();
    }

    // Iteratively natural-join all window result sets
    let mut joined = all_windows.remove(0);
    for window_results in all_windows {
        joined = natural_join(&joined, &window_results);
    }

    joined
}

/// Execute a physical plan against the static-data `SparqlDatabase` and return the results as
/// a list of variable-binding maps.
fn execute_plan_as_bindings(
    static_db: &Arc<Mutex<SparqlDatabase>>,
    plan: &PhysicalOperator,
) -> Vec<HashMap<String, String>> {
    let mut db = static_db.lock().unwrap();
    ExecutionEngine::execute(plan, &mut *db)
}

fn build_cross_window_sds(
    window_configs: &[RSPWindow],
    latest_contents: &HashMap<String, Vec<(Triple, u64)>>,
    static_db: &Arc<Mutex<SparqlDatabase>>,
    dict: &Arc<RwLock<Dictionary>>,
    output_iris: &[String],
) -> Sds {
    let mut sds = Sds::new();

    {
        let dict_r = dict.read().unwrap();
        for window in window_configs {
            let triples = latest_contents
                .get(&window.window_iri)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(triple, event_time)| {
                    let subject = dict_r.decode(triple.subject)?.to_string();
                    let predicate = dict_r.decode(triple.predicate)?.to_string();
                    let object = dict_r.decode(triple.object)?.to_string();
                    Some(WindowedTriple {
                        subject,
                        predicate,
                        object,
                        event_time,
                    })
                })
                .collect();

            sds.windows.insert(
                window.window_iri.clone(),
                WindowData {
                    alpha: window.width as u64,
                    triples,
                },
            );
        }
    }

    for iri in output_iris {
        sds.output_iris.insert(iri.clone());
    }

    let static_triples = {
        let db = static_db.lock().unwrap();
        let dict_r = dict.read().unwrap();
        db.triples
            .iter()
            .filter_map(|triple| {
                let subject = dict_r.decode(triple.subject)?.to_string();
                let predicate = dict_r.decode(triple.predicate)?.to_string();
                let object = dict_r.decode(triple.object)?.to_string();
                Some((subject, predicate, object))
            })
            .collect::<Vec<_>>()
    };

    if !static_triples.is_empty() {
        sds.static_graphs
            .insert(CROSS_WINDOW_STATIC_IRI.to_string(), static_triples);
    }

    sds
}

fn execute_window_plans_on_external_buckets(
    window_configs: &[RSPWindow],
    window_plans: &[PhysicalOperator],
    external_buckets: &HashMap<String, Vec<Triple>>,
    dict: &Arc<RwLock<Dictionary>>,
) -> HashMap<String, Vec<HashMap<String, String>>> {
    let mut materialized = HashMap::new();

    for (window, plan) in window_configs.iter().zip(window_plans.iter()) {
        let mut db = SparqlDatabase::new();
        db.dictionary = Arc::clone(dict);

        if let Some(triples) = external_buckets.get(&window.window_iri) {
            for triple in triples {
                db.add_triple(triple.clone());
            }
        }

        let results = ExecutionEngine::execute(plan, &mut db);
        materialized.insert(window.window_iri.clone(), results);
    }

    materialized
}

fn emit_cross_window_results<O>(
    window_configs: &[RSPWindow],
    window_plans: &[PhysicalOperator],
    static_data_plan: &Option<PhysicalOperator>,
    static_db: &Arc<Mutex<SparqlDatabase>>,
    r2s: &Arc<Mutex<Relation2StreamOperator<O>>>,
    ts: usize,
    consumer: &Arc<dyn Fn(O) -> () + Send + Sync>,
    rules: &[Rule],
    previous_sds_plus: &Arc<Mutex<SdsWithExpiry>>,
    latest_contents: &Arc<Mutex<HashMap<String, Vec<(Triple, u64)>>>>,
    dict: &Arc<RwLock<Dictionary>>,
    output_iris: &[String],
    mode: CrossWindowReasoningMode,
) where
    O: 'static + Clone + Hash + Eq + From<Vec<(String, String)>>,
{
    let latest = latest_contents.lock().unwrap().clone();
    let sds = build_cross_window_sds(window_configs, &latest, static_db, dict, output_iris);

    let external_buckets = match mode {
        CrossWindowReasoningMode::Incremental => {
            let new_sds_plus = {
                let old = previous_sds_plus.lock().unwrap().clone();
                incremental_sds_plus(rules, &sds, &old, dict, ts as u64)
            };

            {
                let mut old = previous_sds_plus.lock().unwrap();
                *old = new_sds_plus.clone();
            }

            let component_iris = all_component_iris(&sds);
            sds_with_expiry_to_external(&new_sds_plus, dict, &component_iris)
        }
        CrossWindowReasoningMode::Naive => naive_sds_plus(rules, &sds, dict, ts as u64),
    };

    let materialized = execute_window_plans_on_external_buckets(
        window_configs,
        window_plans,
        &external_buckets,
        dict,
    );

    emit_results(
        &materialized,
        static_data_plan,
        static_db,
        r2s,
        ts,
        consumer,
    );
}
