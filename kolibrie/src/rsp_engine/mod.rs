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
use crate::rsp::s2r::{ContentContainer, ProbabilisticOccurrence, ReportStrategy, Tick};
use crate::rsp::window_runner::{WindowRunner, WindowSpec};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};
#[cfg(not(test))]
use log::{debug, error};
use shared::query::{Fallback, SyncPolicy};
use shared::rule::Rule;
use shared::term_encoder::SharedTerms;
use shared::terms::TextBindings;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
#[cfg(test)]
use std::{println as debug, println as error};

use crate::database::KolibrieDB;
use crate::rule_execution::process_rule_definition;
use crate::streamertail_optimizer::{ExecutionEngine, LogicalOperator, PhysicalOperator};
use datalog::cross_window_sds::{
    all_component_iris, sds_with_expiry_to_external, Sds, WindowData, WindowedTriple,
};
use datalog::parser_n3_logic::{parse_n3_rules_for_sds, WindowContext};
use datalog::reasoning::materialisation::cross_window_incremental::{
    incremental_sds_plus, SdsWithExpiry,
};
use datalog::reasoning::materialisation::cross_window_naive::naive_sds_plus;
use shared::hybrid::{
    normalize_stream_iri, HybridError, HybridProbabilityResult, SeedId, SeedRegistry,
};
use shared::triple::Triple;

pub mod builder;

pub use crate::rsp::simple_r2r::QueryExecutionMode;
pub use crate::rsp::simple_r2r::SimpleR2R;
pub use builder::{RSPBuilder, RSPQueryConfig};

/// How the engine runs its windows
#[derive(Clone, Copy, Debug)]
pub enum OperationMode {
    /// Everything in the caller's thread
    SingleThread,
    /// One thread per window, with a coordinator joining their results
    MultiThread,
}

/// How derived facts are maintained as windows advance
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrossWindowReasoningMode {
    /// Maintain the derivation across firings, reusing what still holds
    Incremental,
    /// Re-derive from scratch each firing
    Naive,
}

/// Window configuration extracted from parsed RSP-QL query
#[derive(Debug, Clone)]
pub struct RSPWindow {
    /// Name the query refers to this window by
    pub window_iri: String,
    /// Stream it observes
    pub stream_iri: String,
    /// Width in time units; bounds are half-open
    pub width: usize,
    /// How far it advances each step
    pub slide: usize,
    /// What advances its clock
    pub tick: Tick,
    /// When it reports
    pub report_strategy: ReportStrategy,
    /// The query evaluated over this window's content, before planning
    pub query: LogicalOperator,
}

/// RSP-QL Query Plan using Volcano optimizer
#[derive(Debug, Clone)]
pub struct RSPQueryPlan {
    /// One costed plan per window, in the same order as the window configs
    pub window_plans: Vec<PhysicalOperator>,
    /// Plan for patterns outside any window, if the query has them
    pub static_data_plan: Option<PhysicalOperator>,
}

/// Result from a single window execution
#[derive(Debug, Clone)]
pub struct WindowResult {
    /// Which window produced this
    pub window_iri: String,
    /// Solutions, as decoded variable bindings — text, not dictionary ids, because a result
    /// crossing between windows cannot assume one dictionary
    pub results: TextBindings,
    /// When the window fired
    pub timestamp: usize,
    /// The window's content with per-triple timestamps, for cross-window reasoning
    pub raw_triples: Vec<(Triple, u64)>,
}

const CROSS_WINDOW_STATIC_IRI: &str = "urn:kolibrie:static:";

/// The callback the output stream is delivered to
pub struct ResultConsumer<I> {
    /// Invoked once per published solution
    pub function: Arc<dyn Fn(I) -> () + Send + Sync>,
}

impl<I> Debug for ResultConsumer<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultConsumer")
            .field("function", &"<callback>")
            .finish()
    }
}

/// Macro to generate the window processing logic
macro_rules! create_window_processor {
    ($window_iri:expr, $query:expr, $query_execution_mode:expr,
     $r2r_store:expr, $has_joins:expr, $cross_window_enabled:expr,
     $window_result_sender:expr, $r2s_consumer_func:expr,
     $seed_registry:expr, $latest_hybrid_results:expr) => {{
        let mut prev_window_triples: Vec<I> = Vec::new();
        move |content: ContentContainer<I>| {
            debug!(
                "Processing window {} with query: {:?} using {:?} execution",
                $window_iri, $query, $query_execution_mode
            );

            let ts = content.get_last_timestamp_changed();
            let live_seed_ids: Vec<SeedId> = content
                .probabilistic_occurrences()
                .iter()
                .filter(|occurrence| !content.is_deterministic(&occurrence.item))
                .map(|occurrence| occurrence.seed_id)
                .collect();
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
                    results: TextBindings::new(),
                    timestamp: ts,
                    raw_triples,
                };

                if let Err(e) = $window_result_sender.send(window_res) {
                    error!("Failed to send cross-window raw content: {:?}", e);
                }
                return;
            }

            let mut store = $r2r_store.lock().unwrap();

            if let Some(simple_r2r) = store.as_any_mut().downcast_mut::<SimpleR2R>() {
                match $seed_registry
                    .lock()
                    .unwrap()
                    .snapshot_for_ids(live_seed_ids)
                {
                    Ok(snapshot) => simple_r2r.set_live_seed_snapshot(snapshot),
                    Err(error) => error!("Unable to build live hybrid seed snapshot: {}", error),
                }
            }

            for t in &prev_window_triples {
                store.remove(t);
            }
            prev_window_triples.clear();

            for t in content.into_iter() {
                prev_window_triples.push(t.clone());
                store.add(t);
            }

            store.materialize();

            if let Some(simple_r2r) = store.as_any_mut().downcast_mut::<SimpleR2R>() {
                $latest_hybrid_results.lock().unwrap().insert(
                    $window_iri.clone(),
                    simple_r2r.last_hybrid_results().clone(),
                );
            }

            let results = store.execute_query(&$query);
            debug!("Got # results {} for window {}", results.len(), $window_iri);

            drop(store);

            if $has_joins {
                let mut mapped_results = TextBindings::new();
                for res in &results {
                    if let Some(bindings) =
                        (res as &dyn std::any::Any).downcast_ref::<Vec<(String, String)>>()
                    {
                        mapped_results
                            .push_pairs(bindings.iter().map(|(k, v)| (k.as_str(), v.clone())));
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
        })
    }};
}

/// The RSP engine: windows over streams, a query over each, and one output stream
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
    window_result_sender: Option<Sender<WindowResult>>,
    window_result_receiver: Receiver<WindowResult>,
    /// One per window under [`OperationMode::MultiThread`], joined by [`Self::stop`]
    window_threads: Vec<thread::JoinHandle<()>>,
    /// The coordinator, when the query needs one
    coordinator_thread: Option<thread::JoinHandle<()>>,
    rsp_query_plan: RSPQueryPlan,
    /// Latest materialized results per window (replace semantics); SingleThread only
    single_thread_last_materialized: Arc<Mutex<HashMap<String, TextBindings>>>,
    /// Synchronization policy governing multi-window coordination
    sync_policy: SyncPolicy,
    /// Separate store for static background triples (never touched by window processors)
    static_db: Arc<Mutex<KolibrieDB>>,
    /// R2S operator for stream-type filtering (RSTREAM/ISTREAM/DSTREAM)
    r2s_operator: Arc<Mutex<Relation2StreamOperator<O>>>,
    /// Opt-in cross-window SDS+ reasoning state
    cross_window_enabled: bool,
    cross_window_rules: Arc<Vec<Rule>>,
    cross_window_context: Option<WindowContext>,
    cross_window_sds_plus: Arc<Mutex<SdsWithExpiry>>,
    cross_window_latest_contents: Arc<Mutex<HashMap<String, Vec<(Triple, u64)>>>>,
    /// The term space the cross-window reasoner shares with the R2R store
    cross_window_dictionary: Option<SharedTerms>,
    cross_window_output_iris: Arc<Vec<String>>,
    cross_window_reasoning_mode: CrossWindowReasoningMode,
    hybrid_seed_registry: Arc<Mutex<SeedRegistry>>,
    latest_hybrid_results: Arc<Mutex<HashMap<String, HashMap<Triple, HybridProbabilityResult>>>>,
}

impl<I, O> Debug for RSPEngine<I, O>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
    O: Hash,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RSPEngine")
            .field("windows", &self.windows.len())
            .field("window_configs", &self.window_configs.len())
            .field("query_execution_mode", &self.query_execution_mode)
            .field("operation_mode", &self.operation_mode)
            .field("sync_policy", &self.sync_policy)
            .field("cross_window_enabled", &self.cross_window_enabled)
            .field("cross_window_rules", &self.cross_window_rules.len())
            .field(
                "cross_window_reasoning_mode",
                &self.cross_window_reasoning_mode,
            )
            .finish_non_exhaustive()
    }
}

impl<I, O> RSPEngine<I, O>
where
    O: Clone + Hash + Eq + Send + 'static + From<Vec<(String, String)>>,
    I: Eq + PartialEq + Clone + Debug + Hash + Send + 'static,
{
    /// An engine over the given windows and plan
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

        let shared_dict = store
            .as_any_mut()
            .downcast_mut::<SimpleR2R>()
            .map(|s| s.item.terms());

        if let Some(simple_r2r) = store.as_any_mut().downcast_mut::<SimpleR2R>() {
            debug!("Synchronizing R2R dictionary with Query dictionary");

            let mut store_dict = simple_r2r.item.dictionary.write();
            let query_dict = query_config.database.dictionary.read();

            store_dict.merge(&*query_dict);

            drop(store_dict);
            drop(query_dict);
        }

        let mut static_sdb = KolibrieDB::new();
        if let Some(terms) = &shared_dict {
            static_sdb.adopt_terms(terms.clone());
        }
        let static_db = Arc::new(Mutex::new(static_sdb));

        let mut parsed_cross_window_rules = Vec::new();
        let mut cross_window_context = None;
        let mut cross_window_output_iris = Vec::new();
        if let Some(n3_rules) = cross_window_rules {
            let dict = shared_dict.as_ref().ok_or_else(|| {
                "Cross-window SDS+ reasoning requires a SimpleR2R shared dictionary".to_string()
            })?;
            let mut reasoner = datalog::reasoning::Reasoner::new();
            reasoner.adopt_terms(dict.clone());
            let window_widths: HashMap<String, u64> = query_config
                .windows
                .iter()
                .map(|w| (w.window_iri.clone(), w.width as u64))
                .collect();
            let (rules, context) =
                parse_n3_rules_for_sds(n3_rules, &mut reasoner, window_widths)
                    .map_err(|e| format!("Failed to parse cross-window N3 rules: {}", e))?;
            let window_iris: HashSet<String> = query_config
                .windows
                .iter()
                .map(|w| w.window_iri.clone())
                .collect();
            cross_window_output_iris = context
                .all_component_iris
                .iter()
                .filter(|iri| {
                    !window_iris.contains(*iri) && iri.as_str() != CROSS_WINDOW_STATIC_IRI
                })
                .cloned()
                .collect();
            parsed_cross_window_rules = rules;
            cross_window_context = Some(context);
        }

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
                .map(|s| s.item.terms())
            {
                for rule_str in &sparql_rules {
                    let parsed_hybrid_config = crate::parser::parse_combined_query(rule_str)
                        .ok()
                        .and_then(|(_, combined)| combined.single_rule().ok().flatten().cloned())
                        .and_then(|rule| rule.prob_annotation)
                        .and_then(|annotation| annotation.hybrid_config);
                    let mut temp_db = KolibrieDB::new();
                    temp_db.adopt_terms(dict.clone());
                    match process_rule_definition(rule_str, &mut temp_db) {
                        Ok((rule, _)) => {
                            if let Some(simple_r2r) = store.as_any_mut().downcast_mut::<SimpleR2R>()
                            {
                                if let Some(config) = parsed_hybrid_config {
                                    simple_r2r
                                        .set_hybrid_config(config)
                                        .map_err(|error| error.to_string())?;
                                }
                                simple_r2r.rules.push(rule);
                            }
                        }
                        Err(e) => return Err(format!("Failed to parse SPARQL rule: {e}")),
                    }
                }
            }
        }

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

        let (result_sender, result_receiver) = unbounded::<WindowResult>();

        let stream_type = query_config.stream_type.clone();
        let r2s_operator = Arc::new(Mutex::new(Relation2StreamOperator::new(stream_type, 0)));
        let cross_window_enabled = !parsed_cross_window_rules.is_empty();
        if cross_window_enabled
            && store
                .as_any_mut()
                .downcast_mut::<SimpleR2R>()
                .is_some_and(|simple| simple.hybrid_enabled())
        {
            return Err(
                "Hybrid probabilistic materialisation and cross-window SDS+ reasoning cannot be combined in v1"
                    .to_string(),
            );
        }

        let mut engine = RSPEngine {
            windows,
            r2r: Arc::new(Mutex::new(store)),
            r2s_consumer: result_consumer,
            window_configs: query_config.windows.clone(),
            query_execution_mode,
            operation_mode,
            window_result_sender: Some(result_sender),
            window_result_receiver: result_receiver,
            window_threads: Vec::new(),
            coordinator_thread: None,
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
            hybrid_seed_registry: Arc::new(Mutex::new(SeedRegistry::new())),
            latest_hybrid_results: Arc::new(Mutex::new(HashMap::new())),
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

        let mut window_threads = Vec::new();
        for (window_idx, window) in self.windows.iter_mut().enumerate() {
            let query = self.rsp_query_plan.window_plans[window_idx].clone();
            let window_iri = self.window_configs[window_idx].window_iri.clone();
            let window_iri_for_thread = window_iri.clone();
            let query_execution_mode = self.query_execution_mode;
            let window_result_sender = self
                .window_result_sender
                .as_ref()
                .expect("windows are registered during construction, long before stop")
                .clone();
            let r2r_store = self.r2r.clone();
            let cross_window_enabled = self.cross_window_enabled;
            let seed_registry = Arc::clone(&self.hybrid_seed_registry);
            let latest_hybrid_results = Arc::clone(&self.latest_hybrid_results);

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

            let mut processor = create_window_processor!(
                window_iri,
                query,
                query_execution_mode,
                r2r_store,
                has_joins,
                cross_window_enabled,
                window_result_sender,
                r2s_consumer_func,
                seed_registry,
                latest_hybrid_results
            );

            match operation_mode {
                OperationMode::SingleThread => {
                    register_window!(SingleThread, window, processor);
                }
                OperationMode::MultiThread => {
                    window_threads.push(register_window!(
                        MultiThread,
                        window,
                        processor,
                        window_iri_for_thread
                    ));
                }
            }
        }
        self.window_threads = window_threads;
    }

    /// Starts the coordinator thread that joins results from several windows, respecting
    /// `sync_policy`
    fn start_cross_window_coordinator(&mut self)
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

        self.coordinator_thread = Some(thread::spawn(move || {
            let mut last_materialized: HashMap<String, TextBindings> = HashMap::new();
            let mut cycle_triggered: HashSet<String> = HashSet::new();
            let mut cycle_start: Option<Instant> = None;
            let mut max_ts: usize = 0;

            loop {
                let timeout_remaining = match &sync_policy {
                    SyncPolicy::Timeout { duration, .. } => {
                        cycle_start.map(|start| duration.saturating_sub(start.elapsed()))
                    }
                    _ => None,
                };

                let maybe_result: Option<WindowResult> = if let Some(remaining) = timeout_remaining
                {
                    match receiver.recv_timeout(remaining) {
                        Ok(r) => Some(r),
                        Err(RecvTimeoutError::Timeout) => {
                            if !cycle_triggered.is_empty() {
                                match &sync_policy {
                                    SyncPolicy::Timeout {
                                        fallback: Fallback::Steal,
                                        ..
                                    } => {
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
                                                emit_results(
                                                    &last_materialized,
                                                    &static_data_plan,
                                                    &static_db,
                                                    &r2s_operator,
                                                    max_ts,
                                                    &consumer,
                                                );
                                            }
                                        }
                                    }
                                    SyncPolicy::Timeout {
                                        fallback: Fallback::Drop,
                                        ..
                                    } => {}
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
                    last_materialized.insert(
                        window_result.window_iri.clone(),
                        window_result.results.clone(),
                    );
                    if cycle_triggered.is_empty() {
                        cycle_start = Some(Instant::now());
                    }
                    cycle_triggered.insert(window_result.window_iri.clone());

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
                            emit_results(
                                &last_materialized,
                                &static_data_plan,
                                &static_db,
                                &r2s_operator,
                                max_ts,
                                &consumer,
                            );
                        }
                        cycle_triggered.clear();
                        cycle_start = None;
                        max_ts = 0;
                    } else {
                        match &sync_policy {
                            SyncPolicy::Steal => {
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
                                        emit_results(
                                            &last_materialized,
                                            &static_data_plan,
                                            &static_db,
                                            &r2s_operator,
                                            max_ts,
                                            &consumer,
                                        );
                                    }
                                }
                                cycle_triggered.clear();
                                cycle_start = None;
                                max_ts = 0;
                            }
                            SyncPolicy::Wait | SyncPolicy::Timeout { .. } => {
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

            if !cycle_triggered.is_empty() && last_materialized.len() == num_windows {
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
                    emit_results(
                        &last_materialized,
                        &static_data_plan,
                        &static_db,
                        &r2s_operator,
                        max_ts,
                        &consumer,
                    );
                }
            }
            debug!("Coordinator: shutdown complete");
        }));
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
            let s = s.trim_start_matches('<').trim_end_matches('>');
            let s = s.strip_prefix(':').unwrap_or(s);
            s.to_string()
        }

        let input_norm = normalize_stream_iri(stream_iri);

        for (window_idx, window_config) in self.window_configs.iter().enumerate() {
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

    /// Evaluates and publishes whatever the windows currently hold, for
    /// [`OperationMode::SingleThread`]
    pub fn process_single_thread_window_results(&mut self)
    where
        O: From<Vec<(String, String)>>,
    {
        let consumer = self.r2s_consumer.function.clone();
        let num_windows = self.windows.len();
        let sync_policy = self.sync_policy.clone();

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

        if last_mat.len() == num_windows {
            debug!(
                "SingleThread: all {} windows materialized, emitting",
                num_windows
            );
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
                emit_results(
                    &*last_mat,
                    &static_data_plan,
                    &self.static_db,
                    &self.r2s_operator,
                    max_ts,
                    &consumer,
                );
            }

            match sync_policy {
                SyncPolicy::Wait | SyncPolicy::Timeout { .. } => {
                    last_mat.clear();
                }
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
        for window in &mut self.windows {
            window.add_to_window(event_item.clone(), ts);
        }
    }

    /// Flushes every window, closes the output, and waits for the results
    pub fn stop(&mut self) {
        for window in &mut self.windows {
            window.flush();
            window.stop();
        }
        if matches!(self.operation_mode, OperationMode::SingleThread) {
            self.process_single_thread_window_results();
            return;
        }

        for handle in self.window_threads.drain(..) {
            if handle.join().is_err() {
                error!("A window thread panicked; its results are missing");
            }
        }

        drop(self.window_result_sender.take());
        if let Some(coordinator) = self.coordinator_thread.take() {
            if coordinator.join().is_err() {
                error!("The coordinator thread panicked; cross-window results are missing");
            }
        }
    }

    /// Parses stream text into items, through the configured R2R operator
    pub fn parse_data(&mut self, data: &str) -> Vec<I> {
        self.r2r.lock().unwrap().parse_data(data)
    }

    /// Pre-populate the static background store with N-Triples data
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

    /// Return parsed cross-window SDS+ metadata when cross-window reasoning is enabled
    pub fn get_cross_window_context(&self) -> Option<&WindowContext> {
        self.cross_window_context.as_ref()
    }

    /// Return the stream IRIs registered across all configured windows
    pub fn stream_iris(&self) -> Vec<String> {
        self.window_configs
            .iter()
            .map(|w| w.stream_iri.clone())
            .collect()
    }
}

impl<O> RSPEngine<Triple, O>
where
    O: Clone + Hash + Eq + Send + 'static + From<Vec<(String, String)>>,
{
    /// Add one independently probabilistic stream occurrence
    pub fn add_probabilistic_to_stream(
        &mut self,
        stream_iri: &str,
        triple: Triple,
        probability: f64,
        timestamp: usize,
    ) -> Result<SeedId, HybridError> {
        if matches!(self.operation_mode, OperationMode::SingleThread)
            && (self.cross_window_enabled
                || self.windows.len() > 1
                || self.rsp_query_plan.static_data_plan.is_some())
        {
            self.process_single_thread_window_results();
        }

        let (event, seed_id) = {
            let mut registry = self
                .hybrid_seed_registry
                .lock()
                .map_err(|_| HybridError::PoisonedState)?;
            let event = registry.next_event_key(stream_iri, timestamp);
            let seed_id =
                registry.register_occurrence(event.clone(), triple.clone(), probability)?;
            (event, seed_id)
        };
        let occurrence = ProbabilisticOccurrence {
            item: triple,
            event,
            seed_id,
        };
        let input = normalize_stream_iri(stream_iri);
        for (index, config) in self.window_configs.iter().enumerate() {
            if config.stream_iri.starts_with('?')
                || normalize_stream_iri(&config.stream_iri) == input
            {
                if let Some(window) = self.windows.get_mut(index) {
                    window.add_probabilistic_to_window(occurrence.clone());
                }
            }
        }
        Ok(seed_id)
    }

    /// Probabilities from the most recent probabilistic evaluation, by fact
    pub fn latest_hybrid_results(
        &self,
        window_iri: &str,
    ) -> Option<HashMap<Triple, HybridProbabilityResult>> {
        self.latest_hybrid_results
            .lock()
            .ok()
            .and_then(|results| results.get(window_iri).cloned())
    }
}

/// Join all window results, optionally apply the static-data join, apply the R2S operator, and call
/// `consumer` for each output binding
fn emit_results<O>(
    last_materialized: &HashMap<String, TextBindings>,
    static_data_plan: &Option<PhysicalOperator>,
    static_db: &Arc<Mutex<KolibrieDB>>,
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
        joined.join(&static_bindings)
    } else {
        joined
    };

    debug!(
        "emit_results: emitting {} bindings before R2S filter",
        final_results.len()
    );
    let outputs: Vec<O> = (0..final_results.len())
        .map(|row| {
            let mut kv: Vec<(String, String)> = final_results
                .pairs(row)
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect();
            kv.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            kv.into()
        })
        .collect();
    let filtered = r2s.lock().unwrap().eval(outputs, ts);
    for result in filtered {
        (consumer)(result);
    }
}

/// Join the results of every window that has fired
fn join_window_results(window_buffers: &HashMap<String, TextBindings>) -> TextBindings {
    let mut windows = window_buffers.values();
    let Some(first) = windows.next() else {
        return TextBindings::new();
    };
    let mut joined = first.clone();
    for window in windows {
        joined = joined.join(window);
    }
    joined
}

/// Execute a physical plan against the static-data `KolibrieDB` and return the results as a
/// list of variable-binding maps
fn execute_plan_as_bindings(
    static_db: &Arc<Mutex<KolibrieDB>>,
    plan: &PhysicalOperator,
) -> TextBindings {
    let mut db = static_db.lock().unwrap();
    TextBindings::from_maps(&ExecutionEngine::execute(plan, &mut *db))
}

fn build_cross_window_sds(
    window_configs: &[RSPWindow],
    latest_contents: &HashMap<String, Vec<(Triple, u64)>>,
    static_db: &Arc<Mutex<KolibrieDB>>,
    dict: &SharedTerms,
    output_iris: &[String],
) -> Sds {
    let mut sds = Sds::new();

    {
        let dict_r = dict.view();
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
        let dict_r = dict.view();
        db.query_default_triples(None, None, None)
            .into_iter()
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
    dict: &SharedTerms,
) -> HashMap<String, TextBindings> {
    let mut materialized = HashMap::new();

    for (window, plan) in window_configs.iter().zip(window_plans.iter()) {
        let mut db = KolibrieDB::new();
        db.adopt_terms(dict.clone());

        if let Some(triples) = external_buckets.get(&window.window_iri) {
            for triple in triples {
                db.add_triple(triple.clone());
            }
        }

        let results = ExecutionEngine::execute(plan, &mut db);
        materialized.insert(window.window_iri.clone(), TextBindings::from_maps(&results));
    }

    materialized
}

fn emit_cross_window_results<O>(
    window_configs: &[RSPWindow],
    window_plans: &[PhysicalOperator],
    static_data_plan: &Option<PhysicalOperator>,
    static_db: &Arc<Mutex<KolibrieDB>>,
    r2s: &Arc<Mutex<Relation2StreamOperator<O>>>,
    ts: usize,
    consumer: &Arc<dyn Fn(O) -> () + Send + Sync>,
    rules: &[Rule],
    previous_sds_plus: &Arc<Mutex<SdsWithExpiry>>,
    latest_contents: &Arc<Mutex<HashMap<String, Vec<(Triple, u64)>>>>,
    dict: &SharedTerms,
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
