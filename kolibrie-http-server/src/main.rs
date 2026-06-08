/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use kolibrie::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::parser::process_rule_definition;
use kolibrie::rsp_engine::{RSPBuilder, SimpleR2R, OperationMode, QueryExecutionMode, ResultConsumer};
use datalog::reasoning::Reasoner;
use datalog::parser_n3_logic::parse_n3_rule;
use shared::triple::Triple;
use shared::terms::{Term, TriplePattern};
use shared::dictionary::Dictionary;
use datalogmtl::evaluator::DatalogMTLEvaluator;
use datalogmtl::store::IntervalFactStore;
use datalogmtl::syntax::{DatalogMTLRule, TemporalAtom, Interval};
use datalogmtl::stream::{StreamShape, StalenessPolicy, ShapeIngester, RdfEvent};
use serde::{Deserialize, Serialize};

// ── Session state for persistent RSP engines ────────────────────────────────

struct EngineSession {
    engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>>,
    /// Lazily set when the SSE client connects.
    sse_sender: Arc<Mutex<Option<Sender<String>>>>,
}

type Sessions = Arc<Mutex<HashMap<String, EngineSession>>>;

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

// ── DatalogMTL^RDF session state ─────────────────────────────────────────────

struct DmtlSession {
    evaluator: DatalogMTLEvaluator<IntervalFactStore>,
    shape_ingester: Option<ShapeIngester>,
    sse_sender: Arc<Mutex<Option<Sender<String>>>>,
}

type DmtlSessions = Arc<Mutex<HashMap<String, DmtlSession>>>;

// ── Request/response types ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct QueryRequest {
    #[serde(default)]
    sparql: Option<String>,
    #[serde(default)]
    queries: Option<Vec<String>>,
    #[serde(default)]
    rdf: Option<String>,
    // N3 logic rules in { pattern } => { conclusion } syntax with @prefix declarations.
    // Parsed by parse_n3_rule (parser_n3_logic.rs) via the Reasoner — completely separate
    // from the SPARQL RULE syntax handled by process_rule_definition (parser.rs).
    #[serde(default)]
    n3logic: Option<String>,
    #[serde(default)]
    rule: Option<String>,
    #[serde(default)]
    rules: Option<Vec<String>>,
    #[serde(default = "default_format")]
    format: String,
}

// Default format is RDF/XML for backwards compatibility
fn default_format() -> String {
    "rdfxml".to_string()
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    results: Vec<QueryResult>,
}

#[derive(Debug, Serialize)]
struct QueryResult {
    query_index: usize,
    query: String,
    data: Vec<Vec<String>>,
    execution_time_ms: f64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

// ── RSP-QL stateless endpoint (legacy) ──────────────────────────────────────

#[derive(Debug, Deserialize)]
struct StreamEvent {
    stream: String,
    timestamp: usize,
    ntriples: String,
}

#[derive(Debug, Deserialize)]
struct RspQueryRequest {
    query: String,
    #[serde(default)]
    events: Vec<StreamEvent>,
    #[serde(default)]
    static_rdf: Option<String>,
    #[serde(default = "default_format")]
    static_format: String,
}

#[derive(Debug, Serialize)]
struct RspQueryResponse {
    data: Vec<Vec<String>>,
    total_results: usize,
    execution_time_ms: f64,
}

// ── RSP-QL persistent session endpoints ─────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RspRegisterRequest {
    query: String,
    #[serde(default)]
    static_rdf: Option<String>,
    #[serde(default = "default_format")]
    static_format: String,
    #[serde(default)]
    n3logic: Option<String>,
    #[serde(default)]
    sparql_rules: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct RspRegisterResponse {
    session_id: String,
    streams: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RspPushRequest {
    session_id: String,
    stream: String,
    timestamp: usize,
    ntriples: String,
}

// ── DatalogMTL^RDF request/response types ────────────────────────────────────

#[derive(Debug, Deserialize)]
struct DmtlRegisterRequest {
    rules: String,
    #[serde(default)]
    background_ntriples: Option<String>,
    #[serde(default)]
    stream_shapes: Option<String>,
    #[serde(default)]
    horizon_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct DmtlRegisterResponse {
    session_id: String,
    stream_iris: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DmtlPushRequest {
    session_id: String,
    timestamp: u64,
    ntriples: String,
    #[serde(default)]
    stream_iri: Option<String>,
}

#[derive(Debug, Serialize)]
struct DmtlTickResult {
    timestamp: u64,
    derived: Vec<[String; 3]>,
    metrics: serde_json::Value,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Format a decoded dictionary term as an N-Triples token:
/// - plain URIs → `<uri>`
/// - literals (start with `"`) and blank nodes (`_:`) → as-is
fn term_to_ntriples_token(term: &str) -> String {
    if term.starts_with('"') || term.starts_with("_:") {
        term.to_string()
    } else {
        format!("<{}>", term)
    }
}

/// Export every triple in `db` as an N-Triples string.
fn db_to_ntriples(db: &SparqlDatabase) -> String {
    let dict = db.dictionary.read().unwrap();
    let mut out = String::new();
    for triple in &db.triples {
        if let (Some(s), Some(p), Some(o)) = (
            dict.decode(triple.subject),
            dict.decode(triple.predicate),
            dict.decode(triple.object),
        ) {
            out.push_str(&format!(
                "{} {} {} .\n",
                term_to_ntriples_token(s),
                term_to_ntriples_token(p),
                term_to_ntriples_token(o)
            ));
        }
    }
    out
}

/// Convert collected RSP result rows into a table (first row = headers).
fn results_to_table(results: &[Vec<(String, String)>]) -> Vec<Vec<String>> {
    if results.is_empty() {
        return vec![];
    }
    // Collect all variable names while preserving first-seen order.
    let mut headers: Vec<String> = Vec::new();
    for row in results {
        for (key, _) in row {
            if !headers.contains(key) {
                headers.push(key.clone());
            }
        }
    }
    let mut table: Vec<Vec<String>> = vec![headers.clone()];
    for row in results {
        let map: HashMap<String, String> = row.iter().cloned().collect();
        let values: Vec<String> = headers
            .iter()
            .map(|h| map.get(h).cloned().unwrap_or_default())
            .collect();
        table.push(values);
    }
    table
}

// ── Main & connection handling ───────────────────────────────────────────────

fn main() {
    println!("Starting Kolibrie HTTP Server on 0.0.0.0:8080");

    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let dmtl_sessions: DmtlSessions = Arc::new(Mutex::new(HashMap::new()));

    let listener = TcpListener::bind("0.0.0.0:8080")
        .expect("Failed to bind to port 8080");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let sessions = Arc::clone(&sessions);
                let dmtl_sessions = Arc::clone(&dmtl_sessions);
                thread::spawn(move || {
                    handle_client(stream, sessions, dmtl_sessions);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, sessions: Sessions, dmtl_sessions: DmtlSessions) {
    let mut buffer = vec![0u8; 1_048_576]; // 1MB buffer for large RDF data

    match stream.read(&mut buffer) {
        Ok(size) => {
            let request = String::from_utf8_lossy(&buffer[..size]);

            // Parse method and path from the first line early, so we can
            // detect the SSE routes before going through handle_request.
            let first_line = request.lines().next().unwrap_or("");
            let parts: Vec<&str> = first_line.split_whitespace().collect();

            if parts.len() >= 2 {
                let method = parts[0];
                let path = parts[1];

                // SSE handler — must keep the connection open, so it is
                // handled here rather than returning a String from handle_request.
                if method == "GET" && path.starts_with("/rsp/events/") {
                    let session_id = path["/rsp/events/".len()..].to_string();
                    rsp_events_sse(&session_id, stream, &sessions);
                    return;
                }

                if method == "GET" && path.starts_with("/datalogmtl/events/") {
                    let id = path["/datalogmtl/events/".len()..].to_string();
                    handle_dmtl_sse(&id, stream, &dmtl_sessions);
                    return;
                }
            }

            let response = handle_request(&request, &sessions, &dmtl_sessions);
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
        Err(e) => {
            eprintln!("Failed to read from connection: {}", e);
        }
    }
}

fn handle_request(request: &str, sessions: &Sessions, dmtl_sessions: &DmtlSessions) -> String {
    let lines: Vec<&str> = request.lines().collect();

    if lines.is_empty() {
        return error_response(400, "Bad Request");
    }

    let request_line = lines[0];
    let parts: Vec<&str> = request_line.split_whitespace().collect();

    if parts.len() < 2 {
        return error_response(400, "Bad Request");
    }

    let method = parts[0];
    let path = parts[1];

    if method == "GET" && path == "/" {
        return serve_playground();
    }

    if method == "POST" && path == "/query" {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let body = &request[body_start + 4..];
            return execute_sparql_with_context(body);
        }
    }

    if method == "POST" && path == "/rsp-query" {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let body = &request[body_start + 4..];
            return execute_rsp_query(body);
        }
    }

    if method == "POST" && path == "/rsp/register" {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let body = &request[body_start + 4..];
            return rsp_register(body, sessions);
        }
    }

    if method == "POST" && path == "/rsp/push" {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let body = &request[body_start + 4..];
            return rsp_push(body, sessions);
        }
    }

    if method == "POST" && path == "/datalogmtl/register" {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let body = &request[body_start + 4..];
            return handle_dmtl_register(body, dmtl_sessions);
        }
    }

    if method == "POST" && path == "/datalogmtl/push" {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let body = &request[body_start + 4..];
            return handle_dmtl_push(body, dmtl_sessions);
        }
    }

    if method == "OPTIONS" {
        return cors_response();
    }

    error_response(404, "Not Found")
}

// ── RSP persistent session handlers ─────────────────────────────────────────

fn rsp_register(body: &str, sessions: &Sessions) -> String {
    let req: RspRegisterRequest = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("RSP register JSON error: {}", e);
            return json_error_response(&format!("Invalid JSON: {}", e));
        }
    };

    println!("RSP register: building engine for new session");

    // The SSE sender is lazily populated when the browser opens the SSE connection.
    let sse_sender: Arc<Mutex<Option<Sender<String>>>> = Arc::new(Mutex::new(None));
    let sse_sender_for_consumer = Arc::clone(&sse_sender);

    // Consumer: serialize each result row as JSON and forward to the SSE channel.
    let result_consumer = ResultConsumer::<Vec<(String, String)>> {
        function: Arc::new(move |row: Vec<(String, String)>| {
            let map: serde_json::Map<String, serde_json::Value> = row
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            let json = serde_json::to_string(&map).unwrap_or_default();
            if let Some(tx) = sse_sender_for_consumer.lock().unwrap().as_ref() {
                let _ = tx.send(json);
            }
        }),
    };

    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let n3logic = req.n3logic.as_deref().unwrap_or("");
    let sparql_rules = req.sparql_rules.clone().unwrap_or_default();

    let mut engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>> =
        match RSPBuilder::new()
            .add_rsp_ql_query(&req.query)
            .set_operation_mode(OperationMode::SingleThread)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .add_rules(n3logic)
            .add_sparql_rules(sparql_rules)
            .build()
        {
            Ok(e) => e,
            Err(e) => {
                eprintln!("RSP build error: {}", e);
                return json_error_response(&format!("Failed to build RSP engine: {}", e));
            }
        };

    // Load static background data if provided.
    if let Some(static_rdf) = &req.static_rdf {
        if !static_rdf.trim().is_empty() {
            let ntriples = match req.static_format.as_str() {
                "ntriples" => static_rdf.clone(),
                _ => {
                    let mut static_db = SparqlDatabase::new();
                    match req.static_format.as_str() {
                        "turtle" => static_db.parse_turtle(static_rdf),
                        _ => static_db.parse_rdf(static_rdf),
                    }
                    db_to_ntriples(&static_db)
                }
            };
            if !ntriples.is_empty() {
                engine.add_static_ntriples(&ntriples);
                println!("RSP register: loaded static data ({} bytes)", ntriples.len());
            }
        }
    }

    let streams = engine.stream_iris();
    let session_id = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed).to_string();

    sessions.lock().unwrap().insert(
        session_id.clone(),
        EngineSession { engine, sse_sender },
    );

    println!("RSP register: session {} created, streams: {:?}", session_id, streams);

    let response = RspRegisterResponse { session_id, streams };
    let json = serde_json::to_string(&response).unwrap_or_default();
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         \r\n\
         {}",
        json.len(),
        json
    )
}

fn rsp_push(body: &str, sessions: &Sessions) -> String {
    let req: RspPushRequest = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("RSP push JSON error: {}", e);
            return json_error_response(&format!("Invalid JSON: {}", e));
        }
    };

    if req.ntriples.trim().is_empty() {
        return json_ok();
    }

    let mut sessions_lock = sessions.lock().unwrap();
    let session = match sessions_lock.get_mut(&req.session_id) {
        Some(s) => s,
        None => {
            eprintln!("RSP push: session {} not found", req.session_id);
            return json_error_response("Session not found");
        }
    };

    let triples = session.engine.parse_data(&req.ntriples);
    println!(
        "RSP push: {} triple(s) to stream '{}' at t={} (session {})",
        triples.len(),
        req.stream,
        req.timestamp,
        req.session_id
    );

    for triple in triples {
        session.engine.add_to_stream(&req.stream, triple, req.timestamp);
    }

    // Flush any pending channel results (multi-window / static-data join case).
    // For single-window queries, the consumer is called directly from add_to_window
    // already, so this is a no-op in the simple case.
    session.engine.process_single_thread_window_results();

    // Signal end-of-firing to the SSE client so it can flush its display buffer
    // immediately rather than waiting for the debounce timeout.
    if let Some(tx) = session.sse_sender.lock().unwrap().as_ref() {
        let _ = tx.send("__FIRING_END__".to_string());
    }

    json_ok()
}

/// SSE handler — writes the event-stream headers and then blocks, forwarding
/// results to the browser as they arrive via an in-process channel.
fn rsp_events_sse(session_id: &str, mut stream: TcpStream, sessions: &Sessions) {
    // Clone the Arc so we can release the sessions lock before blocking.
    let sse_sender_arc = {
        let lock = sessions.lock().unwrap();
        match lock.get(session_id) {
            Some(s) => Arc::clone(&s.sse_sender),
            None => {
                let resp = error_response(404, "Session not found");
                let _ = stream.write_all(resp.as_bytes());
                return;
            }
        }
    };

    let (tx, rx) = mpsc::channel::<String>();
    sse_sender_arc.lock().unwrap().replace(tx);

    // Write SSE headers — no Content-Length, connection stays open.
    if stream
        .write_all(
            b"HTTP/1.1 200 OK\r\n\
              Content-Type: text/event-stream\r\n\
              Cache-Control: no-cache\r\n\
              Access-Control-Allow-Origin: *\r\n\
              \r\n",
        )
        .is_err()
    {
        return;
    }
    stream.flush().ok();

    println!("RSP SSE: client connected for session {}", session_id);

    // Block-forward events until the client disconnects or the tx is dropped.
    for received in rx {
        let msg = if received == "__FIRING_END__" {
            // Named event so the browser can flush its firing buffer immediately.
            "event: firing\ndata: {}\n\n".to_string()
        } else {
            format!("data: {}\n\n", received)
        };
        if stream.write_all(msg.as_bytes()).is_err() {
            break;
        }
        stream.flush().ok();
    }

    println!("RSP SSE: client disconnected for session {}", session_id);
}

// ── Existing SPARQL and legacy RSP-QL handlers ───────────────────────────────

fn serve_playground() -> String {
    let html = include_str!("../../web/playground.html");
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/html; charset=UTF-8\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         \r\n\
         {}",
        html.len(),
        html
    )
}

fn execute_sparql_with_context(body: &str) -> String {
    let request: QueryRequest = match serde_json::from_str(body) {
        Ok(req) => req,
        Err(e) => {
            eprintln!("JSON parse error: {}", e);
            return json_error_response(&format!("Invalid JSON: {}", e));
        }
    };

    // Collect all queries
    let mut queries = Vec::new();
    if let Some(single_query) = request.sparql {
        queries.push(single_query);
    }
    if let Some(multi_queries) = request.queries {
        queries.extend(multi_queries);
    }

    if queries.is_empty() {
        return json_error_response("No queries provided");
    }

    // Collect all rules
    let mut rules = Vec::new();
    if let Some(single_rule) = request.rule {
        rules.push(single_rule);
    }
    if let Some(multi_rules) = request.rules {
        rules.extend(multi_rules);
    }

    println!("Processing {} query(ies) and {} rule(s)", queries.len(), rules.len());

    let mut database = SparqlDatabase::new();
    let use_optimizer = request.format == "ntriples";

    // Load RDF data once
    if let Some(rdf_data) = request.rdf {
        if !rdf_data.trim().is_empty() {
            match request.format.as_str() {
                "ntriples" => {
                    println!("Parsing N-Triples data with Streamertail optimizer...");
                    database.parse_ntriples_and_add(&rdf_data);
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
                "turtle" => {
                    println!("Parsing Turtle dataset...");
                    database.parse_turtle(&rdf_data);
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
                "rdfxml" | _ => {
                    println!("Parsing RDF/XML data...");
                    database.parse_rdf(&rdf_data);
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
            }
        }
    }

    // Process N3 logic rules (n3logic field) using parse_n3_rule + Reasoner.
    // Syntax: @prefix declarations followed by { premise } => { conclusion } .
    // This is completely separate from the SPARQL RULE syntax — it uses the
    // Reasoner class (datalog/reasoning.rs) exactly as shown in the test example.
    if let Some(ref n3_rules_text) = request.n3logic {
    if !n3_rules_text.trim().is_empty() {
        println!("Processing N3 logic rules from N3 Logic sub-tab...");

        // Mirror the database dictionary so term encodings are shared
        let mut kg = Reasoner::new();
        kg.dictionary = database.dictionary.clone();

        // Decode all triples to owned strings using database.dictionary BEFORE
        // creating kg's mutable borrow — avoids the RwLockReadGuard conflict.
        let decoded_triples: Vec<(String, String, String)> = {
            let dict_guard = kg.dictionary.read().unwrap();
            database.triples.iter()
                .map(|triple| (
                    dict_guard.decode(triple.subject).unwrap_or("").to_string(),
                    dict_guard.decode(triple.predicate).unwrap_or("").to_string(),
                    dict_guard.decode(triple.object).unwrap_or("").to_string(),
                ))
                .collect()
        };

        println!("Decoded triples are: {:?}", decoded_triples);

        // Now load into the Reasoner — no borrow conflict
        for (s, p, o) in &decoded_triples {
            kg.add_abox_triple(s, p, o);
        }

        let n3_text = n3_rules_text.trim();
        match parse_n3_rule(n3_text, &mut kg) {
            Ok((_, (prefixes, rule))) => {
                println!(
                    "N3 rule parsed ({} prefix(es), {} premise(s), {} conclusion(s))",
                    prefixes.len(),
                    rule.premise.len(),
                    rule.conclusion.len(),
                );

                // **IMPORTANT**: Register N3 prefixes with the database so SPARQL rules can use them
                for (prefix, uri) in prefixes {
                    database.prefixes.insert(prefix.to_string(), uri.to_string());
                }

                // Register the rule and infer new facts
                kg.add_rule(rule);
                let inferred = kg.infer_new_facts_semi_naive();
                println!("N3 rule inferred {} fact(s)", inferred.len());

                // Push inferred triples into the database triple store
                for triple in inferred {
                    database.triples.insert(triple);
                }

                // Sync the enriched dictionary back so SPARQL can decode the new terms
                database.dictionary = kg.dictionary.clone();

                if !database.triples.is_empty() {
                    database.invalidate_stats_cache();
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
            }
            Err(e) => {
                eprintln!("N3 rule parse error: {:?}", e);
            }
        }
    }
}

    // Process all SPARQL-syntax rules (RULE :Name :- CONSTRUCT { } WHERE { })
    for (idx, rule_def) in rules.iter().enumerate() {
        if !rule_def.trim().is_empty() {
            println!("Processing rule {}...", idx + 1);
            match process_rule_definition(&rule_def, &mut database) {
                Ok((_, inferred_facts)) => {
                    println!("Rule {} processed, inferred {} facts", idx + 1, inferred_facts.len());
                    if !inferred_facts.is_empty() {
                        database.invalidate_stats_cache();
                        database.get_or_build_stats();
                        database.build_all_indexes();
                    }
                }
                Err(e) => {
                    eprintln!("Rule {} processing error: {:?}", idx + 1, e);
                }
            }
        }
    }

    // Execute all queries
    let mut all_results = Vec::new();

    for (idx, query) in queries.iter().enumerate() {
        println!("Executing query {}/{}...", idx + 1, queries.len());
        let start_time = std::time::Instant::now();

        let results = if use_optimizer {
            execute_query_rayon_parallel2_volcano(query, &mut database)
        } else {
            execute_query(query, &mut database)
        };

        let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;

        all_results.push(QueryResult {
            query_index: idx,
            query: query.clone(),
            data: results,
            execution_time_ms: execution_time,
        });
    }

    let response = QueryResponse { results: all_results };
    let json = match serde_json::to_string(&response) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("Failed to serialize response: {}", e);
            return json_error_response("Failed to serialize results");
        }
    };

    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         \r\n\
         {}",
        json.len(),
        json
    )
}

fn execute_rsp_query(body: &str) -> String {
    let request: RspQueryRequest = match serde_json::from_str(body) {
        Ok(req) => req,
        Err(e) => {
            eprintln!("RSP JSON parse error: {}", e);
            return json_error_response(&format!("Invalid JSON: {}", e));
        }
    };

    println!(
        "RSP: processing query with {} event(s), static_format={}",
        request.events.len(),
        request.static_format
    );

    // Collect results via a shared container that the engine writes into.
    let result_container: Arc<Mutex<Vec<Vec<(String, String)>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let rc_clone = Arc::clone(&result_container);

    let result_consumer = ResultConsumer::<Vec<(String, String)>> {
        function: Arc::new(move |row| {
            rc_clone.lock().unwrap().push(row);
        }),
    };

    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let start_time = std::time::Instant::now();

    let mut engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>> =
        match RSPBuilder::new()
            .add_rsp_ql_query(&request.query)
            .set_operation_mode(OperationMode::SingleThread)
            .add_consumer(result_consumer)
            .add_r2r(r2r)
            .build()
        {
            Ok(e) => e,
            Err(e) => {
                eprintln!("RSP build error: {}", e);
                return json_error_response(&format!("Failed to build RSP engine: {}", e));
            }
        };

    // Load static background data from the SPARQL tab (if provided).
    if let Some(static_rdf) = &request.static_rdf {
        if !static_rdf.trim().is_empty() {
            let ntriples = match request.static_format.as_str() {
                "ntriples" => static_rdf.clone(),
                _ => {
                    let mut static_db = SparqlDatabase::new();
                    match request.static_format.as_str() {
                        "turtle" => static_db.parse_turtle(static_rdf),
                        _ => static_db.parse_rdf(static_rdf),
                    }
                    db_to_ntriples(&static_db)
                }
            };
            if !ntriples.is_empty() {
                engine.add_static_ntriples(&ntriples);
                println!("RSP: loaded static data ({} bytes as N-Triples)", ntriples.len());
            }
        }
    }

    // Sort events by timestamp, then push them to the engine.
    let mut events = request.events;
    events.sort_by_key(|e| e.timestamp);

    for event in &events {
        if event.ntriples.trim().is_empty() {
            continue;
        }
        let triples = engine.parse_data(&event.ntriples);
        println!(
            "RSP: pushing {} triple(s) to stream '{}' at t={}",
            triples.len(),
            event.stream,
            event.timestamp
        );
        for triple in triples {
            engine.add_to_stream(&event.stream, triple, event.timestamp);
        }
    }

    // Flush all pending window results.
    engine.stop();

    let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;

    let results = result_container.lock().unwrap();
    let data = results_to_table(&results);
    let total_results = if data.len() > 1 { data.len() - 1 } else { 0 };

    println!(
        "RSP: done — {} result row(s) in {:.2}ms",
        total_results, execution_time
    );

    let response = RspQueryResponse {
        data,
        total_results,
        execution_time_ms: execution_time,
    };
    let json = match serde_json::to_string(&response) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("RSP serialization error: {}", e);
            return json_error_response("Failed to serialize RSP results");
        }
    };

    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         \r\n\
         {}",
        json.len(),
        json
    )
}

// ── DatalogMTL^RDF handlers ───────────────────────────────────────────────────

fn handle_dmtl_register(body: &str, dmtl_sessions: &DmtlSessions) -> String {
    let req: DmtlRegisterRequest = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(e) => return json_error_response(&format!("Invalid JSON: {}", e)),
    };

    let mut dict = Dictionary::new();
    let rules = match parse_dmtl_rules(&req.rules, &mut dict) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("DMTL rule parse error: {}", e);
            return json_error_response(&format!("Rule parse error: {}", e));
        }
    };

    println!("DMTL register: {} rule(s) parsed", rules.len());

    // Parse stream shapes into the same dict before Arc-wrapping
    let (shapes, stream_iris) = if let Some(ref shape_text) = req.stream_shapes {
        if !shape_text.trim().is_empty() {
            match parse_stream_shapes(shape_text, &mut dict) {
                Ok(s) => {
                    let iris: Vec<String> = s.iter().map(|sh| sh.stream_iri.clone()).collect();
                    (s, iris)
                }
                Err(e) => return json_error_response(&format!("Shape parse error: {}", e)),
            }
        } else {
            (vec![], vec![])
        }
    } else {
        (vec![], vec![])
    };

    let horizon_ms = req.horizon_ms.unwrap_or(60_000);
    let dict_arc: Arc<RwLock<Dictionary>> = Arc::new(RwLock::new(dict));

    let shape_ingester = if shapes.is_empty() {
        None
    } else {
        Some(ShapeIngester::new(shapes, Arc::clone(&dict_arc)))
    };

    let store = IntervalFactStore::new(horizon_ms);

    let mut eval = match DatalogMTLEvaluator::new(rules, store, Arc::clone(&dict_arc)) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("DMTL evaluator error: {}", e);
            return json_error_response(&format!("Evaluator error: {}", e));
        }
    };

    if let Some(bg) = &req.background_ntriples {
        if !bg.trim().is_empty() {
            let triples = tokenize_and_encode_ntriples(bg, &dict_arc);
            println!("DMTL register: {} background triple(s) at t=0", triples.len());
            eval.advance(0, triples);
        }
    }

    let session_id = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed).to_string();
    dmtl_sessions.lock().unwrap().insert(
        session_id.clone(),
        DmtlSession {
            evaluator: eval,
            shape_ingester,
            sse_sender: Arc::new(Mutex::new(None)),
        },
    );
    println!("DMTL register: session {} created, stream_iris: {:?}", session_id, stream_iris);

    let json = serde_json::to_string(&DmtlRegisterResponse { session_id, stream_iris }).unwrap_or_default();
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         \r\n\
         {}",
        json.len(),
        json
    )
}

fn handle_dmtl_push(body: &str, dmtl_sessions: &DmtlSessions) -> String {
    let req: DmtlPushRequest = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(e) => return json_error_response(&format!("Invalid JSON: {}", e)),
    };

    let mut sessions_lock = dmtl_sessions.lock().unwrap();
    let session = match sessions_lock.get_mut(&req.session_id) {
        Some(s) => s,
        None => return json_error_response("Session not found"),
    };

    let dict_arc = Arc::clone(&session.evaluator.dictionary);
    let triples = match (&mut session.shape_ingester, &req.stream_iri) {
        (Some(ingester), Some(iri)) => {
            let raw = tokenize_and_encode_ntriples(&req.ntriples, &dict_arc);
            let event = RdfEvent { stream_iri: iri.clone(), timestamp: req.timestamp, triples: raw };
            ingester.evict_expired(req.timestamp);
            ingester.process_event(&event).into_iter().map(|(t, _)| t).collect()
        }
        _ => tokenize_and_encode_ntriples(&req.ntriples, &dict_arc),
    };
    println!(
        "DMTL push: {} triple(s) at t={} (session {})",
        triples.len(), req.timestamp, req.session_id
    );

    let (derived, metrics) = session.evaluator.advance(req.timestamp, triples);

    let decoded: Vec<[String; 3]> = {
        let dict = session.evaluator.dictionary.read().unwrap();
        derived.iter().map(|t| [
            decode_dmtl_term(dict.decode(t.subject)),
            decode_dmtl_term(dict.decode(t.predicate)),
            decode_dmtl_term(dict.decode(t.object)),
        ]).collect()
    };

    let tick_result = DmtlTickResult {
        timestamp: req.timestamp,
        derived: decoded,
        metrics: serde_json::json!({
            "fixpoint_iters": metrics.fixpoint_iterations,
            "rules_fired": metrics.rules_fired,
            "new_triples": metrics.new_triples,
            "eval_time_us": metrics.eval_time_us,
            "diamond_evals": metrics.diamond_evals,
            "box_evals": metrics.box_evals,
            "since_evals": metrics.since_evals,
            "since_scan_depth": metrics.since_scan_depth,
            "snapshot_count": metrics.snapshot_count,
            "total_triples_in_store": metrics.total_triples_in_store,
        }),
    };

    let tick_json = serde_json::to_string(&tick_result).unwrap_or_default();
    if let Some(tx) = session.sse_sender.lock().unwrap().as_ref() {
        let _ = tx.send(tick_json);
        let _ = tx.send("__TICK_END__".to_string());
    }

    json_ok()
}

fn handle_dmtl_sse(session_id: &str, mut stream: TcpStream, dmtl_sessions: &DmtlSessions) {
    let sse_sender_arc = {
        let lock = dmtl_sessions.lock().unwrap();
        match lock.get(session_id) {
            Some(s) => Arc::clone(&s.sse_sender),
            None => {
                let resp = error_response(404, "Session not found");
                let _ = stream.write_all(resp.as_bytes());
                return;
            }
        }
    };

    let (tx, rx) = mpsc::channel::<String>();
    sse_sender_arc.lock().unwrap().replace(tx);

    if stream
        .write_all(
            b"HTTP/1.1 200 OK\r\n\
              Content-Type: text/event-stream\r\n\
              Cache-Control: no-cache\r\n\
              Access-Control-Allow-Origin: *\r\n\
              \r\n",
        )
        .is_err()
    {
        return;
    }
    stream.flush().ok();
    println!("DMTL SSE: client connected for session {}", session_id);

    for received in rx {
        let msg = if received == "__TICK_END__" {
            "event: tick\ndata: {}\n\n".to_string()
        } else {
            format!("data: {}\n\n", received)
        };
        if stream.write_all(msg.as_bytes()).is_err() {
            break;
        }
        stream.flush().ok();
    }
    println!("DMTL SSE: client disconnected for session {}", session_id);
}

// ── DatalogMTL N-Triples helpers ─────────────────────────────────────────────

/// Decode a dictionary term to an N-Triples token.
/// Literals (start with `"`) and blank nodes (`_:`) are returned as-is;
/// everything else is wrapped in angle brackets.
fn decode_dmtl_term(val: Option<&str>) -> String {
    match val {
        None => "?".to_string(),
        Some(s) if s.starts_with('"') || s.starts_with("_:") => s.to_string(),
        Some(s) => format!("<{}>", s),
    }
}

/// Tokenize N-Triples text and encode each term into the shared dictionary.
fn tokenize_and_encode_ntriples(
    ntriples: &str,
    dict_arc: &Arc<RwLock<Dictionary>>,
) -> Vec<Triple> {
    let mut triples = Vec::new();
    for line in ntriples.lines() {
        if let Some([s, p, o]) = tokenize_ntriples_line(line) {
            let mut dict = dict_arc.write().unwrap();
            let s_id = dict.encode(&s);
            let p_id = dict.encode(&p);
            let o_id = dict.encode(&o);
            triples.push(Triple { subject: s_id, predicate: p_id, object: o_id });
        }
    }
    triples
}

/// Tokenise one N-Triples line. Returns the three raw term strings (IRI
/// brackets stripped; literals kept with quotes; blank nodes kept as-is).
/// Returns `None` for comment lines, blank lines, and malformed lines.
fn tokenize_ntriples_line(line: &str) -> Option<[String; 3]> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let bytes = line.as_bytes();
    let mut i = 0;
    let mut tokens: Vec<String> = Vec::new();

    while tokens.len() < 3 && i < bytes.len() {
        // Skip whitespace
        while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        if i >= bytes.len() { break; }

        match bytes[i] {
            b'<' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != b'>' { i += 1; }
                tokens.push(String::from_utf8_lossy(&bytes[start..i]).into_owned());
                if i < bytes.len() { i += 1; } // skip '>'
            }
            b'"' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' { i += 2; continue; }
                    if bytes[i] == b'"' { i += 1; break; }
                    i += 1;
                }
                // Consume optional @lang tag or ^^<datatype>
                if i < bytes.len() && bytes[i] == b'@' {
                    while i < bytes.len() && bytes[i] != b' ' && bytes[i] != b'\t' { i += 1; }
                } else if i + 1 < bytes.len() && bytes[i] == b'^' && bytes[i + 1] == b'^' {
                    i += 2;
                    if i < bytes.len() && bytes[i] == b'<' {
                        i += 1;
                        while i < bytes.len() && bytes[i] != b'>' { i += 1; }
                        if i < bytes.len() { i += 1; }
                    }
                }
                tokens.push(String::from_utf8_lossy(&bytes[start..i]).into_owned());
            }
            b'_' if i + 1 < bytes.len() && bytes[i + 1] == b':' => {
                let start = i;
                while i < bytes.len()
                    && bytes[i] != b' '
                    && bytes[i] != b'\t'
                    && bytes[i] != b'.'
                {
                    i += 1;
                }
                tokens.push(String::from_utf8_lossy(&bytes[start..i]).into_owned());
            }
            b'.' => break, // end-of-triple marker
            _ => { i += 1; }
        }
    }

    if tokens.len() >= 3 {
        Some([tokens[0].clone(), tokens[1].clone(), tokens[2].clone()])
    } else {
        None
    }
}

// ── DatalogMTL rule text parser ───────────────────────────────────────────────
//
// Rule format:
//   (?x, :wasNear, ?y) :- (?x, :loc, ?l), Diamond[1000,5000](?y, :loc, ?l).
//
// Terms: ?var = Variable, <iri> = Constant (brackets stripped),
//        :name = Constant (stored as ":name"), "lit" = Constant (stored with quotes).

fn parse_dmtl_rules(text: &str, dict: &mut Dictionary) -> Result<Vec<DatalogMTLRule>, String> {
    // Strip line comments and join into one string for depth-0 splitting.
    let cleaned: String = text
        .lines()
        .map(|line| {
            if let Some(pos) = line.find('#') { &line[..pos] } else { line }
        })
        .collect::<Vec<_>>()
        .join(" ");

    let rule_strs = split_at_depth0(&cleaned, '.');
    let mut rules = Vec::new();

    for (i, rule_str) in rule_strs.iter().enumerate() {
        let rule_str = rule_str.trim();
        if rule_str.is_empty() { continue; }

        let sep_pos = find_rule_sep(rule_str)
            .ok_or_else(|| format!("Rule {}: no ':-' found in: '{}'", i, rule_str))?;

        let head_str = rule_str[..sep_pos].trim();
        let body_str = rule_str[sep_pos + 2..].trim();

        let head = parse_dmtl_triple_pattern(head_str, dict)?;
        let body_parts = split_at_depth0(body_str, ',');
        let mut body = Vec::new();
        for part in &body_parts {
            let part = part.trim();
            if !part.is_empty() {
                body.push(parse_dmtl_temporal_atom(part, dict)?);
            }
        }

        rules.push(DatalogMTLRule { id: i.to_string(), head, body });
    }

    Ok(rules)
}

/// Find the byte offset of `:-` at paren/bracket depth 0.
fn find_rule_sep(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => { if depth > 0 { depth -= 1; } }
            b':' if depth == 0 && i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split `s` at every occurrence of `sep` that is at paren/bracket depth 0.
fn split_at_depth0(s: &str, sep: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth: i32 = 0;
    for ch in s.chars() {
        match ch {
            '(' | '[' => { depth += 1; current.push(ch); }
            ')' | ']' => { if depth > 0 { depth -= 1; } current.push(ch); }
            c if c == sep && depth == 0 => {
                parts.push(current.clone());
                current.clear();
            }
            _ => { current.push(ch); }
        }
    }
    if !current.trim().is_empty() {
        parts.push(current);
    }
    parts
}

fn parse_dmtl_interval(s: &str) -> Result<Interval, String> {
    let parts: Vec<&str> = s.splitn(2, ',').collect();
    if parts.len() != 2 {
        return Err(format!("Expected 'start,end' interval, got: '{}'", s));
    }
    let start = parts[0].trim().parse::<u64>()
        .map_err(|e| format!("Invalid interval start '{}': {}", parts[0].trim(), e))?;
    let end = parts[1].trim().parse::<u64>()
        .map_err(|e| format!("Invalid interval end '{}': {}", parts[1].trim(), e))?;
    Ok(Interval { start, end })
}

fn parse_dmtl_term(s: &str, dict: &mut Dictionary) -> Result<Term, String> {
    let s = s.trim();
    if s.starts_with('?') {
        Ok(Term::Variable(s[1..].to_string()))
    } else if s.starts_with('<') && s.ends_with('>') {
        Ok(Term::Constant(dict.encode(&s[1..s.len() - 1])))
    } else if s.starts_with(':') {
        // ":name" stored as-is
        Ok(Term::Constant(dict.encode(s)))
    } else if s.starts_with('"') {
        // literal kept with quotes
        Ok(Term::Constant(dict.encode(s)))
    } else {
        Err(format!("Cannot parse term: '{}'", s))
    }
}

fn parse_dmtl_triple_pattern(s: &str, dict: &mut Dictionary) -> Result<TriplePattern, String> {
    let s = s.trim();
    let inner = if s.starts_with('(') && s.ends_with(')') {
        &s[1..s.len() - 1]
    } else {
        s
    };
    let parts = split_at_depth0(inner, ',');
    if parts.len() != 3 {
        return Err(format!(
            "Expected 3 terms in triple pattern, got {}: '{}'",
            parts.len(), s
        ));
    }
    Ok((
        parse_dmtl_term(parts[0].trim(), dict)?,
        parse_dmtl_term(parts[1].trim(), dict)?,
        parse_dmtl_term(parts[2].trim(), dict)?,
    ))
}

fn parse_dmtl_temporal_atom(s: &str, dict: &mut Dictionary) -> Result<TemporalAtom, String> {
    let s = s.trim();
    if s.starts_with('(') {
        return Ok(TemporalAtom::Base(parse_dmtl_triple_pattern(s, dict)?));
    }
    if let Some(rest) = s.strip_prefix("Diamond[") {
        return parse_dmtl_interval_atom(rest, dict, "Diamond");
    }
    if let Some(rest) = s.strip_prefix("Box[") {
        return parse_dmtl_interval_atom(rest, dict, "Box");
    }
    if let Some(rest) = s.strip_prefix("Prev[") {
        return parse_dmtl_interval_atom(rest, dict, "Prev");
    }
    if let Some(rest) = s.strip_prefix("Since[") {
        return parse_dmtl_since_atom(rest, dict);
    }
    Err(format!("Cannot parse temporal atom: '{}'", s))
}

fn parse_dmtl_interval_atom(
    rest: &str,
    dict: &mut Dictionary,
    kind: &str,
) -> Result<TemporalAtom, String> {
    let close = rest.find(']')
        .ok_or_else(|| format!("Missing ']' in {} interval", kind))?;
    let interval = parse_dmtl_interval(&rest[..close])?;
    let after = rest[close + 1..].trim();
    let inner = parse_dmtl_temporal_atom(after, dict)?;
    match kind {
        "Diamond" => Ok(TemporalAtom::Diamond { interval, inner: Box::new(inner) }),
        "Box"     => Ok(TemporalAtom::Box_ { interval, inner: Box::new(inner) }),
        "Prev"    => Ok(TemporalAtom::Prev { interval, inner: Box::new(inner) }),
        _         => Err(format!("Unknown operator: {}", kind)),
    }
}

fn parse_dmtl_since_atom(rest: &str, dict: &mut Dictionary) -> Result<TemporalAtom, String> {
    let close = rest.find(']')
        .ok_or_else(|| "Missing ']' in Since interval".to_string())?;
    let interval = parse_dmtl_interval(&rest[..close])?;
    let after = rest[close + 1..].trim();

    // Expect (phi_atom, psi_atom) — strip outer parens then split at depth-0 comma
    if !after.starts_with('(') || !after.ends_with(')') {
        return Err(format!("Since expects (phi, psi) after interval, got: '{}'", after));
    }
    let inner = &after[1..after.len() - 1];
    let parts = split_at_depth0(inner, ',');
    if parts.len() < 2 {
        return Err("Since needs at least 2 atoms in (phi, psi)".to_string());
    }
    let phi = parse_dmtl_temporal_atom(parts[0].trim(), dict)?;
    let psi_str = if parts.len() == 2 {
        parts[1].trim().to_string()
    } else {
        parts[1..].iter().map(|s| s.trim()).collect::<Vec<_>>().join(",")
    };
    let psi = parse_dmtl_temporal_atom(&psi_str, dict)?;
    Ok(TemporalAtom::Since {
        interval,
        phi: Box::new(phi),
        psi: Box::new(psi),
    })
}

// ── Stream shape text parser ──────────────────────────────────────────────────
//
// Syntax:
//   STREAM :myStream
//     PATTERN (?s, :pred, ?o)
//     KEY ?s
//     STALENESS 5000
//   .
//
// Keywords are case-sensitive. Blocks are separated by depth-0 `.`.

fn find_whole_word(text: &str, word: &str) -> Option<usize> {
    let wlen = word.len();
    let tbytes = text.as_bytes();
    let wbytes = word.as_bytes();
    let mut i = 0;
    while i + wlen <= text.len() {
        if &tbytes[i..i + wlen] == wbytes {
            let before_ok = i == 0
                || (!tbytes[i - 1].is_ascii_alphanumeric() && tbytes[i - 1] != b'_');
            let after_pos = i + wlen;
            let after_ok = after_pos >= text.len()
                || (!tbytes[after_pos].is_ascii_alphanumeric() && tbytes[after_pos] != b'_');
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn parse_stream_shapes(text: &str, dict: &mut Dictionary) -> Result<Vec<StreamShape>, String> {
    let cleaned: String = text
        .lines()
        .map(|l| if let Some(p) = l.find('#') { &l[..p] } else { l })
        .collect::<Vec<_>>()
        .join(" ");

    let blocks = split_at_depth0(&cleaned, '.');
    let mut shapes = Vec::new();

    for (bi, block) in blocks.iter().enumerate() {
        let block = block.trim();
        if block.is_empty() { continue; }

        // Find positions of each keyword and sort
        let kws = ["STREAM", "PATTERN", "KEY", "STALENESS"];
        let mut kw_positions: Vec<(&str, usize)> = kws
            .iter()
            .filter_map(|&kw| find_whole_word(block, kw).map(|pos| (kw, pos)))
            .collect();
        kw_positions.sort_by_key(|(_, pos)| *pos);

        // Build keyword → content map (text from end-of-keyword to start-of-next-keyword)
        let mut sections: HashMap<&str, String> = HashMap::new();
        for i in 0..kw_positions.len() {
            let (kw, kw_pos) = kw_positions[i];
            let content_start = kw_pos + kw.len();
            let content_end = if i + 1 < kw_positions.len() {
                kw_positions[i + 1].1
            } else {
                block.len()
            };
            sections.insert(kw, block[content_start..content_end].trim().to_string());
        }

        // STREAM: first whitespace-delimited token is the IRI
        let stream_raw = sections
            .get("STREAM")
            .ok_or_else(|| format!("Shape block {}: missing STREAM keyword", bi))?;
        let stream_iri_raw = stream_raw
            .split_whitespace()
            .next()
            .ok_or_else(|| format!("Shape block {}: empty STREAM value", bi))?;
        let stream_iri = if stream_iri_raw.starts_with('<') && stream_iri_raw.ends_with('>') {
            stream_iri_raw[1..stream_iri_raw.len() - 1].to_string()
        } else {
            stream_iri_raw.to_string()
        };

        // PATTERN: collect all (…,…,…) groups
        let pattern_text = sections.get("PATTERN").cloned().unwrap_or_default();
        if pattern_text.trim().is_empty() {
            return Err(format!("Shape '{}': missing PATTERN section", stream_iri));
        }
        let mut event_pattern = Vec::new();
        let pb = pattern_text.as_bytes();
        let mut pi = 0;
        while pi < pb.len() {
            if pb[pi] == b'(' {
                let start = pi;
                let mut depth = 1usize;
                pi += 1;
                while pi < pb.len() && depth > 0 {
                    match pb[pi] {
                        b'(' => depth += 1,
                        b')' => depth -= 1,
                        _ => {}
                    }
                    pi += 1;
                }
                event_pattern.push(parse_dmtl_triple_pattern(&pattern_text[start..pi], dict)?);
            } else {
                pi += 1;
            }
        }
        if event_pattern.is_empty() {
            return Err(format!("Shape '{}': no triple patterns found in PATTERN", stream_iri));
        }

        // KEY: collect ?var tokens
        let key_text = sections.get("KEY").cloned().unwrap_or_default();
        let channel_key: Vec<String> = key_text
            .split_whitespace()
            .filter(|s| s.starts_with('?'))
            .map(|s| s[1..].to_string())
            .collect();

        // STALENESS: first token parsed as u64
        let staleness_text = sections.get("STALENESS").cloned().unwrap_or_default();
        let max_gap_ms = staleness_text
            .split_whitespace()
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        shapes.push(StreamShape {
            stream_iri,
            event_pattern,
            channel_key,
            staleness: StalenessPolicy { max_gap_ms },
        });
    }

    Ok(shapes)
}

// ── Response helpers ─────────────────────────────────────────────────────────

fn json_ok() -> String {
    let json = r#"{"ok":true}"#;
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         \r\n\
         {}",
        json.len(),
        json
    )
}

fn json_error_response(message: &str) -> String {
    let error = ErrorResponse {
        error: message.to_string(),
    };
    let json = serde_json::to_string(&error).unwrap_or_else(|_| {
        r#"{"error":"Internal server error"}"#.to_string()
    });

    format!(
        "HTTP/1.1 400 Bad Request\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         \r\n\
         {}",
        json.len(),
        json
    )
}

fn cors_response() -> String {
    "HTTP/1.1 204 No Content\r\n\
     Access-Control-Allow-Origin: *\r\n\
     Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n\
     Access-Control-Allow-Headers: Content-Type\r\n\
     \r\n".to_string()
}

fn error_response(code: u16, message: &str) -> String {
    format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {}",
        code,
        message,
        message.len(),
        message
    )
}
