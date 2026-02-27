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
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use kolibrie::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::parser::process_rule_definition;
use kolibrie::rsp_engine::{RSPBuilder, SimpleR2R, OperationMode, QueryExecutionMode, ResultConsumer};
use datalog::reasoning::Reasoner;
use datalog::parser_n3_logic::parse_n3_rule;
use shared::triple::Triple;
use serde::{Deserialize, Serialize};

// ── Session state for persistent RSP engines ────────────────────────────────

struct EngineSession {
    engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>>,
    /// Lazily set when the SSE client connects.
    sse_sender: Arc<Mutex<Option<Sender<String>>>>,
}

type Sessions = Arc<Mutex<HashMap<String, EngineSession>>>;

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

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

    let listener = TcpListener::bind("0.0.0.0:8080")
        .expect("Failed to bind to port 8080");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let sessions = Arc::clone(&sessions);
                thread::spawn(move || {
                    handle_client(stream, sessions);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, sessions: Sessions) {
    let mut buffer = vec![0u8; 1_048_576]; // 1MB buffer for large RDF data

    match stream.read(&mut buffer) {
        Ok(size) => {
            let request = String::from_utf8_lossy(&buffer[..size]);

            // Parse method and path from the first line early, so we can
            // detect the SSE route before going through handle_request.
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
            }

            let response = handle_request(&request, &sessions);
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
        Err(e) => {
            eprintln!("Failed to read from connection: {}", e);
        }
    }
}

fn handle_request(request: &str, sessions: &Sessions) -> String {
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

    let mut engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>> =
        match RSPBuilder::new()
            .add_rsp_ql_query(&req.query)
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
    for json in rx {
        let msg = format!("data: {}\n\n", json);
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
