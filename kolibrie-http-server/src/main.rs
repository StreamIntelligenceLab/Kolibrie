/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalog::parser_n3_logic::parse_n3_rule;
use datalog::reasoning::Reasoner;
use kolibrie::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use kolibrie::parser::process_rule_definition;
use kolibrie::rsp_engine::{
    OperationMode, QueryExecutionMode, RSPBuilder, ResultConsumer, SimpleR2R,
};
use kolibrie::sparql_database::SparqlDatabase;
use serde::{Deserialize, Serialize};
use shared::triple::Triple;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// ── Session state for persistent RSP engines ────────────────────────────────

struct EngineSession {
    engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>>,
    /// Lazily set when the SSE client connects.
    sse_sender: Arc<Mutex<Option<Sender<String>>>>,
}

type Sessions = Arc<Mutex<HashMap<String, EngineSession>>>;

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

const READ_CHUNK_SIZE: usize = 8 * 1024;
const MAX_REQUEST_SIZE: usize = 64 * 1024 * 1024;
const INCOMPLETE_JSON_GRACE_PERIOD: Duration = Duration::from_millis(750);

struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

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

fn has_n3_rule_text(text: &str) -> bool {
    text.lines()
        .map(str::trim_start)
        .any(|line| !line.starts_with('#') && line.contains("=>"))
}

fn strip_hash_comments(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_iri = false;
    let mut in_literal = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        match ch {
            '#' if !in_iri && !in_literal => {
                while let Some(next) = chars.next() {
                    if next == '\n' {
                        output.push('\n');
                        break;
                    }
                }
                escaped = false;
            }
            '<' if !in_literal => {
                in_iri = false;
                let mut lookahead = chars.clone();
                while let Some(next) = lookahead.next() {
                    if next == '>' {
                        in_iri = true;
                        break;
                    }
                    if next.is_whitespace() {
                        break;
                    }
                }
                output.push(ch);
                escaped = false;
            }
            '>' if in_iri && !escaped => {
                in_iri = false;
                output.push(ch);
                escaped = false;
            }
            '"' if !in_iri && !escaped => {
                in_literal = !in_literal;
                output.push(ch);
                escaped = false;
            }
            '\\' if in_literal && !escaped => {
                output.push(ch);
                escaped = true;
            }
            _ => {
                output.push(ch);
                escaped = false;
            }
        }
    }

    output
}

fn main() {
    println!("Starting Kolibrie HTTP Server on 0.0.0.0:8080");

    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));

    let listener = TcpListener::bind("0.0.0.0:8080").expect("Failed to bind to port 8080");

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
    match read_http_request(&mut stream) {
        Ok(request) => {
            // SSE handler must keep the connection open, so it is handled here
            // rather than returning a String from handle_request.
            if request.method == "GET" && request.path.starts_with("/rsp/events/") {
                let session_id = request.path["/rsp/events/".len()..].to_string();
                rsp_events_sse(&session_id, stream, &sessions);
                return;
            }

            let response = handle_request(&request, &sessions);
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
        Err(e) if e.kind() == io::ErrorKind::InvalidData => {
            eprintln!("Request rejected: {}", e);
            let response = error_response(413, "Payload Too Large");
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
        Err(e) => {
            eprintln!("Failed to read from connection: {}", e);
            let response = error_response(400, "Bad Request");
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> io::Result<HttpRequest> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let mut request = Vec::with_capacity(READ_CHUNK_SIZE);
    let mut buffer = [0u8; READ_CHUNK_SIZE];

    loop {
        let size = stream.read(&mut buffer)?;
        if size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before request headers were received",
            ));
        }
        request.extend_from_slice(&buffer[..size]);
        ensure_request_size(request.len())?;

        let Some(header_end) = header_delimiter_end(&request) else {
            continue;
        };

        let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
        let (method, path) = parse_request_line(&headers)?;

        if has_chunked_transfer_encoding(&headers) {
            loop {
                match decode_chunked_body(&request[header_end..])? {
                    Some(body) => return Ok(HttpRequest { method, path, body }),
                    None => {
                        let size = stream.read(&mut buffer)?;
                        if size == 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "connection closed before complete chunked body was received",
                            ));
                        }
                        request.extend_from_slice(&buffer[..size]);
                        ensure_request_size(request.len())?;
                    }
                }
            }
        }

        let content_length = parse_content_length(&headers);
        let mut body_end = if let Some(content_length) = content_length {
            let total_size = header_end.checked_add(content_length).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "request is too large")
            })?;
            ensure_request_size(total_size)?;
            total_size
        } else {
            request.len()
        };

        while request.len() < body_end {
            let size = stream.read(&mut buffer)?;
            if size == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before complete request body was received",
                ));
            }
            request.extend_from_slice(&buffer[..size]);
            ensure_request_size(request.len())?;
        }

        if is_json_request(&headers) && json_body_needs_more_bytes(&request[header_end..body_end]) {
            if body_end < request.len() {
                body_end = request.len();
            }

            let extra_bytes =
                read_until_json_complete(stream, &mut request, header_end, &mut body_end)?;
            if extra_bytes > 0 {
                eprintln!(
                    "HTTP JSON body needed {} extra byte(s) beyond the initial framing",
                    extra_bytes
                );
            }
        }

        request.truncate(body_end);
        let body = request[header_end..].to_vec();
        return Ok(HttpRequest { method, path, body });
    }
}

fn parse_request_line(headers: &str) -> io::Result<(String, String)> {
    let request_line = headers.lines().next().unwrap_or("");
    let parts: Vec<&str> = request_line.split_whitespace().collect();

    if parts.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "request line is missing method or path",
        ));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

fn ensure_request_size(size: usize) -> io::Result<()> {
    if size > MAX_REQUEST_SIZE {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("request exceeds {} byte limit", MAX_REQUEST_SIZE),
        ))
    } else {
        Ok(())
    }
}

fn header_delimiter_end(request: &[u8]) -> Option<usize> {
    request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
        .or_else(|| {
            request
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|position| position + 2)
        })
}

fn parse_content_length(headers: &str) -> Option<usize> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse::<usize>().ok()
        } else {
            None
        }
    })
}

fn has_chunked_transfer_encoding(headers: &str) -> bool {
    headers.lines().any(|line| {
        let Some((name, value)) = line.split_once(':') else {
            return false;
        };
        name.eq_ignore_ascii_case("transfer-encoding")
            && value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
    })
}

fn is_json_request(headers: &str) -> bool {
    headers.lines().any(|line| {
        let Some((name, value)) = line.split_once(':') else {
            return false;
        };
        name.eq_ignore_ascii_case("content-type")
            && value
                .split(';')
                .next()
                .map(|content_type| content_type.trim().eq_ignore_ascii_case("application/json"))
                .unwrap_or(false)
    })
}

fn json_body_needs_more_bytes(body: &[u8]) -> bool {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(_) => false,
        Err(e) => e.is_eof(),
    }
}

fn read_until_json_complete(
    stream: &mut TcpStream,
    request: &mut Vec<u8>,
    header_end: usize,
    body_end: &mut usize,
) -> io::Result<usize> {
    let previous_timeout = stream.read_timeout().ok().flatten();
    let _ = stream.set_read_timeout(Some(INCOMPLETE_JSON_GRACE_PERIOD));

    let mut buffer = [0u8; READ_CHUNK_SIZE];
    let mut extra_bytes = 0;

    while json_body_needs_more_bytes(&request[header_end..*body_end]) {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(size) => {
                request.extend_from_slice(&buffer[..size]);
                ensure_request_size(request.len())?;
                *body_end = request.len();
                extra_bytes += size;
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => {
                let _ = stream.set_read_timeout(previous_timeout);
                return Err(e);
            }
        }
    }

    let _ = stream.set_read_timeout(previous_timeout);
    Ok(extra_bytes)
}

fn decode_chunked_body(body: &[u8]) -> io::Result<Option<Vec<u8>>> {
    let mut decoded = Vec::new();
    let mut position = 0;

    loop {
        let Some(line_end) = find_crlf(&body[position..]) else {
            return Ok(None);
        };
        let size_line = &body[position..position + line_end];
        let size_text = std::str::from_utf8(size_line)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk size"))?;
        let size_hex = size_text.split(';').next().unwrap_or("").trim();
        let chunk_size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk size"))?;
        position += line_end + 2;

        if chunk_size == 0 {
            if body.len() < position + 2 {
                return Ok(None);
            }
            if body[position..].starts_with(b"\r\n") {
                return Ok(Some(decoded));
            }
            if let Some(trailer_end) = body[position..]
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
            {
                position += trailer_end + 4;
                let _ = position;
                return Ok(Some(decoded));
            }
            return Ok(None);
        }

        let chunk_end = position
            .checked_add(chunk_size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chunk is too large"))?;
        if body.len() < chunk_end + 2 {
            return Ok(None);
        }
        if &body[chunk_end..chunk_end + 2] != b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk is missing trailing CRLF",
            ));
        }

        decoded.extend_from_slice(&body[position..chunk_end]);
        ensure_request_size(decoded.len())?;
        position = chunk_end + 2;
    }
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|window| window == b"\r\n")
}

fn handle_request(request: &HttpRequest, sessions: &Sessions) -> String {
    let method = request.method.as_str();
    let path = request.path.as_str();

    if method == "GET" && path == "/" {
        return serve_playground();
    }

    if method == "POST" && path == "/query" {
        return match request_body(&request.body) {
            Some(body) => execute_sparql_with_context(body),
            None => json_error_response("Request body is not valid UTF-8"),
        };
    }

    if method == "POST" && path == "/rsp-query" {
        return match request_body(&request.body) {
            Some(body) => execute_rsp_query(body),
            None => json_error_response("Request body is not valid UTF-8"),
        };
    }

    if method == "POST" && path == "/rsp/register" {
        return match request_body(&request.body) {
            Some(body) => rsp_register(body, sessions),
            None => json_error_response("Request body is not valid UTF-8"),
        };
    }

    if method == "POST" && path == "/rsp/push" {
        return match request_body(&request.body) {
            Some(body) => rsp_push(body, sessions),
            None => json_error_response("Request body is not valid UTF-8"),
        };
    }

    if method == "OPTIONS" {
        return cors_response();
    }

    error_response(404, "Not Found")
}

// ── RSP persistent session handlers ─────────────────────────────────────────

fn request_body(body: &[u8]) -> Option<&str> {
    match std::str::from_utf8(body) {
        Ok(body) => Some(body),
        Err(e) => {
            eprintln!("Request body is not valid UTF-8: {}", e);
            None
        }
    }
}

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

    let cleaned_query = strip_hash_comments(&req.query);
    let cleaned_n3logic = req.n3logic.as_deref().map(strip_hash_comments);
    let n3logic = cleaned_n3logic
        .as_deref()
        .filter(|text| has_n3_rule_text(text))
        .unwrap_or("");
    let sparql_rules = req
        .sparql_rules
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|rule| strip_hash_comments(&rule))
        .collect::<Vec<_>>();

    let mut engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>> =
        match RSPBuilder::new()
            .add_rsp_ql_query(&cleaned_query)
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
            let cleaned_static_rdf;
            let static_rdf_for_parse = match req.static_format.as_str() {
                "ntriples" | "turtle" => {
                    cleaned_static_rdf = strip_hash_comments(static_rdf);
                    cleaned_static_rdf.as_str()
                }
                _ => static_rdf.as_str(),
            };
            let ntriples = match req.static_format.as_str() {
                "ntriples" => static_rdf_for_parse.to_string(),
                _ => {
                    let mut static_db = SparqlDatabase::new();
                    match req.static_format.as_str() {
                        "turtle" => static_db.parse_turtle(static_rdf_for_parse),
                        _ => static_db.parse_rdf(static_rdf_for_parse),
                    }
                    db_to_ntriples(&static_db)
                }
            };
            if !ntriples.is_empty() {
                engine.add_static_ntriples(&ntriples);
                println!(
                    "RSP register: loaded static data ({} bytes)",
                    ntriples.len()
                );
            }
        }
    }

    let streams = engine.stream_iris();
    let session_id = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed).to_string();

    sessions
        .lock()
        .unwrap()
        .insert(session_id.clone(), EngineSession { engine, sse_sender });

    println!(
        "RSP register: session {} created, streams: {:?}",
        session_id, streams
    );

    let response = RspRegisterResponse {
        session_id,
        streams,
    };
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

    let ntriples = strip_hash_comments(&req.ntriples);
    if ntriples.trim().is_empty() {
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

    let triples = session.engine.parse_data(&ntriples);
    println!(
        "RSP push: {} triple(s) to stream '{}' at t={} (session {})",
        triples.len(),
        req.stream,
        req.timestamp,
        req.session_id
    );

    for triple in triples {
        session
            .engine
            .add_to_stream(&req.stream, triple, req.timestamp);
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
            eprintln!("JSON parse error after {} byte(s): {}", body.len(), e);
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

    println!(
        "Processing {} query(ies) and {} rule(s)",
        queries.len(),
        rules.len()
    );

    let mut database = SparqlDatabase::new();
    let use_optimizer = request.format == "ntriples";

    // Load RDF data once
    if let Some(rdf_data) = request.rdf {
        if !rdf_data.trim().is_empty() {
            let cleaned_rdf_data;
            let rdf_data_for_parse = match request.format.as_str() {
                "ntriples" | "turtle" => {
                    cleaned_rdf_data = strip_hash_comments(&rdf_data);
                    cleaned_rdf_data.as_str()
                }
                _ => rdf_data.as_str(),
            };
            match request.format.as_str() {
                "ntriples" => {
                    println!("Parsing N-Triples data with Streamertail optimizer...");
                    database.parse_ntriples_and_add(rdf_data_for_parse);
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
                "turtle" => {
                    println!("Parsing Turtle dataset...");
                    database.parse_turtle(rdf_data_for_parse);
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
                "rdfxml" | _ => {
                    println!("Parsing RDF/XML data...");
                    database.parse_rdf(rdf_data_for_parse);
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
        let n3_rules_text = strip_hash_comments(n3_rules_text);
        if has_n3_rule_text(&n3_rules_text) {
            println!("Processing N3 logic rules from N3 Logic sub-tab...");

            // Mirror the database dictionary so term encodings are shared
            let mut kg = Reasoner::new();
            kg.dictionary = database.dictionary.clone();

            // Decode all triples to owned strings using database.dictionary BEFORE
            // creating kg's mutable borrow — avoids the RwLockReadGuard conflict.
            let decoded_triples: Vec<(String, String, String)> = {
                let dict_guard = kg.dictionary.read().unwrap();
                database
                    .triples
                    .iter()
                    .map(|triple| {
                        (
                            dict_guard.decode(triple.subject).unwrap_or("").to_string(),
                            dict_guard
                                .decode(triple.predicate)
                                .unwrap_or("")
                                .to_string(),
                            dict_guard.decode(triple.object).unwrap_or("").to_string(),
                        )
                    })
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
                        database
                            .prefixes
                            .insert(prefix.to_string(), uri.to_string());
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
        let rule_def = strip_hash_comments(rule_def);
        if !rule_def.trim().is_empty() {
            println!("Processing rule {}...", idx + 1);
            match process_rule_definition(&rule_def, &mut database) {
                Ok((_, inferred_facts)) => {
                    println!(
                        "Rule {} processed, inferred {} facts",
                        idx + 1,
                        inferred_facts.len()
                    );
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
        let executable_query = strip_hash_comments(query);

        let results = if use_optimizer {
            execute_query_rayon_parallel2_volcano(&executable_query, &mut database)
        } else {
            execute_query(&executable_query, &mut database)
        };

        let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;

        all_results.push(QueryResult {
            query_index: idx,
            query: query.clone(),
            data: results,
            execution_time_ms: execution_time,
        });
    }

    let response = QueryResponse {
        results: all_results,
    };
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
    let result_container: Arc<Mutex<Vec<Vec<(String, String)>>>> = Arc::new(Mutex::new(Vec::new()));
    let rc_clone = Arc::clone(&result_container);

    let result_consumer = ResultConsumer::<Vec<(String, String)>> {
        function: Arc::new(move |row| {
            rc_clone.lock().unwrap().push(row);
        }),
    };

    let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));

    let start_time = std::time::Instant::now();
    let cleaned_query = strip_hash_comments(&request.query);

    let mut engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>> =
        match RSPBuilder::new()
            .add_rsp_ql_query(&cleaned_query)
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
            let cleaned_static_rdf;
            let static_rdf_for_parse = match request.static_format.as_str() {
                "ntriples" | "turtle" => {
                    cleaned_static_rdf = strip_hash_comments(static_rdf);
                    cleaned_static_rdf.as_str()
                }
                _ => static_rdf.as_str(),
            };
            let ntriples = match request.static_format.as_str() {
                "ntriples" => static_rdf_for_parse.to_string(),
                _ => {
                    let mut static_db = SparqlDatabase::new();
                    match request.static_format.as_str() {
                        "turtle" => static_db.parse_turtle(static_rdf_for_parse),
                        _ => static_db.parse_rdf(static_rdf_for_parse),
                    }
                    db_to_ntriples(&static_db)
                }
            };
            if !ntriples.is_empty() {
                engine.add_static_ntriples(&ntriples);
                println!(
                    "RSP: loaded static data ({} bytes as N-Triples)",
                    ntriples.len()
                );
            }
        }
    }

    // Sort events by timestamp, then push them to the engine.
    let mut events = request.events;
    events.sort_by_key(|e| e.timestamp);

    for event in &events {
        let ntriples = strip_hash_comments(&event.ntriples);
        if ntriples.trim().is_empty() {
            continue;
        }
        let triples = engine.parse_data(&ntriples);
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

#[cfg(test)]
mod tests {
    use super::strip_hash_comments;

    #[test]
    fn strips_hash_comments_without_touching_iris_or_literals() {
        let input = "PREFIX ex: <http://example.org#>\n\
SELECT ?s WHERE { ?s ex:p \"value # kept\" . # remove this\n\
FILTER(?s < 3) # remove too\n\
FILTER(?s<3) # remove compact comparison\n\
}";
        let expected = "PREFIX ex: <http://example.org#>\n\
SELECT ?s WHERE { ?s ex:p \"value # kept\" . \n\
FILTER(?s < 3) \n\
FILTER(?s<3) \n\
}";

        assert_eq!(strip_hash_comments(input), expected);
    }
}

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
    let json = serde_json::to_string(&error)
        .unwrap_or_else(|_| r#"{"error":"Internal server error"}"#.to_string());

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
     \r\n"
        .to_string()
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
