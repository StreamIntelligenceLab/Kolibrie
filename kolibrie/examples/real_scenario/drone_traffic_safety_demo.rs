/*
 * Copyright (c) 2024 Volodymyr Kadzhaia
 * Copyright (c) 2024 Pieter Bonte
 * KU Leuven - Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalogmtl::evaluator::{compute_w_max, DatalogMTLEvaluator};
use datalogmtl::rdf_parser;
use datalogmtl::store::IntervalFactStore;
use datalogmtl::stream::{RdfEvent, ShapeIngester, StreamShape};
use datalogmtl::syntax::{DatalogMTLRule, Mode};
use datalogmtl::validate::validate_rules;
use serde::{Deserialize, Serialize};
use shared::dictionary::Dictionary;
use shared::terms::Term;
use shared::triple::Triple;

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

const PORT: u16 = 7879;
const TICK_MS: u64 = 1000;
const TELEMETRY_STREAM: &str = "http://utm.example.org/telemetry";
const MAX_GAP_MS: u64 = 10_000;

// The evaluator uses DENSE integer semantics: Box/Since require the inner atom
// to hold at EVERY integer time point in their window. So the reasoner's time
// unit must be the sampling period — feeding it milliseconds while sampling
// once a second leaves 999 unsatisfied points between every pair of samples,
// and no universal operator can ever hold. Wall-clock ms stay in the UI only.
/// Fallback staleness when a config declares no stream shape. Live configs take
/// this from the shape's `STALENESS`; rule windows live in `DEFAULT_RULES`.
const MAX_GAP_TICKS: u64 = MAX_GAP_MS / TICK_MS;
/// Nominal store retention. Actual eviction is driven by the evaluator's `w_max`
/// (the widest rule window), which is recomputed on every config swap.
const STORE_HORIZON_TICKS: u64 = 620;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct LatLng {
    lat: f64,
    lng: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ZoneView {
    id: String,
    label: String,
    kind: String,
    center: LatLng,
    radius_m: f64,
}

#[derive(Debug, Clone)]
struct Zone {
    id: &'static str,
    label: &'static str,
    kind: &'static str,
    center: LatLng,
    radius_m: f64,
}

impl Zone {
    fn view(&self) -> ZoneView {
        ZoneView {
            id: self.id.to_string(),
            label: self.label.to_string(),
            kind: self.kind.to_string(),
            center: self.center,
            radius_m: self.radius_m,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct DroneView {
    id: String,
    label: String,
    position: LatLng,
    altitude_m: u32,
    automated: bool,
    link: String,
    current_zone: Option<String>,
    off_plan: bool,
}

#[derive(Debug, Clone, Serialize)]
struct AlertView {
    drone: String,
    rule: String,
    level: String,
    message: String,
    zone: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct TickView {
    time_ms: u64,
    drones: Vec<DroneView>,
    zones: Vec<ZoneView>,
    rdf_events: Vec<String>,
    derived: Vec<String>,
    alerts: Vec<AlertView>,
    /// Version of the rule/shape config that produced this tick. The UI watches
    /// this to confirm a submitted swap has actually landed.
    config_version: u64,
    active_rules: Vec<String>,
    /// Set when a submitted config failed to apply; the old rules keep running.
    config_error: Option<String>,
    metrics: MetricsView,
}

#[derive(Debug, Clone, Serialize)]
struct MetricsView {
    rules_fired: usize,
    new_triples: usize,
    snapshots: usize,
    eval_time_us: u64,
    /// Triples handed to the evaluator this tick — drops to near zero if the
    /// stream shape stops matching the telemetry.
    stream_facts: usize,
    /// Widest rule window; drives store eviction. Confirms a swap refreshed it.
    w_max_ticks: u64,
}

#[derive(Debug, Clone)]
struct DroneRuntime {
    id: &'static str,
    label: &'static str,
    position: LatLng,
    altitude_m: u32,
    automated: bool,
    last_telemetry_ms: u64,
    previous_zones: HashSet<&'static str>,
}

#[derive(Debug)]
struct DemoState {
    started: Instant,
    drone_a: DroneRuntime,
    drone_b: DroneRuntime,
}

#[derive(Debug, Clone, Deserialize)]
struct DroneUpdate {
    lat: f64,
    lng: f64,
}

#[derive(Debug, Clone)]
struct Vocab {
    rdf_type: u32,
    sosa_observation: u32,
    sosa_made_by_sensor: u32,
    sosa_has_result: u32,
    dront_drone: u32,
    dront_telemetry: u32,
    dront_in_zone: u32,
    dront_entered_zone: u32,
    dront_on_flight_plan: u32,
    // Zone classification (dront:status / dront:Restricted) is no longer listed
    // here: it moved into DEFAULT_STATIC so it can be edited during a demo.
    utm_position: u32,
    utm_altitude: u32,
    utm_ais_status: u32,
    utm_active: u32,
    // The rules' OUTPUT vocabulary (violatedZone / status / linkLost /
    // offCourse) is not listed here: it now lives in DEFAULT_RULES, and the
    // parser interns it into this same dictionary when the rules are loaded.
    xsd_false: u32,
    xsd_true: u32,
    drone_a: u32,
    drone_b: u32,
    zone_hospital: u32,
    zone_government: u32,
    zone_event: u32,
}

type EventLog = Arc<Mutex<Vec<String>>>;
type Config = Arc<ConfigHandle>;

/// The rule/shape text currently driving the engine.
#[derive(Debug, Clone, Serialize)]
struct ConfigText {
    rules: String,
    shapes: String,
    static_data: String,
    version: u64,
}

/// A parsed, validated configuration waiting to be picked up by the tick loop.
struct PendingConfig {
    rules: Vec<DatalogMTLRule>,
    shapes: Vec<StreamShape>,
    static_facts: Vec<Triple>,
    text: ConfigText,
    reset_store: bool,
}

/// Shared between the HTTP handlers and the tick loop.
///
/// The handler parses and validates synchronously (so it can report precise
/// errors) and parks the result in `pending`; the tick loop drains it at the top
/// of the next tick. That keeps the swap off the request thread and means no
/// lock is ever held across `evaluator.advance`.
struct ConfigHandle {
    current: Mutex<ConfigText>,
    pending: Mutex<Option<PendingConfig>>,
    version: AtomicU64,
}

impl ConfigHandle {
    fn new(rules: &str, shapes: &str, static_data: &str) -> Self {
        Self {
            current: Mutex::new(ConfigText {
                rules: rules.to_string(),
                shapes: shapes.to_string(),
                static_data: static_data.to_string(),
                version: 1,
            }),
            pending: Mutex::new(None),
            version: AtomicU64::new(1),
        }
    }
}

fn main() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(Mutex::new(DemoState::new()));

    // The dictionary lives out here so the HTTP thread can intern IRIs from
    // user-typed rules into the SAME dictionary the engine matches against —
    // Term::Constant ids are per-Dictionary and would otherwise match nothing.
    let dictionary = Arc::new(RwLock::new(Dictionary::new()));
    let vocab = init_vocab(&dictionary);
    let config: Config =
        Arc::new(ConfigHandle::new(DEFAULT_RULES, DEFAULT_SHAPES, DEFAULT_STATIC));

    start_demo_loop(
        Arc::clone(&events),
        Arc::clone(&state),
        Arc::clone(&dictionary),
        vocab.clone(),
        Arc::clone(&config),
    );
    start_http_server(events, state, dictionary, config, PORT);

    println!("Ghent drone safety demo running at http://127.0.0.1:{}/", PORT);
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

impl DemoState {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            drone_a: DroneRuntime {
                id: "droneA",
                label: "Drone A",
                position: LatLng { lat: 51.0261, lng: 3.7178 },
                altitude_m: 80,
                automated: true,
                last_telemetry_ms: 0,
                previous_zones: HashSet::new(),
            },
            drone_b: DroneRuntime {
                id: "droneB",
                label: "Drone B",
                position: LatLng { lat: 51.0518, lng: 3.7179 },
                altitude_m: 70,
                automated: false,
                last_telemetry_ms: 0,
                previous_zones: HashSet::new(),
            },
        }
    }

    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
}

fn start_demo_loop(
    events: EventLog,
    state: Arc<Mutex<DemoState>>,
    dictionary: Arc<RwLock<Dictionary>>,
    vocab: Vocab,
    config: Config,
) {
    thread::spawn(move || {
        // Parse the shipped defaults through the same parser the editor uses, so
        // a broken default is a loud startup failure rather than a silent
        // never-fires.
        let shapes = rdf_parser::parse_stream_shapes_shared(DEFAULT_SHAPES, &dictionary)
            .expect("default stream shape must parse");
        let rules = rdf_parser::parse_rules_shared(DEFAULT_RULES, &dictionary, Mode::Streaming)
            .expect("default rules must parse");
        let mut static_facts = rdf_parser::parse_facts_shared(DEFAULT_STATIC, &dictionary)
            .expect("default background facts must parse");

        let mut staleness_ticks = shape_staleness(&shapes);
        let mut ingester = ShapeIngester::new(shapes, Arc::clone(&dictionary));
        let mut evaluator = DatalogMTLEvaluator::new(
            rules,
            IntervalFactStore::new(STORE_HORIZON_TICKS),
            Arc::clone(&dictionary),
        )
        .expect("DatalogMTL rule setup failed");

        let mut config_error: Option<String> = None;

        // The reasoner's clock is a COUNTER, not the wall clock. Deriving `t`
        // from elapsed time meant each iteration took `TICK_MS + work`, so the
        // drift eventually swallowed an integer tick — and a hole is fatal under
        // dense semantics: `Box[0,n]` needs the fact at EVERY integer point, so
        // one skipped tick silently disables every universal operator whose
        // window spans it. Sleeping to an absolute deadline keeps the tick
        // sequence contiguous and still self-corrects against the wall clock.
        let started = state.lock().unwrap().started;
        let mut tick: u64 = 0;

        loop {
            // Drain any pending swap before the tick, so we never hold a lock
            // across `advance`.
            let pending = config.pending.lock().unwrap().take();
            if let Some(p) = pending {
                match apply_config(
                    p,
                    &mut ingester,
                    &mut evaluator,
                    &mut staleness_ticks,
                    &mut static_facts,
                    &dictionary,
                    &config,
                ) {
                    Ok(()) => config_error = None,
                    // Keep running on the old rules rather than dropping the demo.
                    Err(e) => config_error = Some(e),
                }
            }

            let view = {
                let mut guard = state.lock().unwrap();
                build_tick(
                    &mut guard,
                    &mut ingester,
                    &mut evaluator,
                    &dictionary,
                    &vocab,
                    &config,
                    staleness_ticks,
                    &static_facts,
                    tick,
                    config_error.clone(),
                )
            };

            if let Ok(json) = serde_json::to_string(&view) {
                push_event(&events, "state", &json);
            }

            tick += 1;
            let target_ms = tick * TICK_MS;
            let elapsed_ms = started.elapsed().as_millis() as u64;
            if target_ms > elapsed_ms {
                thread::sleep(Duration::from_millis(target_ms - elapsed_ms));
            }
            // If the work overran its budget we simply continue immediately:
            // time is caught up by running sooner, never by skipping a tick.
        }
    });
}

/// Staleness of the first shape, in ticks. Falls back to the built-in default
/// when a config declares no shapes.
fn shape_staleness(shapes: &[StreamShape]) -> u64 {
    shapes
        .first()
        .map(|s| s.staleness.max_gap_ms)
        .unwrap_or(MAX_GAP_TICKS)
}

/// Swap in a new rule set and stream shape.
///
/// Both the ingester and the evaluator are rebuilt rather than mutated:
/// `ShapeIngester` has no way to replace its shapes, and `DatalogMTLEvaluator`
/// computes its private `w_max` (which drives store eviction) only in `new` —
/// assigning `.rules` directly would leave eviction pinned to the OLD window and
/// a wider new rule could never fire.
fn apply_config(
    p: PendingConfig,
    ingester: &mut ShapeIngester,
    evaluator: &mut DatalogMTLEvaluator<IntervalFactStore>,
    staleness_ticks: &mut u64,
    static_facts: &mut Vec<Triple>,
    dictionary: &Arc<RwLock<Dictionary>>,
    config: &Config,
) -> Result<(), String> {
    // Re-validate here: it is the only fallible step in `new`, so checking first
    // means the store below is never stranded by a failing rebuild.
    validate_rules(&p.rules)?;

    // Carry the fact history over unless asked to clear it, so windows do not
    // have to refill from scratch after every edit.
    let store = if p.reset_store {
        IntervalFactStore::new(STORE_HORIZON_TICKS)
    } else {
        std::mem::replace(
            &mut evaluator.store,
            IntervalFactStore::new(STORE_HORIZON_TICKS),
        )
    };

    *staleness_ticks = shape_staleness(&p.shapes);
    *static_facts = p.static_facts;
    *ingester = ShapeIngester::new(p.shapes, Arc::clone(dictionary));
    *evaluator = DatalogMTLEvaluator::new(p.rules, store, Arc::clone(dictionary))?;
    *config.current.lock().unwrap() = p.text;
    Ok(())
}

fn start_http_server(
    events: EventLog,
    state: Arc<Mutex<DemoState>>,
    dictionary: Arc<RwLock<Dictionary>>,
    config: Config,
    port: u16,
) {
    thread::spawn(move || {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
            .expect("drone demo server: failed to bind port");
        for stream in listener.incoming().flatten() {
            let events = Arc::clone(&events);
            let state = Arc::clone(&state);
            let dictionary = Arc::clone(&dictionary);
            let config = Arc::clone(&config);
            thread::spawn(move || {
                handle_connection(stream, events, state, dictionary, config)
            });
        }
    });
}

/// Largest request body accepted, so a bad client cannot exhaust memory.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Read one request, honouring `Content-Length`.
///
/// A single fixed-size `read` is not enough here: a rules program easily exceeds
/// one TCP segment, and a truncated body would surface as a confusing JSON parse
/// error. GET requests carry no `Content-Length`, so this returns as soon as the
/// headers are complete and `/` and `/events` behave exactly as before.
fn read_request(stream: &mut TcpStream) -> Option<(String, String, String)> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));

    let mut raw: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];

    // Headers first.
    let header_end = loop {
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if raw.len() > MAX_BODY_BYTES {
            return None;
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => raw.extend_from_slice(&chunk[..n]),
        }
    };

    let head = std::str::from_utf8(&raw[..header_end]).ok()?;
    let request_line = head.lines().next().unwrap_or("");
    let method = request_line.split_whitespace().next().unwrap_or("").to_string();
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/")
        .to_string();

    let content_length = head
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
        .min(MAX_BODY_BYTES);

    // Then top up until the declared body has arrived.
    while raw.len() - header_end < content_length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => raw.extend_from_slice(&chunk[..n]),
        }
    }

    let body = String::from_utf8_lossy(&raw[header_end..]).into_owned();
    Some((method, path, body))
}

fn handle_connection(
    mut stream: TcpStream,
    events: EventLog,
    state: Arc<Mutex<DemoState>>,
    dictionary: Arc<RwLock<Dictionary>>,
    config: Config,
) {
    let Some((method, path, body)) = read_request(&mut stream) else {
        return;
    };

    match (method.as_str(), path.as_str()) {
        ("GET", "/") | ("GET", "/index.html") => serve_html(stream),
        ("GET", "/events") => serve_sse(stream, events),
        ("POST", "/api/drone-b") => update_drone_b(stream, &body, state),
        ("GET", "/api/config") => get_config(stream, &config),
        ("POST", "/api/config") => post_config(stream, &body, &dictionary, &config),
        _ => write_response(stream, 404, "text/plain; charset=utf-8", b"Not found"),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ConfigUpdate {
    rules: String,
    shapes: String,
    #[serde(default)]
    static_data: String,
    #[serde(default)]
    reset_store: bool,
}

fn get_config(stream: TcpStream, config: &Config) {
    let current = config.current.lock().unwrap().clone();
    let payload = serde_json::json!({
        "rules": current.rules,
        "shapes": current.shapes,
        "static_data": current.static_data,
        "version": current.version,
        "default_rules": DEFAULT_RULES,
        "default_shapes": DEFAULT_SHAPES,
        "default_static": DEFAULT_STATIC,
        "tick_ms": TICK_MS,
    });
    write_json(stream, 200, &payload.to_string());
}

/// Parse and validate a submitted config, then park it for the tick loop.
///
/// All three failure modes are reported separately so the editor can say which
/// box is wrong. `validate_rules` matters here because the RDF parser — unlike
/// the MeTeoR one — does not run it, so an unsafe head variable would otherwise
/// only fail later, inside the tick loop, where the user cannot see it.
fn post_config(
    stream: TcpStream,
    body: &str,
    dictionary: &Arc<RwLock<Dictionary>>,
    config: &Config,
) {
    let req: ConfigUpdate = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(e) => return config_error(stream, "request", &e.to_string()),
    };

    let rules = match rdf_parser::parse_rules_shared(&req.rules, dictionary, Mode::Streaming) {
        Ok(r) => r,
        Err(e) => return config_error(stream, "rules", &e),
    };
    let shapes = match rdf_parser::parse_stream_shapes_shared(&req.shapes, dictionary) {
        Ok(s) => s,
        Err(e) => return config_error(stream, "shapes", &e),
    };
    let static_facts = match rdf_parser::parse_facts_shared(&req.static_data, dictionary) {
        Ok(f) => f,
        Err(e) => return config_error(stream, "static", &e),
    };
    if let Err(e) = validate_rules(&rules) {
        return config_error(stream, "validate", &e);
    }

    let version = config.version.fetch_add(1, Ordering::SeqCst) + 1;
    let w_max = compute_w_max(&rules);
    let rule_ids: Vec<String> = rules.iter().map(|r| r.id.clone()).collect();

    let payload = serde_json::json!({
        "ok": true,
        "version": version,
        "rule_ids": rule_ids,
        "w_max_ticks": w_max,
        // Widening a window still needs history the store never kept, so tell
        // the UI how long the new rules need before they can be trusted.
        "warmup_ticks": if req.reset_store { w_max } else { 0 },
    });

    *config.pending.lock().unwrap() = Some(PendingConfig {
        rules,
        shapes,
        static_facts,
        text: ConfigText {
            rules: req.rules,
            shapes: req.shapes,
            static_data: req.static_data,
            version,
        },
        reset_store: req.reset_store,
    });

    write_json(stream, 200, &payload.to_string());
}

fn config_error(stream: TcpStream, stage: &str, message: &str) {
    let payload = serde_json::json!({ "ok": false, "stage": stage, "error": message });
    write_json(stream, 400, &payload.to_string());
}

fn serve_html(stream: TcpStream) {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples/real_scenario/drone_traffic_safety_demo.html");
    match std::fs::read(path) {
        Ok(body) => write_response(stream, 200, "text/html; charset=utf-8", &body),
        Err(_) => write_response(stream, 404, "text/plain; charset=utf-8", b"Demo HTML not found"),
    }
}

fn serve_sse(mut stream: TcpStream, events: EventLog) {
    let headers = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: text/event-stream\r\n",
        "Cache-Control: no-cache\r\n",
        "Access-Control-Allow-Origin: *\r\n",
        "Connection: keep-alive\r\n",
        "\r\n",
    );
    if stream.write_all(headers.as_bytes()).is_err() {
        return;
    }

    let mut cursor = events.lock().unwrap().len().saturating_sub(1);
    let mut idle_ms = 0u64;
    loop {
        let batch: Vec<String> = {
            let log = events.lock().unwrap();
            log[cursor..].to_vec()
        };
        if batch.is_empty() {
            idle_ms += 100;
            if idle_ms >= 15_000 {
                if stream.write_all(b": heartbeat\n\n").is_err() {
                    return;
                }
                idle_ms = 0;
            }
            thread::sleep(Duration::from_millis(100));
        } else {
            for event in &batch {
                if stream.write_all(event.as_bytes()).is_err() {
                    return;
                }
            }
            cursor += batch.len();
            idle_ms = 0;
        }
    }
}

fn update_drone_b(stream: TcpStream, body: &str, state: Arc<Mutex<DemoState>>) {
    let Ok(update) = serde_json::from_str::<DroneUpdate>(body) else {
        write_json(stream, 400, r#"{"ok":false}"#);
        return;
    };

    let mut guard = state.lock().unwrap();
    let now = guard.elapsed_ms();
    guard.drone_b.position = LatLng { lat: update.lat, lng: update.lng };
    guard.drone_b.last_telemetry_ms = now;
    drop(guard);

    write_json(stream, 200, &format!(r#"{{"ok":true,"time_ms":{}}}"#, now));
}

fn write_json(stream: TcpStream, status: u16, body: &str) {
    write_response(stream, status, "application/json", body.as_bytes());
}

fn write_response(mut stream: TcpStream, status: u16, content_type: &str, body: &[u8]) {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        status,
        status_text,
        content_type,
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
}

fn push_event(events: &EventLog, kind: &str, json: &str) {
    let msg = format!("event: {}\ndata: {}\n\n", kind, json);
    events.lock().unwrap().push(msg);
}

fn build_tick(
    state: &mut DemoState,
    ingester: &mut ShapeIngester,
    evaluator: &mut DatalogMTLEvaluator<IntervalFactStore>,
    dictionary: &Arc<RwLock<Dictionary>>,
    vocab: &Vocab,
    config: &Config,
    staleness_ticks: u64,
    static_facts: &[Triple],
    t: u64,
    config_error: Option<String>,
) -> TickView {
    // The stream shape's STALENESS is in ticks; the drone-B freshness checks
    // below are in wall-clock ms, so convert once here. Editing STALENESS in the
    // Rules tab therefore really does change when the link reads as expired.
    let max_gap_ms = staleness_ticks.saturating_mul(TICK_MS);
    // `t` is the caller's contiguous tick counter — never derive it from elapsed
    // time, or drift will skip integers and break dense operators. The animation
    // and the UI clock use the nominal wall-clock position of that tick.
    let tick_ms = t * TICK_MS;
    state.drone_a.position = scripted_drone_a(tick_ms);
    state.drone_a.last_telemetry_ms = tick_ms;

    let zones = zones();
    // Re-asserted every tick; see DEFAULT_STATIC for why.
    let mut triples = static_facts.to_vec();
    let mut rdf_events = Vec::new();

    let mut drone_a = state.drone_a.clone();
    let mut drone_b = state.drone_b.clone();
    let a_triples = telemetry_for_drone(&mut drone_a, t, &zones, vocab, dictionary, ingester);
    triples.extend(a_triples.0);
    rdf_events.extend(a_triples.1);

    let b_has_fresh_telemetry = tick_ms.saturating_sub(state.drone_b.last_telemetry_ms) <= 2_500;
    if b_has_fresh_telemetry {
        let b_triples = telemetry_for_drone(&mut drone_b, t, &zones, vocab, dictionary, ingester);
        triples.extend(b_triples.0);
        rdf_events.extend(b_triples.1);
    }

    // Facts for channels that have gone quiet. These come from the shape's
    // EXPIRY declaration, so what a stale drone "says" is editable alongside
    // everything else rather than hardcoded here. Must run after the telemetry
    // above, or a drone that just reported would also be reported stale.
    let expiry = ingester.expiry_facts(t);
    if !expiry.is_empty() {
        rdf_events.extend(decode_triples(
            &expiry.iter().map(|(tr, _)| tr.clone()).collect::<Vec<_>>(),
            dictionary,
        ));
        triples.extend(expiry.into_iter().map(|(tr, _)| tr));
    }

    state.drone_a.previous_zones = drone_a.previous_zones;
    state.drone_b.previous_zones = drone_b.previous_zones;

    let stream_facts = triples.len();
    let (derived, metrics) = evaluator.advance(t, triples);
    let derived_lines = decode_triples(&derived, dictionary);
    let alerts = alerts_from_derived(&derived, &evaluator.rules, dictionary);

    let active = config.current.lock().unwrap().clone();

    TickView {
        time_ms: tick_ms,
        drones: vec![
            drone_view(&state.drone_a, "active", &zones),
            drone_view(
                &state.drone_b,
                if tick_ms.saturating_sub(state.drone_b.last_telemetry_ms) > max_gap_ms {
                    "expired"
                } else {
                    "active"
                },
                &zones,
            ),
        ],
        zones: zones.iter().map(Zone::view).collect(),
        rdf_events,
        derived: derived_lines,
        alerts,
        config_version: active.version,
        active_rules: evaluator.rules.iter().map(|r| r.id.clone()).collect(),
        config_error,
        metrics: MetricsView {
            rules_fired: metrics.rules_fired,
            new_triples: metrics.new_triples,
            snapshots: metrics.snapshot_count,
            eval_time_us: metrics.eval_time_us,
            stream_facts,
            w_max_ticks: compute_w_max(&evaluator.rules),
        },
    }
}

fn telemetry_for_drone(
    drone: &mut DroneRuntime,
    t: u64,
    zones: &[Zone],
    vocab: &Vocab,
    dictionary: &Arc<RwLock<Dictionary>>,
    ingester: &mut ShapeIngester,
) -> (Vec<Triple>, Vec<String>) {
    let drone_id = if drone.id == "droneA" { vocab.drone_a } else { vocab.drone_b };
    let obs = encode(dictionary, &format!("http://utm.example.org/obs/{}-{}", drone.id, t));
    let tlm = encode(dictionary, &format!("http://utm.example.org/telemetry/{}-{}", drone.id, t));
    let position = encode(dictionary, &format!("{:.6},{:.6}", drone.position.lat, drone.position.lng));
    let altitude = encode(dictionary, &drone.altitude_m.to_string());
    let event = RdfEvent {
        stream_iri: TELEMETRY_STREAM.to_string(),
        timestamp: t,
        triples: vec![
            triple(obs, vocab.rdf_type, vocab.sosa_observation),
            triple(obs, vocab.sosa_made_by_sensor, drone_id),
            triple(obs, vocab.sosa_has_result, tlm),
            triple(drone_id, vocab.rdf_type, vocab.dront_drone),
            triple(tlm, vocab.rdf_type, vocab.dront_telemetry),
            triple(tlm, vocab.utm_position, position),
            triple(tlm, vocab.utm_altitude, altitude),
            triple(tlm, vocab.utm_ais_status, vocab.utm_active),
        ],
    };

    let mut facts: Vec<Triple> = ingester
        .process_event(&event)
        .into_iter()
        .map(|(triple, _)| triple)
        .collect();

    let current_zones: HashSet<&'static str> = zones
        .iter()
        .filter(|zone| haversine_m(drone.position, zone.center) <= zone.radius_m)
        .map(|zone| zone.id)
        .collect();
    for zone in zones {
        let zone_id = zone_id(zone.id, vocab);
        if current_zones.contains(zone.id) {
            facts.push(triple(drone_id, vocab.dront_in_zone, zone_id));
            if !drone.previous_zones.contains(zone.id) {
                facts.push(triple(drone_id, vocab.dront_entered_zone, zone_id));
            }
        }
    }

    let off_plan = current_zones.iter().any(|zone| {
        zones
            .iter()
            .find(|z| z.id == *zone)
            .map(|z| z.kind == "restricted")
            .unwrap_or(false)
    });
    facts.push(triple(
        drone_id,
        vocab.dront_on_flight_plan,
        if off_plan { vocab.xsd_false } else { vocab.xsd_true },
    ));

    drone.previous_zones = current_zones;
    let lines = decode_triples(&event.triples, dictionary);
    (facts, lines)
}

fn drone_view(drone: &DroneRuntime, link: &str, zones: &[Zone]) -> DroneView {
    let current_zone = zones
        .iter()
        .find(|zone| haversine_m(drone.position, zone.center) <= zone.radius_m)
        .map(|zone| zone.label.to_string());
    let off_plan = current_zone.is_some();
    DroneView {
        id: drone.id.to_string(),
        label: drone.label.to_string(),
        position: drone.position,
        altitude_m: drone.altitude_m,
        automated: drone.automated,
        link: link.to_string(),
        current_zone,
        off_plan,
    }
}

/// Level and wording for the rules the demo ships with, keyed by rule id.
/// Custom rules fall back to a generic alert.
const ALERT_TEMPLATES: &[(&str, &str, &str)] = &[
    (
        "sustainedGeofenceViolation",
        "critical",
        "Drone remained inside a restricted zone for the full window.",
    ),
    (
        "controlLinkLoss",
        "warning",
        "Telemetry channel has been expired for the full window.",
    ),
    (
        "offCourseSinceRestrictedEntry",
        "critical",
        "Drone has stayed off its filed flight plan since restricted-zone entry.",
    ),
    (
        "zoneTransition",
        "warning",
        "Drone loitered over UZ Gent, then crossed straight into City Hall airspace.",
    ),
    (
        "zoneTour",
        "critical",
        "Drone toured all three restricted zones in sequence: City Hall, then Citadelpark, then UZ Gent.",
    ),
];

/// Maximum alerts surfaced per tick, so a runaway recursive rule cannot flood
/// the UI.
const MAX_ALERTS: usize = 20;

/// Turn derived triples into alerts.
///
/// `advance` returns triples with no rule provenance, so attribution is
/// reconstructed by finding the rule whose head unifies with each derived
/// triple. Every derived triple came from some rule's head; the only ambiguity
/// is two rules sharing a head shape, which is acceptable here.
fn alerts_from_derived(
    derived: &[Triple],
    rules: &[DatalogMTLRule],
    dictionary: &Arc<RwLock<Dictionary>>,
) -> Vec<AlertView> {
    let dict = dictionary.read().unwrap();
    let mut alerts = Vec::new();
    let mut seen: HashSet<(String, u32)> = HashSet::new();

    for t in derived {
        let Some(rule) = rules.iter().find(|r| head_matches(&r.head, t)) else {
            continue;
        };
        if !seen.insert((rule.id.clone(), t.subject)) {
            continue;
        }

        let template = ALERT_TEMPLATES.iter().find(|(id, _, _)| *id == rule.id);
        let (level, message) = match template {
            Some((_, level, message)) => (level.to_string(), message.to_string()),
            None => ("warning".to_string(), dict.decode_triple(t)),
        };
        // Only report a zone when the rule's head actually carries one.
        let zone = matches!(rule.head.2, Term::Variable(_)).then(|| local(&dict, t.object));

        alerts.push(AlertView {
            drone: local(&dict, t.subject),
            rule: rule.id.clone(),
            level,
            message,
            zone,
        });
        if alerts.len() >= MAX_ALERTS {
            break;
        }
    }
    alerts
}

/// Does a rule head unify with a concrete derived triple?
fn head_matches(head: &(Term, Term, Term), t: &Triple) -> bool {
    fn m(term: &Term, id: u32) -> bool {
        match term {
            Term::Variable(_) => true,
            Term::Constant(c) => *c == id,
            _ => false,
        }
    }
    m(&head.0, t.subject) && m(&head.1, t.predicate) && m(&head.2, t.object)
}

fn decode_triples(triples: &[Triple], dictionary: &Arc<RwLock<Dictionary>>) -> Vec<String> {
    let dict = dictionary.read().unwrap();
    triples.iter().map(|triple| dict.decode_triple(triple)).collect()
}

fn local(dict: &Dictionary, id: u32) -> String {
    dict.decode(id)
        .unwrap_or("unknown")
        .rsplit(|c| c == '/' || c == '#')
        .next()
        .unwrap_or("unknown")
        .to_string()
}

fn init_vocab(dictionary: &Arc<RwLock<Dictionary>>) -> Vocab {
    Vocab {
        rdf_type: encode(dictionary, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        sosa_observation: encode(dictionary, "http://www.w3.org/ns/sosa/Observation"),
        sosa_made_by_sensor: encode(dictionary, "http://www.w3.org/ns/sosa/madeBySensor"),
        sosa_has_result: encode(dictionary, "http://www.w3.org/ns/sosa/hasResult"),
        dront_drone: encode(dictionary, "http://example.org/dront/Drone"),
        dront_telemetry: encode(dictionary, "http://example.org/dront/Telemetry"),
        dront_in_zone: encode(dictionary, "http://example.org/dront/inZone"),
        dront_entered_zone: encode(dictionary, "http://example.org/dront/enteredZone"),
        dront_on_flight_plan: encode(dictionary, "http://example.org/dront/onFlightPlan"),
        utm_position: encode(dictionary, "http://utm.example.org/position"),
        utm_altitude: encode(dictionary, "http://utm.example.org/altitude"),
        utm_ais_status: encode(dictionary, "http://utm.example.org/aisStatus"),
        utm_active: encode(dictionary, "http://utm.example.org/active"),
        xsd_false: encode(dictionary, "false"),
        xsd_true: encode(dictionary, "true"),
        drone_a: encode(dictionary, "http://utm.example.org/droneA"),
        drone_b: encode(dictionary, "http://utm.example.org/droneB"),
        zone_hospital: encode(dictionary, "http://utm.example.org/zone/hospital"),
        zone_government: encode(dictionary, "http://utm.example.org/zone/government"),
        zone_event: encode(dictionary, "http://utm.example.org/zone/event"),
    }
}

/// Default rule program, in the RDF triple-pattern syntax of
/// `datalogmtl::rdf_parser`. This text is the source of truth: it is parsed at
/// startup and shipped to the Rules tab, so what you see is what is running.
///
/// All windows are in TICKS (1 tick == TICK_MS), not milliseconds — the
/// evaluator's dense integer semantics require the time unit to be the sampling
/// period. IRIs must match `init_vocab` exactly or the rules match nothing;
/// `<false>` is the quoting device for the bare string `vocab.xsd_false`.
const DEFAULT_RULES: &str = r#"
PREFIX dront: <http://example.org/dront/>
PREFIX utm:   <http://utm.example.org/>

# Drone stayed inside a restricted zone for 30 consecutive ticks.
[sustainedGeofenceViolation]
(?d, utm:violatedZone, ?z) :-
    Box[0,30](?d, dront:inZone, ?z),
    (?z, dront:status, dront:Restricted).

# Control channel has been expired for 10 consecutive ticks.
[controlLinkLoss]
(?d, utm:status, utm:linkLost) :-
    Box[0,10](?d, utm:channelStatus, utm:expired).

# Off the filed flight plan ever since entering a restricted zone.
[offCourseSinceRestrictedEntry]
(?d, utm:status, utm:offCourse) :-
    (?d, dront:onFlightPlan, <false>),
    (?z, dront:status, dront:Restricted),
    Since[0,600]((?d, dront:onFlightPlan, <false>), (?d, dront:enteredZone, ?z)).

# Loitered 10 ticks in UZ Gent (Restricted1), then APPEARED in City Hall
# (Restricted2) within the next 10. The dwell is on the FIRST zone; a single
# tick in the second is enough, so a brief fly-through still trips it.
# Box[1,11] is offset by one tick so the two stays need not overlap, and the
# Diamond[0,10] is the travel gap. True only while the drone is in City Hall.
[zoneTransition]
(?d, utm:transitioned, ?z2) :-
    (?d, dront:inZone, ?z2),
    (?z2, dront:status, dront:Restricted2),
    Diamond[0,10](Box[1,11]((?d, dront:inZone, ?z),
                            (?z, dront:status, dront:Restricted1))).

# A deliberate tour: City Hall -> Citadelpark -> UZ Gent, dwelling 5 ticks in
# each. Written outside-in, so it reads BACKWARDS in time from "now": the last
# leg first, each Diamond stepping back to the leg before it. The Diamonds are
# the travel gaps, so the legs need not be contiguous (1-15 ticks between them).
# Drag drone B through the three zones to trigger it.
[zoneTour]
(?d, utm:tour, ?z3) :-
    Box[0,5]((?d, dront:inZone, ?z3), (?z3, dront:status, dront:Restricted1)),
    Diamond[6,20](
        Box[0,5]((?d, dront:inZone, ?z2), (?z2, dront:status, dront:Restricted3)),
        Diamond[6,20](
            Box[0,5]((?d, dront:inZone, ?z1), (?z1, dront:status, dront:Restricted2))
        )
    ).
"#;

/// Default stream shape. `STALENESS` is in ticks, like every other window here.
///
/// `EXPIRY` is what a channel transmits once it has gone quiet for `STALENESS`
/// ticks — silence is not itself an event, so `controlLinkLoss` would have
/// nothing to observe without it. It repeats every tick while the channel stays
/// stale, which is what that rule's `Box[0,10]` needs.
///
/// Note the PATTERN can only ever match LESS than the telemetry the demo emits —
/// `telemetry_for_drone` builds a fixed set of 8 triples, so adding a pattern
/// that nothing produces makes the whole event stop matching.
const DEFAULT_SHAPES: &str = r#"
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX sosa:  <http://www.w3.org/ns/sosa/>
PREFIX dront: <http://example.org/dront/>
PREFIX utm:   <http://utm.example.org/>

STREAM <http://utm.example.org/telemetry>
    PATTERN (?obs, rdf:type, sosa:Observation)
            (?obs, sosa:madeBySensor, ?drone)
            (?obs, sosa:hasResult, ?tlm)
            (?drone, rdf:type, dront:Drone)
            (?tlm, rdf:type, dront:Telemetry)
            (?tlm, utm:position, ?pos)
            (?tlm, utm:altitude, ?alt)
            (?tlm, utm:aisStatus, ?status)
    KEY ?drone
    STALENESS 10
    EXPIRY (?drone, utm:channelStatus, utm:expired)
.
"#;

/// Default background facts, as N-Triples: which zones are restricted.
///
/// Both `sustainedGeofenceViolation` and `offCourseSinceRestrictedEntry` join
/// against `(?z, dront:status, dront:Restricted)`, so declassifying a zone here
/// disarms those rules over it — the quickest thing to change during a live
/// demo. Zone geometry stays in Rust; only the classification is reasoned over.
///
/// These are re-asserted on every tick: the store is time-indexed and evicts
/// below `t - w_max`, so a fact inserted only once would age out and the joins
/// would silently stop matching. Coalescing makes that cheap (one interval per
/// fact, not one per tick).
const DEFAULT_STATIC: &str = r#"
# Zone classification. Plain N-Triples — paste in anything an RDF tool emits.
#
# Each zone carries TWO classes. The generic `Restricted` is what the geofence
# rules join on; the numbered one identifies the individual zone for `zoneTour`.
# Drop the generic lines and the geofence rules go quiet with no error.
<http://utm.example.org/zone/hospital> <http://example.org/dront/status> <http://example.org/dront/Restricted> .
<http://utm.example.org/zone/hospital> <http://example.org/dront/status> <http://example.org/dront/Restricted1> .
<http://utm.example.org/zone/government> <http://example.org/dront/status> <http://example.org/dront/Restricted> .
<http://utm.example.org/zone/government> <http://example.org/dront/status> <http://example.org/dront/Restricted2> .
<http://utm.example.org/zone/event> <http://example.org/dront/status> <http://example.org/dront/Restricted> .
<http://utm.example.org/zone/event> <http://example.org/dront/status> <http://example.org/dront/Restricted3> .
"#;

fn zones() -> Vec<Zone> {
    vec![
        Zone {
            id: "hospital",
            label: "UZ Gent Hospital",
            kind: "restricted",
            center: LatLng { lat: 51.0257, lng: 3.7297 },
            radius_m: 620.0,
        },
        Zone {
            id: "government",
            label: "Ghent City Hall",
            kind: "restricted",
            center: LatLng { lat: 51.05444, lng: 3.72528 },
            radius_m: 320.0,
        },
        Zone {
            id: "event",
            label: "Citadelpark Event",
            kind: "restricted",
            center: LatLng { lat: 51.0379, lng: 3.7201 },
            radius_m: 720.0,
        },
    ]
}

fn scripted_drone_a(tick: u64) -> LatLng {
    let cycle = tick % 95_000;
    let lat = 51.02565;
    if cycle <= 12_000 {
        interpolate(LatLng { lat, lng: 3.7178 }, LatLng { lat, lng: 3.7252 }, cycle as f64 / 12_000.0)
    } else if cycle <= 50_000 {
        let progress = (cycle - 12_000) as f64 / 38_000.0;
        interpolate(LatLng { lat, lng: 3.7252 }, LatLng { lat, lng: 3.7316 }, progress)
    } else if cycle <= 68_000 {
        interpolate(
            LatLng { lat, lng: 3.7316 },
            LatLng { lat: 51.0262, lng: 3.7385 },
            (cycle - 50_000) as f64 / 18_000.0,
        )
    } else {
        interpolate(
            LatLng { lat: 51.0262, lng: 3.7385 },
            LatLng { lat, lng: 3.7178 },
            (cycle - 68_000) as f64 / 27_000.0,
        )
    }
}

fn interpolate(a: LatLng, b: LatLng, t: f64) -> LatLng {
    let clamped = t.clamp(0.0, 1.0);
    LatLng {
        lat: a.lat + (b.lat - a.lat) * clamped,
        lng: a.lng + (b.lng - a.lng) * clamped,
    }
}

fn haversine_m(a: LatLng, b: LatLng) -> f64 {
    let r = 6_371_000.0;
    let d_lat = (b.lat - a.lat).to_radians();
    let d_lng = (b.lng - a.lng).to_radians();
    let lat1 = a.lat.to_radians();
    let lat2 = b.lat.to_radians();
    let h = (d_lat / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * (d_lng / 2.0).sin().powi(2);
    2.0 * r * h.sqrt().atan2((1.0 - h).sqrt())
}

fn zone_id(id: &str, vocab: &Vocab) -> u32 {
    match id {
        "hospital" => vocab.zone_hospital,
        "government" => vocab.zone_government,
        "event" => vocab.zone_event,
        _ => vocab.zone_event,
    }
}

fn encode(dictionary: &Arc<RwLock<Dictionary>>, value: &str) -> u32 {
    dictionary.write().unwrap().encode(value)
}

fn triple(subject: u32, predicate: u32, object: u32) -> Triple {
    Triple { subject, predicate, object }
}

