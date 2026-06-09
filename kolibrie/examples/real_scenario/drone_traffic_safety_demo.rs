/*
 * Copyright (c) 2024 Volodymyr Kadzhaia
 * Copyright (c) 2024 Pieter Bonte
 * KU Leuven - Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalogmtl::evaluator::DatalogMTLEvaluator;
use datalogmtl::store::IntervalFactStore;
use datalogmtl::stream::{RdfEvent, ShapeIngester, StalenessPolicy, StreamShape};
use datalogmtl::syntax::{DatalogMTLRule, Interval, TemporalAtom};
use serde::{Deserialize, Serialize};
use shared::dictionary::Dictionary;
use shared::terms::Term;
use shared::triple::Triple;

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

const PORT: u16 = 7879;
const TICK_MS: u64 = 1000;
const TELEMETRY_STREAM: &str = "http://utm.example.org/telemetry";
const MAX_GAP_MS: u64 = 10_000;

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
    metrics: MetricsView,
}

#[derive(Debug, Clone, Serialize)]
struct MetricsView {
    rules_fired: usize,
    new_triples: usize,
    snapshots: usize,
    eval_time_us: u64,
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
    dront_status: u32,
    dront_restricted: u32,
    utm_position: u32,
    utm_altitude: u32,
    utm_ais_status: u32,
    utm_active: u32,
    utm_channel_status: u32,
    utm_expired: u32,
    utm_violated_zone: u32,
    utm_status: u32,
    utm_link_lost: u32,
    utm_off_course: u32,
    xsd_false: u32,
    xsd_true: u32,
    drone_a: u32,
    drone_b: u32,
    zone_hospital: u32,
    zone_government: u32,
    zone_event: u32,
}

type EventLog = Arc<Mutex<Vec<String>>>;

fn main() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(Mutex::new(DemoState::new()));

    start_demo_loop(Arc::clone(&events), Arc::clone(&state));
    start_http_server(events, state, PORT);

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

fn start_demo_loop(events: EventLog, state: Arc<Mutex<DemoState>>) {
    thread::spawn(move || {
        let dictionary = Arc::new(RwLock::new(Dictionary::new()));
        let vocab = init_vocab(&dictionary);
        let shape = telemetry_shape(&vocab);
        let mut ingester = ShapeIngester::new(vec![shape], Arc::clone(&dictionary));
        let mut evaluator = DatalogMTLEvaluator::new(
            drone_rules(&vocab),
            IntervalFactStore::new(620_000),
            Arc::clone(&dictionary),
        )
        .expect("DatalogMTL rule setup failed");

        loop {
            let view = {
                let mut guard = state.lock().unwrap();
                build_tick(&mut guard, &mut ingester, &mut evaluator, &dictionary, &vocab)
            };

            if let Ok(json) = serde_json::to_string(&view) {
                push_event(&events, "state", &json);
            }
            thread::sleep(Duration::from_millis(TICK_MS));
        }
    });
}

fn start_http_server(events: EventLog, state: Arc<Mutex<DemoState>>, port: u16) {
    thread::spawn(move || {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
            .expect("drone demo server: failed to bind port");
        for stream in listener.incoming().flatten() {
            let events = Arc::clone(&events);
            let state = Arc::clone(&state);
            thread::spawn(move || handle_connection(stream, events, state));
        }
    });
}

fn handle_connection(mut stream: TcpStream, events: EventLog, state: Arc<Mutex<DemoState>>) {
    let mut buf = [0u8; 8192];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let req = match std::str::from_utf8(&buf[..n]) {
        Ok(s) => s,
        Err(_) => return,
    };
    let request_line = req.lines().next().unwrap_or("");
    let method = request_line.split_whitespace().next().unwrap_or("");
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/");

    match (method, path) {
        ("GET", "/") | ("GET", "/index.html") => serve_html(stream),
        ("GET", "/events") => serve_sse(stream, events),
        ("POST", "/api/drone-b") => update_drone_b(stream, req, state),
        _ => write_response(stream, 404, "text/plain; charset=utf-8", b"Not found"),
    }
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

fn update_drone_b(mut stream: TcpStream, req: &str, state: Arc<Mutex<DemoState>>) {
    let Some(body) = req.split("\r\n\r\n").nth(1) else {
        write_response(stream, 400, "application/json", br#"{"ok":false}"#);
        return;
    };
    let Ok(update) = serde_json::from_str::<DroneUpdate>(body) else {
        write_response(stream, 400, "application/json", br#"{"ok":false}"#);
        return;
    };

    let mut guard = state.lock().unwrap();
    let now = guard.elapsed_ms();
    guard.drone_b.position = LatLng { lat: update.lat, lng: update.lng };
    guard.drone_b.last_telemetry_ms = now;
    let response = format!(r#"{{"ok":true,"time_ms":{}}}"#, now);
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        response.len()
    );
    let _ = stream.write_all(headers.as_bytes());
    let _ = stream.write_all(response.as_bytes());
}

fn write_response(mut stream: TcpStream, status: u16, content_type: &str, body: &[u8]) {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
) -> TickView {
    let now = state.elapsed_ms();
    let tick = (now / TICK_MS) * TICK_MS;
    state.drone_a.position = scripted_drone_a(tick);
    state.drone_a.last_telemetry_ms = tick;

    let zones = zones();
    let mut triples = static_zone_triples(vocab);
    let mut rdf_events = Vec::new();

    let mut drone_a = state.drone_a.clone();
    let mut drone_b = state.drone_b.clone();
    let a_triples = telemetry_for_drone(&mut drone_a, tick, &zones, vocab, dictionary, ingester);
    triples.extend(a_triples.0);
    rdf_events.extend(a_triples.1);

    let b_has_fresh_telemetry = tick.saturating_sub(state.drone_b.last_telemetry_ms) <= 2_500;
    if b_has_fresh_telemetry {
        let b_triples = telemetry_for_drone(&mut drone_b, tick, &zones, vocab, dictionary, ingester);
        triples.extend(b_triples.0);
        rdf_events.extend(b_triples.1);
    } else if tick.saturating_sub(state.drone_b.last_telemetry_ms) > MAX_GAP_MS {
        triples.push(triple(vocab.drone_b, vocab.utm_channel_status, vocab.utm_expired));
    }

    state.drone_a.previous_zones = drone_a.previous_zones;
    state.drone_b.previous_zones = drone_b.previous_zones;

    let (derived, metrics) = evaluator.advance(tick, triples);
    let derived_lines = decode_triples(&derived, dictionary);
    let alerts = alerts_from_derived(&derived, dictionary, vocab);

    TickView {
        time_ms: tick,
        drones: vec![
            drone_view(&state.drone_a, "active", &zones),
            drone_view(
                &state.drone_b,
                if tick.saturating_sub(state.drone_b.last_telemetry_ms) > MAX_GAP_MS {
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
        metrics: MetricsView {
            rules_fired: metrics.rules_fired,
            new_triples: metrics.new_triples,
            snapshots: metrics.snapshot_count,
            eval_time_us: metrics.eval_time_us,
        },
    }
}

fn telemetry_for_drone(
    drone: &mut DroneRuntime,
    tick: u64,
    zones: &[Zone],
    vocab: &Vocab,
    dictionary: &Arc<RwLock<Dictionary>>,
    ingester: &mut ShapeIngester,
) -> (Vec<Triple>, Vec<String>) {
    let drone_id = if drone.id == "droneA" { vocab.drone_a } else { vocab.drone_b };
    let obs = encode(dictionary, &format!("http://utm.example.org/obs/{}-{}", drone.id, tick));
    let tlm = encode(dictionary, &format!("http://utm.example.org/telemetry/{}-{}", drone.id, tick));
    let position = encode(dictionary, &format!("{:.6},{:.6}", drone.position.lat, drone.position.lng));
    let altitude = encode(dictionary, &drone.altitude_m.to_string());
    let event = RdfEvent {
        stream_iri: TELEMETRY_STREAM.to_string(),
        timestamp: tick,
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

fn alerts_from_derived(
    derived: &[Triple],
    dictionary: &Arc<RwLock<Dictionary>>,
    vocab: &Vocab,
) -> Vec<AlertView> {
    let dict = dictionary.read().unwrap();
    let mut alerts = Vec::new();
    for triple in derived {
        if triple.predicate == vocab.utm_violated_zone {
            alerts.push(AlertView {
                drone: local(&dict, triple.subject),
                rule: "sustainedGeofenceViolation".to_string(),
                level: "critical".to_string(),
                message: "Drone remained inside a restricted zone for 30 seconds.".to_string(),
                zone: Some(local(&dict, triple.object)),
            });
        } else if triple.predicate == vocab.utm_status && triple.object == vocab.utm_link_lost {
            alerts.push(AlertView {
                drone: local(&dict, triple.subject),
                rule: "controlLinkLoss".to_string(),
                level: "warning".to_string(),
                message: "Telemetry channel has been expired for 10 seconds.".to_string(),
                zone: None,
            });
        } else if triple.predicate == vocab.utm_status && triple.object == vocab.utm_off_course {
            alerts.push(AlertView {
                drone: local(&dict, triple.subject),
                rule: "offCourseSinceRestrictedEntry".to_string(),
                level: "critical".to_string(),
                message: "Drone has stayed off its filed flight plan since restricted-zone entry.".to_string(),
                zone: None,
            });
        }
    }
    alerts
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
        dront_status: encode(dictionary, "http://example.org/dront/status"),
        dront_restricted: encode(dictionary, "http://example.org/dront/Restricted"),
        utm_position: encode(dictionary, "http://utm.example.org/position"),
        utm_altitude: encode(dictionary, "http://utm.example.org/altitude"),
        utm_ais_status: encode(dictionary, "http://utm.example.org/aisStatus"),
        utm_active: encode(dictionary, "http://utm.example.org/active"),
        utm_channel_status: encode(dictionary, "http://utm.example.org/channelStatus"),
        utm_expired: encode(dictionary, "http://utm.example.org/expired"),
        utm_violated_zone: encode(dictionary, "http://utm.example.org/violatedZone"),
        utm_status: encode(dictionary, "http://utm.example.org/status"),
        utm_link_lost: encode(dictionary, "http://utm.example.org/linkLost"),
        utm_off_course: encode(dictionary, "http://utm.example.org/offCourse"),
        xsd_false: encode(dictionary, "false"),
        xsd_true: encode(dictionary, "true"),
        drone_a: encode(dictionary, "http://utm.example.org/droneA"),
        drone_b: encode(dictionary, "http://utm.example.org/droneB"),
        zone_hospital: encode(dictionary, "http://utm.example.org/zone/hospital"),
        zone_government: encode(dictionary, "http://utm.example.org/zone/government"),
        zone_event: encode(dictionary, "http://utm.example.org/zone/event"),
    }
}

fn telemetry_shape(vocab: &Vocab) -> StreamShape {
    StreamShape {
        stream_iri: TELEMETRY_STREAM.to_string(),
        event_pattern: vec![
            (var("obs"), constant(vocab.rdf_type), constant(vocab.sosa_observation)),
            (var("obs"), constant(vocab.sosa_made_by_sensor), var("drone")),
            (var("obs"), constant(vocab.sosa_has_result), var("tlm")),
            (var("drone"), constant(vocab.rdf_type), constant(vocab.dront_drone)),
            (var("tlm"), constant(vocab.rdf_type), constant(vocab.dront_telemetry)),
            (var("tlm"), constant(vocab.utm_position), var("pos")),
            (var("tlm"), constant(vocab.utm_altitude), var("alt")),
            (var("tlm"), constant(vocab.utm_ais_status), var("status")),
        ],
        channel_key: vec!["drone".to_string()],
        staleness: StalenessPolicy { max_gap_ms: MAX_GAP_MS },
    }
}

fn drone_rules(vocab: &Vocab) -> Vec<DatalogMTLRule> {
    vec![
        DatalogMTLRule {
            id: "sustainedGeofenceViolation".to_string(),
            head: (var("d"), constant(vocab.utm_violated_zone), var("z")),
            body: vec![
                TemporalAtom::Box_ {
                    interval: Interval { start: 0, end: 30_000 },
                    inner: Box::new(TemporalAtom::Base((
                        var("d"),
                        constant(vocab.dront_in_zone),
                        var("z"),
                    ))),
                },
                TemporalAtom::Base((
                    var("z"),
                    constant(vocab.dront_status),
                    constant(vocab.dront_restricted),
                )),
            ],
        },
        DatalogMTLRule {
            id: "controlLinkLoss".to_string(),
            head: (var("d"), constant(vocab.utm_status), constant(vocab.utm_link_lost)),
            body: vec![TemporalAtom::Box_ {
                interval: Interval { start: 0, end: 10_000 },
                inner: Box::new(TemporalAtom::Base((
                    var("d"),
                    constant(vocab.utm_channel_status),
                    constant(vocab.utm_expired),
                ))),
            }],
        },
        DatalogMTLRule {
            id: "offCourseSinceRestrictedEntry".to_string(),
            head: (var("d"), constant(vocab.utm_status), constant(vocab.utm_off_course)),
            body: vec![
                TemporalAtom::Base((
                    var("d"),
                    constant(vocab.dront_on_flight_plan),
                    constant(vocab.xsd_false),
                )),
                TemporalAtom::Base((
                    var("z"),
                    constant(vocab.dront_status),
                    constant(vocab.dront_restricted),
                )),
                TemporalAtom::Since {
                    interval: Interval { start: 0, end: 600_000 },
                    phi: Box::new(TemporalAtom::Base((
                        var("d"),
                        constant(vocab.dront_on_flight_plan),
                        constant(vocab.xsd_false),
                    ))),
                    psi: Box::new(TemporalAtom::Base((
                        var("d"),
                        constant(vocab.dront_entered_zone),
                        var("z"),
                    ))),
                },
            ],
        },
    ]
}

fn static_zone_triples(vocab: &Vocab) -> Vec<Triple> {
    vec![
        triple(vocab.zone_hospital, vocab.dront_status, vocab.dront_restricted),
        triple(vocab.zone_government, vocab.dront_status, vocab.dront_restricted),
        triple(vocab.zone_event, vocab.dront_status, vocab.dront_restricted),
    ]
}

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

fn var(name: &str) -> Term {
    Term::Variable(name.to_string())
}

fn constant(value: u32) -> Term {
    Term::Constant(value)
}
