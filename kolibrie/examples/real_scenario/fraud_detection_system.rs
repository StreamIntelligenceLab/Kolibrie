/* 
  Pipeline (per transaction, per step):
    RSP-QL window  ->  Datalog Pass 1 (Symbolic -> ML)
                   ->  GradientBoosting ML
                   ->  Datalog Pass 2 (ML -> Symbolic)
                   ->  Fusion -> Verdict

  RSP-QL: RANGE 300 STEP 60 (5-min sliding window, 1-min step).

  Pass-1 Datalog rules (raw features -> symbolic flags):
    R1   velocity1h > 5              -> highVelocity
    R2   amount > 1000               -> largeAmount
    R3   merchantRisk > 70           -> highMerchantRisk
    R4   isForeign > 0 ∧ risk > 70   -> foreignHighRisk
    R5   amount > 1000 ∧ vel > 5     -> riskLevel:high
    R1b  windowVelocity > 3          -> highWindowActivity  (RSP-derived)

  Pass-2 Datalog rules (ML score -> symbolic flags):
    R6   mlFraudScore > 40 ∧ vel > 3 -> mlAssistedAlert
    R7   recentFraudCount > 1        -> historicalPattern

  Fusion thresholds:
    P > 0.80                         -> FRAUD
    P > 0.50 ∧ riskLevel = high      -> FRAUD
    riskLevel = high                 -> SUSPICIOUS
    P > 0.60 ∨ any flag              -> REVIEW
    otherwise                        -> CLEAR

  Parser constraints:
    • SELECT variables must be on a single line (space1 does not match '\n').
    • FILTER thresholds must be integers (parser stops at '.').
*/ 

use kolibrie::execute_ml::{execute_ml_prediction_with_handler, setup_ml_handler, MLPredictTiming};
use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::rsp_engine::{RSPBuilder, SimpleR2R, ResultConsumer, QueryExecutionMode};
use datalog::reasoning::Reasoner;
use ml::MLHandler;
use pyo3::prepare_freethreaded_python;
use shared::triple::Triple;
use shared::rule::Rule;

use std::collections::HashMap;
use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

type EventLog = Arc<Mutex<Vec<String>>>;

fn push_event(events: &EventLog, kind: &str, json: &str) {
    let msg = format!("event: {}\ndata: {}\n\n", kind, json);
    events.lock().unwrap().push(msg);
}

fn handle_sse_connection(mut stream: TcpStream, events: EventLog) {
    let mut buf = [0u8; 2048];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let req = match std::str::from_utf8(&buf[..n]) {
        Ok(s) => s,
        Err(_) => return,
    };
    let path = req.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    match path {
        "/events" => {
            let headers = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Type: text/event-stream\r\n",
                "Cache-Control: no-cache\r\n",
                "Access-Control-Allow-Origin: *\r\n",
                "Connection: keep-alive\r\n",
                "\r\n",
            );
            if stream.write_all(headers.as_bytes()).is_err() { return; }

            let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
            let mut cursor = 0usize;
            let mut idle_ms = 0u64;
            loop {
                let batch: Vec<String> = {
                    let log = events.lock().unwrap();
                    log[cursor..].to_vec()
                };
                if batch.is_empty() {
                    idle_ms += 100;
                    if idle_ms >= 15_000 {
                        // heartbeat every 15 s to keep connection alive
                        if stream.write_all(b": heartbeat\n\n").is_err() { return; }
                        idle_ms = 0;
                    }
                    thread::sleep(Duration::from_millis(100));
                } else {
                    for event in &batch {
                        if stream.write_all(event.as_bytes()).is_err() { return; }
                    }
                    cursor += batch.len();
                    idle_ms = 0;
                }
            }
        }
        "/" | "/index.html" => {
            let html_path = {
                // 1. Co-located next to this source file
                let sibling = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("examples/real_scenario/fraud_detection_dashboard.html");
                if sibling.exists() {
                    sibling
                } else {
                    // 2. Walk up for any other location
                    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                    loop {
                        let candidate = p.join("fraud_detection_dashboard.html");
                        if candidate.exists() { break candidate; }
                        if !p.pop() {
                            break std::path::PathBuf::from("fraud_detection_dashboard.html");
                        }
                    }
                }
            };
            match std::fs::read(&html_path) {
                Ok(body) => {
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(header.as_bytes());
                    let _ = stream.write_all(&body);
                }
                Err(_) => {
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\nDashboard HTML not found");
                }
            }
        }
        _ => {
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n");
        }
    }
}

fn start_sse_server(events: EventLog, port: u16) {
    thread::spawn(move || {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
            .expect("SSE server: failed to bind port");
        for stream in listener.incoming().flatten() {
            let events = Arc::clone(&events);
            thread::spawn(move || handle_sse_connection(stream, events));
        }
    });
}

#[derive(Debug, Clone)]
struct Transaction {
    tx_id:         String,
    account_id:    String,
    /// USD
    amount:        f64,
    /// 0–23
    hour_of_day:   u8,
    /// 0 (Mon) – 6 (Sun)
    day_of_week:   u8,
    /// 0–100; stored as integer to keep SPARQL FILTER thresholds parse-safe
    merchant_risk: u32,
    /// Transactions on this account in the last hour
    velocity_1h:   u32,
    /// km from account home location
    distance_km:   f64,
    /// 1 = cross-border
    is_foreign:    u8,
    /// 1 = physical card present
    card_present:  u8,
}

#[derive(Debug, Clone, PartialEq)]
enum Verdict { Fraud, Suspicious, Review, Clear }

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Verdict::Fraud      => write!(f, "FRAUD"),
            Verdict::Suspicious => write!(f, "SUSPICIOUS"),
            Verdict::Review     => write!(f, "REVIEW"),
            Verdict::Clear      => write!(f, "CLEAR"),
        }
    }
}

#[derive(Debug)]
struct AccountHistory {
    recent_fraud_count: u32,
}

impl AccountHistory {
    fn new() -> Self {
        Self { recent_fraud_count: 0 }
    }

    /// Increments on Fraud/Suspicious (cap 10), decrements otherwise (floor 0).
    fn update(&mut self, _score: f64, verdict: &Verdict) {
        match verdict {
            Verdict::Fraud | Verdict::Suspicious =>
                self.recent_fraud_count = (self.recent_fraud_count + 1).min(10),
            _ => self.recent_fraud_count = self.recent_fraud_count.saturating_sub(1),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    prepare_freethreaded_python();

    let sep = "=".repeat(70);
    println!("\n{}", sep);
    println!("  Neuro-Symbolic Fraud Detection System");
    println!("  SPARQL + RSP-QL + Datalog Reasoning + GradientBoosting ML");
    println!("{}\n", sep);

    let mut database  = setup_knowledge_base();

    // Symbolic rules over raw transaction features
    let (rule_velocity,   _) = process_rule_definition(&rule_suspicious_velocity(),  &mut database)?;
    let (rule_amount,     _) = process_rule_definition(&rule_suspicious_amount(),    &mut database)?;
    let (rule_merch_risk, _) = process_rule_definition(&rule_high_merchant_risk(),   &mut database)?;
    let (rule_pattern,    _) = process_rule_definition(&rule_foreign_high_risk(),    &mut database)?;
    let (rule_high_risk,  _) = process_rule_definition(&rule_high_risk_chained(),    &mut database)?;
    
    // R1b reads ex:windowVelocity, which is written from the RSP snapshot
    let (rule_win_vel,    _) = process_rule_definition(&rule_high_window_activity(), &mut database)?;

    let pass1_rules = vec![
        rule_velocity, rule_amount, rule_merch_risk, rule_pattern, rule_high_risk,
        rule_win_vel,
    ];

    // Rules that read the ML score written back to RDF
    let (rule_ml_alert, _) = process_rule_definition(&rule_ml_assisted_alert(),  &mut database)?;
    let (rule_hist_pat, _) = process_rule_definition(&rule_historical_pattern(), &mut database)?;

    let pass2_rules = vec![rule_ml_alert, rule_hist_pat];

    // Shared buffer: RSP window thread deposits bindings here
    let window_results: Arc<Mutex<Vec<HashMap<String, String>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let window_results_clone = window_results.clone();

    let result_consumer = ResultConsumer {
        function: Arc::new(Box::new(move |bindings: Vec<(String, String)>| {
            let map: HashMap<_, _> = bindings.into_iter().collect();
            window_results_clone.lock().unwrap().push(map);
        })),
    };

    let rsp_query = r#"
        PREFIX ex:  <http://fraud.example.org/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        REGISTER RSTREAM <http://fraud.example.org/out/transactions> AS
        SELECT ?txId ?account ?amount ?hour ?dow ?mRisk ?vel ?dist ?isF ?cp
        FROM NAMED WINDOW :txWindow ON :transactionStream [RANGE 300 STEP 60]
        WHERE {
            WINDOW :txWindow {
                ?txId ex:account      ?account ;
                      ex:amount       ?amount  ;
                      ex:hourOfDay    ?hour    ;
                      ex:dayOfWeek    ?dow     ;
                      ex:merchantRisk ?mRisk   ;
                      ex:velocity1h   ?vel     ;
                      ex:distanceKm   ?dist    ;
                      ex:isForeign    ?isF     ;
                      ex:cardPresent  ?cp      .
            }
        }
    "#;

    let mut engine: kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>> =
        RSPBuilder::new()
            .add_rsp_ql_query(rsp_query)
            .add_consumer(result_consumer)
            .add_r2r(Box::new(SimpleR2R::with_execution_mode(
                QueryExecutionMode::Volcano,
            )))
            .build()?;

    println!(
        "{:<4} {:<18} {:<7} {:>8} {:>5} {:>5} {:>4} {:>7} {:>6} {:>4} {:>3} {:>6} | {:>7} | {:<50} | {}",
        "T", "TxID", "Acct", "Amount", "Hour", "Vel", "wVel",
        "Dist", "Risk%", "For", "CP", "Hist",
        "P(fraud)", "P1-flags -> P2-flags", "Verdict"
    );
    println!("{}", "-".repeat(142));

    let events: EventLog = Arc::new(Mutex::new(Vec::new()));
    start_sse_server(Arc::clone(&events), 7878);
    println!("Dashboard: http://localhost:7878  (open in browser)\n");

    let (ml_handler, best_model_name) = setup_ml_handler("fraud_predictor")?;

    run_simulation(&mut engine, &mut database, &pass1_rules, &pass2_rules, window_results, events, &ml_handler, &best_model_name)?;

    engine.stop();
    thread::sleep(Duration::from_millis(500));
    Ok(())
}


/// Writes Pass-1 symbolic flags and account history count as numeric (0/1) RDF triples
/// so the ML.PREDICT INPUT query can SELECT them as features.
fn write_numeric_flags_to_db(
    database:         &mut SparqlDatabase,
    tx_uri:           &str,
    flags_p1:         &[String],
    hist_fraud_count: u32,
) {
    let flag_pairs = [
        ("highVelocity",    "http://fraud.example.org/flagHighVelocity"),
        ("largeAmount",     "http://fraud.example.org/flagLargeAmount"),
        ("highMerchantRisk","http://fraud.example.org/flagHighMerchantRisk"),
        ("foreignHighRisk", "http://fraud.example.org/flagForeignHighRisk"),
        ("risk:high",       "http://fraud.example.org/flagRiskHigh"),
    ];
    for (flag_name, predicate) in &flag_pairs {
        let value = if flags_p1.iter().any(|f| f == flag_name) { "1" } else { "0" };
        database.add_triple_parts(tx_uri, predicate, value);
    }
    database.add_triple_parts(
        tx_uri,
        "http://fraud.example.org/recentFraudCount",
        &hist_fraud_count.to_string(),
    );
}

/// Extracts all 14 ML feature values for a single transaction from the RDF database.
/// Returns a single-row Vec<HashMap<String, u32>> (dictionary-encoded values).
fn extract_data_for_ml_fraud(
    database: &SparqlDatabase,
    tx_uri:   &str,
) -> Result<Vec<HashMap<String, u32>>, Box<dyn Error>> {
    let feature_predicates = [
        ("amt",   "http://fraud.example.org/amount"),
        ("hour",  "http://fraud.example.org/hourOfDay"),
        ("dow",   "http://fraud.example.org/dayOfWeek"),
        ("mRisk", "http://fraud.example.org/merchantRisk"),
        ("vel",   "http://fraud.example.org/velocity1h"),
        ("dist",  "http://fraud.example.org/distanceKm"),
        ("isF",   "http://fraud.example.org/isForeign"),
        ("cp",    "http://fraud.example.org/cardPresent"),
        ("fHv",   "http://fraud.example.org/flagHighVelocity"),
        ("fLa",   "http://fraud.example.org/flagLargeAmount"),
        ("fHmr",  "http://fraud.example.org/flagHighMerchantRisk"),
        ("fFhr",  "http://fraud.example.org/flagForeignHighRisk"),
        ("fRh",   "http://fraud.example.org/flagRiskHigh"),
        ("cnt",   "http://fraud.example.org/recentFraudCount"),
    ];

    let (tx_id, pred_ids): (Option<u32>, Vec<Option<u32>>) = {
        let dict = database.dictionary.read().unwrap();
        let tx_id = dict.string_to_id.get(tx_uri).copied();
        let pred_ids = feature_predicates
            .iter()
            .map(|(_, uri)| dict.string_to_id.get(*uri).copied())
            .collect();
        (tx_id, pred_ids)
    };

    let tx_id = match tx_id {
        Some(id) => id,
        None => return Ok(vec![]),
    };

    let mut row: HashMap<String, u32> = HashMap::new();
    row.insert("tx".to_string(), tx_id);

    for (i, (var_name, _)) in feature_predicates.iter().enumerate() {
        if let Some(pred_id) = pred_ids[i] {
            if let Some(triple) = database.triples.iter()
                .find(|t| t.subject == tx_id && t.predicate == pred_id)
            {
                row.insert(var_name.to_string(), triple.object);
            }
        }
    }

    // Only return the row if we have all 14 features + the tx subject (15 total)
    if row.len() == 15 {
        Ok(vec![row])
    } else {
        Ok(vec![])
    }
}

/// Runs ML prediction for one transaction driven by the ML.PREDICT clause in
/// `rule_ml_fraud_predict()`. The rule is parsed to extract the model name and
/// input/output contract; `execute_ml_prediction_from_clause` handles model
/// discovery and loading automatically.
fn run_ml_predict_from_clause(
    database:        &SparqlDatabase,
    tx_uri:          &str,
    rule_str:        &str,
    ml_handler:      &MLHandler,
    best_model_name: &str,
) -> Result<(f64, Duration, Duration), Box<dyn Error>> {
    let (_, (combined_rule, _)) = parse_standalone_rule(rule_str)
        .map_err(|e| format!("ML.PREDICT rule parse error: {e:?}"))?;

    let ml_predict = combined_rule.ml_predict.as_ref()
        .ok_or("rule_ml_fraud_predict has no ML.PREDICT clause")?;

    // extract closure: decode feature dict IDs → f64 values for this transaction
    let tx_uri_owned = tx_uri.to_string();
    let extract_fn = move |db: &SparqlDatabase| {
        let rows = extract_data_for_ml_fraud(db, &tx_uri_owned)?;
        if rows.is_empty() {
            return Ok(vec![]);
        }
        let dict = db.dictionary.read().unwrap();
        let decoded: Vec<HashMap<String, f64>> = rows.iter()
            .map(|row| {
                row.iter()
                    .filter_map(|(k, &id)| {
                        dict.decode(id)
                            .and_then(|s| s.parse::<f64>().ok())
                            .map(|v| (k.clone(), v))
                    })
                    .collect()
            })
            .collect();
        Ok(decoded)
    };

    // predict closure: build ordered feature vector from clause-declared variable names,
    // then call the ML model; returns score + timing
    let predict_fn = |handler: &MLHandler,
                      best_model: &str,
                      data: &[HashMap<String, f64>],
                      feature_names: &[String]|
     -> Result<(Vec<f64>, MLPredictTiming), Box<dyn Error>> {
        let t_rust_start = Instant::now();

        let features: Vec<Vec<f64>> = data.iter()
            .map(|row| {
                feature_names.iter()
                    .filter_map(|k| row.get(k.as_str()).copied())
                    .collect()
            })
            .collect();

        if features.is_empty() || features[0].is_empty() {
            let timing = MLPredictTiming {
                total_time: 0.0, rust_to_python_time: 0.0,
                python_preprocessing_time: 0.0, actual_prediction_time: 0.0,
                python_postprocessing_time: 0.0, python_to_rust_time: 0.0,
            };
            return Ok((vec![0.0], timing));
        }

        let t_before_call = Instant::now();
        let result = handler.predict(best_model, features)?;
        let t_p2r = t_before_call.elapsed();
        let t_r2p = t_before_call.duration_since(t_rust_start);

        let timing = MLPredictTiming {
            total_time:                  (t_r2p + t_p2r).as_secs_f64(),
            rust_to_python_time:         t_r2p.as_secs_f64(),
            python_preprocessing_time:   0.0,
            actual_prediction_time:      t_p2r.as_secs_f64(),
            python_postprocessing_time:  0.0,
            python_to_rust_time:         0.0,
        };
        Ok((result.predictions, timing))
    };

    let (scores, timing) =
        execute_ml_prediction_with_handler(ml_predict, database, ml_handler, best_model_name, extract_fn, predict_fn)?;

    let score = scores.first().copied().unwrap_or(0.0);
    let t_r2p = Duration::from_secs_f64(timing.rust_to_python_time);
    let t_p2r = Duration::from_secs_f64(timing.actual_prediction_time);
    Ok((score, t_r2p, t_p2r))
}

fn setup_knowledge_base() -> SparqlDatabase {
    let mut db = SparqlDatabase::new();

    db.prefixes.insert("ex".into(),  "http://fraud.example.org/".into());
    db.prefixes.insert("rdf".into(), "http://www.w3.org/1999/02/22-rdf-syntax-ns#".into());

    for (acct, country, tier) in &[
        ("ACC001", "US", "premium"),
        ("ACC002", "US", "standard"),
        ("ACC003", "GB", "premium"),
        ("ACC004", "DE", "standard"),
        ("ACC005", "FR", "standard"),
    ] {
        let uri = format!("http://fraud.example.org/account/{}", acct);
        db.add_triple_parts(
            &uri,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://fraud.example.org/Account",
        );
        db.add_triple_parts(&uri, "http://fraud.example.org/homeCountry", country);
        db.add_triple_parts(&uri, "http://fraud.example.org/tier", tier);
    }

    for flag in &[
        "highVelocity", "largeAmount", "highMerchantRisk", "foreignHighRisk",
        "highWindowActivity", "mlAssistedAlert", "historicalPattern",
    ] {
        db.add_triple_parts(
            &format!("http://fraud.example.org/{}", flag),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://fraud.example.org/SuspiciousFlag",
        );
    }
    db.add_triple_parts(
        "http://fraud.example.org/high",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://fraud.example.org/HighRiskLevel",
    );

    for cat in &["gambling", "cryptoExchange", "wireTransfer", "prepaidCards"] {
        let uri = format!("http://fraud.example.org/merchant/{}", cat);
        db.add_triple_parts(
            &uri,
            "http://fraud.example.org/riskCategory",
            "http://fraud.example.org/HighRisk",
        );
    }

    db
}

fn rule_suspicious_velocity() -> String {
    r#"PREFIX ex: <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :SuspiciousVelocity :-
CONSTRUCT {
    ?tx ex:suspiciousFlag ex:highVelocity .
}
WHERE {
    ?tx ex:velocity1h ?vel .
    FILTER(?vel > 5)
}
    "#.into()
}

fn rule_suspicious_amount() -> String {
    r#"PREFIX ex: <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :SuspiciousAmount :-
CONSTRUCT {
    ?tx ex:suspiciousFlag ex:largeAmount .
}
WHERE {
    ?tx ex:amount ?amt .
    FILTER(?amt > 1000)
}
    "#.into()
}

fn rule_high_merchant_risk() -> String {
    r#"PREFIX ex: <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :HighMerchantRisk :-
CONSTRUCT {
    ?tx ex:suspiciousFlag ex:highMerchantRisk .
}
WHERE {
    ?tx ex:merchantRisk ?mr .
    FILTER(?mr > 70)
}
    "#.into()
}

fn rule_foreign_high_risk() -> String {
    r#"PREFIX ex: <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :ForeignHighRisk :-
CONSTRUCT {
    ?tx ex:suspiciousFlag ex:foreignHighRisk .
}
WHERE {
    ?tx ex:isForeign    ?isF .
    ?tx ex:merchantRisk ?mr  .
    FILTER(?isF > 0)
    FILTER(?mr > 70)
}
    "#.into()
}

fn rule_high_risk_chained() -> String {
    r#"PREFIX ex: <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :HighRisk :-
CONSTRUCT {
    ?tx ex:riskLevel ex:high .
}
WHERE {
    ?tx ex:amount     ?amt .
    ?tx ex:velocity1h ?vel .
    FILTER(?amt > 1000)
    FILTER(?vel > 5)
}
    "#.into()
}

// R6: amplifies a weak ML signal (score > 40/100) when velocity is also
//     elevated, catching cases the raw rules alone would miss
// R7: flags any transaction from an account with a poor verdict history,
//     injecting longitudinal account context into per-transaction decisions
fn rule_ml_assisted_alert() -> String {
    r#"PREFIX ex: <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :MLAssistedAlert :-
CONSTRUCT {
    ?tx ex:suspiciousFlag ex:mlAssistedAlert .
}
WHERE {
    ?tx ex:mlFraudScore ?score .
    ?tx ex:velocity1h   ?vel   .
    FILTER(?score > 40)
    FILTER(?vel > 3)
}
    "#.into()
}

fn rule_historical_pattern() -> String {
    r#"PREFIX ex: <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :HistoricalPattern :-
CONSTRUCT {
    ?tx ex:suspiciousFlag ex:historicalPattern .
}
WHERE {
    ?tx ex:recentFraudCount ?cnt .
    FILTER(?cnt > 1)
}
    "#.into()
}

fn rule_high_window_activity() -> String {
    r#"PREFIX ex: <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :HighWindowActivity :-
CONSTRUCT {
    ?tx ex:suspiciousFlag ex:highWindowActivity .
}
WHERE {
    ?tx ex:windowVelocity ?wvel .
    FILTER(?wvel > 3)
}
    "#.into()
}

fn rule_ml_fraud_predict() -> String {
    r#"PREFIX ex:  <http://fraud.example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

RULE :FraudMLPredict :-
CONSTRUCT {
    ?tx ex:mlFraudScore ?score .
}
WHERE {
    ?tx rdf:type ex:Transaction .
}
ML.PREDICT(MODEL "fraud_predictor",
    INPUT {
        SELECT ?tx ?amt ?hour ?dow ?mRisk ?vel ?dist ?isF ?cp ?fHv ?fLa ?fHmr ?fFhr ?fRh ?cnt
        WHERE {
            ?tx ex:amount              ?amt   ;
                ex:hourOfDay           ?hour  ;
                ex:dayOfWeek           ?dow   ;
                ex:merchantRisk        ?mRisk ;
                ex:velocity1h          ?vel   ;
                ex:distanceKm          ?dist  ;
                ex:isForeign           ?isF   ;
                ex:cardPresent         ?cp    ;
                ex:flagHighVelocity    ?fHv   ;
                ex:flagLargeAmount     ?fLa   ;
                ex:flagHighMerchantRisk ?fHmr ;
                ex:flagForeignHighRisk ?fFhr  ;
                ex:flagRiskHigh        ?fRh   ;
                ex:recentFraudCount    ?cnt
        }
    },
    OUTPUT ?score
)
    "#.into()
}

fn run_simulation(
    engine:          &mut kolibrie::rsp_engine::RSPEngine<Triple, Vec<(String, String)>>,
    database:        &mut SparqlDatabase,
    pass1_rules:     &[Rule],   // R1–R5, R1b: symbolic rules on raw features
    pass2_rules:     &[Rule],   // R6–R7: rules that read ML output from RDF
    window_results:  Arc<Mutex<Vec<HashMap<String, String>>>>,
    events:          EventLog,
    ml_handler:      &MLHandler,
    best_model_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {

    let ml_rule_str = rule_ml_fraud_predict();

    let accounts = vec!["ACC001", "ACC002", "ACC003", "ACC004", "ACC005"];
    let mut velocity_tracker: HashMap<String, u32>    = HashMap::new();
    let mut account_history:  HashMap<String, AccountHistory> = HashMap::new();

    push_event(&events, "start", r#"{"steps":12,"accounts":5}"#);

    for time_step in 0..12_u64 {
        let mut step_txs: Vec<(Transaction, bool, String, String)> = Vec::new();

        for (idx, account) in accounts.iter().enumerate() {
            let (tx, is_injected_fraud) =
                synthesise_transaction(time_step, idx, account, &mut velocity_tracker);

            let tx_uri   = format!("http://fraud.example.org/tx/{}", tx.tx_id);
            let acct_uri = format!("http://fraud.example.org/account/{}", tx.account_id);

            let triples_ttl = format!(
                r#"<{tx}> <http://fraud.example.org/account>      <{acct}> .
                   <{tx}> <http://fraud.example.org/amount>       "{amt}" .
                   <{tx}> <http://fraud.example.org/hourOfDay>    "{hour}" .
                   <{tx}> <http://fraud.example.org/dayOfWeek>    "{dow}" .
                   <{tx}> <http://fraud.example.org/merchantRisk> "{mr}" .
                   <{tx}> <http://fraud.example.org/velocity1h>   "{vel}" .
                   <{tx}> <http://fraud.example.org/distanceKm>   "{dist}" .
                   <{tx}> <http://fraud.example.org/isForeign>    "{isf}" .
                   <{tx}> <http://fraud.example.org/cardPresent>  "{cp}" ."#,
                tx   = tx_uri,
                acct = acct_uri,
                amt  = tx.amount,
                hour = tx.hour_of_day,
                dow  = tx.day_of_week,
                mr   = tx.merchant_risk,
                vel  = tx.velocity_1h,
                dist = tx.distance_km,
                isf  = tx.is_foreign,
                cp   = tx.card_present,
            );
            for triple in engine.parse_data(&triples_ttl) {
                engine.add_to_stream("transactionStream", triple, (time_step * 60) as usize);
            }

            step_txs.push((tx, is_injected_fraud, tx_uri, acct_uri));
        }

        // bindings into window_results (5 × 9 = 45 triples, ~200 ms)
        thread::sleep(Duration::from_millis(200));

        // drain window snapshot, compute per-account
        // windowVelocity, then run the full reasoning pipeline
        let current_window: Vec<HashMap<String, String>> = {
            window_results.lock().unwrap().drain(..).collect()
        };

        // Count transactions per account visible in the current window
        // Strip angle brackets from URIs that the RSP engine may include
        let mut rsp_vel_map: HashMap<String, u32> = HashMap::new();
        for binding in &current_window {
            if let Some(acct_uri) = binding.get("account") {
                let acct_id = acct_uri
                    .trim_matches(|c| c == '<' || c == '>')
                    .rsplit('/')
                    .next()
                    .unwrap_or(acct_uri.as_str())
                    .to_string();
                *rsp_vel_map.entry(acct_id).or_insert(0) += 1;
            }
        }

        let rsp_vel_str = if rsp_vel_map.is_empty() {
            "(window not fired yet)".to_string()
        } else {
            rsp_vel_map.iter()
                .map(|(k, v)| format!("{}:{}", k, v))
                .collect::<Vec<_>>()
                .join("  ")
        };
        println!(
            "  [RSP T={:<2}] window={:>3} bindings | wVel {}",
            time_step, current_window.len(), rsp_vel_str,
        );
        {
            let vel_json: String = rsp_vel_map.iter()
                .map(|(k, v)| format!("\"{}\":{}", k, v))
                .collect::<Vec<_>>()
                .join(",");
            push_event(&events, "rsp", &format!(
                r#"{{"time_step":{},"window":{},"vel_map":{{{}}}}}"#,
                time_step, current_window.len(), vel_json
            ));
        }

        for (tx, is_injected_fraud, tx_uri, acct_uri) in &step_txs {

            // windowVelocity: RSP-observed count, not the event-embedded velocity_1h
            let window_vel = rsp_vel_map
                .get(tx.account_id.as_str())
                .copied()
                .unwrap_or(0);

            // Write transaction features and RSP-derived window velocity to RDF
            database.add_triple_parts(&tx_uri,
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://fraud.example.org/Transaction");
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/account",      acct_uri);
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/amount",        &tx.amount.to_string());
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/hourOfDay",     &tx.hour_of_day.to_string());
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/dayOfWeek",     &tx.day_of_week.to_string());
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/merchantRisk",  &tx.merchant_risk.to_string());
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/velocity1h",    &tx.velocity_1h.to_string());
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/distanceKm",    &tx.distance_km.to_string());
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/isForeign",     &tx.is_foreign.to_string());
            database.add_triple_parts(&tx_uri, "http://fraud.example.org/cardPresent",   &tx.card_present.to_string());
            
            // Bridge triple: connects RSP stream layer to Datalog (consumed by R1b)
            database.add_triple_parts(
                &tx_uri,
                "http://fraud.example.org/windowVelocity",
                &window_vel.to_string(),
            );

            let history = account_history
                .entry(tx.account_id.clone())
                .or_insert_with(AccountHistory::new);
            let hist_fraud_count = history.recent_fraud_count;

            // Pass 1: symbolic rules on raw features
            let flags_p1 = run_reasoning(database, pass1_rules, &tx_uri);

            // Write Pass-1 flags and account history as numeric RDF triples
            // so ML.PREDICT INPUT can SELECT them as features
            write_numeric_flags_to_db(database, &tx_uri, &flags_p1, hist_fraud_count);

            // ML.PREDICT — driven by rule_ml_fraud_predict() clause
            let (fraud_score, t_r2p, t_p2r) =
                run_ml_predict_from_clause(database, &tx_uri, &ml_rule_str, ml_handler, best_model_name)?;
            println!("  [Timing] Rust -> Python overhead: {:?}", t_r2p);
            println!("  [Timing] Python -> Rust overhead: {:?}", t_p2r);

            // Write ML score back to RDF for Pass-2 Datalog rules (R6, R7)
            let ml_score_int = (fraud_score * 100.0).round() as u32;
            database.add_triple_parts(
                &tx_uri, "http://fraud.example.org/mlFraudScore",
                &ml_score_int.to_string());

            let flags_p2_all = run_reasoning(database, pass2_rules, &tx_uri);
            let flags_p2_new: Vec<String> = flags_p2_all
                .into_iter()
                .filter(|f| !flags_p1.contains(f))
                .collect();

            let mut all_flags = flags_p1.clone();
            for f in &flags_p2_new {
                if !all_flags.contains(f) { all_flags.push(f.clone()); }
            }

            let verdict = fuse_decision(fraud_score, &all_flags);

            account_history
                .get_mut(tx.account_id.as_str())
                .unwrap()
                .update(fraud_score, &verdict);

            let p1_str = if flags_p1.is_empty() { "-".to_string() }
                         else { flags_p1.join(", ") };
            let p2_str = if flags_p2_new.is_empty() { String::new() }
                         else { format!(" -> {}", flags_p2_new.join(", ")) };
            let flag_str = format!("{}{}", p1_str, p2_str);
            let hist_str = format!("h:{}", hist_fraud_count);

            println!(
                "{:<4} {:<18} {:<7} {:>8.2} {:>5} {:>5} {:>4} {:>7.1} {:>6} {:>4} {:>3} {:>6} | {:>7.3} | {:<50} | {}{}",
                time_step,
                &tx.tx_id[..tx.tx_id.len().min(18)],
                tx.account_id,
                tx.amount,
                tx.hour_of_day,
                tx.velocity_1h,
                window_vel,
                tx.distance_km,
                tx.merchant_risk,
                tx.is_foreign,
                tx.card_present,
                hist_str,
                fraud_score,
                flag_str,
                verdict,
                if *is_injected_fraud { "  <- injected fraud" } else { "" },
            );

            {
                let p1_json = flags_p1.iter()
                    .map(|f| format!("\"{}\"", f.replace('"', "\\\"")))
                    .collect::<Vec<_>>().join(",");
                let p2_json = flags_p2_new.iter()
                    .map(|f| format!("\"{}\"", f.replace('"', "\\\"")))
                    .collect::<Vec<_>>().join(",");
                push_event(&events, "tx", &format!(
                    r#"{{"time_step":{},"tx_id":"{}","account_id":"{}","amount":{:.2},"fraud_score":{:.6},"flags_p1":[{}],"flags_p2":[{}],"verdict":"{}","r2p_us":{},"p2r_us":{},"injected":{}}}"#,
                    time_step,
                    tx.tx_id.replace('"', "\\\""),
                    tx.account_id,
                    tx.amount,
                    fraud_score,
                    p1_json, p2_json,
                    verdict,
                    t_r2p.as_micros(),
                    t_p2r.as_micros(),
                    is_injected_fraud,
                ));
            }
        }
    }

    push_event(&events, "done", "{}");
    Ok(())
}

fn run_reasoning(
    database: &mut SparqlDatabase,
    rules:    &[Rule],
    tx_uri:   &str,
) -> Vec<String> {
    let decoded_triples: Vec<(String, String, String)> = {
        let dict = database.dictionary.read().unwrap();
        database.triples.iter()
            .filter_map(|t| Some((
                dict.decode(t.subject)?.to_string(),
                dict.decode(t.predicate)?.to_string(),
                dict.decode(t.object)?.to_string(),
            )))
            .collect()
    };

    let mut reasoner = Reasoner::new();
    reasoner.dictionary = database.dictionary.clone();
    for (s, p, o) in decoded_triples {
        reasoner.add_abox_triple(&s, &p, &o);
    }
    for rule in rules {
        reasoner.add_rule(rule.clone());
    }

    let inferred = reasoner.infer_new_facts_semi_naive();

    for fact in &inferred {
        let decoded = {
            let d = reasoner.dictionary.read().unwrap();
            match (d.decode(fact.subject), d.decode(fact.predicate), d.decode(fact.object)) {
                (Some(s), Some(p), Some(o)) => Some((s.to_string(), p.to_string(), o.to_string())),
                _ => None,
            }
        };
        if let Some((s, p, o)) = decoded {
            database.add_triple_parts(&s, &p, &o);
        }
    }

    let suspicious_pred = "http://fraud.example.org/suspiciousFlag";
    let risk_pred       = "http://fraud.example.org/riskLevel";
    let mut flags       = Vec::new();

    {
        let dict      = database.dictionary.read().unwrap();
        let tx_id_opt = dict.string_to_id.get(tx_uri).copied();
        let sflag_opt = dict.string_to_id.get(suspicious_pred).copied();
        let risk_opt  = dict.string_to_id.get(risk_pred).copied();
        drop(dict);

        if let (Some(tx_id), Some(sflag_id)) = (tx_id_opt, sflag_opt) {
            let dict = database.dictionary.read().unwrap();
            for t in database.triples.iter() {
                if t.subject == tx_id && t.predicate == sflag_id {
                    if let Some(val) = dict.decode(t.object) {
                        let local = val.rsplit('/').next().unwrap_or(val);
                        flags.push(local.to_string());
                    }
                }
            }
        }

        if let (Some(tx_id), Some(risk_id)) = (tx_id_opt, risk_opt) {
            let dict = database.dictionary.read().unwrap();
            for t in database.triples.iter() {
                if t.subject == tx_id && t.predicate == risk_id {
                    if let Some(val) = dict.decode(t.object) {
                        let local = val.rsplit('/').next().unwrap_or(val);
                        flags.push(format!("risk:{}", local));
                    }
                }
            }
        }
    }

    flags.dedup();
    flags
}

fn fuse_decision(fraud_score: f64, symbolic_flags: &[String]) -> Verdict {
    let high_risk      = symbolic_flags.iter().any(|f| f == "risk:high");
    let any_suspicious = !symbolic_flags.is_empty();

    if fraud_score > 0.80 {
        Verdict::Fraud
    } else if fraud_score > 0.50 && high_risk {
        Verdict::Fraud
    } else if high_risk {
        Verdict::Suspicious
    } else if fraud_score > 0.60 || any_suspicious {
        Verdict::Review
    } else {
        Verdict::Clear
    }
}

// 
// Pseudo-random number generator (splitmix64 finalisation mix)
//
// Produces a well-distributed f64 in [0.0, 1.0) from two u64 seeds.
// Different seed constants per feature slot ensure independence.
fn prng(a: u64, b: u64) -> f64 {
    let mut x = a
        .wrapping_mul(0x9e3779b97f4a7c15)
        .wrapping_add(b.wrapping_mul(0x6c62272e07bb0142));
    x ^= x >> 30;
    x  = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x  = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    (x >> 11) as f64 / (1u64 << 53) as f64
}

// 
// Stochastic transaction synthesis
//
// Scenario distribution (seed-determined, no hardcoded patterns):
//   [0.00, 0.25) -> FRAUD      late-night, foreign, CNP, high-risk merchant,
//                              large amount, velocity burst -> ML ≈ 1.0
//   [0.25, 0.45) -> SUSPICIOUS large amount + high velocity (R5 -> risk:high),
//                              but domestic / daytime / card-present -> ML ≈ 0.0
//   [0.45, 0.65) -> REVIEW     one flag fires (highVelocity or highMerchantRisk),
//                              amount < 1000 so R5 cannot chain -> no risk:high
//   [0.65, 1.00) -> CLEAR      all features below every rule threshold -> ML ≈ 0.0
//
// is_injected_fraud is true only for the FRAUD scenario, keeping the console
// marker "← injected fraud" unambiguous.
fn synthesise_transaction(
    time_step:        u64,
    account_idx:      usize,
    account:          &str,
    velocity_tracker: &mut HashMap<String, u32>,
) -> (Transaction, bool) {

    let vel_entry = velocity_tracker.entry(account.to_string()).or_insert(0);
    let scenario  = prng(time_step.wrapping_mul(31), account_idx as u64 + 1);

    let (tx, is_fraud) = if scenario < 0.25 {
        // FRAUD: all signals active
        *vel_entry = (*vel_entry + 4).min(15);
        let vel = *vel_entry;
        let amount      = 1_200.0 + prng(time_step, account_idx as u64 + 100) * 8_000.0;
        let hour        = (prng(time_step, account_idx as u64 + 101) * 4.0) as u8;        // 0–3
        let dow         = (prng(time_step, account_idx as u64 + 102) * 7.0) as u8;
        let merch_risk  = 75 + (prng(time_step, account_idx as u64 + 103) * 20.0) as u32; // 75–95
        let distance_km = 1_000.0 + prng(time_step, account_idx as u64 + 104) * 9_000.0;
        let tx = Transaction {
            tx_id:         format!("TX-{}-{}-FRAUD", account, time_step),
            account_id:    account.to_string(),
            amount, hour_of_day: hour, day_of_week: dow,
            merchant_risk: merch_risk, velocity_1h: vel,
            distance_km, is_foreign: 1, card_present: 0,
        };
        (tx, true)

    } else if scenario < 0.45 {
        // SUSPICIOUS: R5 fires (amount > 1000 ∧ vel > 5 -> risk:high),
        // but domestic context keeps ML ≈ 0.0 -> fuse: risk:high ∧ P ≤ 0.50.
        let vel = 6 + (prng(time_step, account_idx as u64 + 200) * 5.0) as u32; // 6–10
        *vel_entry = vel;
        let amount      = 1_050.0 + prng(time_step, account_idx as u64 + 201) * 2_000.0;
        let hour        = 9 + (prng(time_step, account_idx as u64 + 202) * 9.0) as u8;    // 9–17
        let dow         = (prng(time_step, account_idx as u64 + 203) * 5.0) as u8;         // Mon–Fri
        let merch_risk  = 15 + (prng(time_step, account_idx as u64 + 204) * 45.0) as u32; // 15–60
        let distance_km = 2.0 + prng(time_step, account_idx as u64 + 205) * 30.0;
        let tx = Transaction {
            tx_id:         format!("TX-{}-{}-SUSP", account, time_step),
            account_id:    account.to_string(),
            amount, hour_of_day: hour, day_of_week: dow,
            merchant_risk: merch_risk, velocity_1h: vel,
            distance_km, is_foreign: 0, card_present: 1,
        };
        (tx, false)

    } else if scenario < 0.65 {
        // REVIEW: one flag fires but amount < 1000 prevents R5 from chaining
        // Sub-variant a: highMerchantRisk only or Sub-variant b: highVelocity only
        let sub = prng(time_step, account_idx as u64 + 300);
        let (merch_risk, vel) = if sub < 0.5 {
            *vel_entry = (*vel_entry).saturating_sub(1).min(4);
            let mr = 71 + (prng(time_step, account_idx as u64 + 301) * 24.0) as u32; // 71–95
            (mr, *vel_entry)
        } else {
            let v = 6 + (prng(time_step, account_idx as u64 + 302) * 5.0) as u32;    // 6–10
            *vel_entry = v;
            let mr = 10 + (prng(time_step, account_idx as u64 + 303) * 45.0) as u32; // 10–55
            (mr, v)
        };
        let amount      = 20.0 + prng(time_step, account_idx as u64 + 304) * 920.0;  // < 1000
        let hour        = 8 + (prng(time_step, account_idx as u64 + 305) * 12.0) as u8;
        let dow         = (prng(time_step, account_idx as u64 + 306) * 7.0) as u8;
        let distance_km = 1.0 + prng(time_step, account_idx as u64 + 307) * 40.0;
        let tx = Transaction {
            tx_id:         format!("TX-{}-{}-REVIEW", account, time_step),
            account_id:    account.to_string(),
            amount, hour_of_day: hour, day_of_week: dow,
            merchant_risk: merch_risk, velocity_1h: vel,
            distance_km, is_foreign: 0, card_present: 1,
        };
        (tx, false)

    } else {
        // CLEAR: all features below every rule threshold
        *vel_entry = (*vel_entry).saturating_sub(1);
        let vel         = *vel_entry;
        let amount      = 10.0 + prng(time_step, account_idx as u64 + 400) * 600.0;  // < 1000
        let hour        = 8 + (prng(time_step, account_idx as u64 + 401) * 13.0) as u8;
        let dow         = (prng(time_step, account_idx as u64 + 402) * 7.0) as u8;
        let merch_risk  = 5  + (prng(time_step, account_idx as u64 + 403) * 50.0) as u32;
        let distance_km = 1.0 + prng(time_step, account_idx as u64 + 404) * 30.0;
        let card_pres   = if prng(time_step, account_idx as u64 + 405) > 0.1 { 1 } else { 0 };
        let tx = Transaction {
            tx_id:         format!("TX-{}-{}", account, time_step),
            account_id:    account.to_string(),
            amount, hour_of_day: hour, day_of_week: dow,
            merchant_risk: merch_risk, velocity_1h: vel,
            distance_km, is_foreign: 0, card_present: card_pres,
        };
        (tx, false)
    };

    (tx, is_fraud)
}