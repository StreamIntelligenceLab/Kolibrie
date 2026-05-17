/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use shared::dictionary::Dictionary;
use datalog::cross_window_sds::{
    Sds, WindowData, WindowedTriple,
    annotate_predicate, strip_window_prefix, all_component_iris,
    translate_sds_to_datalog, sds_with_expiry_to_external,
};
use datalog::reasoning::materialisation::cross_window_naive::naive_sds_plus;
use datalog::reasoning::materialisation::cross_window_incremental::{
    incremental_sds_plus, SdsWithExpiry,
};
use datalog::reasoning::Reasoner;
use datalog::parser_n3_logic::{parse_n3_document, parse_n3_rules_for_sds};

fn make_dict() -> Arc<RwLock<Dictionary>> {
    Arc::new(RwLock::new(Dictionary::new()))
}

/// Standard two-window SDS for the sensor+map scenario
fn make_sds() -> Sds {
    let mut sds = Sds::new();
    sds.windows.insert(
        "http://sensor/".to_string(),
        WindowData {
            alpha: 10,
            triples: vec![WindowedTriple {
                subject: "sensorA".to_string(),
                predicate: "reading".to_string(),
                object: "25".to_string(),
                event_time: 5,
            }],
        },
    );
    sds.windows.insert(
        "http://map/".to_string(),
        WindowData {
            alpha: 20,
            triples: vec![WindowedTriple {
                subject: "sensorA".to_string(),
                predicate: "location".to_string(),
                object: "room1".to_string(),
                event_time: 3,
            }],
        },
    );
    sds.output_iris.insert("http://result/".to_string());
    sds
}

const RULE_N3: &str = r#"
@prefix ws: <http://sensor/> .
@prefix wm: <http://map/> .
@prefix wr: <http://result/> .
{ ?s ws:reading ?v . ?s wm:location ?loc } => { ?s wr:hotspot ?loc }
"#;

/// Parse rules from N3, using the shared dict
fn parse_rules(dict: &Arc<RwLock<Dictionary>>) -> Vec<shared::rule::Rule> {
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(dict);
    let window_widths: HashMap<String, u64> = [
        ("http://sensor/".to_string(), 10u64),
        ("http://map/".to_string(), 20u64),
    ]
    .into();
    let (rules, _ctx) =
        parse_n3_rules_for_sds(RULE_N3, &mut reasoner, window_widths).unwrap();
    rules
}

/// Collect triple predicates decoded as strings for a component bucket
fn pred_strings(
    result: &HashMap<String, Vec<shared::triple::Triple>>,
    comp: &str,
    dict: &Arc<RwLock<Dictionary>>,
) -> HashSet<String> {
    result
        .get(comp)
        .map(|triples| {
            triples
                .iter()
                .filter_map(|t| dict.read().unwrap().decode(t.predicate).map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn window_widths() -> HashMap<String, u64> {
    [
        ("http://sensor/".to_string(), 10u64),
        ("http://map/".to_string(), 20u64),
    ]
    .into()
}

#[test]
fn test_annotate_strip_roundtrip() {
    let w = "http://sensor/";
    let l = "reading";
    let annotated = annotate_predicate(w, l);
    let iris = vec![w.to_string()];
    let result = strip_window_prefix(&annotated, &iris);
    assert_eq!(result, Some((w, l)));
}

#[test]
fn test_translate_filters_expired() {
    let dict = make_dict();
    let sds = make_sds();
    let translated = translate_sds_to_datalog(&sds, &dict, 15);
    // Sensor fact (expiry=15) must be filtered at current_time=15
    let expiry15_present = translated.iter().any(|(_, e)| *e == 15);
    assert!(!expiry15_present, "expired fact (expiry=15 at current_time=15) should be filtered");
    // Map fact (expiry=23) should still be present
    let alive = translated.iter().any(|(_, e)| *e == 23);
    assert!(alive, "alive map fact (expiry=23) should remain");
}

#[test]
fn test_translate_includes_alive() {
    let dict = make_dict();
    let sds = make_sds();
    let translated = translate_sds_to_datalog(&sds, &dict, 14);
    assert_eq!(translated.len(), 2);
    let expiries: HashSet<u64> = translated.iter().map(|(_, e)| *e).collect();
    assert!(expiries.contains(&15));
    assert!(expiries.contains(&23));
}

#[test]
fn test_parser_accepts_missing_final_conclusion_dot() {
    let dict = make_dict();
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(&dict);

    let (rules, ctx) =
        parse_n3_rules_for_sds(RULE_N3, &mut reasoner, window_widths()).unwrap();

    assert_eq!(rules.len(), 1);
    assert!(ctx.all_component_iris.contains(&"http://result/".to_string()));
}

#[test]
fn test_parser_shared_prefixes_apply_to_multiple_rules() {
    let dict = make_dict();
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(&dict);

    let input = r#"
@prefix ws: <http://sensor/> .
@prefix wr: <http://result/> .
{ ?s ws:reading ?v } => { ?s wr:first ?v }
{ ?s wr:first ?v } => { ?s wr:second ?v }
"#;

    let (rest, (_prefixes, rules)) = parse_n3_document(input, &mut reasoner).unwrap();
    assert!(rest.trim().is_empty());
    assert_eq!(rules.len(), 2);
}

#[test]
fn test_parser_rejects_leftover_non_whitespace() {
    let dict = make_dict();
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(&dict);

    let invalid = format!("{}\nthis is not a rule", RULE_N3);
    let err = parse_n3_rules_for_sds(&invalid, &mut reasoner, window_widths());

    assert!(err.is_err(), "parser must reject non-whitespace leftovers");
}

#[test]
fn test_naive_produces_hotspot() {
    let dict = make_dict();
    let rules = parse_rules(&dict);
    let sds = make_sds();
    let result = naive_sds_plus(&rules, &sds, &dict, 10);

    assert!(
        result.contains_key("http://result/"),
        "naive must produce facts in http://result/ bucket"
    );
    let preds = pred_strings(&result, "http://result/", &dict);
    assert!(preds.contains("hotspot"), "naive must derive hotspot; got: {:?}", preds);
}

#[test]
fn test_naive_incremental_agree() {
    let dict = make_dict();
    let rules = parse_rules(&dict);
    let sds = make_sds();
    let current_time = 10u64;

    let naive_result = naive_sds_plus(&rules, &sds, &dict, current_time);

    let empty_old: SdsWithExpiry = HashMap::new();
    let incr_internal = incremental_sds_plus(&rules, &sds, &empty_old, &dict, current_time);
    let component_iris = all_component_iris(&sds);
    let incr_result = sds_with_expiry_to_external(&incr_internal, &dict, &component_iris);

    // Compare triple sets (predicate strings) per component
    let naive_preds = pred_strings(&naive_result, "http://result/", &dict);
    let incr_preds = pred_strings(&incr_result, "http://result/", &dict);
    assert_eq!(
        naive_preds, incr_preds,
        "naive and incremental must produce the same predicate set in http://result/"
    );
}

#[test]
fn test_incremental_expiration_times() {
    let dict = make_dict();
    let rules = parse_rules(&dict);
    let sds = make_sds();
    let current_time = 10u64;

    let empty_old: SdsWithExpiry = HashMap::new();
    let result = incremental_sds_plus(&rules, &sds, &empty_old, &dict, current_time);

    let result_bucket = result.get("http://result/").expect("http://result/ bucket missing");
    assert!(!result_bucket.is_empty(), "hotspot triple must be in result bucket");

    for (_triple, &expiry) in result_bucket.iter() {
        assert_eq!(
            expiry, 15,
            "hotspot expiry must be min(15,23)=15; got {}",
            expiry
        );
    }
}

#[test]
fn test_incremental_after_sensor_expiry() {
    let dict = make_dict();
    let rules = parse_rules(&dict);
    let sds = make_sds();

    let empty_old: SdsWithExpiry = HashMap::new();
    let sds_plus_old = incremental_sds_plus(&rules, &sds, &empty_old, &dict, 10);

    let result = incremental_sds_plus(&rules, &sds, &sds_plus_old, &dict, 15);

    let hotspot_present = result
        .get("http://result/")
        .map(|m| !m.is_empty())
        .unwrap_or(false);
    assert!(
        !hotspot_present,
        "hotspot must not exist after sensor window expires"
    );
}

#[test]
fn test_incremental_map_fact_survives() {
    let dict = make_dict();
    let rules = parse_rules(&dict);
    let sds = make_sds();

    let empty_old: SdsWithExpiry = HashMap::new();
    let sds_plus_old = incremental_sds_plus(&rules, &sds, &empty_old, &dict, 10);

    let result = incremental_sds_plus(&rules, &sds, &sds_plus_old, &dict, 15);

    let map_alive = result
        .get("http://map/")
        .map(|m| m.values().any(|&e| e > 15))
        .unwrap_or(false);
    assert!(map_alive, "map fact (expiry=23) must still be alive at t=15");
}

#[test]
fn test_expiry_chain_propagation() {
    let dict = make_dict();
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(&dict);

    let mut sds = Sds::new();
    sds.windows.insert(
        "http://a/".to_string(),
        WindowData {
            alpha: 10,
            triples: vec![WindowedTriple {
                subject: "x".to_string(),
                predicate: "p".to_string(),
                object: "y".to_string(),
                event_time: 5,
            }],
        },
    );
    sds.output_iris.insert("http://b/".to_string());
    sds.output_iris.insert("http://c/".to_string());

    let chain_n3 = r#"
@prefix wa: <http://a/> .
@prefix wb: <http://b/> .
@prefix wc: <http://c/> .
{ ?s wa:p ?o } => { ?s wb:q ?o }
{ ?s wb:q ?o } => { ?s wc:r ?o }
"#;
    let window_widths: HashMap<String, u64> = [("http://a/".to_string(), 10u64)].into();
    let (rules, _ctx) =
        parse_n3_rules_for_sds(chain_n3, &mut reasoner, window_widths).unwrap();

    let empty_old: SdsWithExpiry = HashMap::new();
    let sds_plus_old = incremental_sds_plus(&rules, &sds, &empty_old, &dict, 0);

    let c_expiry_old = sds_plus_old
        .get("http://c/")
        .and_then(|m| m.values().copied().next())
        .unwrap_or(0);
    assert_eq!(c_expiry_old, 15, "initial C expiry should be 15");

    sds.windows
        .get_mut("http://a/")
        .unwrap()
        .triples
        .push(WindowedTriple {
            subject: "x".to_string(),
            predicate: "p".to_string(),
            object: "y".to_string(),
            event_time: 12,
        });

    let sds_plus_new = incremental_sds_plus(&rules, &sds, &sds_plus_old, &dict, 1);

    let c_expiry_new = sds_plus_new
        .get("http://c/")
        .and_then(|m| m.values().copied().max())
        .unwrap_or(0);
    assert_eq!(
        c_expiry_new, 22,
        "after adding fresher A fact, C expiry must propagate to 22; got {}",
        c_expiry_new
    );
}

#[test]
fn test_parse_document_shared_prefixes_multiple_rules() {
    let dict = make_dict();
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(&dict);

    let document = r#"
@prefix a: <http://a/> .
@prefix b: <http://b/> .
@prefix c: <http://c/> .
{ ?s a:p ?o } => { ?s b:q ?o }
{ ?s b:q ?o } => { ?s c:r ?o }
"#;

    let (_, (prefixes, rules)) = parse_n3_document(document, &mut reasoner).unwrap();
    assert_eq!(prefixes.get("a"), Some(&"http://a/".to_string()));
    assert_eq!(rules.len(), 2);
}

#[test]
fn test_parse_missing_conclusion_dot_succeeds() {
    let dict = make_dict();
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(&dict);

    let document = r#"
@prefix a: <http://a/> .
@prefix b: <http://b/> .
{ ?s a:p ?o . } => { ?s b:q ?o }
"#;

    let parsed = parse_n3_document(document, &mut reasoner);
    assert!(
        parsed.is_ok(),
        "final conclusion triple should not require a trailing dot: {:?}",
        parsed
    );
}

#[test]
fn test_parse_document_rejects_leftover_input() {
    let dict = make_dict();
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(&dict);

    let document = r#"
@prefix a: <http://a/> .
@prefix b: <http://b/> .
{ ?s a:p ?o } => { ?s b:q ?o }
this is not a rule
"#;

    assert!(
        parse_n3_document(document, &mut reasoner).is_err(),
        "document parser must reject non-whitespace leftover input"
    );
}
