/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::dictionary::Dictionary;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Canonical annotated predicate: `window_iri + local_name`
pub fn annotate_predicate(window_iri: &str, local_name: &str) -> String {
    format!("{}{}", window_iri, local_name)
}

/// Strip a known component IRI prefix from an annotated predicate
pub fn strip_window_prefix<'a, 'b>(
    annotated: &'b str,
    known_iris: &'a [String],
) -> Option<(&'a str, &'b str)> {
    for iri in known_iris {
        if let Some(local) = annotated.strip_prefix(iri.as_str()) {
            return Some((iri.as_str(), local));
        }
    }
    None
}

/// A single RDF triple from a streaming window, carrying its stream-level timestamp
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowedTriple {
    pub subject: String,
    /// Local predicate name under the owning window IRI.  NOT a full IRI
    pub predicate: String,
    pub object: String,
    /// `t_d`: the timestamp at which this triple arrived in the stream
    pub event_time: u64,
}

/// Configuration and content for one sliding window
#[derive(Clone, Debug)]
pub struct WindowData {
    /// Window width α (in the same time unit as event_time)
    pub alpha: u64,
    pub triples: Vec<WindowedTriple>,
}

/// An RSP-QL Streaming Dataset (SDS) at a point in time
#[derive(Clone, Debug, Default)]
pub struct Sds {
    pub windows: HashMap<String, WindowData>,
    pub static_graphs: HashMap<String, Vec<(String, String, String)>>,
    pub output_iris: HashSet<String>,
}

impl Sds {
    pub fn new() -> Self {
        Self::default()
    }
}

/// All component IRIs in the SDS, sorted longest-first for prefix matching
pub fn all_component_iris(sds: &Sds) -> Vec<String> {
    let mut iris: Vec<String> = sds
        .windows
        .keys()
        .cloned()
        .chain(sds.static_graphs.keys().cloned())
        .chain(sds.output_iris.iter().cloned())
        .collect();
    iris.sort_by(|a, b| b.len().cmp(&a.len()));
    iris
}

/// Translate the alive facts of an SDS at `current_time` into annotated
/// datalog triples with per-fact expiration times
pub fn translate_sds_to_datalog(
    sds: &Sds,
    dict: &Arc<RwLock<Dictionary>>,
    current_time: u64,
) -> Vec<(Triple, u64)> {
    let mut result = Vec::new();

    for (window_iri, window_data) in &sds.windows {
        for wt in &window_data.triples {
            let expiry = wt.event_time.saturating_add(window_data.alpha);
            if expiry <= current_time {
                continue;
            }
            let annotated_pred = annotate_predicate(window_iri, &wt.predicate);
            let (s, p, o) = {
                let mut d = dict.write().unwrap();
                let s = d.encode(&wt.subject);
                let p = d.encode(&annotated_pred);
                let o = d.encode(&wt.object);
                (s, p, o)
            };
            result.push((Triple { subject: s, predicate: p, object: o }, expiry));
        }
    }

    for (static_graph_iri, triples) in &sds.static_graphs {
        for (s_str, p_str, o_str) in triples {
            let annotated_pred = annotate_predicate(static_graph_iri, p_str);
            let (s, p, o) = {
                let mut d = dict.write().unwrap();
                let s = d.encode(s_str);
                let p = d.encode(&annotated_pred);
                let o = d.encode(o_str);
                (s, p, o)
            };
            result.push((Triple { subject: s, predicate: p, object: o }, u64::MAX));
        }
    }

    result
}

/// External / user-facing translation: strip window-IRI prefixes from
/// annotated predicates and route each triple to its component bucket
pub fn translate_datalog_back(
    facts: &[Triple],
    dict: &Arc<RwLock<Dictionary>>,
    sds: &Sds,
) -> HashMap<String, Vec<Triple>> {
    let component_iris = all_component_iris(sds);
    let mut result: HashMap<String, Vec<Triple>> = HashMap::new();

    for triple in facts {
        let pred_str = match dict.read().unwrap().decode(triple.predicate) {
            Some(s) => s.to_string(),
            None => continue,
        };

        if let Some((comp_iri, local_name)) = strip_window_prefix(&pred_str, &component_iris) {
            let stripped_pred = dict.write().unwrap().encode(local_name);
            let stripped = Triple {
                subject: triple.subject,
                predicate: stripped_pred,
                object: triple.object,
            };
            result.entry(comp_iri.to_string()).or_default().push(stripped);
        }
    }

    result
}

/// External view for the incremental `SdsWithExpiry` state
pub fn sds_with_expiry_to_external(
    internal: &HashMap<String, HashMap<Triple, u64>>,
    dict: &Arc<RwLock<Dictionary>>,
    component_iris: &[String],
) -> HashMap<String, Vec<Triple>> {
    let mut result: HashMap<String, Vec<Triple>> = HashMap::new();

    for (comp_iri, fact_map) in internal {
        for (triple, _expiry) in fact_map {
            let pred_str = match dict.read().unwrap().decode(triple.predicate) {
                Some(s) => s.to_string(),
                None => continue,
            };

            if let Some((_matched, local_name)) = strip_window_prefix(&pred_str, component_iris) {
                let stripped_pred = dict.write().unwrap().encode(local_name);
                let stripped = Triple {
                    subject: triple.subject,
                    predicate: stripped_pred,
                    object: triple.object,
                };
                result.entry(comp_iri.clone()).or_default().push(stripped);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};
    use shared::dictionary::Dictionary;

    fn make_dict() -> Arc<RwLock<Dictionary>> {
        Arc::new(RwLock::new(Dictionary::new()))
    }

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
        sds.output_iris.insert("http://result/".to_string());
        sds
    }

    #[test]
    fn annotate_strip_roundtrip() {
        let w = "http://sensor/";
        let l = "reading";
        let annotated = annotate_predicate(w, l);
        let iris = vec![w.to_string()];
        let result = strip_window_prefix(&annotated, &iris);
        assert_eq!(result, Some((w, l)));
    }

    #[test]
    fn strip_longest_prefix_wins() {
        let short = "http://w/";
        let long = "http://w/longer/";
        let annotated = "http://w/longer/pred";
        // sorted longest-first
        let iris = vec![long.to_string(), short.to_string()];
        let result = strip_window_prefix(annotated, &iris);
        assert_eq!(result, Some((long, "pred")));
    }

    #[test]
    fn translate_filters_expired() {
        let dict = make_dict();
        let sds = make_sds(); // event_time=5, alpha=10 → expiry=15
        let translated = translate_sds_to_datalog(&sds, &dict, 15); // current_time=15
        // expiry=15 is NOT > 15, so filtered out
        assert!(translated.is_empty(), "fact with expiry=15 at current_time=15 should be filtered");
    }

    #[test]
    fn translate_includes_alive() {
        let dict = make_dict();
        let sds = make_sds(); // expiry=15
        let translated = translate_sds_to_datalog(&sds, &dict, 14); // current_time=14
        assert_eq!(translated.len(), 1);
        assert_eq!(translated[0].1, 15);
    }

    #[test]
    fn translate_static_gets_max_expiry() {
        let dict = make_dict();
        let mut sds = Sds::new();
        sds.static_graphs.insert(
            "g".to_string(),
            vec![("a".to_string(), "b".to_string(), "c".to_string())],
        );
        let translated = translate_sds_to_datalog(&sds, &dict, 999);
        assert_eq!(translated.len(), 1);
        assert_eq!(translated[0].1, u64::MAX);
    }

    #[test]
    fn translate_back_strips_predicate() {
        let dict = make_dict();
        let sds = make_sds();
        let translated = translate_sds_to_datalog(&sds, &dict, 0);
        let back = translate_datalog_back(
            &translated.iter().map(|(t, _)| t.clone()).collect::<Vec<_>>(),
            &dict,
            &sds,
        );
        assert!(back.contains_key("http://sensor/"));
        let triples = &back["http://sensor/"];
        assert_eq!(triples.len(), 1);
        // predicate should be stripped to "reading"
        let pred_str = dict.read().unwrap().decode(triples[0].predicate).unwrap().to_string();
        assert_eq!(pred_str, "reading");
    }
}
