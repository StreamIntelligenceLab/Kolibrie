/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Drive the datalogmtl engine from a MeTeoR-syntax program + data file and
//! print the full materialization as coalesced `Pred(args)@[l,r]` lines, so the
//! output can be diffed against the MeTeoR reference reasoner.
//!
//! Usage:
//!   meteor_compare --program <file> --data <file> [--horizon T] [--store snapshot|interval]
//!
//! Facts with interval `[l,r]` are densified onto the integer grid (inserted at
//! every tick in `[l,r]`). The engine is advanced over ticks `0..=T`; at each
//! tick every fact holding in the store is recorded, then coalesced on output.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use shared::dictionary::Dictionary;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;

use datalogmtl::evaluator::{compute_w_max, DatalogMTLEvaluator};
use datalogmtl::meteor_fmt::format_tick_sets;
use datalogmtl::parser::{parse_data, parse_program, TemporalFact, RDF_TYPE};
use datalogmtl::store::{IntervalFactStore, TemporalSnapshotStore, TemporalStore};
use datalogmtl::syntax::{DatalogMTLRule, TemporalAtom};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut program_path = None;
    let mut data_path = None;
    let mut horizon_override: Option<u64> = None;
    let mut store_kind = "snapshot".to_string();
    let mut timing = false;
    let mut skip_empty = true;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--program" => { program_path = args.get(i + 1).cloned(); i += 2; }
            "--data" => { data_path = args.get(i + 1).cloned(); i += 2; }
            "--horizon" => {
                horizon_override = args.get(i + 1).and_then(|s| s.parse().ok());
                i += 2;
            }
            "--store" => { store_kind = args.get(i + 1).cloned().unwrap_or_default(); i += 2; }
            // Print timing + fact/derivation counts as JSON instead of the full
            // materialization (for the performance harness).
            "--timing" => { timing = true; i += 1; }
            // Disable skipping provably-empty ticks (for A/B comparison).
            "--no-skip" => { skip_empty = false; i += 1; }
            other => { eprintln!("unknown argument: {}", other); std::process::exit(2); }
        }
    }

    let program_path = program_path.unwrap_or_else(|| fail("missing --program <file>"));
    let data_path = data_path.unwrap_or_else(|| fail("missing --data <file>"));

    let program_text = std::fs::read_to_string(&program_path)
        .unwrap_or_else(|e| fail(&format!("cannot read {}: {}", program_path, e)));
    let data_text = std::fs::read_to_string(&data_path)
        .unwrap_or_else(|e| fail(&format!("cannot read {}: {}", data_path, e)));

    let dict = Arc::new(RwLock::new(Dictionary::new()));
    let rules = parse_program(&program_text, &dict).unwrap_or_else(|e| fail(&e));
    let facts = parse_data(&data_text, &dict).unwrap_or_else(|e| fail(&e));

    let w_max = compute_w_max(&rules);
    let max_fact_end = facts.iter().map(|f| f.end).max().unwrap_or(0);
    let horizon = horizon_override.unwrap_or(max_fact_end + w_max);

    let out = match store_kind.as_str() {
        "snapshot" => run(TemporalSnapshotStore::new(horizon + 1), rules, &facts, &dict, horizon, skip_empty),
        "interval" => run(IntervalFactStore::new(horizon + 1), rules, &facts, &dict, horizon, skip_empty),
        other => fail(&format!("unknown --store '{}' (use snapshot|interval)", other)),
    };

    if timing {
        // Minimal JSON so the perf harness can parse without a JSON dep.
        println!(
            "{{\"reason_ms\": {:.3}, \"atoms\": {}, \"intervals\": {}, \"horizon\": {}}}",
            out.reason_micros as f64 / 1000.0,
            out.atom_count,
            out.interval_lines.len(),
            horizon,
        );
    } else {
        for line in out.interval_lines {
            println!("{}", line);
        }
    }
}

struct RunOutput {
    interval_lines: Vec<String>,
    reason_micros: u128,
    atom_count: usize,
}

/// Run the engine over the relevant ticks in `0..=horizon`, timing the loop.
///
/// A tick is *relevant* only if some base fact lies within a rule's temporal
/// reach of it; other ticks provably derive nothing (no base fact active and
/// none within any operator's lookback), so they are skipped. Reach is
/// `num_rules * w_max` for non-recursive programs (a safe bound on how far a
/// fact can propagate forward through the rule chain); recursive programs fall
/// back to evaluating every tick.
fn run<S: TemporalStore>(
    store: S,
    rules: Vec<DatalogMTLRule>,
    facts: &[TemporalFact],
    dict: &Arc<RwLock<Dictionary>>,
    horizon: u64,
    skip_empty: bool,
) -> RunOutput {
    let relevant = relevant_ticks(&rules, facts, dict, horizon, skip_empty);

    let mut eval = DatalogMTLEvaluator::new(rules, store, dict.clone())
        .unwrap_or_else(|e| fail(&format!("invalid program: {}", e)));

    // Densify facts: tick -> triples valid at that tick.
    let mut by_tick: HashMap<u64, Vec<Triple>> = HashMap::new();
    for f in facts {
        for t in f.start..=f.end.min(horizon) {
            by_tick.entry(t).or_default().push(f.triple.clone());
        }
    }

    let wildcard: TriplePattern = (
        Term::Variable("_s".into()),
        Term::Variable("_p".into()),
        Term::Variable("_o".into()),
    );

    let start = Instant::now();
    let mut holds: HashMap<Triple, BTreeSet<u64>> = HashMap::new();
    for t in 0..=horizon {
        if !relevant[t as usize] { continue; }
        let ingest = by_tick.remove(&t).unwrap_or_default();
        eval.advance(t, ingest);

        // Record every fact (base + derived) active at this tick.
        for b in eval.store.query_at(&wildcard, t) {
            if let (Some(&s), Some(&p), Some(&o)) =
                (b.get("_s"), b.get("_p"), b.get("_o"))
            {
                holds.entry(Triple { subject: s, predicate: p, object: o })
                    .or_default()
                    .insert(t);
            }
        }
    }
    let reason_micros = start.elapsed().as_micros();
    let atom_count = holds.len();

    let dict_guard = dict.read().unwrap();
    RunOutput {
        interval_lines: format_tick_sets(&holds, &dict_guard),
        reason_micros,
        atom_count,
    }
}

/// Mark each tick in `0..=horizon` relevant if some base fact lies within the
/// program's temporal reach of it. Recursive programs (or `skip_empty=false`)
/// mark every tick relevant.
fn relevant_ticks(
    rules: &[DatalogMTLRule],
    facts: &[TemporalFact],
    dict: &Arc<RwLock<Dictionary>>,
    horizon: u64,
    skip_empty: bool,
) -> Vec<bool> {
    let n = (horizon + 1) as usize;
    let rdf_type = dict.write().unwrap().encode(RDF_TYPE);
    let reach = if !skip_empty || is_recursive(rules, rdf_type) {
        horizon
    } else {
        (rules.len() as u64) * compute_w_max(rules)
    };
    if reach >= horizon {
        return vec![true; n];
    }
    // Difference array over [start, end+reach] for each base fact: O(facts + H).
    let mut diff = vec![0i64; n + 1];
    for f in facts {
        let lo = f.start.min(horizon) as usize;
        let hi = (f.end + reach).min(horizon) as usize;
        diff[lo] += 1;
        diff[hi + 1] -= 1;
    }
    let mut relevant = vec![false; n];
    let mut acc = 0i64;
    for t in 0..n {
        acc += diff[t];
        relevant[t] = acc > 0;
    }
    relevant
}

/// The identifying predicate of an atom: for a `?s rdf:type Class` triple it is
/// `Class` (object); otherwise the predicate-position constant. `None` if that
/// position is not a constant.
fn atom_pred_id(pat: &TriplePattern, rdf_type: u32) -> Option<u32> {
    let pred = match &pat.1 { Term::Constant(c) => *c, _ => return None };
    if pred == rdf_type {
        match &pat.2 { Term::Constant(c) => Some(*c), _ => None }
    } else {
        Some(pred)
    }
}

fn collect_body_preds(atom: &TemporalAtom, rdf_type: u32, out: &mut Vec<u32>) {
    match atom {
        TemporalAtom::Base(p) => { if let Some(id) = atom_pred_id(p, rdf_type) { out.push(id); } }
        TemporalAtom::Diamond { inner, .. }
        | TemporalAtom::Box_ { inner, .. }
        | TemporalAtom::Prev { inner, .. } => collect_body_preds(inner, rdf_type, out),
        TemporalAtom::Since { phi, psi, .. } => {
            collect_body_preds(phi, rdf_type, out);
            collect_body_preds(psi, rdf_type, out);
        }
    }
}

/// True if the rule set has a predicate reachable from itself (a cycle in the
/// body-predicate -> head-predicate dependency graph).
fn is_recursive(rules: &[DatalogMTLRule], rdf_type: u32) -> bool {
    let mut edges: HashMap<u32, Vec<u32>> = HashMap::new();
    for r in rules {
        let Some(head) = atom_pred_id(&r.head, rdf_type) else { continue };
        let mut bps = Vec::new();
        for a in &r.body { collect_body_preds(a, rdf_type, &mut bps); }
        for bp in bps { edges.entry(bp).or_default().push(head); }
    }
    let preds: Vec<u32> = edges.keys().copied().collect();
    for &start in &preds {
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            if let Some(nexts) = edges.get(&node) {
                for &m in nexts {
                    if m == start { return true; }
                    if seen.insert(m) { stack.push(m); }
                }
            }
        }
    }
    false
}

fn fail(msg: &str) -> ! {
    eprintln!("error: {}", msg);
    std::process::exit(1);
}
