/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Hash join over solution bindings, in the current representation and the proposed one.
//!
//! This bench exists to falsify a premise, not only to guard a change. The
//! architecture proposal replaces `Bindings = Vec<HashMap<String, u32>>` with a
//! schema+tuple `Relation` (variable names resolved once into column positions,
//! rows as `Vec<u32>`), on the argument that the current shape re-hashes a `String`
//! key for every column of every row and allocates a `HashMap` per row.
//!
//! If schema+tuple is *not* faster here, a central premise of the shared join
//! library is wrong and we should know that before building on it.
//!
//! `Relation` below is a deliberately minimal stand-in — enough to measure the
//! representation, not a proposal for the real API.

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::collections::HashMap;

const LEFT_ROWS: usize = 20_000;
const RIGHT_ROWS: usize = 20_000;
/// Distinct join-key values. Fewer keys means more matches per key, which is what
/// separates "hashing cost dominates" from "output size dominates".
const KEYS: u32 = 2_000;

// ── Current representation: Vec<HashMap<String, u32>> ───────────────────────────

type Bindings = Vec<HashMap<String, u32>>;

fn make_bindings(rows: usize, vars: &[&str], key_var: &str) -> Bindings {
    (0..rows)
        .map(|i| {
            let mut row = HashMap::new();
            for (c, v) in vars.iter().enumerate() {
                let value = if *v == key_var {
                    (i as u32) % KEYS
                } else {
                    (i as u32).wrapping_mul(2_654_435_761).wrapping_add(c as u32)
                };
                row.insert((*v).to_string(), value);
            }
            row
        })
        .collect()
}

/// Mirrors the shape of the engine's existing hash join: build a map keyed by the
/// shared variables, then probe and merge whole rows.
fn hash_join_maps(left: &Bindings, right: &Bindings, shared: &[&str]) -> Bindings {
    let mut index: HashMap<Vec<u32>, Vec<&HashMap<String, u32>>> = HashMap::new();
    for row in right {
        let key: Vec<u32> = shared.iter().filter_map(|v| row.get(*v).copied()).collect();
        if key.len() == shared.len() {
            index.entry(key).or_default().push(row);
        }
    }

    let mut out = Vec::new();
    for lrow in left {
        let key: Vec<u32> = shared.iter().filter_map(|v| lrow.get(*v).copied()).collect();
        if key.len() != shared.len() {
            continue;
        }
        if let Some(matches) = index.get(&key) {
            for rrow in matches {
                let mut merged = lrow.clone();
                for (k, v) in rrow.iter() {
                    merged.entry(k.clone()).or_insert(*v);
                }
                out.push(merged);
            }
        }
    }
    out
}

// ── Proposed representation: schema + tuples ────────────────────────────────────

struct Relation {
    schema: Vec<String>,
    rows: Vec<Vec<u32>>,
}

impl Relation {
    fn column(&self, name: &str) -> Option<usize> {
        self.schema.iter().position(|v| v == name)
    }
}

fn make_relation(rows: usize, vars: &[&str], key_var: &str) -> Relation {
    let schema: Vec<String> = vars.iter().map(|v| (*v).to_string()).collect();
    let rows = (0..rows)
        .map(|i| {
            vars.iter()
                .enumerate()
                .map(|(c, v)| {
                    if *v == key_var {
                        (i as u32) % KEYS
                    } else {
                        (i as u32).wrapping_mul(2_654_435_761).wrapping_add(c as u32)
                    }
                })
                .collect()
        })
        .collect();
    Relation { schema, rows }
}

/// The point: variable names are resolved to column indices **once**, before the
/// loop, so the inner loop touches only integers.
fn hash_join_relation(left: &Relation, right: &Relation, shared: &[&str]) -> Relation {
    let lcols: Vec<usize> = shared.iter().filter_map(|v| left.column(v)).collect();
    let rcols: Vec<usize> = shared.iter().filter_map(|v| right.column(v)).collect();

    let mut out_schema = left.schema.clone();
    let mut carry: Vec<usize> = Vec::new();
    for (i, v) in right.schema.iter().enumerate() {
        if !out_schema.contains(v) {
            out_schema.push(v.clone());
            carry.push(i);
        }
    }

    let mut index: HashMap<Vec<u32>, Vec<&Vec<u32>>> = HashMap::new();
    for row in &right.rows {
        let key: Vec<u32> = rcols.iter().map(|c| row[*c]).collect();
        index.entry(key).or_default().push(row);
    }

    let mut rows = Vec::new();
    for lrow in &left.rows {
        let key: Vec<u32> = lcols.iter().map(|c| lrow[*c]).collect();
        if let Some(matches) = index.get(&key) {
            for rrow in matches {
                let mut merged = lrow.clone();
                merged.extend(carry.iter().map(|c| rrow[*c]));
                rows.push(merged);
            }
        }
    }
    Relation { schema: out_schema, rows }
}

fn bench_join(c: &mut Criterion) {
    let left_vars = ["x", "a", "b"];
    let right_vars = ["x", "c", "d"];
    let shared = ["x"];

    let lb = make_bindings(LEFT_ROWS, &left_vars, "x");
    let rb = make_bindings(RIGHT_ROWS, &right_vars, "x");
    let lr = make_relation(LEFT_ROWS, &left_vars, "x");
    let rr = make_relation(RIGHT_ROWS, &right_vars, "x");

    // Sanity: both shapes must produce the same number of rows, or the comparison
    // is meaningless. A bench that silently measures different work is worse than none.
    let n_maps = hash_join_maps(&lb, &rb, &shared).len();
    let n_rel = hash_join_relation(&lr, &rr, &shared).rows.len();
    assert_eq!(
        n_maps, n_rel,
        "the two representations must join to the same cardinality"
    );

    let mut group = c.benchmark_group("micro_join");
    group.bench_function("vec_hashmap_string_keys", |b| {
        b.iter(|| black_box(hash_join_maps(black_box(&lb), black_box(&rb), &shared).len()))
    });
    group.bench_function("schema_plus_tuples", |b| {
        b.iter(|| black_box(hash_join_relation(black_box(&lr), black_box(&rr), &shared).rows.len()))
    });
    group.finish();
}

criterion_group!(benches, bench_join);
criterion_main!(benches);
