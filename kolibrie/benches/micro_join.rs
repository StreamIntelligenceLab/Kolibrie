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
//! architecture proposal replaces `MapRows = Vec<HashMap<String, u32>>` with a
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
use shared::terms::{Binding, Bindings, UNBOUND};
use std::collections::HashMap;

const LEFT_ROWS: usize = 20_000;
const RIGHT_ROWS: usize = 20_000;
/// Distinct join-key values. Fewer keys means more matches per key, which is what
/// separates "hashing cost dominates" from "output size dominates".
const KEYS: u32 = 2_000;

// ── The shape that was replaced: Vec<HashMap<String, u32>> ─────────────────────
//
// Named `MapRows` here because `MapRows` now means the schema-and-tuples type this
// benchmark was written to argue for. Keeping both lets the comparison still run.

type MapRows = Vec<Binding>;

fn make_bindings(rows: usize, vars: &[&str], key_var: &str) -> MapRows {
    (0..rows)
        .map(|i| {
            let mut row = HashMap::new();
            for (c, v) in vars.iter().enumerate() {
                let value = if *v == key_var {
                    (i as u32) % KEYS
                } else {
                    (i as u32)
                        .wrapping_mul(2_654_435_761)
                        .wrapping_add(c as u32)
                };
                row.insert((*v).to_string(), value);
            }
            row
        })
        .collect()
}

/// Mirrors the shape of the engine's existing hash join: build a map keyed by the
/// shared variables, then probe and merge whole rows.
fn hash_join_maps(left: &MapRows, right: &MapRows, shared: &[&str]) -> MapRows {
    let mut index: HashMap<Vec<u32>, Vec<&HashMap<String, u32>>> = HashMap::new();
    for row in right {
        let key: Vec<u32> = shared.iter().filter_map(|v| row.get(*v).copied()).collect();
        if key.len() == shared.len() {
            index.entry(key).or_default().push(row);
        }
    }

    let mut out = Vec::new();
    for lrow in left {
        let key: Vec<u32> = shared
            .iter()
            .filter_map(|v| lrow.get(*v).copied())
            .collect();
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

// ── Control: a struct that genuinely only wraps the map shape ───────────────────

/// `MapRows` behind a struct with accessors, keeping one `HashMap` per row.
///
/// This exists to answer "why would wrapping a HashMap in a struct cost anything?"
/// It should not, and this arm measures that it does not. `Relation` is a different
/// thing: it does not wrap the per-row map, it removes it, which is where its win
/// and its conversion cost both come from. Separating the two means neither gets
/// credited to the other.
struct WrappedMaps {
    rows: MapRows,
}

impl WrappedMaps {
    fn new(rows: MapRows) -> Self {
        Self { rows }
    }

    fn rows(&self) -> &[HashMap<String, u32>] {
        &self.rows
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn get(&self, row: usize, name: &str) -> Option<u32> {
        self.rows.get(row)?.get(name).copied()
    }
}

/// The same join as `hash_join_maps`, reaching every row through the accessors
/// above rather than touching the field. Identical work, one indirection added.
fn hash_join_wrapped(left: &WrappedMaps, right: &WrappedMaps, shared: &[&str]) -> WrappedMaps {
    let mut index: HashMap<Vec<u32>, Vec<&HashMap<String, u32>>> = HashMap::new();
    for i in 0..right.len() {
        let key: Vec<u32> = shared.iter().filter_map(|v| right.get(i, v)).collect();
        if key.len() == shared.len() {
            index.entry(key).or_default().push(&right.rows()[i]);
        }
    }

    let mut out = Vec::new();
    for i in 0..left.len() {
        let key: Vec<u32> = shared.iter().filter_map(|v| left.get(i, v)).collect();
        if key.len() != shared.len() {
            continue;
        }
        if let Some(matches) = index.get(&key) {
            for rrow in matches {
                let mut merged = left.rows()[i].clone();
                for (k, v) in rrow.iter() {
                    merged.entry(k.clone()).or_insert(*v);
                }
                out.push(merged);
            }
        }
    }
    WrappedMaps::new(out)
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
                        (i as u32)
                            .wrapping_mul(2_654_435_761)
                            .wrapping_add(c as u32)
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
    Relation {
        schema: out_schema,
        rows,
    }
}

// ── Conversion at the boundary ───────────────────────────────────────────────────

/// `Relation` to the map shape, which is what an engine adopting `Relation`
/// internally would have to do at every edge that still returns the old shape —
/// `MtlStore::query_at` in `datalogmtl`, and the four optimizer entry points.
fn relation_to_maps(r: &Relation) -> MapRows {
    r.rows
        .iter()
        .map(|row| {
            let mut out = HashMap::with_capacity(r.schema.len());
            for (name, id) in r.schema.iter().zip(row.iter().copied()) {
                if id != UNBOUND {
                    out.insert(name.clone(), id);
                }
            }
            out
        })
        .collect()
}

/// The map shape to `Relation`, which is what the same engine would do at every
/// edge that still *supplies* the old shape.
///
/// This mirrors `shared::terms::Bindings`'s own construction, including taking the
/// schema as the **union** of every row's variables rather than the first row's.
/// That costs a pass over all rows and a membership test per key, and it is not
/// optional: solution mappings are partial, so a variable may be bound only in a
/// later row. An earlier version of this helper read the first row alone, which
/// measured a conversion the real type cannot perform.
fn maps_to_relation(b: &MapRows) -> Relation {
    let mut schema: Vec<String> = Vec::new();
    for row in b {
        let mut names: Vec<&String> = row.keys().collect();
        names.sort();
        for name in names {
            if !schema.iter().any(|v| v == name) {
                schema.push(name.clone());
            }
        }
    }
    let rows = b
        .iter()
        .map(|row| {
            schema
                .iter()
                .map(|v| row.get(v).copied().unwrap_or(UNBOUND))
                .collect::<Vec<u32>>()
        })
        .collect();
    Relation { schema, rows }
}

/// The same hash join over the real `shared::terms::Bindings`, through its public
/// API rather than the stand-in's bare fields.
///
/// The stand-in has `schema` and `rows` public and is local to this file, so the
/// optimiser sees everything. The real type encapsulates both behind accessors in
/// another crate. This arm exists to check that the measured win survives the API,
/// rather than assuming it does.
fn hash_join_real(left: &Bindings, right: &Bindings, shared: &[&str]) -> Bindings {
    let lk: Vec<usize> = shared.iter().filter_map(|v| left.column(v)).collect();
    let rk: Vec<usize> = shared.iter().filter_map(|v| right.column(v)).collect();
    let right_only: Vec<usize> = (0..right.schema().len())
        .filter(|i| !rk.contains(i))
        .collect();

    let mut out_schema: Vec<String> = left.schema().to_vec();
    for &i in &right_only {
        out_schema.push(right.schema()[i].clone());
    }
    let mut out = Bindings::with_schema(out_schema);

    let mut index: HashMap<Vec<u32>, Vec<&Vec<u32>>> = HashMap::new();
    for row in right.cells() {
        index
            .entry(rk.iter().map(|&i| row[i]).collect())
            .or_default()
            .push(row);
    }
    for lrow in left.cells() {
        let key: Vec<u32> = lk.iter().map(|&i| lrow[i]).collect();
        if let Some(matches) = index.get(&key) {
            for rrow in matches {
                let mut row = lrow.clone();
                for &i in &right_only {
                    row.push(rrow[i]);
                }
                out.push_cells(row);
            }
        }
    }
    out
}

fn make_real(rows: usize, vars: &[&str], key_var: &str) -> Bindings {
    // No conversion step any more: the production type *is* the tuple shape, which
    // is what this bench existed to argue for.
    make_bindings(rows, vars, key_var).into()
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

    let lreal = make_real(LEFT_ROWS, &left_vars, "x");
    let rreal = make_real(RIGHT_ROWS, &right_vars, "x");
    assert_eq!(
        hash_join_real(&lreal, &rreal, &shared).len(),
        n_rel,
        "the real type must join to the same cardinality as the stand-in"
    );

    let mut group = c.benchmark_group("micro_join");
    group.bench_function("wrapped_maps_through_accessors", |b| {
        let wl = WrappedMaps::new(lb.clone());
        let wr = WrappedMaps::new(rb.clone());
        b.iter(|| black_box(hash_join_wrapped(black_box(&wl), black_box(&wr), &shared).len()))
    });
    group.bench_function("vec_hashmap_string_keys", |b| {
        b.iter(|| black_box(hash_join_maps(black_box(&lb), black_box(&rb), &shared).len()))
    });
    group.bench_function("schema_plus_tuples", |b| {
        b.iter(|| {
            black_box(
                hash_join_relation(black_box(&lr), black_box(&rr), &shared)
                    .rows
                    .len(),
            )
        })
    });
    // What the incremental path would actually cost: join cheaply in `Relation`
    // form, then hand the old shape back across an unchanged boundary.
    group.bench_function("relation_join_then_to_maps", |b| {
        b.iter(|| {
            let joined = hash_join_relation(black_box(&lr), black_box(&rr), &shared);
            black_box(relation_to_maps(&joined).len())
        })
    });

    // The other direction: take the old shape in, convert once, then join.
    group.bench_function("maps_to_relation_then_join", |b| {
        b.iter(|| {
            let l = maps_to_relation(black_box(&lb));
            let r = maps_to_relation(black_box(&rb));
            black_box(hash_join_relation(&l, &r, &shared).rows.len())
        })
    });

    // The conversions alone, so the two above can be decomposed.
    let joined_rel = hash_join_relation(&lr, &rr, &shared);
    group.bench_function("convert_result_to_maps", |b| {
        b.iter(|| black_box(relation_to_maps(black_box(&joined_rel)).len()))
    });
    group.bench_function("convert_input_to_relation", |b| {
        b.iter(|| black_box(maps_to_relation(black_box(&lb)).rows.len()))
    });

    // The real type, through its public API, against the stand-in above.
    group.bench_function("schema_plus_tuples_real_type", |b| {
        b.iter(|| black_box(hash_join_real(black_box(&lreal), black_box(&rreal), &shared).len()))
    });

    group.finish();
}

criterion_group!(benches, bench_join);
criterion_main!(benches);
