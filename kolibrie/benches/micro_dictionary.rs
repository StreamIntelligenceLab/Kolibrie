/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Term interning — the hot path underneath every parse and every query.
//!
//! Guards the planned sealing of `Dictionary` (three public fields today) and the
//! `TermId` newtype. Both are changes to a type that every crate touches, and the
//! interning strategy itself may later change to an arena or to content-addressed
//! ids for distribution. This bench is what tells us what such a change costs.
//!
//! Benched from `kolibrie` rather than `shared` deliberately: see the note in
//! `micro_pattern.rs`.

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use shared::dictionary::Dictionary;

const TERMS: usize = 10_000;

fn iris() -> Vec<String> {
    (0..TERMS)
        .map(|i| format!("http://example.org/resource/entity{i}"))
        .collect()
}

fn bench_encode(c: &mut Criterion) {
    let iris = iris();
    let mut group = c.benchmark_group("micro_dictionary");

    // Cold: every term is new, so every call allocates and inserts. This is the
    // parse-a-fresh-dataset path.
    group.bench_function("encode_cold", |b| {
        b.iter_batched(
            Dictionary::new,
            |mut dict| {
                for iri in &iris {
                    black_box(dict.encode(black_box(iri)));
                }
                dict
            },
            BatchSize::SmallInput,
        )
    });

    // Warm: every term already exists, so this is a pure lookup. This is the common
    // case during query execution and the one most sensitive to a representation change.
    let mut warm = Dictionary::new();
    for iri in &iris {
        warm.encode(iri);
    }
    group.bench_function("encode_warm", |b| {
        b.iter(|| {
            for iri in &iris {
                black_box(warm.encode(black_box(iri)));
            }
        })
    });

    let ids: Vec<u32> = iris.iter().map(|iri| warm.encode(iri)).collect();
    group.bench_function("decode", |b| {
        b.iter(|| {
            for id in &ids {
                black_box(warm.decode(black_box(*id)));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_encode);
criterion_main!(benches);
