/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Term interning — the hot path underneath every parse and every query

//! Everything here is single-threaded, so every lock is uncontended — the cheap case, and a
//! misleading one

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use shared::dictionary::Dictionary;
use std::sync::{Arc, RwLock};

const TERMS: usize = 10_000;

fn iris() -> Vec<String> {
    (0..TERMS)
        .map(|i| format!("http://example.org/resource/entity{i}"))
        .collect()
}

fn bench_encode(c: &mut Criterion) {
    let iris = iris();
    let mut group = c.benchmark_group("micro_dictionary");

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

/// `&self`, locking inside
trait EncodeShared {
    fn encode(&self, value: &str) -> u32;
    fn decode(&self, id: u32) -> Option<String>;
    /// The way out of the allocation: lend the term inside the guard instead of handing it back
    fn with_decoded(&self, id: u32, visit: &mut dyn FnMut(Option<&str>));
}

/// `&mut self`, with the caller holding the guard
trait EncodeExclusive {
    fn encode(&mut self, value: &str) -> u32;
    fn decode(&self, id: u32) -> Option<&str>;
}

impl EncodeShared for Arc<RwLock<Dictionary>> {
    fn encode(&self, value: &str) -> u32 {
        self.write().unwrap().encode(value)
    }
    fn decode(&self, id: u32) -> Option<String> {
        self.read().unwrap().decode(id).map(str::to_string)
    }
    fn with_decoded(&self, id: u32, visit: &mut dyn FnMut(Option<&str>)) {
        visit(self.read().unwrap().decode(id));
    }
}

impl EncodeExclusive for Dictionary {
    fn encode(&mut self, value: &str) -> u32 {
        Dictionary::encode(self, value)
    }
    fn decode(&self, id: u32) -> Option<&str> {
        Dictionary::decode(self, id)
    }
}

/// Encoding and decoding under each shape over the same 10,000 warm terms
fn bench_encoder_shapes(c: &mut Criterion) {
    let iris = iris();
    let mut dict = Dictionary::new();
    for iri in &iris {
        dict.encode(iri);
    }
    let ids: Vec<u32> = iris.iter().map(|iri| dict.encode(iri)).collect();
    let handle = Arc::new(RwLock::new(dict));

    let mut group = c.benchmark_group("micro_term_encoder");

    group.bench_function("encode_batched_guard", |b| {
        b.iter(|| {
            let mut guard = handle.write().unwrap();
            for iri in &iris {
                black_box(guard.encode(black_box(iri)));
            }
        })
    });

    group.bench_function("encode_lock_per_call", |b| {
        b.iter(|| {
            for iri in &iris {
                black_box(handle.write().unwrap().encode(black_box(iri)));
            }
        })
    });

    group.bench_function("encode_dyn_interior", |b| {
        let encoder: &dyn EncodeShared = &handle;
        b.iter(|| {
            for iri in &iris {
                black_box(encoder.encode(black_box(iri)));
            }
        })
    });

    group.bench_function("encode_dyn_guard", |b| {
        b.iter(|| {
            let mut guard = handle.write().unwrap();
            let encoder: &mut dyn EncodeExclusive = &mut *guard;
            for iri in &iris {
                black_box(encoder.encode(black_box(iri)));
            }
        })
    });

    group.bench_function("decode_batched_guard", |b| {
        b.iter(|| {
            let guard = handle.read().unwrap();
            for id in &ids {
                black_box(guard.decode(black_box(*id)));
            }
        })
    });

    group.bench_function("decode_lock_per_call", |b| {
        b.iter(|| {
            for id in &ids {
                black_box(handle.read().unwrap().decode(black_box(*id)).is_some());
            }
        })
    });

    group.bench_function("decode_dyn_interior_owned", |b| {
        let encoder: &dyn EncodeShared = &handle;
        b.iter(|| {
            for id in &ids {
                black_box(encoder.decode(black_box(*id)));
            }
        })
    });

    group.bench_function("decode_dyn_guard", |b| {
        b.iter(|| {
            let guard = handle.read().unwrap();
            let encoder: &dyn EncodeExclusive = &*guard;
            for id in &ids {
                black_box(encoder.decode(black_box(*id)));
            }
        })
    });

    group.bench_function("decode_dyn_visitor", |b| {
        let encoder: &dyn EncodeShared = &handle;
        b.iter(|| {
            for id in &ids {
                encoder.with_decoded(black_box(*id), &mut |term| {
                    black_box(term.is_some());
                });
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_encode, bench_encoder_shapes);
criterion_main!(benches);
