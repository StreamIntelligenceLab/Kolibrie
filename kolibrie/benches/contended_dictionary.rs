/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Term interning under contention — what a shared dictionary costs across threads

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use shared::dictionary::Dictionary;
use shared::term_encoder::{SharedTerms, TermEncoder};
use std::sync::{Arc, RwLock};

const TERMS: usize = 10_000;

fn iris() -> Vec<String> {
    (0..TERMS)
        .map(|i| format!("http://example.org/resource/entity{i}"))
        .collect()
}

/// Encode and decode under real contention, which the group above cannot show
fn bench_contention(c: &mut Criterion) {
    let iris = iris();
    let mut warm = Dictionary::new();
    for iri in &iris {
        warm.encode(iri);
    }
    let ids: Vec<u32> = iris.iter().map(|iri| warm.encode(iri)).collect();

    let mut group = c.benchmark_group("micro_term_encoder_contended");

    for threads in [2usize, 4, 8] {
        let chunk = iris.len() / threads;

        group.bench_function(format!("encode_shared/{threads}"), |b| {
            let handle = Arc::new(RwLock::new(warm.clone()));
            b.iter(|| {
                std::thread::scope(|scope| {
                    for part in iris.chunks(chunk) {
                        let handle = Arc::clone(&handle);
                        scope.spawn(move || {
                            for iri in part {
                                black_box(handle.write().unwrap().encode(black_box(iri)));
                            }
                        });
                    }
                });
            })
        });

        group.bench_function(format!("encode_independent/{threads}"), |b| {
            let mut owns: Vec<Dictionary> = (0..threads).map(|_| warm.clone()).collect();
            b.iter(|| {
                std::thread::scope(|scope| {
                    for (own, part) in owns.iter_mut().zip(iris.chunks(chunk)) {
                        scope.spawn(move || {
                            for iri in part {
                                black_box(own.encode(black_box(iri)));
                            }
                        });
                    }
                });
            })
        });

        group.bench_function(format!("decode_shared/{threads}"), |b| {
            let handle = Arc::new(RwLock::new(warm.clone()));
            b.iter(|| {
                std::thread::scope(|scope| {
                    for part in ids.chunks(chunk) {
                        let handle = Arc::clone(&handle);
                        scope.spawn(move || {
                            for id in part {
                                black_box(handle.read().unwrap().decode(black_box(*id)).is_some());
                            }
                        });
                    }
                });
            })
        });
    }

    for threads in [2usize, 4, 8] {
        let chunk = iris.len() / threads;
        group.bench_function(format!("encode_scoped/{threads}"), |b| {
            let terms = SharedTerms::from_dictionary(warm.clone());
            b.iter(|| {
                std::thread::scope(|scope| {
                    for part in iris.chunks(chunk) {
                        let terms = terms.clone();
                        scope.spawn(move || {
                            terms.with_terms(&mut |dict| {
                                for iri in part {
                                    black_box(dict.encode(black_box(iri)));
                                }
                            });
                        });
                    }
                });
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_contention);
criterion_main!(benches);
