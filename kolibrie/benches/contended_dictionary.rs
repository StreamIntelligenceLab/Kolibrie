/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Term interning under contention — what a shared dictionary costs across threads.
//!
//! `micro_dictionary` measures interning single-threaded, where every lock is
//! uncontended. That is the cheap case, and it produced a wrong conclusion once:
//! locking looked like 6 to 17%, which made an `encode(&self)` abstraction with
//! per-call interior locking seem affordable.
//!
//! It is not. Dividing the same 10,000 terms among `n` threads, against the
//! single-threaded reference:
//!
//! | threads | encode, own dict | encode, shared | decode, shared |
//! |---|---:|---:|---:|
//! | 2 | 1.4x faster | 1.6x slower | 3.3x slower |
//! | 4 | 2.0x faster | 2.2x slower | 5.4x slower |
//! | 8 | 1.7x faster | 3.9x slower | 12.3x slower |
//!
//! Per-call locking on a shared dictionary is slower than one thread at two threads.
//! Independent dictionaries scale about 2x, so the cost is the sharing rather than
//! the threading, and reads are worse than writes because a read acquisition still
//! does an atomic read-modify-write on one cache line and the acquisitions
//! ping-pong it between cores.
//!
//! `rsp_engine` spawns a thread per window plus a coordinator, all encoding into one
//! dictionary, so this is the access pattern the real system has.
//!
//! # Why the `heavy` tier
//!
//! It answers a design question rather than guarding a refactor, which is what that
//! tier is for, and it has already answered it. It also costs about 45s per run.
//! Run it when the term-encoding design is revisited, or when something changes how
//! the dictionary is shared.

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use shared::dictionary::Dictionary;
use std::sync::{Arc, RwLock};

const TERMS: usize = 10_000;

fn iris() -> Vec<String> {
    (0..TERMS)
        .map(|i| format!("http://example.org/resource/entity{i}"))
        .collect()
}

/// Encode and decode under real contention, which the group above cannot show.
///
/// Everything above is single-threaded, so its `RwLock` is always uncontended — and
/// an uncontended lock is the cheap case. That matters because the reason the
/// dictionary is an `Arc<RwLock<_>>` at all is cross-thread sharing: `rsp_engine`
/// spawns a thread per window plus a coordinator, and they encode into one
/// dictionary. So the 6% the group above attributes to locking is a floor, not the
/// cost in the mode that forced the lock to exist.
///
/// Each benchmark divides the same 10,000 terms among `n` threads, so total work is
/// constant and only the sharing changes. `independent` gives each thread its own
/// dictionary: it pays the same spawn and join cost but takes no shared lock, which
/// isolates contention from threading overhead. Read the contention cost as
/// `shared` minus `independent` at the same thread count.
///
/// `encode` takes a write lock and therefore serialises completely; `decode` takes a
/// read lock and should not. Whether that asymmetry holds is the question.
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
            // Each thread gets its own *warm* dictionary, cloned once outside the
            // timing loop. Starting them empty would compare cold encodes against
            // the shared version's warm ones and overstate contention by the whole
            // difference between the two, which is a factor of five.
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

    group.finish();
}

criterion_group!(benches, bench_contention);
criterion_main!(benches);
