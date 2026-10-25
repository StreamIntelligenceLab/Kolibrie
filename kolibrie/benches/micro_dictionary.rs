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
//!
//! # The `TermEncoder` shapes
//!
//! The `micro_term_encoder` group answers the question `micro_scan` answered for
//! `QuadSource`: what does putting a trait in front of this cost? Two choices are
//! contested, and unlike the scan case they are not obviously free, because a warm
//! encode is tens of nanoseconds rather than microseconds.
//!
//!   1. `encode(&self)` with locking inside the implementation, versus
//!      `encode(&mut self)` with the caller holding a guard. Only the first keeps
//!      `Arc<RwLock<_>>` out of public signatures, which is the whole point of the
//!      abstraction; the second preserves the batching that 125 call sites in this
//!      workspace currently rely on, each holding one guard across 1 to 9 calls.
//!
//!   2. What `decode` returns. Under interior locking it *cannot* hand back
//!      `Option<&str>`, because the borrow would outlive the guard, so it must
//!      allocate a `String` per call. Today's `decode` is a few nanoseconds, so an
//!      allocation is not a rounding error.
//!
//! The group separates the three costs — the lock, the vtable, and the allocation —
//! so a design can be argued from numbers rather than from taste.
//!
//! ## What it measured
//!
//! Over 10,000 warm terms, against direct access:
//!
//! Over 10,000 warm terms across four runs, with each figure's own noise beside it,
//! because two of these are too noisy to compare and that is part of the result:
//!
//! | shape | encode | decode |
//! |---|---:|---:|
//! | direct, no lock | — (6.1%) | — (4.2%) |
//! | concrete, lock per call | +6% (4.2%) | +17% (0.7%) |
//! | `&dyn` interior lock | +8% (0.4%) | — |
//! | `&dyn` interior lock, visitor `decode` | — | +54% (1.1%) |
//! | `&dyn` interior lock, owned `String` | — | +294% (0.4%) |
//! | concrete, one batched guard | not measurable (8.4%) | not measurable (21.5%) |
//! | `&mut dyn`, caller holds the guard | not measurable (3.0%) | not measurable (21.8%) |
//!
//! **The large cost is what `decode` returns.** An owned `String` per call, which
//! interior locking forces because a borrow cannot outlive the guard taken to get
//! it, is nearly four times the decode itself. Lending through a visitor costs +54%,
//! the same answer `QuadSource::for_each_quad` reached for the same reason.
//!
//! **Locking itself is modest**: +6% on encode, +17% on decode, both measured
//! against a concrete receiver so no vtable is involved.
//!
//! ## Under contention, which reverses the conclusion above
//!
//! Every figure above is single-threaded, so its lock is uncontended. The
//! `micro_term_encoder_contended` group divides the same 10,000 terms among `n`
//! threads. Against the single-threaded reference:
//!
//! | threads | encode, own dict | encode, shared | decode, shared |
//! |---|---:|---:|---:|
//! | 2 | 1.4x faster | 1.6x slower | 3.3x slower |
//! | 4 | 2.0x faster | 2.2x slower | 5.4x slower |
//! | 8 | 1.7x faster | 3.9x slower | 12.3x slower |
//!
//! **Per-call locking on a shared dictionary does not merely fail to scale; it is
//! slower than one thread at two threads, and 4 to 12 times worse at eight.** It is
//! worse for reads than for writes, because a read acquisition still does an atomic
//! read-modify-write on one cache line, so the acquisitions ping-pong it between
//! cores. Independent dictionaries scale about 2x, so the cost is the sharing and
//! not the threading.
//!
//! This reverses what the uncontended numbers suggest. `encode(&self)` with interior
//! per-call locking is the +8% shape single-threaded and a 4x regression at eight
//! threads — and `rsp_engine` spawns a thread per window, which is this access
//! pattern exactly. So the batching that the guard-held rows below could not
//! measure is not an incidental habit of 125 call sites; it is what keeps the
//! current design workable, and an abstraction that forbids it would be a serious
//! concurrency regression dressed as a cleanup.
//!
//! **The two guard-held shapes could not be measured.** Both carry about 21% noise
//! across runs — the worst in the whole suite — and their medians move enough
//! between runs to reverse their order, so no claim about the cost of batching, or
//! about the cost of a vtable, can be drawn from them. A single run suggested the
//! vtable cost 22% on decode; four runs put the dyn variant *below* the concrete
//! one. Treat both rows as absent rather than as evidence, and do not use them as
//! regression guards until they are stabilised.

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

// ── Stand-ins for the proposed TermEncoder shapes ───────────────────────────────

/// `&self`, locking inside. No guard reaches the caller, so `Arc<RwLock<_>>` stays
/// an implementation detail and P5 is satisfied. `decode` has to return an owned
/// `String`: a borrow into the dictionary cannot outlive the guard taken to get it.
trait EncodeShared {
    fn encode(&self, value: &str) -> u32;
    fn decode(&self, id: u32) -> Option<String>;
    /// The way out of the allocation: lend the term inside the guard instead of
    /// handing it back. Same shape as `QuadSource::for_each_quad`, and for the same
    /// reason — one closure per call beats one allocation per call.
    fn with_decoded(&self, id: u32, visit: &mut dyn FnMut(Option<&str>));
}

/// `&mut self`, with the caller holding the guard. Batching survives and `decode`
/// can still lend a `&str`, but the handle has to reach the caller, so the thing
/// the abstraction exists to hide stays visible.
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

/// Encoding and decoding under each shape, over the same 10,000 warm terms as the
/// group above, so a per-term cost can be read straight off the ratio.
fn bench_encoder_shapes(c: &mut Criterion) {
    let iris = iris();
    let mut dict = Dictionary::new();
    for iri in &iris {
        dict.encode(iri);
    }
    let ids: Vec<u32> = iris.iter().map(|iri| dict.encode(iri)).collect();
    let handle = Arc::new(RwLock::new(dict));

    let mut group = c.benchmark_group("micro_term_encoder");

    // ── encode ──────────────────────────────────────────────────────────────────
    // One guard for the whole loop: what the 125 batching call sites do today.
    group.bench_function("encode_batched_guard", |b| {
        b.iter(|| {
            let mut guard = handle.write().unwrap();
            for iri in &iris {
                black_box(guard.encode(black_box(iri)));
            }
        })
    });

    // A lock per call, called concretely. Isolates the lock from the vtable.
    group.bench_function("encode_lock_per_call", |b| {
        b.iter(|| {
            for iri in &iris {
                black_box(handle.write().unwrap().encode(black_box(iri)));
            }
        })
    });

    // A lock per call, through `&dyn`. The difference from the line above is the
    // vtable and nothing else.
    group.bench_function("encode_dyn_interior", |b| {
        let encoder: &dyn EncodeShared = &handle;
        b.iter(|| {
            for iri in &iris {
                black_box(encoder.encode(black_box(iri)));
            }
        })
    });

    // `&mut dyn` with the caller holding the guard: batching plus a vtable.
    group.bench_function("encode_dyn_guard", |b| {
        b.iter(|| {
            let mut guard = handle.write().unwrap();
            let encoder: &mut dyn EncodeExclusive = &mut *guard;
            for iri in &iris {
                black_box(encoder.encode(black_box(iri)));
            }
        })
    });

    // ── decode ──────────────────────────────────────────────────────────────────
    // One read guard, lending `&str`: free of both the lock and the allocation.
    group.bench_function("decode_batched_guard", |b| {
        b.iter(|| {
            let guard = handle.read().unwrap();
            for id in &ids {
                black_box(guard.decode(black_box(*id)));
            }
        })
    });

    // A read lock per call, still lending within the call.
    group.bench_function("decode_lock_per_call", |b| {
        b.iter(|| {
            for id in &ids {
                black_box(handle.read().unwrap().decode(black_box(*id)).is_some());
            }
        })
    });

    // What interior locking actually forces: a lock *and* a `String` per call.
    group.bench_function("decode_dyn_interior_owned", |b| {
        let encoder: &dyn EncodeShared = &handle;
        b.iter(|| {
            for id in &ids {
                black_box(encoder.decode(black_box(*id)));
            }
        })
    });

    // The cheapest guard-based dyn decode: one read guard, lending `&str` through a
    // vtable. The floor for any trait-based decode, and the shape that is only
    // available if the guard reaches the caller.
    group.bench_function("decode_dyn_guard", |b| {
        b.iter(|| {
            let guard = handle.read().unwrap();
            let encoder: &dyn EncodeExclusive = &*guard;
            for id in &ids {
                black_box(encoder.decode(black_box(*id)));
            }
        })
    });

    // The visitor shape: interior locking without the allocation.
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

criterion_group!(
    benches,
    bench_encode,
    bench_encoder_shapes,
    bench_contention
);
criterion_main!(benches);
