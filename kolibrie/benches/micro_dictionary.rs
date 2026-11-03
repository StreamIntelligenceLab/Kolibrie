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
//! Over 10,000 warm terms, from two independent four-run baselines. Quote only the
//! ratios between the low-noise rows: every shape that locks per call measures
//! stably, and every direct or guard-held one does not.
//!
//! | shape | median | noise | usable as an anchor |
//! |---|---:|---:|---|
//! | `decode` direct | 74us | 31% | no |
//! | `decode_batched_guard` | 82us | 11% | no |
//! | `decode_dyn_guard` | 77us | 8% | no |
//! | `decode_lock_per_call` | 83us | 1.0% | yes |
//! | `decode_dyn_visitor` | 109us | 1.0% | yes |
//! | `decode_dyn_interior_owned` | 278us | 0.5% | yes |
//! | `encode_warm` direct | 183us | 8% | no |
//! | `encode_lock_per_call` | 193us | 2.1% | yes |
//! | `encode_dyn_interior` | 200us | 1.2% | yes |
//!
//! Per-call locking dominates its own measurement and so swamps scheduling
//! variance; the tight loops are at the mercy of it. `decode` direct measured 4.2%
//! noise in one baseline and 31.2% in the next on identical code, which is why no
//! figure here is given relative to it.
//!
//! What the stable rows support:
//!
//!   * **An owned `String` return costs 2.55x a visitor** (278us against 109us).
//!     Interior locking forces the owned return, because a borrow cannot outlive
//!     the guard it came from, so this is the cost of the obvious design and the
//!     reason the visitor exists.
//!   * **A visitor costs 31% over per-call locking that lends** (109us against
//!     83us) — the price of not handing out a guard.
//!   * **A vtable costs about 4% on encode** (200us against 193us, both stable).
//!     This is the only dispatch figure here worth anything; an earlier claim of
//!     22% on decode came from comparing two rows that both turned out to be noisy.
//!
//! What they do not support: any statement of overhead against direct access. The
//! direct rows are too unstable, so the absolute cost of the abstraction is not
//! measured here, only the differences between its candidate shapes.
//!

//! ## Under contention
//!
//! Everything here is single-threaded, so every lock is uncontended — the cheap
//! case, and a misleading one. `benches/contended_dictionary.rs` measures the same
//! operations across threads and reaches the opposite conclusion: per-call locking
//! on a shared dictionary is slower than a single thread at two threads and 4 to 12
//! times worse at eight. Read that before drawing anything from the locking rows
//! here. It sits in the `heavy` tier because it answers a design question rather
//! than guarding a refactor.
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

criterion_group!(benches, bench_encode, bench_encoder_shapes);
criterion_main!(benches);
