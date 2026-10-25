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
//! | shape | encode | decode |
//! |---|---:|---:|
//! | direct, no lock | — | — |
//! | concrete, one batched guard | +2% | +7% |
//! | `&mut dyn`, caller holds the guard | +0.4% | +30% |
//! | concrete, lock per call | +6% | +13% |
//! | `&dyn` interior lock, visitor `decode` | +8% | +46% |
//! | `&dyn` interior lock, owned `String` | — | +272% |
//!
//! **Locking is cheap, and the batching 125 call sites rely on buys about 3%**, so
//! it is not worth designing around. That removes the main objection to
//! `encode(&self)`, which is the only shape that keeps `Arc<RwLock<_>>` out of
//! public signatures.
//!
//! **Dispatch is free on encode and is not on decode.** A vtable costs nothing
//! measurable against a ~19ns encode, and about 22% against a ~7ns decode. The
//! smaller the operation, the more a layer of indirection shows, which is why
//! `micro_scan`'s result — dispatch free at every selectivity — does not carry over.
//!
//! **The large cost is what `decode` returns.** An owned `String` per call, which
//! interior locking forces because a borrow cannot outlive the guard taken to get
//! it, is roughly three times the decode itself. Lending through a visitor recovers
//! most of it, the same answer `QuadSource::for_each_quad` reached for the same
//! reason. No shape is both free and guard-less, so `decode` trades a few
//! nanoseconds per call for P5 compliance and should be documented as that rather
//! than as a free abstraction.

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
