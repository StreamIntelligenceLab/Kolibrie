/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reaching the term dictionary: what the sealed accessors cost.
//!
//! **The question this was written for is answered.** It originally compared a direct
//! `db.dictionary` field read against an `#[inline]` accessor and an
//! `#[inline(never)]` one, to find out whether sealing `SparqlDatabase`'s fields
//! would cost anything before converting twenty-odd fields and their call sites. The
//! three arms landed within 1.3% of each other — inside the noise floor even for the
//! outlined version — so sealing went ahead on measured rather than assumed grounds.
//! That comparison is no longer expressible here: `dictionary` is private, so a bench
//! outside the crate cannot read the field. The original is in the history at the
//! commit that sealed it.
//!
//! What it guards now is the two paths a caller actually has:
//!
//!   * `dictionary()` — the guard accessor, which is what essentially all 96
//!     rewritten call sites use.
//!   * `dictionary_handle()` — the documented escape hatch for sharing the encoding
//!     with another engine, which costs an `Arc` clone before the lock.
//!
//! The outlined arm stays as the pessimistic bound: if the guard ever stops being
//! inlined, the gap between it and `guard_accessor` is where that shows up.

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kolibrie::sparql_database::SparqlDatabase;

/// Deliberately not inlinable, as the pessimistic bound on the guard accessor.
#[inline(never)]
fn next_id_outlined(db: &SparqlDatabase) -> u32 {
    db.dictionary().next_id
}

fn bench_access(c: &mut Criterion) {
    let database = SparqlDatabase::new();
    for i in 0..1_000 {
        database
            .dictionary_mut()
            .encode(&format!("http://example.org/e{i}"));
    }

    let mut group = c.benchmark_group("micro_field_access");

    group.bench_function("guard_accessor", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for _ in 0..1_000 {
                n += black_box(&database).dictionary().next_id as usize;
            }
            black_box(n)
        })
    });

    group.bench_function("handle_then_lock", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for _ in 0..1_000 {
                n += black_box(&database)
                    .dictionary_handle()
                    .read()
                    .unwrap()
                    .next_id as usize;
            }
            black_box(n)
        })
    });

    group.bench_function("outlined_guard", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for _ in 0..1_000 {
                n += next_id_outlined(black_box(&database)) as usize;
            }
            black_box(n)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_access);
criterion_main!(benches);
