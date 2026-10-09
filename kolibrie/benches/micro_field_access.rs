/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Does sealing `SparqlDatabase` cost anything?
//!
//! The proposal makes all 23 public fields private behind accessors. Received
//! wisdom says a `#[inline]` accessor compiles to the same code as a field read —
//! but that is a claim about this codebase's call sites and optimisation settings,
//! not a law. Measuring it on *one* field now is far cheaper than discovering a
//! regression after converting twenty-three and nineteen dependent modules.
//!
//! `dictionary` is the field to measure: it is read 36 times across the crate, more
//! than any other, and it is read inside query execution rather than at setup.
//!
//! If the two columns here are indistinguishable, sealing is free and the
//! encapsulation work proceeds without a performance argument against it.

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kolibrie::sparql_database::SparqlDatabase;
use shared::dictionary::Dictionary;
use std::sync::{Arc, RwLock};

/// Stand-in for the accessor that sealing would introduce.
#[inline]
fn dictionary_of(db: &SparqlDatabase) -> &Arc<RwLock<Dictionary>> {
    &db.dictionary
}

/// Deliberately not inlinable — the pessimistic bound. If the gap between this and
/// the field read is large, sealing needs `#[inline]` discipline rather than being
/// automatically free, and that is worth knowing up front.
#[inline(never)]
fn dictionary_of_never(db: &SparqlDatabase) -> &Arc<RwLock<Dictionary>> {
    &db.dictionary
}

fn bench_access(c: &mut Criterion) {
    let mut database = SparqlDatabase::new();
    for i in 0..1_000 {
        database
            .dictionary
            .write()
            .unwrap()
            .encode(&format!("http://example.org/e{i}"));
    }

    let mut group = c.benchmark_group("micro_field_access");

    group.bench_function("direct_field", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for _ in 0..1_000 {
                n += black_box(&database).dictionary.read().unwrap().next_id as usize;
            }
            black_box(n)
        })
    });

    group.bench_function("inline_accessor", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for _ in 0..1_000 {
                n += dictionary_of(black_box(&database)).read().unwrap().next_id as usize;
            }
            black_box(n)
        })
    });

    group.bench_function("outlined_accessor", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for _ in 0..1_000 {
                n += dictionary_of_never(black_box(&database)).read().unwrap().next_id as usize;
            }
            black_box(n)
        })
    });

    group.finish();
}

criterion_group!(benches, bench_access);
criterion_main!(benches);
