/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reaching the term dictionary: what the sealed accessors cost

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kolibrie::database::KolibrieDB;
use shared::term_encoder::TermEncoder;

/// Deliberately not inlinable, as the pessimistic bound on the guard accessor
#[inline(never)]
fn next_id_outlined(db: &KolibrieDB) -> u32 {
    db.dictionary().next_id
}

fn bench_access(c: &mut Criterion) {
    let database = KolibrieDB::new();
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

    group.bench_function("terms_then_scope", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for _ in 0..1_000 {
                black_box(&database).terms().with_terms(&mut |dict| {
                    n += dict.next_id as usize;
                });
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
