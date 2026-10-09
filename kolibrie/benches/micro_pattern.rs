/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Pattern matching and triple construction — the reasoner's inner loop.
//!
//! **Why this bench lives in `kolibrie` and not in `datalog`.** `matches_rule_pattern`
//! and `construct_triple` are planned to move into `shared`, which turns every call
//! site into a cross-crate call. Cross-crate calls can only be inlined through LTO,
//! and `lto = "thin"` reduces that loss without eliminating it. Benching from a
//! *different crate than the one defining the function* is therefore the measurement
//! that matters; a bench inside `datalog` would inline freely and tell us nothing.
//!
//! Keep this file calling through whatever path currently exports these functions, so
//! the before/after comparison stays honest when they move.

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use datalog::reasoning::{construct_triple, matches_rule_pattern};
use shared::dictionary::Dictionary;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use std::collections::HashMap;

const FACTS: usize = 10_000;
const SUBJECTS: u32 = 500;
const PREDICATES: u32 = 10;

/// A fact table with a realistic mix: many subjects, few predicates, so a
/// predicate-bound pattern matches a meaningful fraction rather than all or nothing.
fn facts() -> Vec<Triple> {
    (0..FACTS as u32)
        .map(|i| Triple {
            subject: i % SUBJECTS,
            predicate: SUBJECTS + (i % PREDICATES),
            object: SUBJECTS + PREDICATES + (i % 997),
        })
        .collect()
}

fn var(name: &str) -> Term {
    Term::Variable(name.to_string())
}

fn bench_matches(c: &mut Criterion) {
    let facts = facts();
    let mut group = c.benchmark_group("micro_pattern/matches_rule_pattern");

    // Three selectivities. The all-variable case is the reasoner's common shape;
    // the fully-bound case isolates the call overhead itself, which is what a lost
    // inline shows up in most clearly.
    let patterns: Vec<(&str, TriplePattern)> = vec![
        ("all_variables", (var("s"), var("p"), var("o"))),
        (
            "predicate_bound",
            (var("s"), Term::Constant(SUBJECTS), var("o")),
        ),
        (
            "fully_bound",
            (
                Term::Constant(0),
                Term::Constant(SUBJECTS),
                Term::Constant(SUBJECTS + PREDICATES),
            ),
        ),
    ];

    for (name, pattern) in &patterns {
        group.bench_function(*name, |b| {
            b.iter_batched(
                HashMap::new,
                |mut bindings| {
                    let mut hits = 0usize;
                    for fact in &facts {
                        bindings.clear();
                        if matches_rule_pattern(
                            black_box(pattern),
                            black_box(fact),
                            &mut bindings,
                        ) {
                            hits += 1;
                        }
                    }
                    black_box(hits)
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_construct(c: &mut Criterion) {
    let mut dict = Dictionary::new();
    let conclusion: TriplePattern = (var("s"), Term::Constant(SUBJECTS), var("o"));
    let bindings: Vec<HashMap<String, u32>> = (0..1_000u32)
        .map(|i| {
            let mut m = HashMap::new();
            m.insert("s".to_string(), i % SUBJECTS);
            m.insert("o".to_string(), SUBJECTS + PREDICATES + i);
            m
        })
        .collect();

    c.bench_function("micro_pattern/construct_triple", |b| {
        b.iter(|| {
            let mut last = Triple { subject: 0, predicate: 0, object: 0 };
            for vars in &bindings {
                last = construct_triple(
                    black_box(&conclusion),
                    black_box(vars),
                    black_box(&mut dict),
                );
            }
            black_box(last)
        })
    });
}

criterion_group!(benches, bench_matches, bench_construct);
criterion_main!(benches);
