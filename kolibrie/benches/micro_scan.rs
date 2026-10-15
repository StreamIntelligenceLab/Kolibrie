/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Index scanning — and the cost of putting a trait in front of it.
//!
//! This bench answers a design question rather than only guarding a change. The
//! architecture proposal replaces direct `DatasetIndex` access with a `QuadSource`
//! trait so the planner and the reasoners can compile without the engine. Two
//! choices in that trait are contested:
//!
//!   1. static dispatch (`&impl QuadSource`) vs dynamic (`&dyn QuadSource`)
//!   2. row-at-a-time (`Iterator<Item = Quad>`) vs batch (`Iterator<Item = Vec<Quad>>`)
//!
//! Dynamic dispatch and per-call allocation are both cheap in isolation and
//! expensive in a hot loop, so the honest way to settle it is to measure all four
//! shapes against the current direct call before committing.
//!
//! The traits below are throwaway stand-ins for the proposed API, defined here so
//! the measurement can happen without first building the real thing.

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use shared::dataset_index::{DatasetIndex, GraphId, Quad};
use shared::triple::Triple;

const SUBJECTS: u32 = 5_000;
const PREDICATES: u32 = 8;

fn index() -> DatasetIndex {
    let mut index = DatasetIndex::new();
    for i in 0..SUBJECTS {
        for p in 0..PREDICATES {
            index.insert_triple(&Triple {
                subject: i,
                predicate: SUBJECTS + p,
                object: SUBJECTS + PREDICATES + (i % 1_000),
            });
        }
    }
    index
}

// ── Stand-ins for the proposed QuadSource shapes ────────────────────────────────

/// Statically dispatched, returns an owned Vec — closest to today's `query_graph`.
trait ScanStatic {
    fn scan(&self, g: GraphId, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Quad>;
}

/// Dynamically dispatched. Object-safe, so it can be stored as `Box<dyn …>`.
trait ScanDyn {
    fn scan(&self, g: GraphId, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Quad>;
}

/// Dynamically dispatched AND row-at-a-time, which is the shape that allocates a
/// boxed iterator per call. This is the combination most likely to cost real time.
trait ScanDynIter {
    fn scan<'a>(
        &'a self,
        g: GraphId,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Box<dyn Iterator<Item = Quad> + 'a>;
}

impl ScanStatic for DatasetIndex {
    fn scan(&self, g: GraphId, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Quad> {
        self.query_graph(g, s, p, o)
    }
}

impl ScanDyn for DatasetIndex {
    fn scan(&self, g: GraphId, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Quad> {
        self.query_graph(g, s, p, o)
    }
}

impl ScanDynIter for DatasetIndex {
    fn scan<'a>(
        &'a self,
        g: GraphId,
        s: Option<u32>,
        p: Option<u32>,
        o: Option<u32>,
    ) -> Box<dyn Iterator<Item = Quad> + 'a> {
        Box::new(self.query_graph(g, s, p, o).into_iter())
    }
}

fn consume(quads: &[Quad]) -> usize {
    quads.iter().map(|q| q.subject as usize).sum()
}

fn bench_dispatch(c: &mut Criterion) {
    let index = index();
    let dyn_ref: &dyn ScanDyn = &index;
    let dyn_iter_ref: &dyn ScanDynIter = &index;

    // Three selectivities, because dispatch overhead is a fixed cost per call: it is
    // invisible on a scan returning 160k quads and dominant on one returning 8.
    let cases: [(&str, Option<u32>, Option<u32>, Option<u32>); 3] = [
        ("selective_subject", Some(42), None, None),
        ("medium_predicate", None, Some(SUBJECTS), None),
        ("broad_all", None, None, None),
    ];

    for (name, s, p, o) in cases {
        let mut group = c.benchmark_group(format!("micro_scan/{name}"));

        group.bench_function("direct", |b| {
            b.iter(|| {
                let quads =
                    index.query_graph(GraphId::Default, black_box(s), black_box(p), black_box(o));
                black_box(consume(&quads))
            })
        });

        group.bench_function("static_trait", |b| {
            b.iter(|| {
                let quads = ScanStatic::scan(
                    &index,
                    GraphId::Default,
                    black_box(s),
                    black_box(p),
                    black_box(o),
                );
                black_box(consume(&quads))
            })
        });

        group.bench_function("dyn_trait", |b| {
            b.iter(|| {
                let quads =
                    dyn_ref.scan(GraphId::Default, black_box(s), black_box(p), black_box(o));
                black_box(consume(&quads))
            })
        });

        group.bench_function("dyn_boxed_iter", |b| {
            b.iter(|| {
                let total: usize = dyn_iter_ref
                    .scan(GraphId::Default, black_box(s), black_box(p), black_box(o))
                    .map(|q| q.subject as usize)
                    .sum();
                black_box(total)
            })
        });

        group.finish();
    }
}

criterion_group!(benches, bench_dispatch);
criterion_main!(benches);
