/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Index scanning — and the cost of putting a trait in front of it

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

/// Statically dispatched, returns an owned Vec — closest to today's `query_graph`
trait ScanStatic {
    fn scan(&self, g: GraphId, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Quad>;
}

/// Dynamically dispatched
trait ScanDyn {
    fn scan(&self, g: GraphId, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Quad>;
}

/// Dynamically dispatched AND row-at-a-time, which is the shape that allocates a boxed iterator per
/// call
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
