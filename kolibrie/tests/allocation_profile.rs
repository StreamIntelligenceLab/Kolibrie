/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use kolibrie::execute_query::{execute_query_rayon_parallel2_volcano, execute_sparql_update};
use kolibrie::sparql_database::SparqlDatabase;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout)
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size.saturating_sub(layout.size()), Ordering::Relaxed);
        System.realloc(pointer, layout, new_size)
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const SUBJECTS: usize = 5_000;
const NAMED_GRAPHS: usize = 4;
const DEPARTMENTS: usize = 50;
const REGIONS: usize = 10;
const EX: &str = "http://example.org/";

fn setup_database() -> SparqlDatabase {
    let mut database = SparqlDatabase::new();

    let mut default_graph = String::with_capacity(SUBJECTS * 160);
    for i in 0..SUBJECTS {
        let salary = 40_000 + (i % 60_000);
        default_graph.push_str(&format!(
            "<{EX}e{i}> <{EX}salary> \"{salary}\" .\n<{EX}e{i}> <{EX}dept> <{EX}d{}> .\n",
            i % DEPARTMENTS
        ));
    }
    execute_sparql_update(
        &format!("INSERT DATA {{\n{default_graph}\n}}"),
        &mut database,
    )
    .expect("default graph fixture must load");

    for graph in 0..NAMED_GRAPHS {
        let mut named = String::with_capacity((SUBJECTS / NAMED_GRAPHS) * 90);
        for i in (graph..SUBJECTS).step_by(NAMED_GRAPHS) {
            named.push_str(&format!(
                "<{EX}e{i}> <{EX}region> <{EX}r{}> .\n",
                i % REGIONS
            ));
        }
        execute_sparql_update(
            &format!("INSERT DATA {{ GRAPH <{EX}g{graph}> {{\n{named}\n}} }}"),
            &mut database,
        )
        .expect("named graph fixture must load");
    }

    database
}

const QUERIES: [(&str, &str); 6] = [
    (
        "graph_variable_scan",
        "SELECT ?s ?r WHERE { GRAPH ?g { ?s <http://example.org/region> ?r } }",
    ),
    (
        // A GRAPH pattern fed by an earlier binding pattern, so the graph scope
        "graph_variable_after_join",
        "SELECT ?s ?d ?r WHERE { ?s <http://example.org/dept> ?d . GRAPH ?g { ?s <http://example.org/region> ?r } }",
    ),
    (
        "graph_fixed_scan",
        "SELECT ?s ?r WHERE { GRAPH <http://example.org/g0> { ?s <http://example.org/region> ?r } }",
    ),
    (
        "union_branches",
        "SELECT ?s WHERE { { ?s <http://example.org/salary> ?v } UNION { ?s <http://example.org/dept> ?d } UNION { GRAPH ?g { ?s <http://example.org/region> ?r } } }",
    ),
    (
        "filter_numeric",
        "SELECT ?s ?v WHERE { ?s <http://example.org/salary> ?v . FILTER(?v > 70000) }",
    ),
    (
        "join_two_patterns",
        "SELECT ?s ?v ?d WHERE { ?s <http://example.org/salary> ?v . ?s <http://example.org/dept> ?d }",
    ),
];

#[test]
fn allocation_profile() {
    let mut database = setup_database();

    // Warm up so thread-pool and dictionary growth are not attributed to a query
    for (_, sparql) in QUERIES {
        execute_query_rayon_parallel2_volcano(sparql, &mut database);
    }

    println!("\n{:<22} {:>14} {:>16} {:>10}", "query", "allocations", "bytes", "rows");
    println!("{}", "-".repeat(66));

    for (name, sparql) in QUERIES {
        let before_allocations = ALLOCATIONS.load(Ordering::Relaxed);
        let before_bytes = BYTES.load(Ordering::Relaxed);

        let rows = execute_query_rayon_parallel2_volcano(sparql, &mut database).len();

        let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before_allocations;
        let bytes = BYTES.load(Ordering::Relaxed) - before_bytes;

        println!("{name:<22} {allocations:>14} {bytes:>16} {rows:>10}");
    }
    println!();
}
