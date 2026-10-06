/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![cfg(feature = "exec-stats")]

use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::streamertail_optimizer::execution::exec_stats;
use kolibrie::streamertail_optimizer::PhysicalOperator;
use shared::dataset_index::{GraphTerm, QuadPattern};
use shared::terms::{Term, TriplePattern};
use std::sync::Mutex;

mod common;

/// Serializes access to the process-global counters
static COUNTER_LOCK: Mutex<()> = Mutex::new(());

fn var(name: &str) -> Term {
    Term::Variable(name.to_string())
}

fn constant(database: &SparqlDatabase, value: &str) -> Term {
    Term::Constant(database.dictionary.write().unwrap().encode(value))
}

fn quad(pattern: TriplePattern) -> QuadPattern {
    QuadPattern {
        subject: pattern.0,
        predicate: pattern.1,
        object: pattern.2,
        graph: GraphTerm::Default,
    }
}

/// Runs a plan and reports the work it performed
fn work_of(database: &mut SparqlDatabase, plan: &PhysicalOperator) -> exec_stats::Snapshot {
    exec_stats::reset();
    let rows = plan.execute_with_ids(database);
    let snapshot = exec_stats::snapshot();
    // Touch the result so the execution cannot be optimized away
    assert!(rows.len() < usize::MAX);
    snapshot
}

fn star_fixture() -> SparqlDatabase {
    let mut database = SparqlDatabase::new();
    let mut triples = String::new();
    for subject in 0..40 {
        triples.push_str(&format!(
            "<http://ex/s{subject}> <http://ex/knows> <http://ex/o{subject}> .\n"
        ));
        triples.push_str(&format!(
            "<http://ex/s{subject}> <http://ex/age> \"{subject}\" .\n"
        ));
        // Only a fifth of the subjects have a city, making that arm selective
        if subject % 5 == 0 {
            triples.push_str(&format!(
                "<http://ex/s{subject}> <http://ex/city> <http://ex/c{subject}> .\n"
            ));
        }
    }
    common::load(&mut database, &triples);
    database
}

/// The consensus asks for `StarJoin` to be measured before any decision about
#[test]
fn star_join_performs_the_same_work_as_a_bind_join_chain() {
    let _guard = COUNTER_LOCK.lock().unwrap();
    let mut database = star_fixture();

    let knows = constant(&database, "http://ex/knows");
    let age = constant(&database, "http://ex/age");
    let city = constant(&database, "http://ex/city");

    let patterns: Vec<TriplePattern> = vec![
        (var("s"), knows, var("o")),
        (var("s"), age, var("a")),
        (var("s"), city, var("c")),
    ];

    let star = PhysicalOperator::StarJoin {
        join_var: "s".to_string(),
        patterns: patterns.clone(),
    };
    let chain = PhysicalOperator::bind_join(
        PhysicalOperator::bind_join(
            PhysicalOperator::quad_index_scan(quad(patterns[0].clone())),
            PhysicalOperator::quad_index_scan(quad(patterns[1].clone())),
        ),
        PhysicalOperator::quad_index_scan(quad(patterns[2].clone())),
    );

    let star_work = work_of(&mut database, &star);
    let chain_work = work_of(&mut database, &chain);

    // `StarJoin`'s executor simply chains scans with a hard-coded default
    assert_eq!(
        star_work.quads_examined, chain_work.quads_examined,
        "StarJoin examined a different number of quads than the equivalent chain: \
         star={star_work:?} chain={chain_work:?}"
    );
    assert_eq!(
        star_work.scan_probes, chain_work.scan_probes,
        "StarJoin issued a different number of probes: star={star_work:?} chain={chain_work:?}"
    );
}

#[test]
fn a_selective_leading_pattern_examines_fewer_quads() {
    let _guard = COUNTER_LOCK.lock().unwrap();
    let mut database = star_fixture();

    let knows = constant(&database, "http://ex/knows");
    let city = constant(&database, "http://ex/city");

    // Leading with the selective arm binds ?s eight times instead of scanning the wide one
    let selective_first = PhysicalOperator::bind_join(
        PhysicalOperator::quad_index_scan(quad((var("s"), city.clone(), var("c")))),
        PhysicalOperator::quad_index_scan(quad((var("s"), knows.clone(), var("o")))),
    );
    let wide_first = PhysicalOperator::bind_join(
        PhysicalOperator::quad_index_scan(quad((var("s"), knows, var("o")))),
        PhysicalOperator::quad_index_scan(quad((var("s"), city, var("c")))),
    );

    let selective_work = work_of(&mut database, &selective_first);
    let wide_work = work_of(&mut database, &wide_first);

    assert!(
        selective_work.quads_examined < wide_work.quads_examined,
        "join order made no difference to work performed: selective={selective_work:?} wide={wide_work:?}"
    );
}

/// A merged query default has to suppress duplicates; a single default graph
#[test]
fn a_single_default_graph_scan_examines_each_quad_once() {
    let _guard = COUNTER_LOCK.lock().unwrap();
    let mut database = star_fixture();
    let knows = constant(&database, "http://ex/knows");

    let scan = PhysicalOperator::quad_index_scan(quad((var("s"), knows, var("o"))));
    let work = work_of(&mut database, &scan);

    assert_eq!(work.scan_probes, 1, "one graph should mean one probe");
    assert_eq!(
        work.quads_examined, 40,
        "each matching quad should be examined exactly once: {work:?}"
    );
}
