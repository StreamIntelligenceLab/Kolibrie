/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

// Usage: cargo run --release --example replay_profile -- [members] [evals]

use kolibrie::execute_query::execute_query_rayon_parallel2_volcano;
use kolibrie::sparql_database::{PlanningStatsPolicy, SparqlDatabase};
use std::time::{Duration, Instant};

const BASE: &str = "http://www.semanticweb.org/ontologies/2015/trainbenchmark#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// Two-pattern join; requires statistics
const JOIN_OF_TWO: &str = r#"
PREFIX base: <http://www.semanticweb.org/ontologies/2015/trainbenchmark#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?switch ?segment WHERE {
  ?switch rdf:type base:Switch .
  ?switch base:connectsTo ?segment .
}
"#;

/// Single pattern; statistics are skipped
const SINGLE_PATTERN: &str = r#"
PREFIX base: <http://www.semanticweb.org/ontologies/2015/trainbenchmark#>
SELECT ?switch ?segment WHERE {
  ?switch base:connectsTo ?segment .
}
"#;

/// The trainbenchmark RouteSensor shape, without its unsupported NOT EXISTS
const SEVEN_PATTERN_JOIN: &str = r#"
PREFIX base: <http://www.semanticweb.org/ontologies/2015/trainbenchmark#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?route ?sensor ?swP ?sw WHERE {
  ?route base:follows ?swP .
  ?swP base:target ?sw .
  ?sw base:monitoredBy ?sensor .
  ?route rdf:type base:Route .
  ?swP rdf:type base:SwitchPosition .
  ?sw rdf:type base:Switch .
  ?sensor rdf:type base:Sensor .
}
"#;

/// Inserts one stream member
fn insert_member(database: &mut SparqlDatabase, member: usize) {
    let switch = format!("{BASE}sw_{member}");
    let segment = format!("{BASE}seg_{member}");
    let sensor = format!("{BASE}sensor_{member}");
    let route = format!("{BASE}route_{member}");
    let position = format!("{BASE}swp_{member}");

    database.add_triple_parts(&switch, RDF_TYPE, &format!("{BASE}Switch"));
    database.add_triple_parts(&segment, RDF_TYPE, &format!("{BASE}Segment"));
    database.add_triple_parts(&sensor, RDF_TYPE, &format!("{BASE}Sensor"));
    database.add_triple_parts(&route, RDF_TYPE, &format!("{BASE}Route"));
    database.add_triple_parts(&position, RDF_TYPE, &format!("{BASE}SwitchPosition"));
    database.add_triple_parts(&switch, &format!("{BASE}connectsTo"), &segment);
    database.add_triple_parts(&switch, &format!("{BASE}monitoredBy"), &sensor);
    database.add_triple_parts(&route, &format!("{BASE}follows"), &position);
    database.add_triple_parts(&position, &format!("{BASE}target"), &switch);
    database.add_triple_parts(
        &segment,
        &format!("{BASE}length"),
        &format!("{}", member % 50),
    );
}

struct Replay {
    total: Duration,
    query: Duration,
    rebuilds: u64,
    rebuild_time: Duration,
    rows: usize,
}

/// Inserts every member, evaluating the query once every `every` members
fn replay(members: usize, every: usize, policy: PlanningStatsPolicy, query: &str) -> Replay {
    let mut database = SparqlDatabase::new();
    database.planning_stats_policy = policy;

    let started = Instant::now();
    let mut query_time = Duration::ZERO;
    let mut rows = 0;
    for member in 0..members {
        insert_member(&mut database, member);
        if member % every == 0 {
            let at = Instant::now();
            rows = execute_query_rayon_parallel2_volcano(query, &mut database).len();
            query_time += at.elapsed();
        }
    }

    let (rebuilds, rebuild_time) = database.stats_rebuild_metrics();
    Replay {
        total: started.elapsed(),
        query: query_time,
        rebuilds,
        rebuild_time,
        rows,
    }
}

fn report(label: &str, evals: usize, run: &Replay) {
    println!(
        "  {label:<24} total {:>9.3?}  query {:>9.3?}  rebuilds {:>4}/{:<4} ({:>9.3?})  rows {}",
        run.total, run.query, run.rebuilds, evals, run.rebuild_time, run.rows
    );
}

fn argument(position: usize, fallback: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn main() {
    let members = argument(1, 10_000);
    let evals = argument(2, 20);

    // Ingest alone
    let mut database = SparqlDatabase::new();
    let started = Instant::now();
    for member in 0..members {
        insert_member(&mut database, member);
    }
    let ingest = started.elapsed();
    let quads = database.dataset_index.count_quads();
    println!("store: {quads} quads from {members} members");
    println!("ingest total            : {ingest:?}");
    println!("ingest per quad         : {:?}", ingest / quads.max(1) as u32);

    // Cost of one statistics rebuild
    database.invalidate_stats_cache();
    let started = Instant::now();
    let _ = database.get_or_build_stats();
    println!("stats rebuild (cold)    : {:?}", started.elapsed());

    let started = Instant::now();
    let _ = database.get_or_build_stats();
    println!("stats lookup  (warm)    : {:?}", started.elapsed());

    let started = Instant::now();
    for _ in 0..10 {
        database.invalidate_stats_cache();
        std::hint::black_box(database.get_or_build_stats());
    }
    println!("stats rebuild (10x mean): {:?}", started.elapsed() / 10);

    // Query cost with warm statistics
    let _ = execute_query_rayon_parallel2_volcano(JOIN_OF_TWO, &mut database);
    let started = Instant::now();
    for _ in 0..evals {
        std::hint::black_box(execute_query_rayon_parallel2_volcano(
            JOIN_OF_TWO,
            &mut database,
        ));
    }
    let warm = started.elapsed();
    println!(
        "{evals} evals, cache warm   : {warm:?} ({:?} each)",
        warm / evals.max(1) as u32
    );

    let rows = execute_query_rayon_parallel2_volcano(SEVEN_PATTERN_JOIN, &mut database).len();
    let started = Instant::now();
    for _ in 0..5 {
        std::hint::black_box(execute_query_rayon_parallel2_volcano(
            SEVEN_PATTERN_JOIN,
            &mut database,
        ));
    }
    println!(
        "7-pattern join, warm    : {:?} each, {rows} rows",
        started.elapsed() / 5
    );

    // Writes and queries interleaved
    let every = 100;
    let count = members / every;
    println!("\nreplay shape: {count} evals interleaved with {members} member inserts");

    let fresh = replay(members, every, PlanningStatsPolicy::AlwaysFresh, JOIN_OF_TWO);
    report("AlwaysFresh", count, &fresh);

    for (minimum_batch, refresh_fraction) in [(100u64, 0.50f64), (250, 0.25), (500, 0.10)] {
        let run = replay(
            members,
            every,
            PlanningStatsPolicy::Bounded {
                minimum_batch,
                refresh_fraction,
            },
            JOIN_OF_TWO,
        );
        assert_eq!(
            run.rows, fresh.rows,
            "bounded statistics must not change the result"
        );
        let policy = PlanningStatsPolicy::Bounded {
            minimum_batch,
            refresh_fraction,
        };
        let suffix = if policy == PlanningStatsPolicy::DEFAULT_BOUNDED { " (default)" } else { "" };
        report(
            &format!("Bounded {{{minimum_batch}, {refresh_fraction}}}{suffix}"),
            count,
            &run,
        );
    }

    println!("\nsame replay, single-pattern query (no join order to choose)");
    let trivial = replay(
        members,
        every,
        PlanningStatsPolicy::AlwaysFresh,
        SINGLE_PATTERN,
    );
    report("AlwaysFresh", count, &trivial);
}
