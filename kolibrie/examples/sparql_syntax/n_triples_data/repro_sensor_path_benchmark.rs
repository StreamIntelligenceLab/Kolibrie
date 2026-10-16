/*
 * Copyright (c) 2025 Volodymyr Kadzhaia
 * Copyright (c) 2025 Pieter Bonte
 * KU Leuven - Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::execute_query::execute;
use kolibrie::sparql_database::SparqlDatabase;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const REPRO_QUERY: &str = r#"
PREFIX base: <http://www.semanticweb.org/ontologies/2015/trainbenchmark#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?sensor ?segment1 ?segment2 ?segment3 ?segment4 ?segment5 ?segment6 WHERE {
    ?segment1 base:connectsTo ?segment2 .
    ?segment2 base:connectsTo ?segment3 .
    ?segment3 base:connectsTo ?segment4 .
    ?segment4 base:connectsTo ?segment5 .
    ?segment5 base:connectsTo ?segment6 .
    ?sensor rdf:type base:Sensor .
    ?segment1 base:monitoredBy ?sensor .
    ?segment2 base:monitoredBy ?sensor .
    ?segment3 base:monitoredBy ?sensor .
    ?segment4 base:monitoredBy ?sensor .
    ?segment5 base:monitoredBy ?sensor .
    ?segment6 base:monitoredBy ?sensor .
    ?segment1 rdf:type base:Segment .
    ?segment2 rdf:type base:Segment .
    ?segment3 rdf:type base:Segment .
    ?segment4 rdf:type base:Segment .
    ?segment5 rdf:type base:Segment .
    ?segment6 rdf:type base:Segment .
}
"#;

#[derive(Debug)]
struct QueryMeasurement {
    rows: Vec<Vec<String>>,
    elapsed: Duration,
    start_rss_bytes: Option<u64>,
    peak_rss_bytes: Option<u64>,
    end_rss_bytes: Option<u64>,
}

/// Sensors in the generated stand-in corpus, each monitoring one chain.
const GENERATED_SENSORS: usize = 2_000;

/// Segments per chain. Six is the query's window, so a chain of this length yields
/// `GENERATED_CHAIN - 5` matches, and anything shorter yields none.
const GENERATED_CHAIN: usize = 10;

/// One chain in this many is left unmatched, by having its middle segment monitored
/// by nobody. Without that the query matches every window it looks at, which makes
/// the join cheaper than the corpus this was written against.
const GENERATED_BROKEN_IN: usize = 4;

/// N-Triples in the shape [`REPRO_QUERY`] looks for: chains of `base:connectsTo`
/// segments, all `base:monitoredBy` one `base:Sensor`, everything typed.
///
/// A stand-in for the Train Benchmark corpus, not a reproduction of it. Timings taken
/// against this are comparable with each other and with nothing else.
fn generated_dataset() -> String {
    const BASE: &str = "http://www.semanticweb.org/ontologies/2015/trainbenchmark#";
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let mut out = String::new();
    for sensor in 0..GENERATED_SENSORS {
        let broken = sensor % GENERATED_BROKEN_IN == 0;
        out.push_str(&format!(
            "<{BASE}sensor{sensor}> <{RDF_TYPE}> <{BASE}Sensor> .\n"
        ));
        for segment in 0..GENERATED_CHAIN {
            let id = format!("{BASE}segment{sensor}_{segment}");
            out.push_str(&format!("<{id}> <{RDF_TYPE}> <{BASE}Segment> .\n"));
            // The break leaves one segment unmonitored, so no window containing it
            // can satisfy the query's six `monitoredBy` patterns.
            if !(broken && segment == GENERATED_CHAIN / 2) {
                out.push_str(&format!(
                    "<{id}> <{BASE}monitoredBy> <{BASE}sensor{sensor}> .\n"
                ));
            }
            if segment + 1 < GENERATED_CHAIN {
                out.push_str(&format!(
                    "<{id}> <{BASE}connectsTo> <{BASE}segment{sensor}_{}> .\n",
                    segment + 1
                ));
            }
        }
    }
    out
}

/// The corpus file, if one is to be found.
///
/// First argument, then `KOLIBRIE_REPRO_DATASET`, then the path this example was
/// written against — which `.gitignore` excludes, so a fresh checkout never has it.
/// Returns `None` when none of them is readable, and the corpus is generated instead;
/// the example used to fail with a bare `No such file or directory` here.
fn dataset_path() -> Option<String> {
    let mut candidates = std::env::args()
        .nth(1)
        .into_iter()
        .chain(std::env::var("KOLIBRIE_REPRO_DATASET").ok())
        .chain(std::iter::once(
            "../benchmark_dataset/repro-data.nt".to_string(),
        ));
    candidates.find(|path| fs::metadata(path).is_ok())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))?;

    let data = match dataset_path() {
        Some(path) => {
            let bytes = fs::metadata(&path)?.len();
            let data = fs::read_to_string(&path)?;
            println!("Dataset: {}", path);
            println!("File size: {:.2} MB", bytes as f64 / 1_048_576.0);
            data
        }
        None => {
            let data = generated_dataset();
            println!(
                "Dataset: generated ({} sensors, chains of {})",
                GENERATED_SENSORS, GENERATED_CHAIN
            );
            println!("Size: {:.2} MB", data.len() as f64 / 1_048_576.0);
            println!(
                "NOTE: this is a stand-in, not the Train Benchmark corpus. Timings are \
                 comparable with each other and with nothing else. Pass a path to an \
                 N-Triples file, or set KOLIBRIE_REPRO_DATASET, to measure the real one."
            );
            data
        }
    };
    let line_count = data.lines().count();
    println!("Input lines: {}", line_count);

    let load_start = Instant::now();
    let mut db = SparqlDatabase::new();
    db.parse_ntriples_and_add(&data);
    let load_time = load_start.elapsed();
    println!("Loaded triples: {}", db.default_triple_count());
    println!("Load time: {:.3}s", load_time.as_secs_f64());

    let stats_start = Instant::now();
    db.get_or_build_stats();
    db.build_all_indexes();
    let stats_index_time = stats_start.elapsed();
    println!("Stats/index time: {:.3}s", stats_index_time.as_secs_f64());

    let mut legacy_db = db.clone();
    let mut optimized_db = db.clone();

    let legacy = measure_query("execute (warmup)", || {
        execute(REPRO_QUERY, &mut legacy_db)
            .expect("query should run")
            .into_rows()
    });
    print_measurement("execute (warmup)", &legacy);

    let optimized = measure_query("execute", || {
        execute(REPRO_QUERY, &mut optimized_db)
            .expect("query should run")
            .into_rows()
    });
    print_measurement("execute", &optimized);

    if legacy.rows.len() != optimized.rows.len() {
        eprintln!(
            "WARNING: result count mismatch: execute_query={} optimized={}",
            legacy.rows.len(),
            optimized.rows.len()
        );
    } else {
        println!("Result counts match: {}", legacy.rows.len());
    }

    Ok(())
}

fn measure_query<F>(name: &str, mut f: F) -> QueryMeasurement
where
    F: FnMut() -> Vec<Vec<String>>,
{
    println!("Running {}...", name);
    let start_rss = current_rss_bytes();
    let stop = Arc::new(AtomicBool::new(false));
    let sampler_stop = Arc::clone(&stop);
    let sampler_start = start_rss.unwrap_or(0);

    let sampler = thread::spawn(move || {
        let mut peak = sampler_start;
        while !sampler_stop.load(Ordering::Relaxed) {
            if let Some(rss) = current_rss_bytes() {
                peak = peak.max(rss);
            }
            thread::sleep(Duration::from_millis(10));
        }
        if let Some(rss) = current_rss_bytes() {
            peak = peak.max(rss);
        }
        if peak == 0 {
            None
        } else {
            Some(peak)
        }
    });

    let start = Instant::now();
    let rows = f();
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    let peak_rss = sampler.join().unwrap_or(None);
    let end_rss = current_rss_bytes();

    QueryMeasurement {
        rows,
        elapsed,
        start_rss_bytes: start_rss,
        peak_rss_bytes: peak_rss,
        end_rss_bytes: end_rss,
    }
}

fn print_measurement(name: &str, measurement: &QueryMeasurement) {
    println!("{} rows: {}", name, measurement.rows.len());
    println!("{} time: {:.3}s", name, measurement.elapsed.as_secs_f64());

    if let Some(start) = measurement.start_rss_bytes {
        println!("{} start RSS: {:.2} MB", name, bytes_to_mb(start));
    }
    if let Some(peak) = measurement.peak_rss_bytes {
        println!("{} sampled peak RSS: {:.2} MB", name, bytes_to_mb(peak));
    }
    if let Some(end) = measurement.end_rss_bytes {
        println!("{} end RSS: {:.2} MB", name, bytes_to_mb(end));
    }
    if let (Some(start), Some(peak)) = (measurement.start_rss_bytes, measurement.peak_rss_bytes) {
        println!(
            "{} sampled peak delta: {:.2} MB",
            name,
            bytes_to_mb(peak.saturating_sub(start))
        );
    }
}

fn bytes_to_mb(bytes: u64) -> f64 {
    bytes as f64 / 1_048_576.0
}

// SAFETY: `counters` is zeroed and has its `cb` set to its own size before the call,
// which is the contract `GetProcessMemoryInfo` documents; the pointer is to a live
// local, and `GetCurrentProcess` returns a pseudo-handle that needs no closing. The
// return value is checked before any field is read.
#[allow(unsafe_code)]
#[cfg(windows)]
fn current_rss_bytes() -> Option<u64> {
    use std::mem::{size_of, zeroed};
    use winapi::um::processthreadsapi::GetCurrentProcess;
    use winapi::um::psapi::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};

    unsafe {
        let mut counters: PROCESS_MEMORY_COUNTERS = zeroed();
        counters.cb = size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        let ok = GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        );
        if ok == 0 {
            None
        } else {
            Some(counters.WorkingSetSize as u64)
        }
    }
}

// SAFETY: `sysconf` takes an integer name and returns a long, touching no memory the
// caller owns. `_SC_PAGESIZE` is a valid name, and a non-positive result is treated as
// failure rather than used as a multiplier.
#[allow(unsafe_code)]
#[cfg(all(unix, not(target_os = "macos")))]
fn current_rss_bytes() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        None
    } else {
        Some(resident_pages * page_size as u64)
    }
}

// SAFETY: `getrusage` writes into the `rusage` it is given, and a zeroed `rusage` is a
// valid initial value for a plain C struct of integers. `RUSAGE_SELF` is a valid who
// argument, and the return value is checked before `ru_maxrss` is read.
//
// Note this measures something different from the other two: `ru_maxrss` is *peak*
// resident size, where /proc/self/statm and `WorkingSetSize` are current. On macOS it
// is already in bytes, where Linux reports it in kilobytes. So the figure this example
// prints on macOS is a high-water mark and will not fall back down.
#[allow(unsafe_code)]
#[cfg(target_os = "macos")]
fn current_rss_bytes() -> Option<u64> {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            Some(usage.ru_maxrss as u64)
        } else {
            None
        }
    }
}
