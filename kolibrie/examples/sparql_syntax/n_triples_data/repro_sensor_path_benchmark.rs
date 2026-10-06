/*
 * Copyright (c) 2025 Volodymyr Kadzhaia
 * Copyright (c) 2025 Pieter Bonte
 * KU Leuven - Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::execute_query::execute_query_rayon_parallel2_volcano;
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))?;

    let file_path = "../benchmark_dataset/repro-data.nt";
    let metadata = fs::metadata(file_path)?;
    let data = fs::read_to_string(file_path)?;
    let line_count = data.lines().count();

    println!("Dataset: {}", file_path);
    println!("File size: {:.2} MB", metadata.len() as f64 / 1_048_576.0);
    println!("Input lines: {}", line_count);

    let load_start = Instant::now();
    let mut db = SparqlDatabase::new();
    db.parse_ntriples_and_add(&data);
    let load_time = load_start.elapsed();
    println!("Loaded triples: {}", db.dataset_index.len_default());
    println!("Load time: {:.3}s", load_time.as_secs_f64());

    let stats_start = Instant::now();
    db.get_or_build_stats();
    db.build_all_indexes();
    let stats_index_time = stats_start.elapsed();
    println!("Stats/index time: {:.3}s", stats_index_time.as_secs_f64());

    let mut legacy_db = db.clone();
    let mut optimized_db = db.clone();

    let legacy = measure_query("execute_query_rayon_parallel2_volcano (warmup)", || {
        kolibrie::execute_query::execute_query_rayon_parallel2_volcano(REPRO_QUERY, &mut legacy_db)
    });
    print_measurement("execute_query_rayon_parallel2_volcano (warmup)", &legacy);

    let optimized = measure_query("execute_query_rayon_parallel2_volcano", || {
        execute_query_rayon_parallel2_volcano(REPRO_QUERY, &mut optimized_db)
    });
    print_measurement("execute_query_rayon_parallel2_volcano", &optimized);

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
