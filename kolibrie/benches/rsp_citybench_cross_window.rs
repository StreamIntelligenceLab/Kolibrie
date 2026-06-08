use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use kolibrie::rsp_engine::{
    CrossWindowReasoningMode, OperationMode, QueryExecutionMode, RSPEngine, RSPBuilder,
    ResultConsumer, SimpleR2R,
};
use shared::triple::Triple;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SIZES: &[usize] = &[100, 500, 1_000, 5_000, 10_000, 50_000];
const UPDATE_RATIOS: &[usize] = &[1, 10, 50, 100];

const WARMUP_BASE_TS: usize = 1;
const UPDATE_BASE_TS: usize = 121;

const RSP_QUERY: &str = r#"
REGISTER RSTREAM <http://citybench.example/out> AS
SELECT *
FROM NAMED WINDOW :trafficWindow ON :trafficStream [RANGE 120 STEP 60]
FROM NAMED WINDOW :parkingWindow ON :parkingStream [RANGE 180 STEP 60]
WHERE {
    WINDOW :trafficWindow {
        ?road <http://citybench.example/congested> <true> .
    }
    WINDOW :parkingWindow {
        ?lot <http://citybench.example/nearRoad> ?road .
    }
}
"#;

const CROSS_WINDOW_RULES: &str = r#"
{
  ?road <:trafficWindowhttp://citybench.example/avgSpeed> ?speed .
  ?lot <:parkingWindowhttp://citybench.example/nearRoad> ?road .
  ?lot <:parkingWindowhttp://citybench.example/occupancy> ?occupancy
}
=>
{
  ?road <:trafficWindowhttp://citybench.example/congested> <true>
}
"#;

#[derive(Clone)]
struct CityEvent {
    stream: &'static str,
    data: String,
    ts: usize,
}

fn make_engine(
    mode: CrossWindowReasoningMode,
) -> (
    RSPEngine<Triple, Vec<(String, String)>>,
    Arc<Mutex<Vec<Vec<(String, String)>>>>,
) {
    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);
    let consumer = ResultConsumer {
        function: Arc::new(move |row: Vec<(String, String)>| {
            results_clone.lock().unwrap().push(row);
        }),
    };

    let engine = RSPBuilder::new()
        .add_rsp_ql_query(RSP_QUERY)
        .add_cross_window_rules(CROSS_WINDOW_RULES)
        .set_cross_window_reasoning_mode(mode)
        .add_consumer(consumer)
        .add_r2r(Box::new(SimpleR2R::with_execution_mode(
            QueryExecutionMode::Volcano,
        )))
        .set_operation_mode(OperationMode::SingleThread)
        .build()
        .expect("RSP CityBench cross-window engine must build");

    (engine, results)
}

fn traffic_event(road_id: usize, ts: usize) -> CityEvent {
    CityEvent {
        stream: "trafficStream",
        data: format!(
            "<http://citybench.example/road_{}> <http://citybench.example/avgSpeed> \"{}\" .",
            road_id,
            20 + (road_id % 80)
        ),
        ts,
    }
}

fn parking_events(lot_id: usize, road_id: usize, ts: usize) -> [CityEvent; 2] {
    [
        CityEvent {
            stream: "parkingStream",
            data: format!(
                "<http://citybench.example/lot_{}> <http://citybench.example/nearRoad> <http://citybench.example/road_{}> .",
                lot_id, road_id
            ),
            ts,
        },
        CityEvent {
            stream: "parkingStream",
            data: format!(
                "<http://citybench.example/lot_{}> <http://citybench.example/occupancy> \"{}\" .",
                lot_id,
                30 + (lot_id % 70)
            ),
            ts,
        },
    ]
}

fn generate_initial_stream(size: usize) -> Vec<CityEvent> {
    let lots = (size / 4).max(1);
    let mut events = Vec::with_capacity(size + lots * 2);

    for road_id in 0..size {
        events.push(traffic_event(road_id, WARMUP_BASE_TS + (road_id % 50)));
    }

    for lot_id in 0..lots {
        let road_id = (lot_id * 4) % size.max(1);
        events.extend(parking_events(
            lot_id,
            road_id,
            WARMUP_BASE_TS + (lot_id % 50),
        ));
    }

    events
}

fn generate_update_stream(size: usize, update_ratio_percent: usize) -> Vec<CityEvent> {
    let traffic_updates = (size.saturating_mul(update_ratio_percent) / 100).max(1);
    let lots = (size / 4).max(1);
    let parking_updates = (lots.saturating_mul(update_ratio_percent) / 100).max(1);
    let mut events = Vec::with_capacity(traffic_updates + parking_updates * 2);

    for road_id in 0..traffic_updates {
        events.push(traffic_event(road_id, UPDATE_BASE_TS + (road_id % 30)));
    }

    for lot_id in 0..parking_updates {
        let road_id = (lot_id * 4) % size.max(1);
        events.extend(parking_events(
            lot_id,
            road_id,
            UPDATE_BASE_TS + (lot_id % 30),
        ));
    }

    events
}

fn feed_events(engine: &mut RSPEngine<Triple, Vec<(String, String)>>, events: &[CityEvent]) {
    for event in events {
        let triples = engine.parse_data(&event.data);
        for triple in triples {
            engine.add_to_stream(event.stream, triple, event.ts);
        }
    }
}

fn prepare_engine_for_update(
    mode: CrossWindowReasoningMode,
    size: usize,
    update_ratio_percent: usize,
) -> (
    RSPEngine<Triple, Vec<(String, String)>>,
    Arc<Mutex<Vec<Vec<(String, String)>>>>,
    Vec<CityEvent>,
) {
    let (mut engine, results) = make_engine(mode);
    let initial = generate_initial_stream(size);
    feed_events(&mut engine, &initial);
    engine.process_single_thread_window_results();
    results.lock().unwrap().clear();

    let updates = generate_update_stream(size, update_ratio_percent);
    (engine, results, updates)
}

fn run_rsp_scenario(mode: CrossWindowReasoningMode, size: usize, ratio: usize) -> BTreeSet<String> {
    let (mut engine, results, updates) = prepare_engine_for_update(mode, size, ratio);
    feed_events(&mut engine, &updates);
    engine.stop();

    let signature = results
        .lock()
        .unwrap()
        .iter()
        .map(|row| {
            let mut row = row.clone();
            row.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            row.into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    signature
}

fn assert_same_rsp_output(size: usize, ratio: usize) {
    let naive = run_rsp_scenario(CrossWindowReasoningMode::Naive, size, ratio);
    let incremental = run_rsp_scenario(CrossWindowReasoningMode::Incremental, size, ratio);
    assert_eq!(naive, incremental);
}

fn bench_rsp_citybench_cross_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("rsp_citybench_cross_window");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    for &size in SIZES {
        group.throughput(Throughput::Elements(size as u64));
        for &ratio in UPDATE_RATIOS {
            let correctness_size = size.min(1_000);
            assert_same_rsp_output(correctness_size, ratio);

            let label = format!("size_{}_updates_{}pct", size, ratio);
            group.bench_with_input(
                BenchmarkId::new("rsp_naive_full_sds_plus", &label),
                &(size, ratio),
                |b, &(size, ratio)| {
                    b.iter_batched(
                        || {
                            prepare_engine_for_update(
                                CrossWindowReasoningMode::Naive,
                                size,
                                ratio,
                            )
                        },
                        |(mut engine, results, updates)| {
                            feed_events(&mut engine, black_box(&updates));
                            engine.stop();
                            black_box(results.lock().unwrap().len())
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("rsp_incremental_sds_plus", &label),
                &(size, ratio),
                |b, &(size, ratio)| {
                    b.iter_batched(
                        || {
                            prepare_engine_for_update(
                                CrossWindowReasoningMode::Incremental,
                                size,
                                ratio,
                            )
                        },
                        |(mut engine, results, updates)| {
                            feed_events(&mut engine, black_box(&updates));
                            engine.stop();
                            black_box(results.lock().unwrap().len())
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_rsp_citybench_correctness(c: &mut Criterion) {
    c.bench_function("rsp_citybench_cross_window_correctness", |b| {
        b.iter(|| assert_same_rsp_output(500, 10));
    });
}

criterion_group!(
    benches,
    bench_rsp_citybench_cross_window,
    bench_rsp_citybench_correctness
);
criterion_main!(benches);
