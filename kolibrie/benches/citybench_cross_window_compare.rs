use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use datalog::cross_window_sds::{
    all_component_iris, sds_with_expiry_to_external, Sds, WindowData, WindowedTriple,
};
use datalog::parser_n3_logic::parse_n3_rules_for_sds;
use datalog::reasoning::materialisation::cross_window_incremental::{
    incremental_sds_plus, SdsWithExpiry,
};
use datalog::reasoning::materialisation::cross_window_naive::naive_sds_plus;
use datalog::reasoning::Reasoner;
use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::triple::Triple;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

const TRAFFIC_IRI: &str = "http://citybench.example/traffic/";
const PARKING_IRI: &str = "http://citybench.example/parking/";
const RESULT_IRI: &str = "http://citybench.example/result/";

const INITIAL_TIME: u64 = 0;
const CURRENT_TIME: u64 = 60;
const TRAFFIC_ALPHA: u64 = 120;
const PARKING_ALPHA: u64 = 180;

const SIZES: &[usize] = &[100, 500, 1_000, 5_000, 10_000, 50_000];
const UPDATE_RATIOS: &[usize] = &[1, 10, 50, 100];

const CROSS_WINDOW_RULES: &str = r#"
@prefix traffic: <http://citybench.example/traffic/> .
@prefix parking: <http://citybench.example/parking/> .
@prefix result: <http://citybench.example/result/> .
{ ?road traffic:avgSpeed ?speed . ?lot parking:nearRoad ?road . ?lot parking:occupancy ?occupancy } => { ?road result:congested <true> }
"#;

fn parse_rules(dict: &Arc<RwLock<Dictionary>>) -> Vec<Rule> {
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(dict);
    let window_widths = HashMap::from([
        (TRAFFIC_IRI.to_string(), TRAFFIC_ALPHA),
        (PARKING_IRI.to_string(), PARKING_ALPHA),
    ]);

    parse_n3_rules_for_sds(CROSS_WINDOW_RULES, &mut reasoner, window_widths)
        .expect("CityBench-inspired cross-window rules must parse")
        .0
}

fn make_citybench_sds(size: usize, update_ratio_percent: usize) -> Sds {
    let mut sds = Sds::new();
    sds.output_iris.insert(RESULT_IRI.to_string());

    let update_count = size.saturating_mul(update_ratio_percent) / 100;
    let mut traffic = Vec::with_capacity(size);
    for i in 0..size {
        let event_time = if i < update_count {
            CURRENT_TIME + (i % 5) as u64
        } else {
            1 + (i % 50) as u64
        };

        traffic.push(WindowedTriple {
            subject: format!("road_{}", i),
            predicate: "avgSpeed".to_string(),
            object: format!("{}", 20 + (i % 80)),
            event_time,
        });
    }
    sds.windows.insert(
        TRAFFIC_IRI.to_string(),
        WindowData {
            alpha: TRAFFIC_ALPHA,
            triples: traffic,
        },
    );

    let lots = (size / 4).max(1);
    let parking_update_count = lots.saturating_mul(update_ratio_percent) / 100;
    let mut parking = Vec::with_capacity(lots * 2);
    for lot_id in 0..lots {
        let event_time = if lot_id < parking_update_count {
            CURRENT_TIME + (lot_id % 5) as u64
        } else {
            1 + (lot_id % 90) as u64
        };
        let road_id = (lot_id * 4) % size.max(1);

        parking.push(WindowedTriple {
            subject: format!("lot_{}", lot_id),
            predicate: "nearRoad".to_string(),
            object: format!("road_{}", road_id),
            event_time,
        });
        parking.push(WindowedTriple {
            subject: format!("lot_{}", lot_id),
            predicate: "occupancy".to_string(),
            object: format!("{}", 30 + (lot_id % 70)),
            event_time,
        });
    }
    sds.windows.insert(
        PARKING_IRI.to_string(),
        WindowData {
            alpha: PARKING_ALPHA,
            triples: parking,
        },
    );

    sds
}

fn signature(
    result: &HashMap<String, Vec<Triple>>,
    dict: &Arc<RwLock<Dictionary>>,
) -> BTreeSet<String> {
    let d = dict.read().unwrap();
    let mut out = BTreeSet::new();

    for (component, triples) in result {
        for triple in triples {
            let s = d.decode(triple.subject).unwrap_or("?");
            let p = d.decode(triple.predicate).unwrap_or("?");
            let o = d.decode(triple.object).unwrap_or("?");
            out.insert(format!("{}|{}|{}|{}", component, s, p, o));
        }
    }

    out
}

fn prepare_incremental_state(
    rules: &[Rule],
    initial_sds: &Sds,
    dict: &Arc<RwLock<Dictionary>>,
) -> SdsWithExpiry {
    incremental_sds_plus(rules, initial_sds, &HashMap::new(), dict, INITIAL_TIME)
}

fn assert_same_results(size: usize, update_ratio_percent: usize) {
    let dict = Arc::new(RwLock::new(Dictionary::new()));
    let rules = parse_rules(&dict);
    let initial_sds = make_citybench_sds(size, 0);
    let next_sds = make_citybench_sds(size, update_ratio_percent);
    let previous = prepare_incremental_state(&rules, &initial_sds, &dict);

    let naive = naive_sds_plus(&rules, &next_sds, &dict, CURRENT_TIME);
    let incremental = incremental_sds_plus(&rules, &next_sds, &previous, &dict, CURRENT_TIME);
    let component_iris = all_component_iris(&next_sds);
    let incremental_external =
        sds_with_expiry_to_external(&incremental, &dict, &component_iris);

    assert_eq!(
        signature(&naive, &dict),
        signature(&incremental_external, &dict),
        "naive and incremental must compute the same SDS+ triples"
    );
}

fn bench_citybench_cross_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("citybench_cross_window_compare");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    for &size in SIZES {
        group.throughput(Throughput::Elements(size as u64));

        for &ratio in UPDATE_RATIOS {
            assert_same_results(size.min(1_000), ratio);
            let label = format!("size_{}_updates_{}pct", size, ratio);

            group.bench_with_input(
                BenchmarkId::new("naive_full_rematerialization", &label),
                &(size, ratio),
                |b, &(size, ratio)| {
                    let dict = Arc::new(RwLock::new(Dictionary::new()));
                    let rules = parse_rules(&dict);
                    let next_sds = make_citybench_sds(size, ratio);

                    // Explicit warmup outside the measured loop.
                    black_box(naive_sds_plus(&rules, &next_sds, &dict, CURRENT_TIME));

                    b.iter(|| {
                        black_box(naive_sds_plus(
                            black_box(&rules),
                            black_box(&next_sds),
                            black_box(&dict),
                            CURRENT_TIME,
                        ))
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("incremental_sds_plus", &label),
                &(size, ratio),
                |b, &(size, ratio)| {
                    let dict = Arc::new(RwLock::new(Dictionary::new()));
                    let rules = parse_rules(&dict);
                    let initial_sds = make_citybench_sds(size, 0);
                    let next_sds = make_citybench_sds(size, ratio);

                    // Warmup/state preparation is intentionally outside the measured loop
                    let previous = prepare_incremental_state(&rules, &initial_sds, &dict);
                    black_box(incremental_sds_plus(
                        &rules,
                        &next_sds,
                        &previous,
                        &dict,
                        CURRENT_TIME,
                    ));

                    b.iter(|| {
                        black_box(incremental_sds_plus(
                            black_box(&rules),
                            black_box(&next_sds),
                            black_box(&previous),
                            black_box(&dict),
                            CURRENT_TIME,
                        ))
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_citybench_correctness(c: &mut Criterion) {
    c.bench_function("citybench_cross_window_correctness", |b| {
        b.iter(|| assert_same_results(1_000, 10));
    });
}

criterion_group!(
    benches,
    bench_citybench_cross_window,
    bench_citybench_correctness
);
criterion_main!(benches);
