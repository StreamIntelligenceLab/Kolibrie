use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
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

const TRAFFIC_IRI: &str = "http://traffic/";
const PARKING_IRI: &str = "http://parking/";
const RESULT_IRI: &str = "http://result/";
const CURRENT_TIME: u64 = 60;

const RULE_N3: &str = r#"
@prefix wt: <http://traffic/> .
@prefix wp: <http://parking/> .
@prefix wr: <http://result/> .
{ ?road wt:avgSpeed ?s . ?lot wp:nearRoad ?road . ?lot wp:occupancy ?occ } => { ?road wr:congested <true> }
"#;

fn parse_rules(dict: &Arc<RwLock<Dictionary>>) -> Vec<Rule> {
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(dict);
    let window_widths: HashMap<String, u64> = [
        (TRAFFIC_IRI.to_string(), 60),
        (PARKING_IRI.to_string(), 120),
    ]
    .into();
    parse_n3_rules_for_sds(RULE_N3, &mut reasoner, window_widths)
        .expect("benchmark rule must parse")
        .0
}

fn make_sds(n: usize, update_ratio_percent: usize) -> Sds {
    let mut sds = Sds::new();
    sds.output_iris.insert(RESULT_IRI.to_string());

    let update_count = n.saturating_mul(update_ratio_percent) / 100;
    let mut traffic_triples = Vec::with_capacity(n);
    for i in 0..n {
        let event_time = if i < update_count {
            CURRENT_TIME + (i % 10) as u64
        } else {
            1 + (i % 59) as u64
        };
        traffic_triples.push(WindowedTriple {
            subject: format!("road_{}", i),
            predicate: "avgSpeed".to_string(),
            object: format!("{}", 20 + (i % 80)),
            event_time,
        });
    }
    sds.windows.insert(
        TRAFFIC_IRI.to_string(),
        WindowData {
            alpha: 60,
            triples: traffic_triples,
        },
    );

    let lot_count = (n / 4).max(1);
    let parking_update_count = lot_count.saturating_mul(update_ratio_percent) / 100;
    let mut parking_triples = Vec::with_capacity(lot_count * 2);
    for j in 0..lot_count {
        let event_time = if j < parking_update_count {
            CURRENT_TIME + (j % 10) as u64
        } else {
            1 + (j % 119) as u64
        };
        let road_idx = (j * 4) % n.max(1);
        parking_triples.push(WindowedTriple {
            subject: format!("lot_{}", j),
            predicate: "nearRoad".to_string(),
            object: format!("road_{}", road_idx),
            event_time,
        });
        parking_triples.push(WindowedTriple {
            subject: format!("lot_{}", j),
            predicate: "occupancy".to_string(),
            object: format!("{}", 50 + (j % 50)),
            event_time,
        });
    }
    sds.windows.insert(
        PARKING_IRI.to_string(),
        WindowData {
            alpha: 120,
            triples: parking_triples,
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

fn assert_correctness(n: usize, update_ratio: usize) {
    let dict = Arc::new(RwLock::new(Dictionary::new()));
    let rules = parse_rules(&dict);
    let sds_initial = make_sds(n, 0);
    let sds_next = make_sds(n, update_ratio);

    let old: SdsWithExpiry =
        incremental_sds_plus(&rules, &sds_initial, &HashMap::new(), &dict, 0);
    let incremental = incremental_sds_plus(&rules, &sds_next, &old, &dict, CURRENT_TIME);
    let component_iris = all_component_iris(&sds_next);
    let incremental_external =
        sds_with_expiry_to_external(&incremental, &dict, &component_iris);
    let naive = naive_sds_plus(&rules, &sds_next, &dict, CURRENT_TIME);

    assert_eq!(signature(&naive, &dict), signature(&incremental_external, &dict));
}

fn bench_naive(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_window_naive");
    group.sample_size(10);

    for &n in &[100usize, 500, 1_000, 5_000, 10_000, 50_000] {
        for &ratio in &[1usize, 10, 50, 100] {
            group.bench_with_input(
                BenchmarkId::new(format!("n_{}", n), ratio),
                &(n, ratio),
                |b, &(n, ratio)| {
                    b.iter_batched(
                        || {
                            let dict = Arc::new(RwLock::new(Dictionary::new()));
                            let rules = parse_rules(&dict);
                            let sds = make_sds(n, ratio);
                            (dict, rules, sds)
                        },
                        |(dict, rules, sds)| {
                            black_box(naive_sds_plus(
                                black_box(&rules),
                                black_box(&sds),
                                black_box(&dict),
                                CURRENT_TIME,
                            ))
                        },
                        criterion::BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_incremental(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_window_incremental");
    group.sample_size(10);

    for &n in &[100usize, 500, 1_000, 5_000, 10_000, 50_000] {
        for &ratio in &[1usize, 10, 50, 100] {
            group.bench_with_input(
                BenchmarkId::new(format!("n_{}", n), ratio),
                &(n, ratio),
                |b, &(n, ratio)| {
                    b.iter_batched(
                        || {
                            let dict = Arc::new(RwLock::new(Dictionary::new()));
                            let rules = parse_rules(&dict);
                            let sds_initial = make_sds(n, 0);
                            let sds_next = make_sds(n, ratio);
                            let old = incremental_sds_plus(
                                &rules,
                                &sds_initial,
                                &HashMap::new(),
                                &dict,
                                0,
                            );
                            (dict, rules, sds_next, old)
                        },
                        |(dict, rules, sds_next, old)| {
                            black_box(incremental_sds_plus(
                                black_box(&rules),
                                black_box(&sds_next),
                                black_box(&old),
                                black_box(&dict),
                                CURRENT_TIME,
                            ))
                        },
                        criterion::BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_correctness_check(c: &mut Criterion) {
    c.bench_function("cross_window_correctness_check", |b| {
        b.iter(|| assert_correctness(1_000, 10))
    });
}

criterion_group!(
    benches,
    bench_naive,
    bench_incremental,
    bench_correctness_check
);
criterion_main!(benches);
