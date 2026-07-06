use criterion::{black_box, criterion_group, criterion_main, Criterion};
use shared::hybrid::{
    compile_lineage_to_sdd, evaluate_hybrid, evaluate_topk, AlertDecision, HybridConfig,
    LineageStore, SeedRegistry,
};
use shared::triple::Triple;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture() -> (
    Arc<Mutex<LineageStore>>,
    Arc<shared::hybrid::SeedSnapshot>,
    shared::hybrid::LineageId,
) {
    let mut registry = SeedRegistry::new();
    let mut ids = Vec::new();
    for index in 0..16u32 {
        let triple = Triple {
            subject: index,
            predicate: 100,
            object: 200,
        };
        ids.push(
            registry
                .register_static(triple, 0.9 - index as f64 * 0.02)
                .unwrap(),
        );
    }
    let snapshot = Arc::new(registry.snapshot_all());
    let mut store = LineageStore::new();
    let common = store.literal(ids[0]);
    let mut paths = Vec::new();
    for id in ids.iter().skip(1) {
        let branch = store.literal(*id);
        paths.push(store.and([common, branch]));
    }
    let root = store.or(paths);
    (Arc::new(Mutex::new(store)), snapshot, root)
}

fn bench_hybrid(c: &mut Criterion) {
    let (store, seeds, root) = fixture();
    let no_alert = HybridConfig {
        threshold: 0.99,
        ..HybridConfig::default()
    };
    print_quality_report(&store, &seeds, root);

    c.bench_function("hybrid/topk_only", |b| {
        b.iter(|| {
            let guard = store.lock().unwrap();
            evaluate_topk(
                black_box(&guard),
                black_box(&seeds),
                root,
                8,
                Duration::from_millis(25),
                100_000,
            )
            .unwrap()
        })
    });
    c.bench_function("hybrid/escalated_exact", |b| {
        b.iter(|| {
            evaluate_hybrid(
                black_box(&store),
                black_box(&seeds),
                root,
                black_box(&no_alert),
            )
        })
    });
    c.bench_function("hybrid/full_sdd", |b| {
        b.iter(|| {
            let guard = store.lock().unwrap();
            compile_lineage_to_sdd(
                black_box(&guard),
                black_box(&seeds),
                root,
                no_alert.sdd_budget,
                no_alert.sdd_node_budget,
            )
            .unwrap()
        })
    });
}

fn percentile(sorted: &[u128], percentile: f64) -> u128 {
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index]
}

fn print_quality_report(
    store: &Arc<Mutex<LineageStore>>,
    seeds: &Arc<shared::hybrid::SeedSnapshot>,
    root: shared::hybrid::LineageId,
) {
    const SAMPLES: usize = 200;
    let exact_probability = {
        let guard = store.lock().unwrap();
        let compiled =
            compile_lineage_to_sdd(&guard, seeds, root, Duration::from_secs(1), 1_000_000).unwrap();
        compiled.manager.wmc(compiled.root)
    };
    let mut latencies = Vec::with_capacity(SAMPLES);
    let mut escalations = 0usize;
    let mut exact_survivors = 0usize;
    let mut missed_survivors = 0usize;
    let mut sdd_nodes = 0usize;

    for index in 0..SAMPLES {
        let threshold = if index % 2 == 0 { 0.2 } else { 0.99 };
        let config = HybridConfig {
            threshold,
            ..HybridConfig::default()
        };
        let started = Instant::now();
        let result = evaluate_hybrid(store, seeds, root, &config);
        latencies.push(started.elapsed().as_nanos());
        let metrics = result.metrics();
        if metrics.exact_used {
            escalations += 1;
        }
        sdd_nodes += metrics.sdd_nodes;
        let is_survivor = exact_probability >= threshold;
        if is_survivor {
            exact_survivors += 1;
            if result.decision() != AlertDecision::Alert {
                missed_survivors += 1;
            }
        }
    }

    latencies.sort_unstable();
    let recall = if exact_survivors == 0 {
        1.0
    } else {
        (exact_survivors - missed_survivors) as f64 / exact_survivors as f64
    };
    assert_eq!(
        missed_survivors, 0,
        "hybrid evaluation must not miss exact survivors"
    );
    println!(
        "hybrid quality: p50={}ns p95={}ns p99={}ns avg_sdd_nodes={:.1} escalation_rate={:.3} survivor_recall={:.3}",
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
        sdd_nodes as f64 / SAMPLES as f64,
        escalations as f64 / SAMPLES as f64,
        recall,
    );
}

criterion_group!(benches, bench_hybrid);
criterion_main!(benches);
