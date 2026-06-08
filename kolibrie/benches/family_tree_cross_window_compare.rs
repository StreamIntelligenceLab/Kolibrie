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

const PARENT_OF_SDS_STREAM_IRI: &str = "http://family.example/sds/stream1/parent-facts/";
const FAMILY_SDS_STREAM_IRI: &str = "http://family.example/sds/stream2/family-facts/";

const INITIAL_TIME: u64 = 0;
const CURRENT_TIME: u64 = 1_000;
const WINDOW_ALPHA: u64 = 10_000;

const PERSON_COUNTS: &[usize] = &[250, 500, 1_000, 2_500, 5_000, 10_000];
const NEW_RATIOS: &[usize] = &[70, 60, 50, 40, 30, 20, 10];

const FAMILY_RULES: &str = r#"
@prefix stream1: <http://family.example/sds/stream1/parent-facts/> .
@prefix stream2: <http://family.example/sds/stream2/family-facts/> .

{ ?p stream1:parentOf ?c } => { ?p stream2:ancestorOf ?c }
{ ?p stream1:parentOf ?c } => { ?c stream2:childOf ?p }
{ ?gp stream1:parentOf ?p . ?p stream1:parentOf ?c } => { ?gp stream2:grandparentOf ?c }
{ ?a stream1:parentOf ?b . ?b stream2:ancestorOf ?c } => { ?a stream2:ancestorOf ?c }
{ ?p stream1:parentOf ?x . ?p stream1:parentOf ?y } => { ?x stream2:siblingOf ?y }
{ ?p stream2:male <true> . ?p stream1:parentOf ?c } => { ?p stream2:fatherOf ?c }
{ ?p stream2:female <true> . ?p stream1:parentOf ?c } => { ?p stream2:motherOf ?c }
{ ?x stream2:male <true> . ?x stream2:siblingOf ?y } => { ?x stream2:brotherOf ?y }
{ ?x stream2:female <true> . ?x stream2:siblingOf ?y } => { ?x stream2:sisterOf ?y }
"#;

#[derive(Clone)]
struct TwoStreamFamilyFacts {
    // SDS stream 1: parentOf only
    parent_of: Vec<WindowedTriple>,
    // SDS stream 2: all non-parentOf asserted family facts
    other: Vec<WindowedTriple>,
}

fn parse_rules(dict: &Arc<RwLock<Dictionary>>) -> Vec<Rule> {
    let mut reasoner = Reasoner::new();
    reasoner.dictionary = Arc::clone(dict);
    let window_widths = HashMap::from([
        (PARENT_OF_SDS_STREAM_IRI.to_string(), WINDOW_ALPHA),
        (FAMILY_SDS_STREAM_IRI.to_string(), WINDOW_ALPHA),
    ]);

    parse_n3_rules_for_sds(FAMILY_RULES, &mut reasoner, window_widths)
        .expect("family-tree cross-window rules must parse")
        .0
}

fn generate_two_stream_family_facts(
    person_count: usize,
    new_ratio_percent: usize,
) -> TwoStreamFamilyFacts {
    let new_parent_count = person_count
        .saturating_sub(1)
        .saturating_mul(new_ratio_percent)
        / 100;
    let old_parent_count = person_count.saturating_sub(1) - new_parent_count;

    let new_other_count = person_count.saturating_mul(new_ratio_percent) / 100;
    let old_other_count = person_count - new_other_count;

    let mut parent_of = Vec::with_capacity(person_count.saturating_sub(1));
    for child in 1..person_count {
        let parent = (child - 1) / 2;
        let event_time = if child <= old_parent_count {
            1
        } else {
            CURRENT_TIME
        };

        parent_of.push(WindowedTriple {
            subject: format!("person_{}", parent),
            predicate: "parentOf".to_string(),
            object: format!("person_{}", child),
            event_time,
        });
    }

    let mut other = Vec::with_capacity(person_count);
    for person in 0..person_count {
        let event_time = if person < old_other_count {
            1
        } else {
            CURRENT_TIME
        };
        let predicate = if person % 2 == 0 { "male" } else { "female" };

        other.push(WindowedTriple {
            subject: format!("person_{}", person),
            predicate: predicate.to_string(),
            object: "true".to_string(),
            event_time,
        });
    }

    TwoStreamFamilyFacts { parent_of, other }
}

fn slice_old(facts: &[WindowedTriple]) -> Vec<WindowedTriple> {
    facts
        .iter()
        .filter(|fact| fact.event_time < CURRENT_TIME)
        .cloned()
        .collect()
}

fn build_two_stream_sds(
    parent_facts: Vec<WindowedTriple>,
    other_facts: Vec<WindowedTriple>,
) -> Sds {
    let mut sds = Sds::new();
    sds.windows.insert(
        PARENT_OF_SDS_STREAM_IRI.to_string(),
        WindowData {
            alpha: WINDOW_ALPHA,
            triples: parent_facts,
        },
    );
    sds.windows.insert(
        FAMILY_SDS_STREAM_IRI.to_string(),
        WindowData {
            alpha: WINDOW_ALPHA,
            triples: other_facts,
        },
    );
    sds
}

fn build_old_sds(facts: &TwoStreamFamilyFacts) -> Sds {
    build_two_stream_sds(slice_old(&facts.parent_of), slice_old(&facts.other))
}

fn build_next_sds(facts: &TwoStreamFamilyFacts) -> Sds {
    build_two_stream_sds(facts.parent_of.clone(), facts.other.clone())
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

fn prepare_old_materialization(
    rules: &[Rule],
    old_sds: &Sds,
    dict: &Arc<RwLock<Dictionary>>,
) -> SdsWithExpiry {
    incremental_sds_plus(rules, old_sds, &HashMap::new(), dict, INITIAL_TIME)
}

fn assert_same_results(person_count: usize, new_ratio_percent: usize) {
    let dict = Arc::new(RwLock::new(Dictionary::new()));
    let rules = parse_rules(&dict);
    let facts = generate_two_stream_family_facts(person_count, new_ratio_percent);
    let old_sds = build_old_sds(&facts);
    let next_sds = build_next_sds(&facts);
    let old_materialization = prepare_old_materialization(&rules, &old_sds, &dict);

    let naive = naive_sds_plus(&rules, &next_sds, &dict, CURRENT_TIME);
    let incremental = incremental_sds_plus(
        &rules,
        &next_sds,
        &old_materialization,
        &dict,
        CURRENT_TIME,
    );
    let component_iris = all_component_iris(&next_sds);
    let incremental_external =
        sds_with_expiry_to_external(&incremental, &dict, &component_iris);

    assert_eq!(signature(&naive, &dict), signature(&incremental_external, &dict));
}

fn bench_family_tree_cross_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("family_tree_cross_window_compare");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    for &person_count in PERSON_COUNTS {
        group.throughput(Throughput::Elements(person_count as u64));

        for &new_ratio in NEW_RATIOS {
            assert_same_results(person_count.min(1_000), new_ratio);
            let label = format!("persons_{}_new_{}pct", person_count, new_ratio);

            group.bench_with_input(
                BenchmarkId::new("naive_from_scratch_update", &label),
                &(person_count, new_ratio),
                |b, &(person_count, new_ratio)| {
                    let dict = Arc::new(RwLock::new(Dictionary::new()));
                    let rules = parse_rules(&dict);
                    let facts = generate_two_stream_family_facts(person_count, new_ratio);
                    let next_sds = build_next_sds(&facts);

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
                BenchmarkId::new("incremental_update", &label),
                &(person_count, new_ratio),
                |b, &(person_count, new_ratio)| {
                    let dict = Arc::new(RwLock::new(Dictionary::new()));
                    let rules = parse_rules(&dict);
                    let facts = generate_two_stream_family_facts(person_count, new_ratio);
                    let old_sds = build_old_sds(&facts);
                    let next_sds = build_next_sds(&facts);

                    let old_materialization =
                        prepare_old_materialization(&rules, &old_sds, &dict);
                    black_box(incremental_sds_plus(
                        &rules,
                        &next_sds,
                        &old_materialization,
                        &dict,
                        CURRENT_TIME,
                    ));

                    b.iter(|| {
                        black_box(incremental_sds_plus(
                            black_box(&rules),
                            black_box(&next_sds),
                            black_box(&old_materialization),
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

fn bench_family_tree_correctness(c: &mut Criterion) {
    c.bench_function("family_tree_cross_window_correctness", |b| {
        b.iter(|| assert_same_results(1_000, 30));
    });
}

criterion_group!(
    benches,
    bench_family_tree_cross_window,
    bench_family_tree_correctness
);
criterion_main!(benches);
