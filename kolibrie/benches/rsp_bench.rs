use std::sync::{Arc, Mutex};
use std::time::Instant;
use datalog::knowledge_graph::KnowledgeGraph;
use shared::rule::Rule;
use shared::terms::Term;
use criterion::*;



use datalog::ruleindex::RuleIndexer;
use kolibrie::rsp::{OperationMode, ResultConsumer, RSPBuilder, SimpleR2R};
use kolibrie::sparql_database::SparqlDatabase;
use rsp::r2s::StreamOperator;
use rsp::s2r::{ReportStrategy, Tick};

fn rsp(length: usize, window_size:usize, slide: usize){
    let result_container = Arc::new(Mutex::new(Vec::new()));
    let result_container_clone = Arc::clone(&result_container);
    let function = Box::new(move |r| {
        result_container_clone.lock().unwrap().push(r);
    });
    let result_consumer = ResultConsumer{function: Arc::new(function)};
    let mut r2r = Box::new(SimpleR2R {item: SparqlDatabase::new()});
    let mut engine = RSPBuilder::new(window_size,slide)
        .add_tick(Tick::TimeDriven)
        .add_report_strategy(ReportStrategy::OnWindowClose)
        .add_query("SELECT ?s WHERE{ ?s a <http://www.w3.org/test/SuperType>}")
        .add_consumer(result_consumer)
        .add_r2r(r2r)
        .add_r2s(StreamOperator::RSTREAM)
        .set_operation_mode(OperationMode::SingleThread)
        .build();
    for i in 0..length {
        let data = format!("<http://test.be/subject{}> a <http://www.w3.org/test/SuperType> .", i);
        let triples = engine.parse_data(&data);
        for triple in triples{
            engine.add(triple,i);
        }
    }
    engine.stop();
}

fn rsp_benchmark(c: &mut Criterion) {
    // c.bench_function("Semi naive 10", |b| {
    //     b.iter(|| execute_semi_naive(10,abox_size))
    // });
    // c.bench_function("Semi naive 100", |b| {
    //     b.iter(|| execute_semi_naive(100,abox_size))
    // });
    // c.bench_function("Semi naive parallel 10", |b| {
    //     b.iter(|| execute_semi_naive_parallel(10,abox_size))
    // });
    // c.bench_function("Semi naive parallel 100", |b| {
    //     b.iter(|| execute_semi_naive_parallel(100,abox_size))
    // });
    c.bench_function("RSP length 1000, window: 10, slide 2", |b| {
        b.iter(|| rsp(1000,10,2))
    });
    c.bench_function("RSP length 10000, window: 10, slide 2", |b| {
        b.iter(|| rsp(10000,10,2))
    });
    c.bench_function("RSP length 100000, window: 10, slide 2", |b| {
        b.iter(|| rsp(100000,10,2))
    });
}
criterion_group!(benches,  rsp_benchmark);
criterion_main!(benches);