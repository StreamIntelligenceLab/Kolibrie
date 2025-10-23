
use datalog::knowledge_graph::KnowledgeGraph;
use shared::rule::Rule;
use shared::terms::Term;
use criterion::*;


use datalog::ruleindex::RuleIndexer;

fn generate_hierarchy(depth: usize, reasoner: &mut KnowledgeGraph) -> Vec<Rule>{
    let mut rules = Vec::new();
    for i in 0..depth {
        let rule = Rule {
            premise: vec![
                (Term::Variable("X".to_string()),
                 Term::Constant(reasoner.dictionary.encode("a")),
                 Term::Constant(reasoner.dictionary.encode(format!("C{}", i).as_str()))
                )
            ],
            conclusion: vec![(Term::Variable("X".to_string()),
                              Term::Constant(reasoner.dictionary.encode("a")),
                              Term::Constant(reasoner.dictionary.encode(format!("C{}", i + 1).as_str())))],
            filters: vec![],
        };
        rules.push(rule);
    }
    rules
}
fn generate_abox(size: usize)-> Vec<(String, String, String)>{
    let mut abox = Vec::new();
    for i in 0..size{
        abox.push((format!("subject{}",i),"a".to_string(), "C0".to_string()))
    }
    abox
}

fn execute_semi_naive(depth: usize, abox_size:usize) {
    let mut reasoner = KnowledgeGraph::new();
    let rules = generate_hierarchy(depth, &mut reasoner);
    for rule in rules{
        reasoner.add_rule(rule);
    }    //add aBox
    let abox = generate_abox(abox_size);
    for (s,p,o) in abox{
        reasoner.add_abox_triple(s.as_str(),p.as_str(),o.as_str());

    }
    let results = reasoner.infer_new_facts_semi_naive();
}
fn execute_semi_naive_parallel(depth: usize, abox_size: usize) {
    let mut reasoner = KnowledgeGraph::new();
    let rules = generate_hierarchy(depth, &mut reasoner);
    for rule in rules{
        reasoner.add_rule(rule);
    }    //add aBox
    let abox = generate_abox(abox_size);
    for (s,p,o) in abox{
        reasoner.add_abox_triple(s.as_str(),p.as_str(),o.as_str());

    }    let results = reasoner.infer_new_facts_semi_naive_parallel();
}
fn execute_semi_naive_rule_index(depth: usize, abox_size:usize) {
    let mut reasoner = KnowledgeGraph::new();
    let rules = generate_hierarchy(depth, &mut reasoner);
    let mut rule_index = RuleIndexer::new();
    for rule in rules{
        rule_index.add(rule);
    }    //add aBox
    let abox = generate_abox(abox_size);
    for (s,p,o) in abox{
        reasoner.add_abox_triple(s.as_str(),p.as_str(),o.as_str());

    }    let results = reasoner.infer_new_facts_semi_naive_with_rule_index(&rule_index);
}

fn my_benchmark2(c: &mut Criterion) {

    c.bench_function("Semi naive 10", |b| {
        b.iter(|| execute_semi_naive(10,10))
    });
    c.bench_function("Semi naive 100", |b| {
        b.iter(|| execute_semi_naive(100,10))
    });
    c.bench_function("Semi naive parallel 10", |b| {
        b.iter(|| execute_semi_naive_parallel(10,10))
    });
    c.bench_function("Semi naive parallel 100", |b| {
        b.iter(|| execute_semi_naive_parallel(100,10))
    });
    c.bench_function("Semi naive ruleindex 10", |b| {
        b.iter(|| execute_semi_naive_rule_index(10,10))
    });
    c.bench_function("Semi naive ruleindex 100", |b| {
        b.iter(|| execute_semi_naive_rule_index(100,10))
    });
}
criterion_group!(benches,  my_benchmark2);
criterion_main!(benches);