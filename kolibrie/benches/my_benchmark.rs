extern crate criterion;
extern crate kolibrie;

use criterion::*;
use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

fn setup_database() -> SparqlDatabase {
    // Set current directory to the root of the project
    std::env::set_current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))
        .expect("Failed to set project root as current directory");
    let file_path = "../datasets/synthetic_data_employee_100K.rdf";
    let mut db = SparqlDatabase::new();
    db.parse_rdf_from_file(&file_path);
    db
}

fn execute_sample_query(database: &mut SparqlDatabase) {
    let sparql = r#"
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?employee ?workplaceHomepage ?salary
    WHERE {
        ?employee foaf:workplaceHomepage ?workplaceHomepage .
        ?employee ds:annual_salary ?salary
    }"#;
    execute_query(sparql, database);
}

fn execute_sample_query_normal(database: &mut SparqlDatabase) {
    let sparql = r#"
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?employee ?workplaceHomepage ?salary
    WHERE {
        ?employee foaf:workplaceHomepage ?workplaceHomepage .
        ?employee ds:annual_salary ?salary
    }"#;
    execute_query_normal(sparql, database);
}

fn execute_sample_query_normal_simd(database: &mut SparqlDatabase) {
    let sparql = r#"
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?employee ?workplaceHomepage ?salary
    WHERE {
        ?employee foaf:workplaceHomepage ?workplaceHomepage .
        ?employee ds:annual_salary ?salary
    }"#;
    execute_query_normal_simd(sparql, database);
}

fn execute_sample_query_rayon_simd(database: &mut SparqlDatabase) {
    let sparql = r#"
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?employee ?workplaceHomepage ?salary
    WHERE {
        ?employee foaf:workplaceHomepage ?workplaceHomepage .
        ?employee ds:annual_salary ?salary
    }"#;
    execute_query_rayon_simd(sparql, database);
}

fn my_benchmark(c: &mut Criterion) {
    let mut db = setup_database();

    // Benchmark for executing SPARQL query
    c.bench_function("execute_query_join", |b| {
        b.iter(|| execute_sample_query(&mut db))
    });

    c.bench_function("execute_query_normal", |b| {
        b.iter(|| execute_sample_query_normal(&mut db))
    });

    c.bench_function("execute_query_normal_simd", |b| {
        b.iter(|| execute_sample_query_normal_simd(&mut db))
    });

    c.bench_function("execute_query_rayon_simd", |b| {
        b.iter(|| execute_sample_query_rayon_simd(&mut db))
    });
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);
