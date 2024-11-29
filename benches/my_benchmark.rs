extern crate criterion;
extern crate kolibrie;

use criterion::*;
use kolibrie::parser::*;
use kolibrie::sparql_database::*;

fn setup_database() -> SparqlDatabase {
    // Set current directory to the root of the project
    std::env::set_current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))
        .expect("Failed to set project root as current directory");
    let file_path = "datasets/synthetic_data_employee_100K.rdf";
    let mut db = SparqlDatabase::new();
    db.parse_rdf_from_file(&file_path);
    db
}

fn filter_salaries(db: &SparqlDatabase) -> SparqlDatabase {
    db.filter(|triple| {
        let predicate = db.dictionary.decode(triple.predicate).unwrap();
        let object = db.dictionary.decode(triple.object).unwrap();
        predicate.ends_with("annual_salary") && object.parse::<f64>().unwrap_or(0.0) > 100000.0
    })
}

fn execute_sample_query(database: &mut SparqlDatabase) {
    // SELECT ?employee ?salary WHERE {?employee ds:annual_salary ?salary}
    /* 
    SELECT ?employee ?workplaceHomepage ?salary 
    WHERE { ?employee foaf:workplaceHomepage ?workplaceHomepage . 
        ?employee ds:annual_salary ?salary
    }
    */
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

fn execute_sample_query_1(database: &mut SparqlDatabase) {
    let sparql = r#"
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    SELECT ?employee ?salary 
    WHERE {
        ?employee ds:annual_salary ?salary 
        FILTER(?salary > 75000)
    }"#;
    execute_query(sparql, database);
}

fn my_benchmark(c: &mut Criterion) {
    let mut db = setup_database();

    // Benchmark for filtering salaries
    c.bench_function("filter_salaries", |b| b.iter(|| filter_salaries(&db)));

    let mut group = c.benchmark_group("sample-size-example");
    group.sample_size(10);

    // Benchmark for executing SPARQL query
    group.bench_function("execute_query_join", |b| {
        b.iter(|| execute_sample_query(&mut db))
    });

    group.bench_function("execute_query_filter", |b| b.iter( || execute_sample_query_1(&mut db)));
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);
