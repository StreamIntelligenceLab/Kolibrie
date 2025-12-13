/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

extern crate criterion;
extern crate kolibrie;

use criterion::*;
use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

// Simple query
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

fn execute_sample_query_volcano(database: &mut SparqlDatabase) {
    let sparql = r#"
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?employee ?workplaceHomepage ?salary
    WHERE {
        ?employee foaf:workplaceHomepage ?workplaceHomepage .
        ?employee ds:annual_salary ?salary
    }"#;
    execute_query_rayon_parallel2_volcano(sparql, database);
}

/////////////////////////////////////////////////////////////////////////
// Complex query
fn execute_sample_query_complex(database: &mut SparqlDatabase) {
    let sparql = r#"
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    SELECT ?title
    WHERE {
        {
            SELECT ?title
            WHERE {
                ?employee foaf:title ?title .
                ?employee foaf:title "Developer" .
            }
        }
    }"#;
    execute_query(sparql, database);
}

fn execute_sample_query_volcano_complex(database: &mut SparqlDatabase) {
    let sparql = r#"
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    SELECT ?title
    WHERE {
        {
            SELECT ?title
            WHERE {
                ?employee foaf:title ?title .
                ?employee foaf:title "Developer" .
            }
        }
    }"#;
    execute_query_rayon_parallel2_volcano(sparql, database);
}

fn my_benchmark(c: &mut Criterion) {
    let mut db = setup_database();

    // Benchmark for executing SPARQL query
    c.bench_function("execute_query_join parallel", |b| {
        b.iter(|| execute_sample_query(&mut db))
    });

    c.bench_function("execute_query_volcano", |b| {
        b.iter(|| execute_sample_query_volcano(&mut db))
    });
}

fn my_benchmark2(c: &mut Criterion) {
    let mut db = setup_database();

    // Benchmark for executing SPARQL query
    c.bench_function("COMPLEX QUERY: execute_query_join parallel", |b| {
        b.iter(|| execute_sample_query_complex(&mut db))
    });

    c.bench_function("COMPLEX QUERY: execute_query_volcano", |b| {
        b.iter(|| execute_sample_query_volcano_complex(&mut db))
    });
}

criterion_group!(benches, my_benchmark, my_benchmark2);
criterion_main!(benches);
