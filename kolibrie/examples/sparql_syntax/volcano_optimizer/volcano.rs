/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::prelude::*;
use kolibrie::streamertail_optimizer::operators::logical::LogicalOperator;
use kolibrie::streamertail_optimizer::optimizer::Streamertail;
use kolibrie::streamertail_optimizer::types::Condition;
use kolibrie::streamertail_optimizer::utils::build_logical_plan;
use shared::terms::Term;
use shared::triple::*;
use std::time::Instant;

fn simple_streamertail_optimizer() {
    println!("=== Simple Volcano Optimizer Example ===");

    let mut database = KolibrieDB::new();

    let alice_id = database.dictionary_mut().encode("http://example.org/alice");
    let bob_id = database.dictionary_mut().encode("http://example.org/bob");
    let name_id = database.dictionary_mut().encode("foaf:name");
    let age_id = database.dictionary_mut().encode("foaf:age");
    let alice_name = database.dictionary_mut().encode("Alice");
    let bob_name = database.dictionary_mut().encode("Bob");
    let age_30 = database.dictionary_mut().encode("30");
    let age_25 = database.dictionary_mut().encode("25");

    database.add_triple(Triple {
        subject: alice_id,
        predicate: name_id,
        object: alice_name,
    });

    database.add_triple(Triple {
        subject: bob_id,
        predicate: name_id,
        object: bob_name,
    });

    database.add_triple(Triple {
        subject: alice_id,
        predicate: age_id,
        object: age_30,
    });

    database.add_triple(Triple {
        subject: bob_id,
        predicate: age_id,
        object: age_25,
    });

    let name_scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(name_id),
        Term::Variable("name".to_string()),
    ));

    let age_scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(age_id),
        Term::Variable("age".to_string()),
    ));

    let logical_plan = LogicalOperator::join(name_scan, age_scan);

    let mut optimizer = Streamertail::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    let start = Instant::now();
    let results = best_plan.execute(&mut database);
    let duration = start.elapsed();

    println!("Query execution time: {:?}", duration);
    println!("Found {} results:", results.len());
    for result in &results {
        println!("  {:?}", result);
    }
    println!();
}

fn streamertail_optimizer_multiple_triples() {
    println!("=== Multiple Triples Example ===");

    let mut database = KolibrieDB::new();

    for i in 0..10 {
        let person_uri = format!("http://example.org/person{}", i);
        let age_value = format!("{}", 20 + i);

        let person_id = database.dictionary_mut().encode(&person_uri);
        let name_id = database.dictionary_mut().encode("foaf:name");
        let age_id = database.dictionary_mut().encode("foaf:age");
        let type_id = database.dictionary_mut().encode("rdf:type");
        let person_type = database.dictionary_mut().encode("foaf:Person");
        let name_value = database.dictionary_mut().encode(&format!("Person{}", i));
        let age_value_id = database.dictionary_mut().encode(&age_value);

        database.add_triple(Triple {
            subject: person_id,
            predicate: name_id,
            object: name_value,
        });

        database.add_triple(Triple {
            subject: person_id,
            predicate: age_id,
            object: age_value_id,
        });

        database.add_triple(Triple {
            subject: person_id,
            predicate: type_id,
            object: person_type,
        });
    }

    println!(
        "Added {} triples to the database",
        database.default_triple_count()
    );

    let type_id = database.dictionary_mut().encode("rdf:type");
    let person_type = database.dictionary_mut().encode("foaf:Person");
    let age_id = database.dictionary_mut().encode("foaf:age");

    let type_scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(type_id),
        Term::Constant(person_type),
    ));

    let age_scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(age_id),
        Term::Variable("age".to_string()),
    ));

    let condition = Condition::new("age".to_string(), ">".to_string(), "25".to_string());
    let filtered_age = LogicalOperator::selection(age_scan, condition);

    let logical_plan = LogicalOperator::join(type_scan, filtered_age);

    let mut optimizer = Streamertail::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    let start = Instant::now();
    let results = best_plan.execute(&mut database);
    let duration = start.elapsed();

    println!("Query execution time: {:?}", duration);
    println!("Found {} results:", results.len());
    for result in results.iter().take(5) {
        println!("  {:?}", result);
    }
    println!();
}

fn streamertail_optimizer_rdf() {
    println!("=== RDF Example with SPARQL Parsing ===");

    let mut database = KolibrieDB::new();

    for i in 0..10 {
        let person_uri = format!("http://example.org/person{}", i);
        let company_uri = format!("http://example.org/company{}", i % 3);

        let person_id = database.dictionary_mut().encode(&person_uri);
        let works_at_id = database
            .dictionary_mut()
            .encode("http://example.org/worksAt");
        let located_id = database
            .dictionary_mut()
            .encode("http://example.org/located");
        let company_id = database.dictionary_mut().encode(&company_uri);
        let location = database.dictionary_mut().encode(&format!("City{}", i % 3));

        database.add_triple(Triple {
            subject: person_id,
            predicate: works_at_id,
            object: company_id,
        });

        database.add_triple(Triple {
            subject: company_id,
            predicate: located_id,
            object: location,
        });
    }

    println!(
        "Added {} triples to the database",
        database.default_triple_count()
    );

    let variables = vec![("VAR", "person"), ("VAR", "location")];
    let patterns = vec![
        ("?person", "http://example.org/worksAt", "?org"),
        ("?org", "http://example.org/located", "?location"),
    ];
    let filters = vec![];
    let mut prefixes = std::collections::HashMap::new();
    prefixes.insert("ex".to_string(), "http://example.org/".to_string());

    let logical_plan = build_logical_plan(
        variables,
        patterns,
        filters,
        &prefixes,
        &mut database,
        &[],
        None,
    );

    let mut optimizer = Streamertail::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    let start = Instant::now();
    let results = best_plan.execute(&mut database);
    let duration = start.elapsed();

    println!("Query execution time: {:?}", duration);
    println!("Found {} results:", results.len());
    for result in results.iter().take(5) {
        println!("  {:?}", result);
    }
    println!();
}

fn streamertail_optimizer_index_patterns() {
    println!("=== Index Pattern Examples ===");

    let mut database = KolibrieDB::new();

    for i in 0..100 {
        let person_uri = format!("http://example.org/person{}", i);
        let age_value = format!("{}", i);

        let person_id = database.dictionary_mut().encode(&person_uri);
        let age_id = database.dictionary_mut().encode("foaf:age");
        let type_id = database.dictionary_mut().encode("rdf:type");
        let person_type = database.dictionary_mut().encode("foaf:Person");
        let age_value_id = database.dictionary_mut().encode(&age_value);

        database.add_triple(Triple {
            subject: person_id,
            predicate: type_id,
            object: person_type,
        });

        database.add_triple(Triple {
            subject: person_id,
            predicate: age_id,
            object: age_value_id,
        });
    }

    println!(
        "Added {} triples to the database",
        database.default_triple_count()
    );

    let age_id = database.dictionary_mut().encode("foaf:age");
    let person_50_id = database
        .dictionary_mut()
        .encode("http://example.org/person50");
    let age_42_id = database.dictionary_mut().encode("42");
    let age_69_id = database.dictionary_mut().encode("69");

    let patterns = vec![
        (
            "Unbound pattern",
            (
                Term::Variable("s".to_string()),
                Term::Variable("p".to_string()),
                Term::Variable("o".to_string()),
            ),
        ),
        (
            "Predicate+Object bound",
            (
                Term::Variable("s".to_string()),
                Term::Constant(age_id),
                Term::Constant(age_42_id),
            ),
        ),
        (
            "Subject bound",
            (
                Term::Constant(person_50_id),
                Term::Variable("p".to_string()),
                Term::Variable("o".to_string()),
            ),
        ),
        (
            "Predicate bound",
            (
                Term::Variable("s".to_string()),
                Term::Constant(age_id),
                Term::Variable("o".to_string()),
            ),
        ),
        (
            "Object bound",
            (
                Term::Variable("s".to_string()),
                Term::Variable("p".to_string()),
                Term::Constant(age_69_id),
            ),
        ),
        (
            "Fully bound",
            (
                Term::Constant(person_50_id),
                Term::Constant(age_id),
                Term::Constant(age_42_id),
            ),
        ),
    ];

    for (i, (description, pattern)) in patterns.iter().enumerate() {
        let logical_plan = LogicalOperator::scan(pattern.clone());
        let mut optimizer = Streamertail::new(&database);
        let best_plan = optimizer.find_best_plan(&logical_plan);

        println!("\n=== QUERY {} - {} ===", i + 1, description);
        println!("Pattern: {:?}", pattern);
        println!("Physical Plan: {:?}", best_plan);

        let start = Instant::now();
        let results = best_plan.execute(&mut database);
        let duration = start.elapsed();

        println!("Execution time: {:?}", duration);
        println!("Got {} result(s).", results.len());
        for r in results.iter().take(3) {
            println!("  -> {:?}", r);
        }
    }
}

fn streamertail_optimizer_performance_test() {
    println!("=== Performance Test ===");

    let mut database = KolibrieDB::new();

    println!("Adding triples...");
    let start = Instant::now();

    for i in 0..1000 {
        let person_uri = format!("http://example.org/person{}", i);
        let company_uri = format!("http://example.org/company{}", i % 10);

        let person_id = database.dictionary_mut().encode(&person_uri);
        let name_id = database.dictionary_mut().encode("foaf:name");
        let age_id = database.dictionary_mut().encode("foaf:age");
        let works_at_id = database.dictionary_mut().encode("ex:worksAt");
        let company_id = database.dictionary_mut().encode(&company_uri);
        let name_value = database.dictionary_mut().encode(&format!("Person{}", i));
        let age_value = database
            .dictionary_mut()
            .encode(&format!("{}", 20 + (i % 50)));

        database.add_triple(Triple {
            subject: person_id,
            predicate: name_id,
            object: name_value,
        });

        database.add_triple(Triple {
            subject: person_id,
            predicate: age_id,
            object: age_value,
        });

        database.add_triple(Triple {
            subject: person_id,
            predicate: works_at_id,
            object: company_id,
        });
    }

    let data_load_time = start.elapsed();
    println!("Data loading time: {:?}", data_load_time);
    println!(
        "Added {} triples to the database",
        database.default_triple_count()
    );

    let name_id = database.dictionary_mut().encode("foaf:name");
    let age_id = database.dictionary_mut().encode("foaf:age");
    let works_at_id = database.dictionary_mut().encode("ex:worksAt");

    println!("\n--- Query 1: Simple Scan ---");
    let logical_plan1 = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(name_id),
        Term::Variable("name".to_string()),
    ));

    let start = Instant::now();
    let mut optimizer1 = Streamertail::new(&database);
    let physical_plan1 = optimizer1.find_best_plan(&logical_plan1);
    let optimization_time1 = start.elapsed();

    let start = Instant::now();
    let results1 = physical_plan1.execute(&mut database);
    let execution_time1 = start.elapsed();

    println!("Optimization time: {:?}", optimization_time1);
    println!("Execution time: {:?}", execution_time1);
    println!("Results: {} rows", results1.len());

    println!("\n--- Query 2: Join Query ---");
    let name_scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(name_id),
        Term::Variable("name".to_string()),
    ));

    let age_scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(age_id),
        Term::Variable("age".to_string()),
    ));

    let logical_plan2 = LogicalOperator::join(name_scan, age_scan);

    let start = Instant::now();
    let mut optimizer2 = Streamertail::new(&database);
    let physical_plan2 = optimizer2.find_best_plan(&logical_plan2);
    let optimization_time2 = start.elapsed();

    let start = Instant::now();
    let results2 = physical_plan2.execute(&mut database);
    let execution_time2 = start.elapsed();

    println!("Optimization time: {:?}", optimization_time2);
    println!("Execution time: {:?}", execution_time2);
    println!("Results: {} rows", results2.len());

    println!("\n--- Query 3: Complex Join with Filter ---");
    let works_scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(works_at_id),
        Term::Variable("company".to_string()),
    ));

    let name_scan_clone = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(name_id),
        Term::Variable("name".to_string()),
    ));
    let joined = LogicalOperator::join(name_scan_clone, works_scan);
    let condition = Condition::new("name".to_string(), "=".to_string(), "Person100".to_string());
    let logical_plan3 = LogicalOperator::selection(joined, condition);

    let start = Instant::now();
    let mut optimizer3 = Streamertail::new(&database);
    let physical_plan3 = optimizer3.find_best_plan(&logical_plan3);
    let optimization_time3 = start.elapsed();

    let start = Instant::now();
    let results3 = physical_plan3.execute(&mut database);
    let execution_time3 = start.elapsed();

    println!("Optimization time: {:?}", optimization_time3);
    println!("Execution time: {:?}", execution_time3);
    println!("Results: {} rows", results3.len());

    println!("\n=== Performance Summary ===");
    println!("Data loading: {:?}", data_load_time);
    println!(
        "Simple scan: opt={:?}, exec={:?}",
        optimization_time1, execution_time1
    );
    println!(
        "Join query: opt={:?}, exec={:?}",
        optimization_time2, execution_time2
    );
    println!(
        "Complex query: opt={:?}, exec={:?}",
        optimization_time3, execution_time3
    );
}

fn main() {
    simple_streamertail_optimizer();
    println!("============================================");

    streamertail_optimizer_multiple_triples();
    println!("============================================");

    streamertail_optimizer_rdf();
    println!("============================================");

    streamertail_optimizer_index_patterns();
    println!("============================================");

    streamertail_optimizer_performance_test();
    println!("============================================");

    println!("All examples completed successfully!");
}
