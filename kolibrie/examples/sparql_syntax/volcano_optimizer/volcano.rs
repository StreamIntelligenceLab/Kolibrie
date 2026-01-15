/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::sparql_database::*;
use kolibrie::streamertail_optimizer::*;
use shared::terms::Term;
use shared::triple::*;
use std::time::Instant;

fn simple_streamertail_optimizer() {
    println!("=== Simple Volcano Optimizer Example ===");

    // Step 1: Execute the physical plan on a sample database
    let mut database = SparqlDatabase::new();

    // Add some sample data
    let alice_id = database.dictionary.encode("http://example.org/alice");
    let bob_id = database.dictionary.encode("http://example.org/bob");
    let name_id = database.dictionary.encode("foaf:name");
    let age_id = database.dictionary.encode("foaf:age");
    let alice_name = database.dictionary.encode("Alice");
    let bob_name = database.dictionary.encode("Bob");
    let age_30 = database.dictionary.encode("30");
    let age_25 = database.dictionary.encode("25");

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

    // Step 2: Create the logical plan using new API
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

    // Step 3: Initialize the optimizer and optimize
    let mut optimizer = Streamertail::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    // Step 4: Execute the optimized plan
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

    // Step 1: Execute the physical plan on a sample database
    let mut database = SparqlDatabase::new();

    // Step 2: Add some triples to the database
    for i in 0..10 {
        let person_uri = format!("http://example.org/person{}", i);
        let age_value = format!("{}", 20 + i);

        let person_id = database.dictionary.encode(&person_uri);
        let name_id = database.dictionary.encode("foaf:name");
        let age_id = database.dictionary.encode("foaf:age");
        let type_id = database.dictionary.encode("rdf:type");
        let person_type = database.dictionary.encode("foaf:Person");
        let name_value = database.dictionary.encode(&format!("Person{}", i));
        let age_value_id = database.dictionary.encode(&age_value);

        // Add name triple
        database.add_triple(Triple {
            subject: person_id,
            predicate: name_id,
            object: name_value,
        });

        // Add age triple
        database.add_triple(Triple {
            subject: person_id,
            predicate: age_id,
            object: age_value_id,
        });

        // Add type triple
        database.add_triple(Triple {
            subject: person_id,
            predicate: type_id,
            object: person_type,
        });
    }

    println!("Added {} triples to the database", database.triples.len());

    // Step 3: Create the logical plan with joins
    let type_id = database.dictionary.encode("rdf:type");
    let person_type = database.dictionary.encode("foaf:Person");
    let age_id = database.dictionary.encode("foaf:age");

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

    // Create a filter condition
    let condition = Condition::new("age".to_string(), ">".to_string(), "25".to_string());
    let filtered_age = LogicalOperator::selection(age_scan, condition);

    let logical_plan = LogicalOperator::join(type_scan, filtered_age);

    // Step 4: Initialize the optimizer and optimize
    let mut optimizer = Streamertail::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    // Step 5: Execute the optimized plan
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

    // Step 1: Execute the physical plan on a sample database
    let mut database = SparqlDatabase::new();

    // Step 2: Add some triples to the database
    for i in 0..10 {
        let person_uri = format!("http://example.org/person{}", i);
        let company_uri = format!("http://example.org/company{}", i % 3);

        let person_id = database.dictionary.encode(&person_uri);
        let works_at_id = database.dictionary.encode("http://example.org/worksAt");
        let located_id = database.dictionary.encode("http://example.org/located");
        let company_id = database.dictionary.encode(&company_uri);
        let location = database.dictionary.encode(&format!("City{}", i % 3));

        // Person works at company
        database.add_triple(Triple {
            subject: person_id,
            predicate: works_at_id,
            object: company_id,
        });

        // Company located in city
        database.add_triple(Triple {
            subject: company_id,
            predicate: located_id,
            object: location,
        });
    }

    println!("Added {} triples to the database", database.triples.len());

    // Step 3: Create logical plan using build_logical_plan utility
    let variables = vec![("VAR", "person"), ("VAR", "location")];
    let patterns = vec![
        ("?person", "http://example.org/worksAt", "?org"),
        ("?org", "http://example.org/located", "?location"),
    ];
    let filters = vec![];
    let mut prefixes = std::collections::HashMap::new();
    prefixes.insert("ex".to_string(), "http://example.org/".to_string());

    let logical_plan = build_logical_plan(variables, patterns, filters, &prefixes, &mut database, &[], None);

    // Step 4: Initialize the optimizer and optimize
    let mut optimizer = Streamertail::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    // Step 5: Execute the optimized plan
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

    // Step 1: Initialize the database
    let mut database = SparqlDatabase::new();

    // Step 2: Add triples to the database
    for i in 0..100 {
        let person_uri = format!("http://example.org/person{}", i);
        let age_value = format!("{}", i);

        let person_id = database.dictionary.encode(&person_uri);
        let age_id = database.dictionary.encode("foaf:age");
        let type_id = database.dictionary.encode("rdf:type");
        let person_type = database.dictionary.encode("foaf:Person");
        let age_value_id = database.dictionary.encode(&age_value);

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

    println!("Added {} triples to the database", database.triples.len());

    let age_id = database.dictionary.encode("foaf:age");
    let person_50_id = database.dictionary.encode("http://example.org/person50");
    let age_42_id = database.dictionary.encode("42");
    let age_69_id = database.dictionary.encode("69");

    // Define different triple patterns to test various index strategies
    let patterns = vec![
        // (1) No bounds => full table scan
        (
            "Unbound pattern",
            (
                Term::Variable("s".to_string()),
                Term::Variable("p".to_string()),
                Term::Variable("o".to_string()),
            ),
        ),
        // (2) predicate+object => POS index
        (
            "Predicate+Object bound",
            (
                Term::Variable("s".to_string()),
                Term::Constant(age_id),
                Term::Constant(age_42_id),
            ),
        ),
        // (3) subject bound => SPO index
        (
            "Subject bound",
            (
                Term::Constant(person_50_id),
                Term::Variable("p".to_string()),
                Term::Variable("o".to_string()),
            ),
        ),
        // (4) predicate bound => PSO index
        (
            "Predicate bound",
            (
                Term::Variable("s".to_string()),
                Term::Constant(age_id),
                Term::Variable("o".to_string()),
            ),
        ),
        // (5) object bound => OSP index
        (
            "Object bound",
            (
                Term::Variable("s".to_string()),
                Term::Variable("p".to_string()),
                Term::Constant(age_69_id),
            ),
        ),
        // (6) fully bound => direct check
        (
            "Fully bound",
            (
                Term::Constant(person_50_id),
                Term::Constant(age_id),
                Term::Constant(age_42_id),
            ),
        ),
    ];

    // For each pattern, build + optimize + execute
    for (i, (description, pattern)) in patterns.iter().enumerate() {
        let logical_plan = LogicalOperator::scan(pattern.clone());
        let mut optimizer = Streamertail::new(&database);
        let best_plan = optimizer.find_best_plan(&logical_plan);

        println!("\n=== QUERY {} - {} ===", i + 1, description);
        println!("Pattern: {:?}", pattern);
        println!("Physical Plan: {:?}", best_plan);

        // Execute the best plan
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

    // Step 1: Initialize the database with larger dataset
    let mut database = SparqlDatabase::new();

    // Step 2: Add many triples to test performance
    println!("Adding triples...");
    let start = Instant::now();

    for i in 0..1000 {
        let person_uri = format!("http://example.org/person{}", i);
        let company_uri = format!("http://example.org/company{}", i % 10);

        let person_id = database.dictionary.encode(&person_uri);
        let name_id = database.dictionary.encode("foaf:name");
        let age_id = database.dictionary.encode("foaf:age");
        let works_at_id = database.dictionary.encode("ex:worksAt");
        let company_id = database.dictionary.encode(&company_uri);
        let name_value = database.dictionary.encode(&format!("Person{}", i));
        let age_value = database.dictionary.encode(&format!("{}", 20 + (i % 50)));

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
    println!("Added {} triples to the database", database.triples.len());

    // Test different query types
    let name_id = database.dictionary.encode("foaf:name");
    let age_id = database.dictionary.encode("foaf:age");
    let works_at_id = database.dictionary.encode("ex:worksAt");

    // Query 1: Simple scan
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

    // Query 2: Join query
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

    // Query 3: Complex join with filter
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
