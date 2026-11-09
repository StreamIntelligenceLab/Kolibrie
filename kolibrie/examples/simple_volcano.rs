/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::database::KolibrieDB;
use kolibrie::streamertail_optimizer::operators::logical::LogicalOperator;
use kolibrie::streamertail_optimizer::optimizer::Streamertail;
use kolibrie::streamertail_optimizer::types::Condition;
use shared::terms::Term;
use shared::triple::Triple;
use std::time::Instant;

fn main() {
    println!("=== Simple Volcano Optimizer Example ===\n");

    let mut database = KolibrieDB::new();

    add_sample_data(&mut database);

    simple_scan_example(&mut database);

    join_example(&mut database);

    filter_example(&mut database);

    println!("=== Example completed successfully! ===");
}

fn add_sample_data(database: &mut KolibrieDB) {
    println!("Adding sample data...");

    let mut dict = database.dictionary_mut();

    let alice_id = dict.encode("http://example.org/alice");
    let bob_id = dict.encode("http://example.org/bob");
    let charlie_id = dict.encode("http://example.org/charlie");

    let name_id = dict.encode("http://example.org/name");
    let age_id = dict.encode("http://example.org/age");
    let works_at_id = dict.encode("http://example.org/worksAt");

    let alice_name = dict.encode("Alice");
    let bob_name = dict.encode("Bob");
    let charlie_name = dict.encode("Charlie");
    let age_25 = dict.encode("25");
    let age_30 = dict.encode("30");
    let age_35 = dict.encode("35");
    let company_id = dict.encode("http://example.org/company");

    drop(dict);

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
        subject: charlie_id,
        predicate: name_id,
        object: charlie_name,
    });

    database.add_triple(Triple {
        subject: alice_id,
        predicate: age_id,
        object: age_25,
    });

    database.add_triple(Triple {
        subject: bob_id,
        predicate: age_id,
        object: age_30,
    });

    database.add_triple(Triple {
        subject: charlie_id,
        predicate: age_id,
        object: age_35,
    });

    database.add_triple(Triple {
        subject: alice_id,
        predicate: works_at_id,
        object: company_id,
    });

    database.add_triple(Triple {
        subject: bob_id,
        predicate: works_at_id,
        object: company_id,
    });

    println!(
        "Added {} triples to the database.\n",
        database.default_triple_count()
    );
}

fn simple_scan_example(database: &mut KolibrieDB) {
    println!("=== Example 1: Simple Scan ===");

    let mut dict = database.dictionary_mut();
    let name_id = dict.encode("http://example.org/name");
    drop(dict);

    let logical_plan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(name_id),
        Term::Variable("name".to_string()),
    ));

    let start = Instant::now();
    let mut optimizer = Streamertail::new(database);
    let physical_plan = optimizer.find_best_plan(&logical_plan);
    let optimization_time = start.elapsed();

    println!("Optimization completed in {:?}", optimization_time);
    println!("Physical plan: {:?}\n", physical_plan);

    let start = Instant::now();
    let results = physical_plan.execute(database);
    let execution_time = start.elapsed();

    println!("Execution completed in {:?}", execution_time);
    println!("Found {} results:", results.len());
    for result in &results {
        println!("  {:?}", result);
    }
    println!();
}

fn join_example(database: &mut KolibrieDB) {
    println!("=== Example 2: Join Query ===");

    let mut dict = database.dictionary_mut();
    let name_id = dict.encode("http://example.org/name");
    let age_id = dict.encode("http://example.org/age");
    drop(dict);

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

    let start = Instant::now();
    let mut optimizer = Streamertail::new(database);
    let physical_plan = optimizer.find_best_plan(&logical_plan);
    let optimization_time = start.elapsed();

    println!("Optimization completed in {:?}", optimization_time);
    println!("Physical plan: {:?}\n", physical_plan);

    let start = Instant::now();
    let results = physical_plan.execute(database);
    let execution_time = start.elapsed();

    println!("Execution completed in {:?}", execution_time);
    println!("Found {} results:", results.len());
    for result in &results {
        println!("  {:?}", result);
    }
    println!();
}

fn filter_example(database: &mut KolibrieDB) {
    println!("=== Example 3: Filter Query ===");

    let mut dict = database.dictionary_mut();
    let name_id = dict.encode("http://example.org/name");
    drop(dict);

    let scan = LogicalOperator::scan((
        Term::Variable("person".to_string()),
        Term::Constant(name_id),
        Term::Variable("name".to_string()),
    ));

    let condition = Condition::new("name".to_string(), "=".to_string(), "Alice".to_string());
    let logical_plan = LogicalOperator::selection(scan, condition);

    let start = Instant::now();
    let mut optimizer = Streamertail::new(database);
    let physical_plan = optimizer.find_best_plan(&logical_plan);
    let optimization_time = start.elapsed();

    println!("Optimization completed in {:?}", optimization_time);
    println!("Physical plan: {:?}\n", physical_plan);

    let start = Instant::now();
    let results = physical_plan.execute(database);
    let execution_time = start.elapsed();

    println!("Execution completed in {:?}", execution_time);
    println!("Found {} results:", results.len());
    for result in &results {
        println!("  {:?}", result);
    }
    println!();
}
