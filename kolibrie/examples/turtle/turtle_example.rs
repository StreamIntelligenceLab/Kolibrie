/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

extern crate kolibrie;
use kolibrie::sparql_database::*;

fn main() {
    let mut db = SparqlDatabase::new();

    // Employee dataset in Turtle format
    let turtle_data = r#"
        <http://example.org/employee1> <http://example.org/name> "Alice" .
        <http://example.org/employee1> <http://example.org/jobTitle> "Engineer" .
        <http://example.org/employee1> <http://example.org/salary> "6000" .
        
        <http://example.org/employee2> <http://example.org/name> "Bob" .
        <http://example.org/employee2> <http://example.org/jobTitle> "Designer" .
        <http://example.org/employee2> <http://example.org/salary> "4500" .
        
        <http://example.org/employee3> <http://example.org/name> "Charlie" .
        <http://example.org/employee3> <http://example.org/jobTitle> "Manager" .
        <http://example.org/employee3> <http://example.org/salary> "7000" .
    "#;
    db.parse_turtle(turtle_data);

    // Filter employees with salary greater than 5000 using QueryBuilder
    let filtered_triples = db.query()
        .with_predicate("<http://example.org/salary>")
        .filter(|triple| {
            if let Some(object) = db.dictionary.decode(triple.object) {
                return object.parse::<i32>().unwrap_or(0) > 5000;
            }
            false
        })
        .get_triples();

    // Print the filtered triples
    println!("Filtered Triples:");
    for triple in filtered_triples.clone() {
        let subject = db.dictionary.decode(triple.subject).unwrap_or("");
        let predicate = db.dictionary.decode(triple.predicate).unwrap_or("");
        let object = db.dictionary.decode(triple.object).unwrap_or("");
        println!("{} {} {} .", subject, predicate, object);
    }
}
