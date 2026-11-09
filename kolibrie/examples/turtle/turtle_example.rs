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
use kolibrie::prelude::*;

fn main() {
    let mut db = KolibrieDB::new();

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

    let filtered_triples = db
        .query()
        .with_predicate("<http://example.org/salary>")
        .filter(|triple| {
            let dict = db.dictionary();
            if let Some(object) = dict.decode(triple.object) {
                let result = object.parse::<i32>().unwrap_or(0) > 5000;
                drop(dict);
                return result;
            }
            drop(dict);
            false
        })
        .get_triples();

    println!("Filtered Triples:");
    for triple in filtered_triples.clone() {
        let dict = db.dictionary();
        let subject = dict.decode(triple.subject).unwrap_or("").to_string();
        let predicate = dict.decode(triple.predicate).unwrap_or("").to_string();
        let object = dict.decode(triple.object).unwrap_or("").to_string();
        drop(dict);
        println!("{} {} {} .", subject, predicate, object);
    }
}
