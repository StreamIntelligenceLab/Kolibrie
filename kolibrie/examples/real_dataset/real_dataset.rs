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
use std::fs::File;
use std::io::Read;

fn filter_example() {
    use std::collections::{HashMap, HashSet};

    let project_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let rdf_file_path = project_root.join("../datasets/synthetic_data_employee_4.rdf");

    let mut file = File::open(&rdf_file_path).expect("Unable to open file");
    let mut rdf_data = String::new();
    file.read_to_string(&mut rdf_data)
        .expect("Unable to read file");

    let mut database = KolibrieDB::new();
    database.parse_rdf(&rdf_data);

    let high_salary_triples = database
        .query()
        .filter(|triple| {
            let dict = database.dictionary();
            if let Some(predicate) = dict.decode(triple.predicate) {
                if predicate.ends_with("annual_salary") {
                    if let Some(object) = dict.decode(triple.object) {
                        if let Ok(salary) = object.parse::<f64>() {
                            return salary > 80000.0;
                        }
                    }
                }
            }
            false
        })
        .get_triples();

    let mut high_salary_subjects = HashSet::new();
    let mut subject_to_salary = HashMap::new();

    let dict = database.dictionary();
    for triple in &high_salary_triples {
        let subject = triple.subject;
        let salary = dict.decode(triple.object).unwrap_or("0.0");
        high_salary_subjects.insert(subject);
        subject_to_salary.insert(subject, salary.to_string());
    }
    drop(dict);

    let name_triples = database
        .query()
        .filter(|triple| {
            let dict = database.dictionary();
            if let Some(predicate) = dict.decode(triple.predicate) {
                if predicate.ends_with("name") && high_salary_subjects.contains(&triple.subject) {
                    return true;
                }
            }
            false
        })
        .get_triples();

    println!("Employees with salary > 80000:");
    let dict = database.dictionary();
    for triple in name_triples.clone() {
        let subject = triple.subject;
        let name = dict.decode(triple.object).unwrap_or("");
        let salary = subject_to_salary
            .get(&subject)
            .cloned()
            .unwrap_or_else(|| "Unknown".to_string());
        println!("Name: {}, Salary: {}", name, salary);
    }
}

fn main() {
    filter_example();
}
