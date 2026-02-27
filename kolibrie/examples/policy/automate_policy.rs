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
use shared::triple::*;
use kolibrie::utils::current_timestamp;

fn main() {
    let mut db = SparqlDatabase::new();

    // Set up a sliding window with a width of 10 seconds and a slide interval of 5 seconds
    db.set_sliding_window(10, 5);

    let mut counter = 1;

    loop {
        let mut dict = db.dictionary.write().unwrap();
        let subject = dict.encode(&format!("subject{}", counter));
        let predicate = dict.encode(&format!("predicate{}", counter));
        let object = dict.encode(&format!("object{}", counter));
        drop(dict);

        // Simulate adding triples with timestamps in a loop
        let triple = Triple {
            subject,
            predicate,
            object,
        };

        db.add_stream_data(triple.clone(), current_timestamp());

        // Wait for some time to simulate the stream processing
        std::thread::sleep(std::time::Duration::from_secs(1));

        // Automatically evaluate policies
        let auto_policy_triples = db.auto_policy_evaluation();
        println!(
            "Automatically Evaluated Policy triples: {:?}",
            auto_policy_triples
        );

        // Increment the counter for the next triple
        counter += 1;

        // Break the loop after adding a few triples for demonstration
        if counter > 20 {
            break;
        }
    }
}
