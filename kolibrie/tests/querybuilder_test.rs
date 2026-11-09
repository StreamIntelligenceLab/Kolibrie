/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

extern crate kolibrie;
use kolibrie::database::KolibrieDB;
use kolibrie::query_builder::QueryBuilder;
use kolibrie::rsp::r2s::StreamOperator;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_query_integration() {
        let db = KolibrieDB::new();

        let mut query = QueryBuilder::new(&db)
            .window(10, 2)
            .with_predicate("knows")
            .with_stream_operator(StreamOperator::RSTREAM)
            .as_stream()
            .expect("Failed to create streaming query");

        query.add_stream_triple("Alice", "knows", "Bob", 1).unwrap();
        query
            .add_stream_triple("Bob", "knows", "Charlie", 2)
            .unwrap();
        query
            .add_stream_triple("Alice", "likes", "Pizza", 3)
            .unwrap();

        let results = query.get_stream_results();

        println!("Streaming results: {:?}", results);

        query.stop_stream();
    }

    #[test]
    fn test_istream_operator() {
        let db = KolibrieDB::new();

        let mut query = QueryBuilder::new(&db)
            .window(10, 2)
            .with_subject_like("Alice")
            .with_stream_operator(StreamOperator::ISTREAM)
            .as_stream()
            .expect("Failed to create streaming query");

        query.add_stream_triple("Alice", "knows", "Bob", 1).unwrap();
        let results1 = query.get_stream_results();

        query
            .add_stream_triple("Alice", "knows", "Charlie", 5)
            .unwrap();
        let results2 = query.get_stream_results();

        println!("ISTREAM results 1: {:?}", results1);
        println!("ISTREAM results 2: {:?}", results2);

        query.stop_stream();
    }
}
