extern crate kolibrie;
use kolibrie::sparql_database::*;
use kolibrie::triple::*;
use kolibrie::utils::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_database() -> SparqlDatabase {
        let rdf_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                  xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/person1">
              <ex:name>John Doe</ex:name>
              <ex:age>30</ex:age>
              <ex:worksFor rdf:resource="http://example.org/company1"/>
            </rdf:Description>
            <rdf:Description rdf:about="http://example.org/company1">
              <ex:name>Company One</ex:name>
              <ex:industry>Software</ex:industry>
            </rdf:Description>
        </rdf:RDF>
        "#;

        let mut db = SparqlDatabase::new();
        db.parse_rdf(rdf_data);
        db
    }

    #[test]
    fn test_time_based_window() {
        let mut db = setup_database();
        let now = current_timestamp();

        let subject = db.dictionary.encode("http://example.org/person1");
        let predicate = db.dictionary.encode("ex:isNearby");
        let object = db.dictionary.encode("http://example.org/shop1");

        db.add_stream_data(
            Triple {
                subject,
                predicate,
                object,
            },
            now,
        );

        db.add_stream_data(
            Triple {
                subject,
                predicate,
                object,
            },
            now + 5,
        );

        let windowed_triples = db.time_based_window(now, now + 10);
        assert_eq!(windowed_triples.len(), 2);
    }

    #[test]
    fn test_filter() {
        let db = setup_database();
        let filtered_db = db.filter(|triple| {
            let predicate = db.dictionary.decode(triple.predicate).unwrap();
            predicate.contains("name")
        });
        assert_eq!(filtered_db.triples.len(), 2);
        assert!(filtered_db.triples.iter().all(|t| {
            let predicate = db.dictionary.decode(t.predicate).unwrap();
            predicate == "ex:name"
        }));
    }

    #[test]
    fn test_distinct() {
        let mut db = setup_database();
        db.triples.insert(Triple {
            subject: db.dictionary.encode("http://example.org/person1"),
            predicate: db.dictionary.encode("ex:name"),
            object: db.dictionary.encode("John Doe"),
        });

        let distinct_db = db.distinct();
        assert_eq!(distinct_db.triples.len(), 5);
    }

    #[test]
    fn test_order_by() {
        let db = setup_database();
        let ordered_triples = db.order_by(|triple| {
            db.dictionary.decode(triple.object).unwrap().to_string()
        });
        assert_eq!(db.dictionary.decode(ordered_triples[0].object).unwrap(), "30");
        assert_eq!(db.dictionary.decode(ordered_triples[1].object).unwrap(), "Company One");
    }

    #[test]
    fn test_group_by() {
        let db = setup_database();
        let groups = db.group_by(|triple| {
            db.dictionary.decode(triple.predicate).unwrap().to_string()
        });
        assert_eq!(groups["ex:name"].len(), 2);
        assert_eq!(groups["ex:age"].len(), 1);
        assert_eq!(groups["ex:worksFor"].len(), 1);
        assert_eq!(groups["ex:industry"].len(), 1);
    }

    #[test]
    fn test_union() {
        let mut db1 = setup_database();
        let mut db2 = SparqlDatabase::new();
        db2.parse_rdf(
            r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                  xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/person2">
              <ex:name>Jane Doe</ex:name>
              <ex:age>28</ex:age>
              <ex:worksFor rdf:resource="http://example.org/company1"/>
            </rdf:Description>
        </rdf:RDF>
        "#,
        );

        let union_db = db1.union(&db2);
        assert_eq!(union_db.triples.len(), 8);
    }

    #[test]
    fn test_join() {
        let mut db1 = setup_database();
        let mut db2 = SparqlDatabase::new();
        db2.parse_rdf(
            r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                  xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/company1">
              <ex:location>USA</ex:location>
            </rdf:Description>
        </rdf:RDF>
        "#,
        );

        let mut un = db1.union(&db2);
        let joined_db = un.join(&un.clone(), "ex:worksFor");
        assert_eq!(joined_db.triples.len(), 1);
        let joined_triple = joined_db.triples.iter().next().unwrap();
        assert_eq!(db1.dictionary.decode(joined_triple.subject).unwrap(), "http://example.org/person1");
        assert_eq!(db1.dictionary.decode(joined_triple.predicate).unwrap(), "ex:worksFor");
        assert_eq!(db1.dictionary.decode(joined_triple.object).unwrap(), "http://example.org/company1");
    }

    #[test]
    fn test_add_stream_data() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        db.add_stream_data(triple.clone(), 1000);

        assert_eq!(db.streams.len(), 1);
        assert_eq!(db.streams[0].triple, triple);
        assert_eq!(db.streams[0].timestamp, 1000);
    }

    #[test]
    fn test_sliding_window() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        db.add_stream_data(triple.clone(), current_timestamp());
        db.set_sliding_window(5, 10);

        let result = db.evaluate_sliding_window();
        assert!(result.contains(&triple));

        // Simulate time passing and add another triple outside the window
        std::thread::sleep(std::time::Duration::from_secs(5));
        let triple2 = Triple {
            subject: db.dictionary.encode("s2"),
            predicate: db.dictionary.encode("p2"),
            object: db.dictionary.encode("o2"),
        };
        db.add_stream_data(triple2.clone(), current_timestamp() + 10);

        let result = db.evaluate_sliding_window();
        assert!(!result.contains(&triple2));
    }

    #[test]
    fn test_istream() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        let initial_time = current_timestamp();
        db.add_stream_data(triple.clone(), initial_time);

        // Add new triple after initial timestamp
        let triple2 = Triple {
            subject: db.dictionary.encode("s2"),
            predicate: db.dictionary.encode("p2"),
            object: db.dictionary.encode("o2"),
        };
        db.add_stream_data(triple2.clone(), initial_time + 10);

        let istream_result = db.istream(initial_time);
        assert_eq!(istream_result.len(), 1);
        assert_eq!(istream_result[0], triple2);
    }

    #[test]
    fn test_dstream() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        let initial_time = current_timestamp();
        db.add_stream_data(triple.clone(), initial_time);

        // Add new triple after initial timestamp
        let triple2 = Triple {
            subject: db.dictionary.encode("s2"),
            predicate: db.dictionary.encode("p2"),
            object: db.dictionary.encode("o2"),
        };
        db.add_stream_data(triple2.clone(), initial_time + 5);

        // Evaluate dstream after removing the first triple (simulating stream expiration)
        let dstream_result = db.dstream(initial_time, initial_time + 5);
        assert_eq!(dstream_result.len(), 0);
        assert_eq!(dstream_result.is_empty(), true);
    }

    #[test]
    fn test_rstream() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        let initial_time = current_timestamp();
        db.add_stream_data(triple.clone(), initial_time);

        // Add new triple within the window
        let triple2 = Triple {
            subject: db.dictionary.encode("s2"),
            predicate: db.dictionary.encode("p2"),
            object: db.dictionary.encode("o2"),
        };
        db.add_stream_data(triple2.clone(), initial_time + 5);

        // Evaluate rstream within a time window
        let rstream_result = db.rstream(initial_time, initial_time + 10);
        assert_eq!(rstream_result.len(), 2);
        assert!(rstream_result.contains(&triple));
        assert!(rstream_result.contains(&triple2));
    }

    #[test]
    fn test_window_close_policy() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        db.add_stream_data(triple.clone(), current_timestamp());
        db.set_sliding_window(5, 10);

        // Simulate time passing to trigger the window close policy
        std::thread::sleep(std::time::Duration::from_secs(15));

        let result = db.window_close_policy();
        println!("Result after window close policy: {:?}", result);
        assert!(result.is_empty());
    }

    #[test]
    fn test_content_change_policy() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        db.add_stream_data(triple.clone(), current_timestamp());
        db.set_sliding_window(5, 10);

        // Modify the triples in the stream
        let triple2 = Triple {
            subject: db.dictionary.encode("s2"),
            predicate: db.dictionary.encode("p2"),
            object: db.dictionary.encode("o2"),
        };
        db.add_stream_data(triple2.clone(), current_timestamp() + 5);

        let result = db.content_change_policy();
        assert!(result.contains(&triple));
        assert!(result.contains(&triple2));
    }

    #[test]
    fn test_non_empty_content_policy() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        db.add_stream_data(triple.clone(), current_timestamp());
        db.set_sliding_window(5, 10);

        let result = db.non_empty_content_policy();
        assert!(result.contains(&triple));
    }

    #[test]
    fn test_periodic_policy() {
        let mut db = SparqlDatabase::new();
        let triple = Triple {
            subject: db.dictionary.encode("s1"),
            predicate: db.dictionary.encode("p1"),
            object: db.dictionary.encode("o1"),
        };
        db.add_stream_data(triple.clone(), current_timestamp());
        db.set_sliding_window(5, 5);

        // Simulate time passing to trigger the periodic policy
        std::thread::sleep(std::time::Duration::from_secs(5));

        let result = db.periodic_policy(std::time::Duration::from_secs(5));
        assert!(result.contains(&triple));
    }
}
