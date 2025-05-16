/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

extern crate quick_xml;
extern crate serde;
extern crate serde_json;
extern crate kolibrie;

use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;
use kolibrie::sparql_database::*;
use shared::triple::Triple;
use kolibrie::utils::current_timestamp;

#[derive(Debug)]
pub struct RDFTriple {
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

pub fn parse_rdf(xml: &str) -> Vec<RDFTriple> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf: Vec<u8> = Vec::new();
    let mut current_subject = String::new();
    let mut current_predicate = String::new();
    let mut triples = Vec::new();
    let mut in_predicate = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag_name = String::from_utf8(e.name().0.to_vec()).unwrap();
                match tag_name.as_str() {
                    "rdf:Description" => {
                        for attr in e.attributes() {
                            let attr = attr.unwrap();
                            if attr.key == quick_xml::name::QName(b"rdf:about") {
                                current_subject = String::from_utf8(attr.value.to_vec()).unwrap();
                            }
                        }
                    }
                    _ => {
                        current_predicate = tag_name.clone();
                        in_predicate = true;
                    }
                }
            }
            Ok(Event::Text(e)) => {
                if in_predicate {
                    let text = e.unescape().unwrap().to_string();
                    if !current_subject.is_empty() && !current_predicate.is_empty() {
                        triples.push(RDFTriple {
                            subject: current_subject.clone(),
                            predicate: format!(
                                "http://example.org/stuff/1.0/{}",
                                &current_predicate[3..]
                            ),
                            object: text,
                        });
                        current_predicate.clear();
                        in_predicate = false;
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let tag_name = String::from_utf8(e.name().0.to_vec()).unwrap();
                if tag_name == current_predicate {
                    current_predicate.clear();
                    in_predicate = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => (),
        }
        buf.clear();
    }
    triples
}

pub fn execute_sparql_query(triples: &[RDFTriple], query: &str) -> Vec<HashMap<String, String>> {
    let mut results = Vec::new();
    let select_clause = query.split("SELECT").collect::<Vec<&str>>()[1]
        .split("WHERE")
        .collect::<Vec<&str>>()[0]
        .trim();
    let variables: Vec<String> = select_clause
        .split_whitespace()
        .map(|v| v.replace("?", ""))
        .collect();

    let where_clause = query.split("WHERE {").collect::<Vec<&str>>()[1]
        .trim_end_matches("}")
        .trim();

    let patterns: Vec<Vec<String>> = where_clause
        .split(" . ")
        .map(|pattern| {
            pattern
                .split_whitespace()
                .map(|part| part.replace("?", ""))
                .collect()
        })
        .collect();

    if patterns.len() == 1 && patterns[0].len() == 5 {
        let pattern = &patterns[0];
        for triple in triples {
            if triple.predicate
                == format!(
                    "http://example.org/stuff/1.0/{}",
                    pattern[1].replace("ex:", "")
                )
            {
                let mut result = HashMap::new();
                result.insert(pattern[2].clone(), triple.object.clone());
                results.push(result);
            }
        }
    } else {
        let mut intermediate_results_first: Vec<HashMap<String, String>> = vec![HashMap::new()];
        let mut intermediate_results_second: Vec<HashMap<String, String>> = vec![HashMap::new()];

        for pattern in &patterns {
            let mut new_results = Vec::new();
            for triple in triples {
                if triple.predicate
                    == format!(
                        "http://example.org/stuff/1.0/{}",
                        pattern[1].replace("ex:", "")
                    )
                {
                    for result in &intermediate_results_first {
                        if result.get(&pattern[0]).unwrap_or(&triple.subject) == &triple.subject {
                            let mut new_result = result.clone();
                            new_result.insert(pattern[0].clone(), triple.subject.clone());
                            new_result.insert(pattern[2].clone(), triple.object.clone());
                            new_results.push(new_result);
                        }
                    }
                }
            }
            intermediate_results_first = new_results;
        }

        for pattern in &patterns {
            let mut new_results = Vec::new();
            for triple in triples {
                if triple.predicate
                    == format!(
                        "http://example.org/stuff/1.0/{}",
                        pattern[6].replace("ex:", "")
                    )
                {
                    for result in &intermediate_results_second {
                        if result.get(&pattern[0]).unwrap_or(&triple.subject) == &triple.subject {
                            let mut new_result = result.clone();
                            new_result.insert(pattern[0].clone(), triple.subject.clone());
                            new_result.insert(pattern[6].clone(), triple.object.clone());
                            new_results.push(new_result);
                        }
                    }
                }
            }
            intermediate_results_second = new_results;
        }

        let mut combined_results: Vec<HashMap<String, String>> = Vec::new();
        for name_result in &intermediate_results_first {
            for city_result in &intermediate_results_second {
                if name_result.get("person") == city_result.get("person") {
                    let mut combined_result = name_result.clone();
                    for (key, value) in city_result {
                        combined_result.insert(key.clone(), value.clone());
                    }
                    combined_results.push(combined_result);
                }
            }
        }

        for result in combined_results {
            let mut final_result = HashMap::new();
            for var in &variables {
                if let Some(value) = result.get(var) {
                    final_result.insert(var.clone(), value.clone());
                }
            }
            results.push(final_result);
        }
    }

    results
}

fn main() {
    let rdf_data = r#"
    <?xml version="1.0"?>
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              xmlns:ex="http://example.org/">
        <rdf:Description rdf:about="http://example.org/person1">
          <ex:name>John Doe</ex:name>
          <ex:age>30</ex:age>
          <ex:worksAt rdf:resource="http://example.org/location1"/>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org/location1">
          <ex:hasName>Headquarters</ex:hasName>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org/person2">
          <ex:name>Jane Smith</ex:name>
          <ex:age>25</ex:age>
          <ex:worksAt rdf:resource="http://example.org/location1"/>
        </rdf:Description>
        <rdf:Description rdf:about="http://example.org/location2">
          <ex:hasName>Branch Office</ex:hasName>
        </rdf:RDF>
    "#;

    let mut db = SparqlDatabase::new();
    db.parse_rdf(rdf_data);

    let filtered_db = db.filter(|triple| {
        let predicate = db.dictionary.decode(triple.predicate).unwrap();
        predicate.contains("name")
    });
    let distinct_db = db.distinct();
    let ordered_db = db.order_by(|triple| db.dictionary.decode(triple.object).unwrap().to_string());

    filtered_db.print("Filtered Triples:", false);
    distinct_db.print("Distinct Triples:", false);
    println!("Ordered Triples:");
    for triple in ordered_db {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db.dictionary.decode(triple.subject).unwrap(),
            db.dictionary.decode(triple.predicate).unwrap(),
            db.dictionary.decode(triple.object).unwrap()
        );
    }

    // Find who works at Headquarters
    let results = db.select_by_value("Headquarters");
    println!("Results for people working at Headquarters:");
    for triple in &results {
        println!(
            "Subject: {}, Predicate: {}, Object: {}",
            db.dictionary.decode(triple.subject).unwrap(),
            db.dictionary.decode(triple.predicate).unwrap(),
            db.dictionary.decode(triple.object).unwrap()
        );
    }

    // Find who works at Headquarters using select_by_variable
    let results = db.select_by_variable(None, None, Some("Headquarters"));
    println!("Results for people working at Headquarters:");
    for triple in &results {
        println!(
            "Subject: {}, Predicate: {}, Object: {}",
            db.dictionary.decode(triple.subject).unwrap(),
            db.dictionary.decode(triple.predicate).unwrap(),
            db.dictionary.decode(triple.object).unwrap()
        );
    }

    // Group by predicate
    db.print("Grouped by Predicate:", true);

    // Union with another database
    let mut other_db = SparqlDatabase::new();
    let other_rdf_data = r#"
    <?xml version="1.0"?>
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              xmlns:ex="http://example.org/">
        <rdf:Description rdf:about="http://example.org/person3">
          <ex:name>Jim Beam</ex:name>
          <ex:age>40</ex:age>
          <ex:worksAt rdf:resource="http://example.org/location2"/>
        </rdf:Description>
    </rdf:RDF>
    "#;
    other_db.parse_rdf(other_rdf_data);

    let mut union_db = db.union(&other_db);
    union_db.print("Union of Databases:", false);

    // Perform a join operation on "worksAt" predicate within the same dataset
    let joined_db_same = db.join(&db.clone(), "ex:worksAt");
    joined_db_same.print("Joined Triples (Same Dataset):", false);

    // Merge the databases first and then perform the join operation
    // let mut merged_db = db.union(&other_db);
    let joined_db_merged = union_db.join(&union_db.clone(), "ex:worksAt");
    joined_db_merged.print("Joined Triples (Merged Datasets):", false);

    let joined_db = other_db.join_by_variable(&other_db, None, Some("worksAt"), None);
    joined_db.print("Joined Triples by Variable:", false);

    // Add stream data with timestamps
    let person1_subject = db.dictionary.encode("http://example.org/person1");
    let is_nearby_predicate = db.dictionary.encode("ex:isNearby");
    let shop1_object = db.dictionary.encode("http://example.org/shop1");

    db.add_stream_data(
        Triple {
            subject: person1_subject,
            predicate: is_nearby_predicate,
            object: shop1_object,
        },
        current_timestamp(),
    );

    let person2_subject = db.dictionary.encode("http://example.org/person2");
    let shop2_object = db.dictionary.encode("http://example.org/shop2");

    db.add_stream_data(
        Triple {
            subject: person2_subject,
            predicate: is_nearby_predicate,
            object: shop2_object,
        },
        current_timestamp() + 5,
    );

    // Apply time-based window and print results
    let windowed_triples = db.time_based_window(current_timestamp(), current_timestamp() + 10);
    println!("Time-based Window Triples:");
    for triple in windowed_triples {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db.dictionary.decode(triple.subject).unwrap(),
            db.dictionary.decode(triple.predicate).unwrap(),
            db.dictionary.decode(triple.object).unwrap()
        );
    }

    println!("==================================================================================================");
    println!("IStream, RStream and DStream check");
    let mut db3 = SparqlDatabase::new();

    let triple1 = Triple {
        subject: db3.dictionary.encode("subject1"),
        predicate: db3.dictionary.encode("predicate1"),
        object: db3.dictionary.encode("object1"),
    };
    let triple2 = Triple {
        subject: db3.dictionary.encode("subject2"),
        predicate: db3.dictionary.encode("predicate2"),
        object: db3.dictionary.encode("object2"),
    };

    // Simulate adding triples with timestamps
    db3.add_stream_data(triple1.clone(), current_timestamp());
    db3.add_stream_data(triple2.clone(), current_timestamp() + 10);

    // Assuming we want to check streams between these timestamps
    let last_timestamp = current_timestamp() - 20;
    let current_timestamp_t = current_timestamp();

    // IStream
    let istream_triples = db3.istream(last_timestamp);
    println!("IStream triples:");
    for triple in istream_triples {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db3.dictionary.decode(triple.subject).unwrap(),
            db3.dictionary.decode(triple.predicate).unwrap(),
            db3.dictionary.decode(triple.object).unwrap()
        );
    }

    // DStream
    let dstream_triples = db3.dstream(last_timestamp, current_timestamp_t);
    println!("DStream triples:");
    for triple in dstream_triples {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db3.dictionary.decode(triple.subject).unwrap(),
            db3.dictionary.decode(triple.predicate).unwrap(),
            db3.dictionary.decode(triple.object).unwrap()
        );
    }

    // RStream
    let rstream_triples = db3.rstream(last_timestamp, current_timestamp_t);
    println!("RStream triples:");
    for triple in rstream_triples {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db3.dictionary.decode(triple.subject).unwrap(),
            db3.dictionary.decode(triple.predicate).unwrap(),
            db3.dictionary.decode(triple.object).unwrap()
        );
    }

    println!("==================================================================================================");
    println!("Sliding window check");
    let mut db4 = SparqlDatabase::new();

    // Set up a sliding window with a width of 10 seconds and a slide interval of 5 seconds
    db4.set_sliding_window(10, 5);

    let triple11 = Triple {
        subject: db4.dictionary.encode("subject1"),
        predicate: db4.dictionary.encode("predicate1"),
        object: db4.dictionary.encode("object1"),
    };
    let triple22 = Triple {
        subject: db4.dictionary.encode("subject2"),
        predicate: db4.dictionary.encode("predicate2"),
        object: db4.dictionary.encode("object2"),
    };

    // Simulate adding triples with timestamps
    db4.add_stream_data(triple11.clone(), current_timestamp());
    db4.add_stream_data(triple22.clone(), current_timestamp() + 10);

    // Wait for some time to simulate the stream processing
    std::thread::sleep(std::time::Duration::from_secs(5));

    // Evaluate the sliding window
    let window_triples1 = db4.evaluate_sliding_window();
    println!("Sliding Window triples:");
    for triple in window_triples1 {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db4.dictionary.decode(triple.subject).unwrap(),
            db4.dictionary.decode(triple.predicate).unwrap(),
            db4.dictionary.decode(triple.object).unwrap()
        );
    }

    println!("==================================================================================================");
    println!("Policy check");
    let mut db45 = SparqlDatabase::new();

    // Set up a sliding window with a width of 10 seconds and a slide interval of 5 seconds
    db45.set_sliding_window(10, 5);

    let triple115 = Triple {
        subject: db45.dictionary.encode("subject1"),
        predicate: db45.dictionary.encode("predicate1"),
        object: db45.dictionary.encode("object1"),
    };
    let triple225 = Triple {
        subject: db45.dictionary.encode("subject2"),
        predicate: db45.dictionary.encode("predicate2"),
        object: db45.dictionary.encode("object2"),
    };

    // Simulate adding triples with timestamps
    db45.add_stream_data(triple115.clone(), current_timestamp());
    db45.add_stream_data(triple225.clone(), current_timestamp() + 10);

    // Wait for some time to simulate the stream processing
    std::thread::sleep(std::time::Duration::from_secs(5));

    // Evaluate the sliding window
    let window_triples15 = db45.evaluate_sliding_window();
    println!("Sliding Window triples:");
    for triple in window_triples15 {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db45.dictionary.decode(triple.subject).unwrap(),
            db45.dictionary.decode(triple.predicate).unwrap(),
            db45.dictionary.decode(triple.object).unwrap()
        );
    }

    // Apply different policies
    let window_close_triples = db45.window_close_policy();
    println!("Window Close Policy triples:");
    for triple in window_close_triples {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db45.dictionary.decode(triple.subject).unwrap(),
            db45.dictionary.decode(triple.predicate).unwrap(),
            db45.dictionary.decode(triple.object).unwrap()
        );
    }

    let content_change_triples = db45.content_change_policy();
    println!("Content Change Policy triples:");
    for triple in content_change_triples {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db45.dictionary.decode(triple.subject).unwrap(),
            db45.dictionary.decode(triple.predicate).unwrap(),
            db45.dictionary.decode(triple.object).unwrap()
        );
    }

    let non_empty_content_triples = db45.non_empty_content_policy();
    println!("Non-empty Content Policy triples:");
    for triple in non_empty_content_triples {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db45.dictionary.decode(triple.subject).unwrap(),
            db45.dictionary.decode(triple.predicate).unwrap(),
            db45.dictionary.decode(triple.object).unwrap()
        );
    }

    let periodic_triples = db45.periodic_policy(std::time::Duration::new(5, 0));
    println!("Periodic Policy triples:");
    for triple in periodic_triples {
        println!(
            "  Subject: {}, Predicate: {}, Object: {}",
            db45.dictionary.decode(triple.subject).unwrap(),
            db45.dictionary.decode(triple.predicate).unwrap(),
            db45.dictionary.decode(triple.object).unwrap()
        );
    }

    println!("==================================================================================================");
    // Parsing
    let rdf_data = r#"
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:ex="http://example.org/stuff/1.0/">
              <rdf:Description rdf:about="http://example.org/person01">
                <ex:name>John Doe</ex:name>
                <ex:age>30</ex:age>
                <ex:city>New York</ex:city>
              </rdf:Description>
              <rdf:Description rdf:about="http://example.org/person02">
                <ex:name>Jane Smith</ex:name>
                <ex:age>25</ex:age>
                <ex:city>Los Angeles</ex:city>
              </rdf:Description>
              <rdf:Description rdf:about="http://example.org/person03">
                <ex:name>Emily Johnson</ex:name>
                <ex:age>35</ex:age>
                <ex:city>Chicago</ex:city>
              </rdf:Description>
            </rdf:RDF>
    "#;

    let triples = parse_rdf(rdf_data);

    let query = r#"
    PREFIX ex: <http://example.org/stuff/1.0/>
    
    SELECT ?name
    WHERE {
      ?person ex:name ?name .
    }
    "#;

    let results = execute_sparql_query(&triples, query);

    for result in results {
        for (key, value) in result {
            println!("{}: {}", key, value);
        }
    }

    println!("====================================");

    let query_2 = r#"
    PREFIX ex: <http://example.org/stuff/1.0/>
    
    SELECT ?name ?city
    WHERE {
      ?person ex:name ?name .
      ?person ex:city ?city .
    }
    "#;
    let results = execute_sparql_query(&triples, query_2);
    for result in results {
        for (key, value) in result {
            println!("{}: {}", key, value);
        }
    }
}
