use kolibrie::parser::*;
use kolibrie::sparql_database::*;
use shared::triple::*;
use kolibrie::volcano_optimizer::*;
use std::time::Instant;

fn simple_volcano_optimizer() {
    // Step 1: Execute the physical plan on a sample database
    let mut database = SparqlDatabase::new();

    // Step 2: Create the logical plan
    let logical_plan = LogicalOperator::Join {
        left: Box::new(LogicalOperator::Scan {
            pattern: TriplePattern {
                subject: Some("?person".to_string()),
                predicate: Some("rdf:type".to_string()),
                object: Some("foaf:Person".to_string()),
            },
        }),
        right: Box::new(LogicalOperator::Selection {
            predicate: Box::new(LogicalOperator::Scan {
                pattern: TriplePattern {
                    subject: Some("?person".to_string()),
                    predicate: Some("foaf:age".to_string()),
                    object: Some("?age".to_string()),
                },
            }),
            condition: Condition {
                variable: "?age".to_string(),
                operator: ">".to_string(),
                value: "30".to_string(),
            },
        }),
    };

    // Step 3: Initialize the optimizer and find the best plan
    let mut optimizer = VolcanoOptimizer::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    // Add some triples to the database
    let person_triple = Triple {
        subject: database.dictionary.encode("http://example.org/alice"),
        predicate: database.dictionary.encode("rdf:type"),
        object: database.dictionary.encode("foaf:Person"),
    };

    let age_triple = Triple {
        subject: database.dictionary.encode("http://example.org/alice"),
        predicate: database.dictionary.encode("foaf:age"),
        object: database.dictionary.encode("35"),
    };

    database.triples.insert(person_triple);
    database.triples.insert(age_triple);

    let results = best_plan.execute(&mut database);

    // Output the results
    if results.is_empty() {
        println!("No results found.");
    } else {
        for result in results {
            println!("Result is {:?}", result);
        }
    }
}

fn volcano_optmizer_multiple_triples() {
    // Step 1: Initialize the database
    let mut database = SparqlDatabase::new();

    // Step 2: Add multiple triples to the database
    for i in 0..5 {
        let person_uri = format!("http://example.org/person{}", i);
        let person_triple = Triple {
            subject: database.dictionary.encode(&person_uri),
            predicate: database.dictionary.encode("rdf:type"),
            object: database.dictionary.encode("foaf:Person"),
        };

        let age = 20 + (i % 50); // Ages between 20 and 69
        let age_triple = Triple {
            subject: database.dictionary.encode(&person_uri),
            predicate: database.dictionary.encode("foaf:age"),
            object: database.dictionary.encode(&age.to_string()),
        };

        database.triples.insert(person_triple);
        database.triples.insert(age_triple);
    }

    database.build_all_indexes();

    // Step 3: Create the logical plan
    let logical_plan = LogicalOperator::Join {
        left: Box::new(LogicalOperator::Scan {
            pattern: TriplePattern {
                subject: Some("?person".to_string()),
                predicate: Some("rdf:type".to_string()),
                object: Some("foaf:Person".to_string()),
            },
        }),
        right: Box::new(LogicalOperator::Selection {
            predicate: Box::new(LogicalOperator::Scan {
                pattern: TriplePattern {
                    subject: Some("?person".to_string()),
                    predicate: Some("foaf:age".to_string()),
                    object: Some("?age".to_string()),
                },
            }),
            condition: Condition {
                variable: "?age".to_string(),
                operator: ">".to_string(),
                value: "30".to_string(),
            },
        }),
    };

    // Step 4: Initialize the optimizer and find the best plan
    let mut optimizer = VolcanoOptimizer::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    // Step 5: Execute the best plan
    let start = Instant::now();
    let results = best_plan.execute(&mut database);
    let duration = start.elapsed();
    println!("Query execution time: {:?}", duration);

    // Step 6: Output the results
    if results.is_empty() {
        println!("No results found.");
    } else {
        for result in results.iter().take(10) {
            // Print first 10 results
            println!("Result: {:?}", result);
        }
        println!("Total results: {}", results.len());
    }
}

fn volcano_optmizer_rdf() {
    // Read the RDF data
    let rdf_data = r#"
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/">
          <rdf:Description rdf:about="http://example.org/peter">
            <ex:worksAt rdf:resource="http://example.org/kulak"/>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/kulak">
            <ex:located rdf:resource="http://example.org/kortrijk"/>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/charlotte">
            <ex:worksAt rdf:resource="http://example.org/ughent"/>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/ughent">
            <ex:located rdf:resource="http://example.org/ghent"/>
          </rdf:Description>
        </rdf:RDF>
    "#;

    // Create the database and parse the RDF data
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);
    database.build_all_indexes();

    // Define the SPARQL query
    let sparql = r#"
    PREFIX ex: <http://example.org/> 
    SELECT ?person ?location 
    WHERE {
        ?person ex:worksAt ?org . 
        ?org ex:located ?location
    }"#;

    // Parse the SPARQL query
    if let Ok((_, (_, variables, patterns, filters, _, prefixes, _, _, _))) = parse_sparql_query(sparql) {
        // Merge prefixes into the database
        database.prefixes.extend(prefixes.clone());

        // Extract variables for the logical plan
        let variables_for_plan: Vec<(&str, &str)> = variables
            .iter()
            .map(|(agg_type, var, _)| (*agg_type, *var))
            .collect();

        // Build the logical plan
        let logical_plan = build_logical_plan(
            variables_for_plan,
            patterns.clone(),
            filters.clone(),
            &prefixes.clone(),
            &database,
        );

        // Create the volcano optimizer
        let mut optimizer = VolcanoOptimizer::new(&database);

        // Find the best physical plan
        let physical_plan = optimizer.find_best_plan(&logical_plan);

        println!("Logical Plan: {:?}", logical_plan);
        println!("Optimized Physical Plan: {:?}", physical_plan);

        // Execute the physical plan
        let results = physical_plan.execute(&mut database);

        // Extract selected variables
        let selected_vars: Vec<String> = variables
            .iter()
            .filter(|(agg_type, _, _)| *agg_type == "VAR")
            .map(|(_, var, _)| var.to_string())
            .collect();

        // Now print the results
        println!("Results:");
        for result in results {
            for var in &selected_vars {
                if let Some(value) = result.get(var) {
                    println!("{} = {}", var, value);
                }
            }
            println!("---");
        }
    } else {
        eprintln!("Failed to parse the SPARQL query.");
    }
}

fn check_all_indexes_usage() {
    // 1. Create the database
    let mut database = SparqlDatabase::new();

    // 2. Insert some data. For demonstration, let's insert the same
    //    data as in your "volcano_optmizer_multiple_triples" function,
    //    but we specifically want a triple with age="42" for "person42".
    for i in 0..100 {
        let person_uri = format!("http://example.org/person{}", i);
        // e.g. all persons have type foaf:Person
        let person_triple = Triple {
            subject: database.dictionary.encode(&person_uri),
            predicate: database.dictionary.encode("rdf:type"),
            object: database.dictionary.encode("foaf:Person"),
        };
        database.triples.insert(person_triple);

        // let's define everyone's age is exactly i
        // so person42 -> age: "42"
        let age_triple = Triple {
            subject: database.dictionary.encode(&person_uri),
            predicate: database.dictionary.encode("foaf:age"),
            object: database.dictionary.encode(&i.to_string()),
        };
        database.triples.insert(age_triple);
    }

    // 3. Build all indexes
    database.build_all_indexes();

    // 4. Patterns designed to trigger each index path
    let patterns = vec![
        // (1) subject+object known => soIndex
        //    This looks for all predicates connecting person42 -> "42"
        TriplePattern {
            subject: Some("http://example.org/person42".to_string()),
            predicate: Some("?p".to_string()),
            object: Some("42".to_string()),
        },
        // (2) predicate+object => poIndex
        //    This finds all subjects whose predicate is foaf:age and object is "42"
        TriplePattern {
            subject: Some("?s".to_string()),
            predicate: Some("foaf:age".to_string()),
            object: Some("42".to_string()),
        },
        // (3) subject bound => sIndex
        //    All facts about person100 (which exists if i in 0..100)
        TriplePattern {
            subject: Some("http://example.org/person50".to_string()),
            predicate: Some("?p".to_string()),
            object: Some("?o".to_string()),
        },
        // (4) predicate bound => pIndex
        //    All (subject, object) pairs for the known predicate foaf:age
        TriplePattern {
            subject: Some("?s".to_string()),
            predicate: Some("foaf:age".to_string()),
            object: Some("?o".to_string()),
        },
        // (5) object bound => oIndex
        //    All subjects with object = "69" (which doesn't exist in i=0..100 but you get the idea)
        TriplePattern {
            subject: Some("?s".to_string()),
            predicate: Some("?p".to_string()),
            object: Some("69".to_string()),
        },
        // (6) fully bound => direct check
        //    Is (person42, foaf:age, "42") in the DB? Yes, if i=42
        TriplePattern {
            subject: Some("http://example.org/person42".to_string()),
            predicate: Some("foaf:age".to_string()),
            object: Some("42".to_string()),
        },
    ];

    // 5. For each pattern, build + optimize + execute
    for (i, pattern) in patterns.iter().enumerate() {
        let logical_plan = LogicalOperator::Scan {
            pattern: pattern.clone(),
        };
        let mut optimizer = VolcanoOptimizer::new(&database);
        let best_plan = optimizer.find_best_plan(&logical_plan);

        println!("\n=== QUERY {} ===", i + 1);
        println!("Logical Plan: {:?}", logical_plan);
        println!("Optimized Physical Plan: {:?}", best_plan);

        // Execute the best plan
        let results = best_plan.execute(&mut database);
        println!("Got {} result(s).", results.len());
        for r in results.iter().take(5) {
            println!("  -> {:?}", r);
        }
    }
}

fn volcano_optimizer_pos_index() {
    // Step 1: Initialize the database
    let mut database = SparqlDatabase::new();

    // Step 2: Add 100,000 triples to the database
    for i in 0..5 {
        let person_uri = format!("http://example.org/person{}", i);
        
        // Triple for "rdf:type" -> "Person"
        let person_triple = Triple {
            subject: database.dictionary.encode(&person_uri),
            predicate: database.dictionary.encode("rdf:type"),
            object: database.dictionary.encode("http://example.org/Person"),
        };

        // Triple for "loves" relationship
        let loves_object_uri = format!("http://example.org/person{}", (i + 1) % 100_000);
        let loves_triple = Triple {
            subject: database.dictionary.encode(&person_uri),
            predicate: database.dictionary.encode("http://example.org/loves"),
            object: database.dictionary.encode(&loves_object_uri),
        };

        database.triples.insert(person_triple);
        database.triples.insert(loves_triple);
    }

    // decode database.triples.subject using database.dictionary
    for triple in &database.triples {
        println!("Subject: {:?}", database.dictionary.decode(triple.subject));
        println!("Predicate: {:?}", database.dictionary.decode(triple.predicate));
        println!("Object: {:?}", database.dictionary.decode(triple.object));
    }

    // Build POS index in bulk
    database.build_all_indexes();

    // Step 3: Create a logical plan using the POS index
    let logical_plan = LogicalOperator::Scan {
        pattern: TriplePattern {
            subject: Some("?person".to_string()),
            predicate: Some("http://example.org/loves".to_string()),
            object: Some("?loved_person".to_string()),
        },
    };

    // Step 4: Initialize the optimizer and optimize using the POS index
    let mut optimizer = VolcanoOptimizer::new(&database);
    let best_plan = optimizer.find_best_plan(&logical_plan);

    println!("Logical Plan: {:?}", logical_plan);
    println!("Optimized Physical Plan: {:?}", best_plan);

    // Step 5: Execute the optimized plan
    let start = Instant::now();
    let result = best_plan.execute(&mut database);
    let duration = start.elapsed();
    println!("Query execution time: {:?}", duration);
    println!("{:?}", result);
}

fn main() {
    std::env::set_var("RUST_BACKTRACE", "1");
    simple_volcano_optimizer();
    println!("============================================");
    volcano_optmizer_multiple_triples();
    println!("============================================");
    volcano_optmizer_rdf();
    println!("============================================");
    check_all_indexes_usage();
    println!("============================================");
    volcano_optimizer_pos_index();
}