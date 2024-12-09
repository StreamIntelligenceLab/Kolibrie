use kolibrie::parser::*;
use kolibrie::sparql_database::*;
use kolibrie::triple::*;
use kolibrie::volcano_optimizer::*;

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
    for i in 0..1000 {
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
    let results = best_plan.execute(&mut database);

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

    // Define the SPARQL query
    let sparql = r#"PREFIX ex: <http://example.org/> SELECT ?person ?location WHERE {?person ex:worksAt ?org . ?org ex:located ?location}"#;

    // Parse the SPARQL query
    if let Ok((_, (_, variables, patterns, filters, _, prefixes, _, _))) = parse_sparql_query(sparql) {
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
        let results = physical_plan.execute(&database);

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

fn main() {
    std::env::set_var("RUST_BACKTRACE", "1");
    simple_volcano_optimizer();
    println!("============================================");
    volcano_optmizer_multiple_triples();
    println!("============================================");
    volcano_optmizer_rdf();
}
