use kolibrie::execute_query::{execute_query_rayon_parallel2_volcano, execute_sparql_update};
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::streamertail_optimizer::DatabaseStats;
use shared::dataset_index::GraphId;

fn query(database: &mut SparqlDatabase, sparql: &str) -> Vec<Vec<String>> {
    execute_query_rayon_parallel2_volcano(sparql, database)
}

#[test]
fn illegal_bound_update_positions_are_skipped() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:source> <urn:literal> "not-an-iri" .
            <urn:source> <urn:iri> <urn:valid-subject> .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let skipped = execute_sparql_update(
        r#"
        INSERT {
            ?value <urn:as-subject> <urn:o> .
            <urn:s> ?value <urn:o> .
            GRAPH ?value { <urn:s> <urn:p> <urn:o> }
        }
        WHERE { <urn:source> <urn:literal> ?value }
        "#,
        &mut database,
    )
    .unwrap();
    assert_eq!(skipped.inserted_quads, 0);
    assert!(query(
        &mut database,
        "SELECT ?s WHERE { ?s <urn:as-subject> <urn:o> }"
    )
    .is_empty());
    assert!(query(&mut database, "SELECT ?g WHERE { GRAPH ?g {} }").is_empty());

    let inserted = execute_sparql_update(
        r#"
        INSERT {
            ?target <urn:as-subject> <urn:o> .
            GRAPH ?target { <urn:s> <urn:p> <urn:o> }
        }
        WHERE { <urn:source> <urn:iri> ?target }
        "#,
        &mut database,
    )
    .unwrap();
    assert_eq!(inserted.inserted_quads, 2);
    assert_eq!(
        query(
            &mut database,
            "SELECT ?s WHERE { ?s <urn:as-subject> <urn:o> }"
        ),
        vec![vec!["urn:valid-subject".to_string()]]
    );
    assert_eq!(
        query(
            &mut database,
            "SELECT ?g WHERE { GRAPH ?g { <urn:s> <urn:p> <urn:o> } }"
        ),
        vec![vec!["urn:valid-subject".to_string()]]
    );
}

#[test]
fn nquads_round_trip_keeps_default_and_named_quads() {
    let mut source = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <http://example.com/default> <http://example.com/p> <urn:object> .
            _:blank <http://example.com/p> "quoted \"value\"" .
            GRAPH <http://example.com/g> {
                <http://example.com/named> <http://example.com/p> "value" .
            }
        }
        "#,
        &mut source,
    )
    .unwrap();

    let serialized = source.generate_nquads();
    assert!(
        serialized.contains("<http://example.com/default> <http://example.com/p> <urn:object> .")
    );
    assert!(serialized.contains("_:kolibrie-update-"));
    assert!(serialized.contains("\"quoted \\\"value\\\"\""));
    assert!(!serialized.contains("<_:"));
    assert!(serialized.contains(
        "<http://example.com/named> <http://example.com/p> \"value\" <http://example.com/g> ."
    ));

    let mut restored = SparqlDatabase::new();
    restored.parse_nquads_and_add(&serialized);
    assert_eq!(
        query(
            &mut restored,
            "SELECT ?s WHERE { ?s <http://example.com/p> <urn:object> }"
        ),
        vec![vec!["http://example.com/default".to_string()]]
    );
    assert_eq!(
        query(
            &mut restored,
            r#"
            SELECT ?s WHERE {
                GRAPH <http://example.com/g> {
                    ?s <http://example.com/p> "value"
                }
            }
            "#
        ),
        vec![vec!["http://example.com/named".to_string()]]
    );
    let escaped_literal_rows = query(
        &mut restored,
        r#"SELECT ?s WHERE { ?s <http://example.com/p> "quoted \"value\"" }"#,
    );
    assert_eq!(escaped_literal_rows.len(), 1);
    assert!(escaped_literal_rows[0][0].starts_with("_:kolibrie-update-"));
}

#[test]
fn http_sparql_query_encodings_use_the_unified_query_executor() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        "INSERT DATA { <urn:http-subject> <urn:p> <urn:o> }",
        &mut database,
    )
    .unwrap();

    let direct = concat!(
        "POST /sparql HTTP/1.1\r\n",
        "Content-Type: application/sparql-query\r\n",
        "\r\n",
        "SELECT ?s WHERE { ?s <urn:p> <urn:o> }"
    );
    assert_eq!(database.handle_http_request(direct), "urn:http-subject");

    let form = concat!(
        "POST /sparql HTTP/1.1\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "\r\n",
        "query=SELECT%20%3Fs%20WHERE%20%7B%20%3Fs%20%3Curn%3Ap%3E%20",
        "%3Curn%3Ao%3E%20%7D"
    );
    assert_eq!(database.handle_http_request(form), "urn:http-subject");

    let get = concat!(
        "GET /sparql?query=SELECT%20%3Fs%20WHERE%20%7B%20%3Fs%20",
        "%3Curn%3Ap%3E%20%3Curn%3Ao%3E%20%7D HTTP/1.1\r\n\r\n"
    );
    assert_eq!(database.handle_http_request(get), "urn:http-subject");

    let update_on_query_endpoint = concat!(
        "POST /sparql HTTP/1.1\r\n",
        "Content-Type: application/sparql-query\r\n",
        "\r\n",
        "INSERT DATA { <urn:must-not-exist> <urn:p> <urn:o> }"
    );
    assert!(database
        .handle_http_request(update_on_query_endpoint)
        .starts_with("Query Failed:"));
    assert!(query(
        &mut database,
        "SELECT ?s WHERE { <urn:must-not-exist> <urn:p> ?s }"
    )
    .is_empty());

    let form_update = concat!(
        "POST /sparql HTTP/1.1\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "\r\n",
        "update=INSERT+%7B+%3Curn%3Aform%26subject%3E+%3Curn%3Acopied%3E+%3Fo+%7D+",
        "WHERE+%7B+%3Curn%3Ahttp-subject%3E+%3Curn%3Ap%3E+%3Fo+",
        "FILTER%28%3Fo+%3D+%3Curn%3Ao%3E%29+%7D"
    );
    assert!(database
        .handle_http_request(form_update)
        .starts_with("Update Successful"));
    assert_eq!(
        query(&mut database, "SELECT ?s WHERE { ?s <urn:copied> <urn:o> }"),
        vec![vec!["urn:form&subject".to_string()]]
    );
}

#[test]
fn update_blank_node_allocation_skips_persisted_lexical_collisions() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update("INSERT DATA { _:first <urn:seed> <urn:o> }", &mut database).unwrap();
    let allocated = query(
        &mut database,
        "SELECT ?blank WHERE { ?blank <urn:seed> <urn:o> }",
    )[0][0]
        .clone();
    let allocation = allocated
        .strip_prefix("_:kolibrie-update-")
        .and_then(|suffix| suffix.split_once('-'))
        .and_then(|(number, _)| number.parse::<u64>().ok())
        .unwrap();

    // Preload a range of labels that a restarted/process-local allocator
    // could otherwise mistake for new blank nodes.
    for candidate in allocation + 1..=allocation + 512 {
        database.add_triple_parts(
            &format!("_:kolibrie-update-{candidate}-second"),
            "urn:collision",
            "urn:o",
        );
    }

    execute_sparql_update(
        "INSERT DATA { _:second <urn:fresh> <urn:o> }",
        &mut database,
    )
    .unwrap();
    let fresh = query(
        &mut database,
        "SELECT ?blank WHERE { ?blank <urn:fresh> <urn:o> }",
    );
    assert_eq!(fresh.len(), 1);
    let fresh_allocation = fresh[0][0]
        .strip_prefix("_:kolibrie-update-")
        .and_then(|suffix| suffix.split_once('-'))
        .and_then(|(number, _)| number.parse::<u64>().ok())
        .unwrap();
    assert!(fresh_allocation > allocation + 512);
}

#[test]
fn statistics_include_terms_that_exist_only_in_named_graphs() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        "INSERT DATA { GRAPH <urn:g> { <urn:s> <urn:p> <urn:o> } }",
        &mut database,
    )
    .unwrap();

    let predicate = database.dictionary.write().unwrap().encode("urn:p");
    let graph = database.dictionary.write().unwrap().encode("urn:g");
    let stats = DatabaseStats::gather_stats_fast(&database);

    assert_eq!(stats.total_triples, 1);
    assert_eq!(stats.get_predicate_cardinality(predicate), 1);
    assert_eq!(stats.get_graph_cardinality(GraphId::Default), 0);
    assert_eq!(stats.get_graph_cardinality(GraphId::Named(graph)), 1);
    assert_eq!(stats.named_graph_count, 1);
}
