use kolibrie::execute_query::{execute_query_rayon_parallel2_volcano, execute_sparql_update};
use kolibrie::sparql_database::SparqlDatabase;

fn query(database: &mut SparqlDatabase, sparql: &str) -> Vec<Vec<String>> {
    execute_query_rayon_parallel2_volcano(sparql, database)
}

#[test]
fn graph_composes_with_filter_bind_and_values() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            GRAPH <urn:g1> { <urn:s1> <urn:p> "keep" }
            GRAPH <urn:g2> { <urn:s2> <urn:p> "drop" }
        }
        "#,
        &mut database,
    )
    .unwrap();

    let rows = query(
        &mut database,
        r#"
        SELECT ?g ?s ?label WHERE {
            VALUES ?wanted { "keep" }
            GRAPH ?g {
                ?s <urn:p> ?value .
                FILTER (?value = ?wanted)
                BIND(CONCAT(?value, "-ok") AS ?label)
            }
        }
        "#,
    );

    assert_eq!(
        rows,
        vec![vec![
            "urn:g1".to_string(),
            "urn:s1".to_string(),
            "keep-ok".to_string(),
        ]]
    );
}

#[test]
fn dollar_variables_and_arithmetic_filter_comments_execute_end_to_end() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:s1> <urn:value> 1 .
            <urn:s2> <urn:value> 2 .
            <urn:s3> <urn:value> 3 .
        }
        "#,
        &mut database,
    )
    .unwrap();

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT $s WHERE {
                $s <urn:value> $value .
                FILTER (
                    ($value # a comment is SPARQL whitespace
                     + 1) >= (2 * 2)
                )
            }
            ORDER BY $s
            "#,
        ),
        vec![vec!["urn:s3".to_string()]]
    );
}

#[test]
fn every_supported_sparql_literal_quote_form_lowers_identically() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:double-short> <urn:value> "same" .
            <urn:single-short> <urn:value> 'same' .
            <urn:double-long> <urn:value> """same""" .
            <urn:single-long> <urn:value> '''same''' .
        }
        "#,
        &mut database,
    )
    .unwrap();

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?s WHERE { ?s <urn:value> "same" }
            ORDER BY ?s
            "#,
        ),
        vec![
            vec!["urn:double-long".to_string()],
            vec!["urn:double-short".to_string()],
            vec!["urn:single-long".to_string()],
            vec!["urn:single-short".to_string()],
        ]
    );
}

#[test]
fn iri_and_prefixed_name_escapes_normalize_during_the_single_lowering_step() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        PREFIX ex: <http://example.com/>
        PREFIX urn: <http://must-not-expand-literals.example/>
        INSERT DATA {
            GRAPH <urn:g> { <urn:s1> <urn:value> "urn:literal" }
            GRAPH ex:graph\.one { <urn:s2> <urn:value> "escaped graph" }
        }
        "#,
        &mut database,
    )
    .unwrap();

    assert_eq!(
        query(
            &mut database,
            r#"
            PREFIX urn: <http://must-not-expand-literals.example/>
            SELECT ?s WHERE {
                GRAPH <urn:\u0067> { ?s <urn:value> "urn:literal" }
            }
            "#,
        ),
        vec![vec!["urn:s1".to_string()]]
    );
    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?g WHERE {
                GRAPH ?g { ?s <urn:value> "urn:literal" }
                FILTER (?g = <urn:\u0067>)
            }
            "#,
        ),
        vec![vec!["urn:g".to_string()]]
    );
    assert_eq!(
        query(
            &mut database,
            r#"
            PREFIX ex: <http://example.com/>
            SELECT ?s WHERE {
                GRAPH ex:graph\.one { ?s <urn:value> "escaped graph" }
            }
            "#,
        ),
        vec![vec!["urn:s2".to_string()]]
    );
}

#[test]
fn graph_union_subquery_preserves_unbound_columns() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            GRAPH <urn:g1> { <urn:s1> <urn:p> "one" }
            GRAPH <urn:g2> { <urn:s2> <urn:q> "present" }
        }
        "#,
        &mut database,
    )
    .unwrap();

    let mut rows = query(
        &mut database,
        r#"
        SELECT ?s ?o WHERE {
            { GRAPH <urn:g1> {
                { SELECT ?s ?o WHERE { ?s <urn:p> ?o } }
            } }
            UNION
            { GRAPH <urn:g2> { ?s <urn:q> "present" } }
        }
        "#,
    );
    rows.sort();

    assert_eq!(
        rows,
        vec![
            vec!["urn:s1".to_string(), "one".to_string()],
            vec!["urn:s2".to_string(), String::new()],
        ]
    );
}

#[test]
fn from_and_from_named_replace_the_query_dataset() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:default> <urn:p> "default" .
            GRAPH <urn:g1> {
                <urn:shared> <urn:p> "same" .
                <urn:one> <urn:p> "one" .
            }
            GRAPH <urn:g2> {
                <urn:shared> <urn:p> "same" .
                <urn:two> <urn:p> "two" .
            }
        }
        "#,
        &mut database,
    )
    .unwrap();

    let mut merged = query(
        &mut database,
        r#"
        SELECT ?s
        FROM <urn:g1>
        FROM <urn:g2>
        WHERE { ?s <urn:p> ?o }
        "#,
    );
    merged.sort();
    assert_eq!(
        merged,
        vec![
            vec!["urn:one".to_string()],
            vec!["urn:shared".to_string()],
            vec!["urn:two".to_string()],
        ]
    );

    assert!(query(
        &mut database,
        r#"
        SELECT ?s
        FROM NAMED <urn:g1>
        WHERE { ?s <urn:p> ?o }
        "#,
    )
    .is_empty());

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?s
            FROM NAMED <urn:g1>
            WHERE { GRAPH <urn:g1> { ?s <urn:p> "one" } }
            "#,
        ),
        vec![vec!["urn:one".to_string()]]
    );
}

#[test]
fn graph_variable_is_compatible_with_triple_variables() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            GRAPH <urn:g1> { <urn:g1> <urn:p> <urn:o> }
            GRAPH <urn:g2> { <urn:different> <urn:p> <urn:o> }
        }
        "#,
        &mut database,
    )
    .unwrap();

    assert_eq!(
        query(
            &mut database,
            "SELECT ?g WHERE { GRAPH ?g { ?g <urn:p> <urn:o> } }",
        ),
        vec![vec!["urn:g1".to_string()]]
    );
}

#[test]
fn modify_where_uses_unified_graph_union_and_filter_plan() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            GRAPH <urn:g1> { <urn:s1> <urn:old> "one" }
            GRAPH <urn:g2> { <urn:s2> <urn:old> "two" }
        }
        "#,
        &mut database,
    )
    .unwrap();

    let summary = execute_sparql_update(
        r#"
        DELETE { GRAPH ?g { ?s <urn:old> ?o } }
        INSERT { GRAPH ?g { ?s <urn:new> ?o } }
        WHERE {
            { GRAPH ?g {
                ?s <urn:old> ?o .
                FILTER (?o = "one")
            } }
            UNION
            { GRAPH ?g {
                ?s <urn:old> ?o .
                FILTER (?o = "two")
            } }
        }
        "#,
        &mut database,
    )
    .unwrap();

    assert_eq!(summary.deleted_quads, 2);
    assert_eq!(summary.inserted_quads, 2);
    assert!(query(
        &mut database,
        "SELECT ?g WHERE { GRAPH ?g { ?s <urn:old> ?o } }",
    )
    .is_empty());
    assert_eq!(
        query(
            &mut database,
            "SELECT ?g ?s WHERE { GRAPH ?g { ?s <urn:new> ?o } }",
        )
        .len(),
        2
    );
}

#[test]
fn insert_template_blank_nodes_are_fresh_per_solution() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:s1> <urn:source> "one" .
            <urn:s2> <urn:source> "two" .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let summary = execute_sparql_update(
        r#"
        INSERT {
            GRAPH <urn:generated> {
                _:result <urn:owner> ?s .
                _:result <urn:value> ?value .
            }
        }
        WHERE { ?s <urn:source> ?value }
        "#,
        &mut database,
    )
    .unwrap();
    assert_eq!(summary.inserted_quads, 4);

    let mut rows = query(
        &mut database,
        r#"
        SELECT ?result ?owner ?value WHERE {
            GRAPH <urn:generated> {
                ?result <urn:owner> ?owner .
                ?result <urn:value> ?value .
            }
        }
        ORDER BY ?owner
        "#,
    );
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0][0], rows[1][0]);
    rows.iter_mut().for_each(|row| {
        assert!(row[0].starts_with("_:kolibrie-update-"));
    });
    assert_eq!(rows[0][1..], ["urn:s1", "one"]);
    assert_eq!(rows[1][1..], ["urn:s2", "two"]);
}

#[test]
fn unbound_template_terms_are_skipped_and_invalid_updates_do_not_mutate() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:bound> <urn:p> "value" .
            <urn:unbound> <urn:q> "marker" .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let summary = execute_sparql_update(
        r#"
        INSERT { ?s <urn:copy> ?value }
        WHERE {
            { ?s <urn:p> ?value }
            UNION
            { ?s <urn:q> "marker" }
        }
        "#,
        &mut database,
    )
    .unwrap();
    assert_eq!(summary.inserted_quads, 1);
    assert_eq!(
        query(
            &mut database,
            "SELECT ?s ?value WHERE { ?s <urn:copy> ?value }",
        ),
        vec![vec!["urn:bound".to_string(), "value".to_string()]]
    );

    assert!(execute_sparql_update(
        r#"
        DELETE { _:illegal <urn:p> ?value }
        INSERT { <urn:must-not-exist> <urn:p> ?value }
        WHERE { <urn:bound> <urn:p> ?value }
        "#,
        &mut database,
    )
    .is_err());
    assert!(query(
        &mut database,
        "SELECT ?value WHERE { <urn:must-not-exist> <urn:p> ?value }",
    )
    .is_empty());
}

#[test]
fn empty_insert_data_graph_block_does_not_create_a_graph() {
    let mut database = SparqlDatabase::new();
    let summary = execute_sparql_update(
        "INSERT DATA { GRAPH <urn:empty-from-data> {} }",
        &mut database,
    )
    .unwrap();
    assert_eq!(summary.inserted_quads, 0);
    assert!(query(&mut database, "SELECT ?g WHERE { GRAPH ?g {} }").is_empty());
}

#[test]
fn graph_union_composes_with_aggregation_order_and_limit() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            GRAPH <urn:g1> { <urn:s1> <urn:value> 1 }
            GRAPH <urn:g2> { <urn:s2> <urn:value> 2 }
        }
        "#,
        &mut database,
    )
    .unwrap();

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT SUM(?value) AS ?total WHERE {
                { GRAPH <urn:g1> { ?s <urn:value> ?value } }
                UNION
                { GRAPH <urn:g2> { ?s <urn:value> ?value } }
            }
            "#,
        ),
        vec![vec!["3".to_string()]]
    );

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?s WHERE {
                { GRAPH <urn:g1> { ?s <urn:value> ?value } }
                UNION
                { GRAPH <urn:g2> { ?s <urn:value> ?value } }
            }
            ORDER BY DESC(?value)
            LIMIT 1
            "#,
        ),
        vec![vec!["urn:s2".to_string()]]
    );
}

#[test]
fn legacy_and_http_adapters_route_through_the_same_update_executor() {
    let mut database = SparqlDatabase::new();

    // The error-preserving API is standards-only.
    assert!(
        execute_sparql_update("INSERT { <urn:legacy> <urn:p> <urn:o> }", &mut database,).is_err()
    );

    // The historical query adapter opts into the standalone DATA alias.
    assert!(query(&mut database, "INSERT { <urn:legacy> <urn:p> <urn:o> }",).is_empty());
    assert_eq!(
        query(&mut database, "SELECT ?s WHERE { ?s <urn:p> <urn:o> }",),
        vec![vec!["urn:legacy".to_string()]]
    );

    let direct_request = concat!(
        "POST /sparql HTTP/1.1\r\n",
        "Content-Type: application/sparql-update\r\n",
        "\r\n",
        "INSERT DATA { <urn:http-direct> <urn:p> <urn:o> }"
    );
    assert!(database
        .handle_http_request(direct_request)
        .starts_with("Update Successful"));

    let form_request = concat!(
        "POST /sparql HTTP/1.1\r\n",
        "Content-Type: application/x-www-form-urlencoded\r\n",
        "\r\n",
        "update=INSERT%20DATA%20%7B%20%3Curn%3Ahttp-form%3E%20",
        "%3Curn%3Ap%3E%20%3Curn%3Ao%3E%20%7D"
    );
    assert!(database
        .handle_http_request(form_request)
        .starts_with("Update Successful"));

    let mut rows = query(
        &mut database,
        "SELECT ?s WHERE { ?s <urn:p> <urn:o> } ORDER BY ?s",
    );
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec!["urn:http-direct".to_string()],
            vec!["urn:http-form".to_string()],
            vec!["urn:legacy".to_string()],
        ]
    );
}
