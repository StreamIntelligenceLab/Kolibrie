use kolibrie::execute_query::{execute_query_rayon_parallel2_volcano, execute_sparql_update};
use kolibrie::sparql_database::SparqlDatabase;

fn query(database: &mut SparqlDatabase, sparql: &str) -> Vec<Vec<String>> {
    execute_query_rayon_parallel2_volcano(sparql, database)
}

#[test]
fn nested_graph_and_union_preserve_solution_multiplicity() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            GRAPH ex:g1 { ex:s1 ex:p "one" . }
            GRAPH ex:g2 { ex:s2 ex:q "two" . }
        }
        "#,
        &mut database,
    )
    .unwrap();

    let mut rows = query(
        &mut database,
        r#"
        PREFIX ex: <http://example.org/>
        SELECT ?g ?s WHERE {
            { GRAPH ?g { ?s ex:p "one" } }
            UNION
            { GRAPH ?g { { ?s ex:q "two" } } }
        }
        "#,
    );
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![
                "http://example.org/g1".to_string(),
                "http://example.org/s1".to_string(),
            ],
            vec![
                "http://example.org/g2".to_string(),
                "http://example.org/s2".to_string(),
            ],
        ]
    );

    let rows = query(
        &mut database,
        r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE {
            { GRAPH ex:g1 { ?s ex:p "one" } }
            UNION
            { GRAPH ex:g1 { ?s ex:p "one" } }
        }
        "#,
    );
    assert_eq!(rows.len(), 2, "UNION is a multiset union");

    let rows = query(
        &mut database,
        r#"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?s WHERE {
            { GRAPH ex:g1 { ?s ex:p "one" } }
            UNION
            { GRAPH ex:g1 { ?s ex:p "one" } }
        }
        "#,
    );
    assert_eq!(rows, vec![vec!["http://example.org/s1".to_string()]]);
}

#[test]
fn insert_and_delete_data_work_in_default_and_named_graphs() {
    let mut database = SparqlDatabase::new();
    let inserted = execute_sparql_update(
        r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            ex:default ex:p "default" .
            GRAPH ex:g { ex:named ex:p "named" . }
        }
        "#,
        &mut database,
    )
    .unwrap();
    assert_eq!(inserted.inserted_quads, 2);

    assert_eq!(
        query(
            &mut database,
            "PREFIX ex: <http://example.org/> SELECT ?s WHERE { ?s ex:p \"default\" }",
        ),
        vec![vec!["http://example.org/default".to_string()]]
    );
    assert_eq!(
        query(
            &mut database,
            "PREFIX ex: <http://example.org/> SELECT ?s WHERE { GRAPH ex:g { ?s ex:p \"named\" } }",
        ),
        vec![vec!["http://example.org/named".to_string()]]
    );

    let deleted = execute_sparql_update(
        r#"
        PREFIX ex: <http://example.org/>
        DELETE DATA { GRAPH ex:g { ex:named ex:p "named" } }
        "#,
        &mut database,
    )
    .unwrap();
    assert_eq!(deleted.deleted_quads, 1);

    // Deleting the last quad leaves the named graph identity available to an
    // empty GRAPH pattern.
    assert_eq!(
        query(&mut database, "SELECT ?g WHERE { GRAPH ?g {} }",),
        vec![vec!["http://example.org/g".to_string()]]
    );
}

#[test]
fn insert_delete_and_combined_modify_use_where_bindings() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            ex:a ex:old "one" .
            ex:b ex:old "two" .
        }
        "#,
        &mut database,
    )
    .unwrap();

    execute_sparql_update(
        r#"
        PREFIX ex: <http://example.org/>
        INSERT { GRAPH ex:copy { ?s ex:value ?o } }
        WHERE { ?s ex:old ?o }
        "#,
        &mut database,
    )
    .unwrap();

    execute_sparql_update(
        r#"
        PREFIX ex: <http://example.org/>
        DELETE { ?s ex:old ?o }
        INSERT { ?s ex:new ?o }
        WHERE { ?s ex:old ?o }
        "#,
        &mut database,
    )
    .unwrap();

    assert!(query(
        &mut database,
        "PREFIX ex: <http://example.org/> SELECT ?s WHERE { ?s ex:old ?o }"
    )
    .is_empty());
    assert_eq!(
        query(
            &mut database,
            "PREFIX ex: <http://example.org/> SELECT ?s WHERE { ?s ex:new ?o }",
        )
        .len(),
        2
    );
    assert_eq!(
        query(
            &mut database,
            "PREFIX ex: <http://example.org/> SELECT ?s WHERE { GRAPH ex:copy { ?s ex:value ?o } }",
        )
        .len(),
        2
    );

    execute_sparql_update(
        r#"
        PREFIX ex: <http://example.org/>
        DELETE WHERE { GRAPH ex:copy { ?s ex:value ?o } }
        "#,
        &mut database,
    )
    .unwrap();
    assert!(query(
        &mut database,
        "PREFIX ex: <http://example.org/> SELECT * WHERE { GRAPH ex:copy { ?s ?p ?o } }"
    )
    .is_empty());
}

#[test]
fn delete_template_where_and_invalid_data_are_handled() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update("INSERT DATA { <urn:s> <urn:p> \"value\" }", &mut database).unwrap();

    let summary = execute_sparql_update(
        "DELETE { ?s <urn:p> ?o } WHERE { ?s <urn:p> ?o }",
        &mut database,
    )
    .unwrap();
    assert_eq!(summary.deleted_quads, 1);

    assert!(execute_sparql_update("INSERT DATA { ?s <urn:p> <urn:o> }", &mut database,).is_err());
    assert!(execute_sparql_update("DELETE DATA { _:b <urn:p> <urn:o> }", &mut database,).is_err());
}

#[test]
fn rebuilding_indexes_keeps_named_quads_and_empty_graphs() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            GRAPH <urn:non-empty> { <urn:s> <urn:p> <urn:o> }
            GRAPH <urn:becomes-empty> { <urn:x> <urn:p> <urn:o> }
        }
        "#,
        &mut database,
    )
    .unwrap();
    execute_sparql_update(
        "DELETE DATA { GRAPH <urn:becomes-empty> { <urn:x> <urn:p> <urn:o> } }",
        &mut database,
    )
    .unwrap();

    database.build_all_indexes();

    assert_eq!(
        query(
            &mut database,
            "SELECT ?s WHERE { GRAPH <urn:non-empty> { ?s <urn:p> <urn:o> } }",
        ),
        vec![vec!["urn:s".to_string()]]
    );
    let mut graphs = query(&mut database, "SELECT ?g WHERE { GRAPH ?g {} }");
    graphs.sort();
    assert_eq!(
        graphs,
        vec![
            vec!["urn:becomes-empty".to_string()],
            vec!["urn:non-empty".to_string()],
        ]
    );
}

#[test]
fn graph_variables_flow_from_where_into_delete_and_insert_templates() {
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
        WHERE  { GRAPH ?g { ?s <urn:old> ?o } }
        "#,
        &mut database,
    )
    .unwrap();
    assert_eq!(summary.deleted_quads, 2);
    assert_eq!(summary.inserted_quads, 2);

    assert!(query(
        &mut database,
        "SELECT ?g WHERE { GRAPH ?g { ?s <urn:old> ?o } }"
    )
    .is_empty());
    let mut rows = query(
        &mut database,
        "SELECT ?g ?s WHERE { GRAPH ?g { ?s <urn:new> ?o } }",
    );
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec!["urn:g1".to_string(), "urn:s1".to_string()],
            vec!["urn:g2".to_string(), "urn:s2".to_string()],
        ]
    );
}
