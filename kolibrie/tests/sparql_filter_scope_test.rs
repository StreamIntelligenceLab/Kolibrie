use kolibrie::execute_query::{execute_query_rayon_parallel2_volcano, execute_sparql_update};
use kolibrie::sparql_database::SparqlDatabase;

fn query(database: &mut SparqlDatabase, sparql: &str) -> Vec<Vec<String>> {
    execute_query_rayon_parallel2_volcano(sparql, database)
}

fn database_with_scoped_data() -> SparqlDatabase {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:default-keep> <urn:p> "keep" .
            <urn:default-drop> <urn:p> "drop" .
            GRAPH <urn:g1> { <urn:named-keep> <urn:p> "keep" }
            GRAPH <urn:g2> { <urn:named-drop> <urn:p> "drop" }
        }
        "#,
        &mut database,
    )
    .unwrap();
    database
}

#[test]
fn filter_before_triple_sees_the_whole_group() {
    let mut database = database_with_scoped_data();

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?s WHERE {
                FILTER (?value = "keep")
                ?s <urn:p> ?value .
            }
            "#,
        ),
        vec![vec!["urn:default-keep".to_string()]]
    );
}

#[test]
fn filter_before_bind_sees_the_binding_created_later_in_the_group() {
    let mut database = database_with_scoped_data();

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?s ?label WHERE {
                FILTER (?label = "keep-ok")
                ?s <urn:p> ?value .
                BIND(CONCAT(?value, "-ok") AS ?label)
            }
            "#,
        ),
        vec![vec!["urn:default-keep".to_string(), "keep-ok".to_string(),]]
    );
}

#[test]
fn graph_local_filter_before_triple_stays_in_the_nested_graph_scope() {
    let mut database = database_with_scoped_data();

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?g ?s WHERE {
                GRAPH ?g {
                    FILTER (?value = "keep")
                    ?s <urn:p> ?value .
                }
            }
            "#,
        ),
        vec![vec!["urn:g1".to_string(), "urn:named-keep".to_string(),]]
    );
}

#[test]
fn outer_filter_before_graph_sees_bindings_from_the_graph_child() {
    let mut database = database_with_scoped_data();

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?g ?s WHERE {
                FILTER (?value = "keep")
                GRAPH ?g {
                    ?s <urn:p> ?value .
                }
            }
            "#,
        ),
        vec![vec!["urn:g1".to_string(), "urn:named-keep".to_string(),]]
    );
}

#[test]
fn arithmetic_filter_before_triple_uses_the_complete_group_scope() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:two> <urn:number> 2 .
            <urn:one> <urn:number> 1 .
        }
        "#,
        &mut database,
    )
    .unwrap();

    assert_eq!(
        query(
            &mut database,
            r#"
            SELECT ?s WHERE {
                FILTER (?number # arithmetic comments are whitespace
                    + 1 > 2)
                ?s <urn:number> ?number .
            }
            "#,
        ),
        vec![vec!["urn:two".to_string()]]
    );
}
