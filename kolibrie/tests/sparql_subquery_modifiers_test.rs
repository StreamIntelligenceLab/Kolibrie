use kolibrie::execute_query::{execute_query_rayon_parallel2_volcano, execute_sparql_update};
use kolibrie::sparql_database::SparqlDatabase;

fn query(database: &mut SparqlDatabase, sparql: &str) -> Vec<Vec<String>> {
    execute_query_rayon_parallel2_volcano(sparql, database)
}

#[test]
fn equal_sized_values_union_branches_keep_their_own_rows() {
    let mut database = SparqlDatabase::new();
    let rows = query(
        &mut database,
        r#"
        SELECT ?value WHERE {
            { VALUES ?value { "first" } }
            UNION
            { VALUES ?value { "second" } }
        }
        ORDER BY ?value
        "#,
    );

    assert_eq!(
        rows,
        vec![vec!["first".to_string()], vec!["second".to_string()],]
    );
}

#[test]
fn select_star_subquery_exports_all_inner_bindings() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:s1> <urn:p> "one" .
            <urn:s2> <urn:p> "two" .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let rows = query(
        &mut database,
        r#"
        SELECT ?s ?value WHERE {
            { SELECT * WHERE { ?s <urn:p> ?value } }
        }
        ORDER BY ?s
        "#,
    );

    assert_eq!(
        rows,
        vec![
            vec!["urn:s1".to_string(), "one".to_string()],
            vec!["urn:s2".to_string(), "two".to_string()],
        ]
    );
}

#[test]
fn subquery_applies_distinct_order_and_limit_before_outer_join() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:s1> <urn:p> "one" .
            <urn:s2> <urn:p> "two" .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let rows = query(
        &mut database,
        r#"
        SELECT ?s WHERE {
            {
                SELECT DISTINCT ?s WHERE {
                    { ?s <urn:p> ?value }
                    UNION
                    { ?s <urn:p> ?value }
                }
                ORDER BY DESC(?s)
                LIMIT 1
            }
        }
        "#,
    );

    assert_eq!(rows, vec![vec!["urn:s2".to_string()]]);
}

#[test]
fn subquery_groups_and_materializes_aggregate_aliases() {
    let mut database = SparqlDatabase::new();
    execute_sparql_update(
        r#"
        INSERT DATA {
            <urn:s1> <urn:group> "a" .
            <urn:s1> <urn:value> 1 .
            <urn:s2> <urn:group> "a" .
            <urn:s2> <urn:value> 2 .
            <urn:s3> <urn:group> "b" .
            <urn:s3> <urn:value> 4 .
        }
        "#,
        &mut database,
    )
    .unwrap();

    let rows = query(
        &mut database,
        r#"
        SELECT ?group ?total WHERE {
            {
                SELECT ?group SUM(?value) AS ?total WHERE {
                    ?s <urn:group> ?group .
                    ?s <urn:value> ?value .
                }
                GROUP BY ?group
                ORDER BY DESC(?total)
                LIMIT 1
            }
        }
        "#,
    );

    assert_eq!(rows, vec![vec!["b".to_string(), "4".to_string()]]);
}
