mod common;
use common::query;
use kolibrie::sparql_database::SparqlDatabase;

fn assert_parity(expression: &str, input: &str, expected: &str) {
    let inner = format!("SELECT ({expression} AS ?result) WHERE {{ {input} }}");
    let outer = format!("SELECT ?result WHERE {{ {{ {inner} }} }}");
    for text in [inner, outer] {
        let mut db = SparqlDatabase::new();
        assert_eq!(
            query(&mut db, &text),
            vec![vec![expected.to_string()]],
            "{text}"
        );
    }
}

#[test]
fn every_aggregate_form_has_top_level_and_subquery_parity() {
    let input = "VALUES ?x { 1 1 3 UNDEF }";
    for (expression, expected) in [
        ("COUNT(?x)", "3"),
        ("COUNT(*)", "4"),
        ("COUNT(DISTINCT ?x)", "2"),
        ("COUNT(DISTINCT *)", "3"),
        ("SUM(?x)", "5"),
        ("SUM(DISTINCT ?x)", "4"),
        ("AVG(DISTINCT ?x)", "2"),
        ("MIN(DISTINCT ?x)", "1"),
        ("MAX(DISTINCT ?x)", "3"),
    ] {
        assert_parity(expression, input, expected);
    }
}

#[test]
fn star_distinct_uses_complete_mappings_not_only_projection() {
    assert_parity(
        "COUNT(DISTINCT *)",
        "VALUES (?x ?hidden) { (1 2) (1 3) (1 2) }",
        "2",
    );
    assert_parity(
        "COUNT(DISTINCT *)",
        r#"VALUES ?x { UNDEF "" UNDEF "" }"#,
        "2",
    );
    assert_parity("COUNT(?x)", r#"VALUES ?x { UNDEF "" }"#, "1");
}

#[test]
fn empty_input_and_invalid_numeric_operands_are_explicit() {
    for expression in [
        "COUNT(?x)",
        "COUNT(*)",
        "COUNT(DISTINCT *)",
        "SUM(?x)",
        "AVG(?x)",
        "SUM(DISTINCT ?x)",
        "AVG(DISTINCT ?x)",
    ] {
        assert_parity(expression, "VALUES ?x { }", "0");
    }
    for expression in ["MIN(?x)", "MAX(?x)", "MIN(DISTINCT ?x)", "MAX(DISTINCT ?x)"] {
        assert_parity(expression, "VALUES ?x { }", "");
    }
    for expression in ["SUM(?x)", "AVG(?x)", "SUM(DISTINCT ?x)", "AVG(DISTINCT ?x)"] {
        assert_parity(expression, r#"VALUES ?x { 1 "invalid" 2 }"#, "");
    }
    let mut db = SparqlDatabase::new();
    assert!(query(
        &mut db,
        "SELECT ?x (COUNT(*) AS ?n) WHERE { VALUES ?x { } } GROUP BY ?x"
    )
    .is_empty());
    assert!(kolibrie::execute_query::execute_sparql_query(
        "SELECT (SUM(*) AS ?n) WHERE { }",
        &mut db
    )
    .is_err());
}
