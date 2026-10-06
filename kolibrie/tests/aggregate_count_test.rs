mod common;
use common::query;
use kolibrie::sparql_database::SparqlDatabase;

fn parity(input: &str, expected: Vec<Vec<&str>>) {
    for text in [
        input.to_string(),
        format!("SELECT * WHERE {{ {{ {input} }} }}"),
    ] {
        let mut db = SparqlDatabase::new();
        let expected: Vec<Vec<String>> = expected
            .iter()
            .map(|row| row.iter().map(|v| v.to_string()).collect())
            .collect();
        assert_eq!(query(&mut db, &text), expected, "{text}");
    }
}

#[test]
fn count_aliases_preserve_multiplicity_and_exclude_unbound_values() {
    // Keep wrapping SELECT aliases in alphabetical order
    parity(
        "SELECT (COUNT(?x) AS ?a) (COUNT(*) AS ?b) WHERE { VALUES ?x { 1 1 UNDEF } }",
        vec![vec!["2", "3"]],
    );
    parity(
        r#"SELECT (COUNT(?x) AS ?a) (COUNT(*) AS ?b) WHERE { VALUES ?x { "" UNDEF } }"#,
        vec![vec!["1", "2"]],
    );
}

#[test]
fn empty_ungrouped_and_grouped_counts_are_different() {
    parity(
        "SELECT (COUNT(*) AS ?n) WHERE { VALUES ?x { } }",
        vec![vec!["0"]],
    );
    parity(
        "SELECT ?x (COUNT(*) AS ?n) WHERE { VALUES ?x { } } GROUP BY ?x",
        vec![],
    );
}
