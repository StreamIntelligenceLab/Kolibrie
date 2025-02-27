use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

fn n3_simple_query() {
    let mut db = SparqlDatabase::new();

    let n3_data = r#"
        @prefix ex: <http://example.org/> .
        ex:john ex:hasFriend ex:jane .
        ex:jane ex:name "Jane Doe" .
        ex:john ex:name "John Smith" .
    "#;

    db.parse_n3(n3_data);

    let sparql_query = r#"
    PREFIX ex: <http://example.org/> 
    SELECT ?name 
    WHERE {
        ?person ex:hasFriend ?friend . 
        ?friend ex:name ?name
    }"#;

    let results = execute_query(sparql_query, &mut db);
    println!("Results:");
    for result in results {
        if let [name] = &result[..] {
            println!("{}", name);
        }
    }
}

fn main() {
    n3_simple_query();
}