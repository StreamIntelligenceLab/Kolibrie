use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

fn simple_select() {
    let rdf_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/person/Alice">
                <ex:name>Alice</ex:name>
                <ex:knows rdf:resource="http://example.org/person/Bob"/>
            </rdf:Description>
            <rdf:Description rdf:about="http://example.org/person/Bob">
                <ex:name>Bob</ex:name>
                <ex:knows rdf:resource="http://example.org/person/Charlie"/>
            </rdf:Description>
            <rdf:Description rdf:about="http://example.org/person/Charlie">
                <ex:name>Charlie</ex:name>
            </rdf:Description>
        </rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    let sparql = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?friendName
    WHERE {
        ?person ex:name "Alice" .
        ?person ex:knows ?friend
        {
            SELECT ?friend ?friendName
            WHERE {
                ?friend ex:name ?friendName .
            }
        }
    }"#;

    let results = execute_query(sparql, &mut database);

    // Now the main function is responsible for printing the results
    println!("Results: {:?}", results);
}

fn main() {
  simple_select();
}