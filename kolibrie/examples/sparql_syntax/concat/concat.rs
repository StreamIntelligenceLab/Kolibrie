use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

fn concat() {
    let rdf_data = r#"
    <?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:foaf="http://xmlns.com/foaf/0.1/">

  <rdf:Description rdf:about="_:a">
    <foaf:givenName>John</foaf:givenName>
    <foaf:surname>Doe</foaf:surname>
  </rdf:Description>

</rdf:RDF>
    "#;
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    let sparql = r#"
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?name
    WHERE {
        ?P foaf:givenName ?G .
        ?P foaf:surname ?S
        BIND(CONCAT(?G, " ", ?S) AS ?name)
    }"#;

    let results = execute_query(sparql, &mut database);

    println!("Results: {:?}", results);
}

fn main() {
    concat();
}
