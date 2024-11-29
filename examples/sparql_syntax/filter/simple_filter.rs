use kolibrie::parser::*;
use kolibrie::sparql_database::*;

fn select() {
    // Define the RDF/XML string representing the inserted triples
    let rdf_xml = r#"
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://example.org/">
    <rdf:Description rdf:about="http://example.org/person1">
        <ex:hasOccupation>Engineer</ex:hasOccupation>
    </rdf:Description>
    <rdf:Description rdf:about="http://example.org/person2">
        <ex:hasOccupation>Artist</ex:hasOccupation>
    </rdf:Description>
    <rdf:Description rdf:about="http://example.org/person3">
        <ex:hasOccupation>Doctor</ex:hasOccupation>
    </rdf:Description>
</rdf:RDF>
"#;

    // Initialize a sample database (assuming SparqlDatabase and Triple are implemented)
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml);

    // Define an INSERT SPARQL query
    let sparql_query = r#"PREFIX ex: <http://example.org/> SELECT ?person WHERE {?person ex:hasOccupation "Engineer"}"#;

    // Execute the query on the database
    let results = execute_query(sparql_query, &mut database);

    println!("{:?}", results);

    for triple in &database.triples {
        let subject = database.dictionary.decode(triple.subject).unwrap_or_default();
        let predicate = database.dictionary.decode(triple.predicate).unwrap_or_default();
        let object = database.dictionary.decode(triple.object).unwrap_or_default();
        println!("Triple: ({}, {}, {})", subject, predicate, object);
    }
    

    // Output the results (if any)
    for result in results {
        if let [person] = &result[..] {
            println!("{}", person);
        }
    }
}

fn main() {
    select();
}

