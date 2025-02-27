use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

// Adding udf
fn udf() {
    let rdf_data = r##"
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.com/">
    <rdf:Description rdf:about="http://example.com/resource1">
        <ex:predicate>value1</ex:predicate>
    </rdf:Description>
    <rdf:Description rdf:about="http://example.com/resource2">
        <ex:predicate>value2</ex:predicate>
    </rdf:Description>
</rdf:RDF>
    "##;
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    database.register_udf("concatValues", |args: Vec<&str>| {
		args.join("_") // Concatenates arguments with an underscore
	});

    let sparql = r#"
    PREFIX ex: <http://example.com/>
    SELECT ?subject ?result
    WHERE {
      ?subject ex:predicate ?object
      BIND(concatValues(?object, "suffix") AS ?result)
    }"#;

    let results = execute_query(sparql, &mut database);

    // Now the main function is responsible for printing the results
    println!("Results:{:?}", results);
}

fn main() {
	udf();
}