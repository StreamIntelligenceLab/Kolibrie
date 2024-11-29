extern crate kolibrie;
use kolibrie::sparql_database::*;

fn main() {
    let mut db = SparqlDatabase::new();

    // Employee dataset in Turtle format
    let turtle_data = r#"
        <http://example.org/employee1> <http://example.org/name> "Alice" .
        <http://example.org/employee1> <http://example.org/jobTitle> "Engineer" .
        <http://example.org/employee1> <http://example.org/salary> "6000" .
        
        <http://example.org/employee2> <http://example.org/name> "Bob" .
        <http://example.org/employee2> <http://example.org/jobTitle> "Designer" .
        <http://example.org/employee2> <http://example.org/salary> "4500" .
        
        <http://example.org/employee3> <http://example.org/name> "Charlie" .
        <http://example.org/employee3> <http://example.org/jobTitle> "Manager" .
        <http://example.org/employee3> <http://example.org/salary> "7000" .
    "#;
    db.parse_turtle(turtle_data);

    // Filter employees with salary greater than 5000
    let filtered_db = db.filter(|triple| {
        let predicate = db.dictionary.decode(triple.predicate).unwrap();
        if predicate == "<http://example.org/salary>" {
            let object = db.dictionary.decode(triple.object).unwrap();
            return object.parse::<i32>().unwrap_or(0) > 5000;
        }
        false
    });

    // Print the filtered triples
    filtered_db.print("Filtered Triples:", false);
}
