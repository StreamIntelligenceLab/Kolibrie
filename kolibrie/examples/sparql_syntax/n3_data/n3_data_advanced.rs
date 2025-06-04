/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

fn n3_simple_query() {
    let mut db = SparqlDatabase::new();

    let n3_data = r#"
        @prefix ex: <http://example.org/>.
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
        @prefix currency: <http://purl.org/commerce#>.

        ex:john ex:name "John Smith" ;
                ex:hasFriend ex:jane ;
                ex:hasJob ex:softwareEngineer ;
                ex:annualSalary "75000"^^xsd:integer ;
                ex:salaryCurrency currency:USD.

        ex:jane 
        ex:name "Jane Doe" ; # Changed to singular "Jane" for consistency, adjust as needed
        ex:hasJob ex:doctor ;
        ex:annualSalary "95000"^^xsd:integer ;
        ex:salaryCurrency currency:USD.

        ex:softwareEngineer 
        ex:jobTitle "Software Engineer".

        ex:doctor 
        ex:jobTitle "Doctor/Physician".
    "#;

    db.parse_n3(n3_data);

    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?name ?salary
    WHERE {
        ?person ex:name ?name .
        ?person ex:annualSalary ?salary
    }"#;

    let results = execute_query(sparql_query, &mut db);
    println!("Results:");
    for result in results {
        if let [name, salary] = &result[..] {
            println!("{} {}", name, salary);
        }
    }
}

fn main() {
    n3_simple_query();
}

