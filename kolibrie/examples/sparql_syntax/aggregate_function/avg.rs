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

fn avg() {
    let rdf_data = r#"
    <?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#" xmlns:socrata="http://www.socrata.com/rdf/terms#" xmlns:dcat="http://www.w3.org/ns/dcat#" xmlns:ods="http://open-data-standards.github.com/2012/01/open-data-standards#" xmlns:dcterm="http://purl.org/dc/terms/" xmlns:geo="http://www.w3.org/2003/01/geo/wgs84_pos#" xmlns:skos="http://www.w3.org/2004/02/skos/core#" xmlns:foaf="http://xmlns.com/foaf/0.1/" xmlns:dsbase="https://data.cityofchicago.org/resource/" xmlns:ds="https://data.cityofchicago.org/resource/xzkq-xp2w/">
<rdf:Description rdf:about="http://example.org/employee1">
        <foaf:name>http://example.org/employee1</foaf:name>
        <foaf:title>Developer</foaf:title>
        <foaf:workplaceHomepage>Company Name</foaf:workplaceHomepage>
        <ds:full_or_part_time>F</ds:full_or_part_time>
        <ds:salary_or_hourly>SALARY</ds:salary_or_hourly>
        <ds:annual_salary>73681</ds:annual_salary>
    </rdf:Description>
<rdf:Description rdf:about="http://example.org/employee2">
        <foaf:name>http://example.org/employee2</foaf:name>
        <foaf:title>Developer</foaf:title>
        <foaf:workplaceHomepage>Company Name</foaf:workplaceHomepage>
        <ds:full_or_part_time>F</ds:full_or_part_time>
        <ds:salary_or_hourly>SALARY</ds:salary_or_hourly>
        <ds:annual_salary>83504</ds:annual_salary>
    </rdf:Description>
<rdf:Description rdf:about="http://example.org/employee3">
        <foaf:name>http://example.org/employee3</foaf:name>
        <foaf:title>Developer</foaf:title>
        <foaf:workplaceHomepage>Company Name</foaf:workplaceHomepage>
        <ds:full_or_part_time>F</ds:full_or_part_time>
        <ds:salary_or_hourly>SALARY</ds:salary_or_hourly>
        <ds:annual_salary>90065</ds:annual_salary>
    </rdf:Description>
<rdf:Description rdf:about="http://example.org/employee4">
        <foaf:name>http://example.org/employee4</foaf:name>
        <foaf:title>Manager</foaf:title>
        <foaf:workplaceHomepage>Company Name</foaf:workplaceHomepage>
        <ds:full_or_part_time>F</ds:full_or_part_time>
        <ds:salary_or_hourly>SALARY</ds:salary_or_hourly>
        <ds:annual_salary>67751</ds:annual_salary>
    </rdf:Description>
</rdf:RDF>
    "#;
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_data);

    let sparql = r#"
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/> 
    SELECT AVG(?salary) AS ?average_salary 
    WHERE {
        ?employee ds:annual_salary ?salary
    } 
    GROUPBY ?average_salary"#;

    let results = execute_query(sparql, &mut database);

    println!("Results:");
    for result in results {
        if let [avg_salary] = &result[..] {
            println!("AVG(?salary) = {}", avg_salary);
        }
    }
}

fn main() {
    avg();
}
