use kolibrie::parser::*;
use kolibrie::sparql_database::*;

// Simple select all
fn simple_select_all() {
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

    let sparql = r#"PREFIX foaf: <http://xmlns.com/foaf/0.1/> PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/> SELECT * WHERE {?employee foaf:name ?name . ?employee foaf:title ?title . ?employee foaf:workplaceHomepage ?workplaceHomepage . ?employee ds:full_or_part_time ?full_or_part_time . ?employee ds:salary_or_hourly ?salary_or_hourly . ?employee ds:annual_salary ?salary}"#;

    let results = execute_query(sparql, &mut database);

    // Now the main function is responsible for printing the results
    println!("Results:");
    for result in results {
        let name = &result[0];
        let title = &result[1];
        let workplace = &result[2];
        let fp = &result[3];
        let sh = &result[4];
        let ans = &result[5];

        println!(
            "?name = {} ?title = {} ?workplace = {} ?fp = {} ?sh = {} ?salary = {}",
            name, title, workplace, fp, sh, ans
        );
    }
}

fn main() {
    simple_select_all();
}