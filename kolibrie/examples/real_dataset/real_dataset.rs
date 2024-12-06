extern crate kolibrie;
use std::fs::File;
use std::io::Read;
use kolibrie::sparql_database::*;

fn filter_example() {
    let mut file = File::open("/mnt/e/Projects/RustProj/kolibrie/datasets/xzkq-xp2w.rdf")
        .expect("Unable to open file");
    let mut rdf_data = String::new();
    file.read_to_string(&mut rdf_data)
        .expect("Unable to read file");

    let mut database = SparqlDatabase::new();
    database.parse_rdf(&rdf_data);

    // Filter triples where the predicate is "salary" and the object (salary value) is greater than 100000
    let filtered_db = database.filter(|triple| {
        let predicate = database.dictionary.decode(triple.predicate).unwrap();
        let object = database.dictionary.decode(triple.object).unwrap();
        predicate.ends_with("annual_salary") && object.parse::<f64>().unwrap_or(0.0) > 100000.0
    });

    // Iterate over the filtered triples and print the name and salary
    for triple in filtered_db.triples {
        let subject = &triple.subject;
        let salary = database.dictionary.decode(triple.object).unwrap();

        // Find the name associated with the subject
        let name_triple = database.triples.iter().find(|t| {
            t.subject == *subject
                && database
                    .dictionary
                    .decode(t.predicate)
                    .unwrap()
                    .ends_with("name")
        });

        if let Some(name_triple) = name_triple {
            let name = database.dictionary.decode(name_triple.object).unwrap();
            println!("Name: {}, Salary: {}", name, salary);
        }
    }
}

fn join_example() {
    let mut file =
        File::open("E:\\Projects\\RustProj\\kolibrie\\synthetic_employee_data11.rdf")
            .expect("Unable to open file");
    let mut rdf_data = String::new();
    file.read_to_string(&mut rdf_data)
        .expect("Unable to read file");

    let mut db = SparqlDatabase::new();
    db.parse_rdf(&rdf_data);

    let mut file2 =
        File::open("E:\\Projects\\RustProj\\kolibrie\\synthetic_employee_data12.rdf")
            .expect("Unable to open file");
    let mut rdf_data2 = String::new();
    file2
        .read_to_string(&mut rdf_data2)
        .expect("Unable to read file");

    let mut db2 = SparqlDatabase::new();
    db2.parse_rdf(&rdf_data2);

    let mut merged_db = db.union(&db2);
    let joined_db_merged = merged_db.par_join(&merged_db.clone(), "foaf:workplaceHomepage");
    joined_db_merged.print("Joined Triples (Merged Datasets):", false);
}

fn main() {
    filter_example();
    join_example();
}
