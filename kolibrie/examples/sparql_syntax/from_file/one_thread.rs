use kolibrie::execute_query::*;
use kolibrie::sparql_database::SparqlDatabase;

fn simple_select_synth_data(file_path: &str) {
    let file = std::fs::read_to_string(file_path)
                                        .expect("Error of finding file");
    let mut database = SparqlDatabase::new();

    database.parse_rdf(&file);

    let sparql = r#"
    PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?employee ?workplaceHomepage ?salary
    WHERE {
        ?employee foaf:workplaceHomepage ?workplaceHomepage .
        ?employee ds:annual_salary ?salary
    }"#;

    let results = execute_query(sparql, &mut database);

    println!("Results:");
    for result in results {
        if let [employee, workplace_homepage, salary] = &result[..] {
            // Process your results here
            println!("?employee = {} ?workplaceHomepage = {} ?salary = {}", employee, workplace_homepage, salary);
        }
    }
}

fn main() {
    // Set current directory to the root of the project
    std::env::set_current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))
        .expect("Failed to set project root as current directory");
    let file_path = "datasets/synthetic_data_employee_100K.rdf";
    simple_select_synth_data(file_path);
}