use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use shared::GPU_MODE_ENABLED;

/*
For unix:
export LD_LIBRARY_PATH=/mnt/d/Projects/RustProj/Stream_Reasoning/kolibrie/src/cuda/:$LD_LIBRARY_PATH
cmake .
cmake --build .

For windows:
!!!Developer Command Prompt for VS 2022 for building cmake!!!
cmake -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=Release .
cmake --build .
 */

fn simple_select_synth_data(file_path: &str) {
    let mut database = SparqlDatabase::new();

    // Call `parser_rdf_from_file` function for multi-threading processing
    database.parse_rdf_from_file(file_path);

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

#[gpu::main]
fn main() {
    // Set current directory to the root of the project
    std::env::set_current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")))
        .expect("Failed to set project root as current directory");
    let file_path = "../datasets/synthetic_data_employee_4.rdf";
    simple_select_synth_data(&file_path);
}
