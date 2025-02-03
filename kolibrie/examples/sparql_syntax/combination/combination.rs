use kolibrie::parser::*;

fn main() {
    let input = r#"PREFIX ex: <http://example.org#>
RULE :OverheatingAlert(?room) :- 
    WHERE { 
        ?reading ex:room ?room ; 
                 ex:temperature ?temp 
    } 
    => 
    { 
        ?room ex:overheatingAlert true .
    }.
SELECT ?room
WHERE { 
    :OverheatingAlert(?room) 
}"#;
    match parse_combined_query(input) {
        Ok((rest, combined_query)) => {
            println!("Remaining: {:?}", rest);
            println!("Parsed Combined Query: {:#?}", combined_query);
        }
        Err(e) => {
            println!("Parse error: {:?}", e);
        }
    }
}
