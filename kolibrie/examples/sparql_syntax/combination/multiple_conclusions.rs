use kolibrie::parser::*;
use kolibrie::execute_query::*;
use kolibrie::sparql_database::SparqlDatabase;
use datalog::knowledge_graph::KnowledgeGraph;
use shared::terms::Term;

fn main() {
    std::env::set_var("RUST_BACKTRACE", "1");
    let rdf_xml_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org#"
                 xmlns:alert="http://example.org/alerts#">
          <rdf:Description rdf:about="http://example.org#Room101">
            <ex:temperature>75</ex:temperature>
            <ex:room>Room101</ex:room>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Sensor1">
            <ex:room>Room101</ex:room>
            <ex:temperature>90</ex:temperature>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Room102">
            <ex:temperature>35</ex:temperature>
            <ex:room>Room102</ex:room>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Sensor2">
            <ex:room>Room102</ex:room>
            <ex:temperature>70</ex:temperature>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Room103">
            <ex:temperature>45</ex:temperature>
            <ex:room>Room103</ex:room>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org#Sensor3">
            <ex:room>Room103</ex:room>
            <ex:temperature>190</ex:temperature>
          </rdf:Description>
        </rdf:RDF>
    "#;

    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml_data);
    println!("Database RDF triples: {:#?}", database.triples);

    // Explicitly register the prefixes BEFORE processing any queries
    database.prefixes.insert("ex".to_string(), "http://example.org#".to_string());
    database.prefixes.insert("alert".to_string(), "http://example.org/alerts#".to_string());

    let mut kg = KnowledgeGraph::new();
    for triple in database.triples.iter() {
        let subject = database.dictionary.decode(triple.subject);
        let predicate = database.dictionary.decode(triple.predicate);
        let object = database.dictionary.decode(triple.object);
        kg.add_abox_triple(&subject.unwrap(), &predicate.unwrap(), &object.unwrap());
    }
    println!("KnowledgeGraph ABox loaded.");

    let combined_query_input = r#"PREFIX ex: <http://example.org#>
PREFIX alert: <http://example.org/alerts#>
RULE :OverheatingAlert(?room, ?temp) :- 
    WHERE { 
        ?reading ex:room ?room ; 
                 ex:temperature ?temp .
        FILTER (?temp > 80)
    } 
    => 
    { 
        ?room ex:overheatingAlert true ;
             alert:status "Critical" ;
             alert:timestamp "2025-04-19T13:49:18Z" ;
             alert:requiresAction true .
    }.
SELECT ?room ?temp ?status ?timestamp ?action
WHERE { 
    :OverheatingAlert(?room, ?temp) .
    ?room alert:status ?status .
    ?room alert:timestamp ?timestamp .
    ?room alert:requiresAction ?action .
}"#;

    let (_rest, combined_query) = parse_combined_query(combined_query_input)
        .expect("Failed to parse combined query");
    println!("Combined query parsed successfully.");
    println!("Parsed combined query prefixes: {:?}", combined_query.prefixes);

    // Make sure to update the database prefixes with ones from the query
    for (prefix, uri) in &combined_query.prefixes {
        database.prefixes.insert(prefix.clone(), uri.clone());
    }

    // Debug the prefixes
    println!("Database prefixes after update: {:?}", database.prefixes);

    if let Some(rule) = combined_query.rule {
        let dynamic_rule = convert_combined_rule(rule, &mut database.dictionary, &combined_query.prefixes);
        println!("Dynamic rule: {:#?}", dynamic_rule);
        kg.add_rule(dynamic_rule.clone());
        println!("Rule added to KnowledgeGraph.");
    
        // Process and store all conclusions in the rule_map
        for conclusion in &dynamic_rule.conclusion {
            if let Term::Constant(code) = conclusion.1 {
                let expanded = database.dictionary.decode(code).unwrap_or_else(|| "");
                let local = if let Some(idx) = expanded.rfind('#') {
                    &expanded[idx + 1..]
                } else if let Some(idx) = expanded.rfind(':') {
                    &expanded[idx + 1..]
                } else {
                    &expanded
                };
                let rule_key = local.to_lowercase();
                database.rule_map.insert(rule_key, expanded.to_string());
            }
        }
    }
    
    let inferred_facts = kg.infer_new_facts_semi_naive();
    println!("Inferred {} new fact(s):", inferred_facts.len());
    for triple in inferred_facts.iter() {
        println!("{}", database.triple_to_string(triple, &database.dictionary));
        database.triples.insert(triple.clone());
    }
    
    // Split the query into separate parts
    let parts: Vec<&str> = combined_query_input.split("SELECT").collect();
    let mut select_query = String::from("PREFIX ex: <http://example.org#>\nPREFIX alert: <http://example.org/alerts#>\n\nSELECT");
    if parts.len() > 1 {
        select_query.push_str(parts[1]);
    }
    
    println!("Executing query with explicit prefixes: {}", select_query);
    let query_results = execute_query(&select_query, &mut database);
    println!("Query results: {:?}", query_results);
}