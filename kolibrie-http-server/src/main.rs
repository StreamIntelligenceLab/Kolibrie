/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use kolibrie::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::parser::process_rule_definition;
use datalog::reasoning::Reasoner;
use datalog::parser_n3_logic::parse_n3_rule;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct QueryRequest {
    #[serde(default)]
    sparql: Option<String>,
    #[serde(default)]
    queries: Option<Vec<String>>,
    #[serde(default)]
    rdf: Option<String>,
    // N3 logic rules in { pattern } => { conclusion } syntax with @prefix declarations.
    // Parsed by parse_n3_rule (parser_n3_logic.rs) via the Reasoner — completely separate
    // from the SPARQL RULE syntax handled by process_rule_definition (parser.rs).
    #[serde(default)]
    n3logic: Option<String>,
    #[serde(default)]
    rule: Option<String>,
    #[serde(default)]
    rules: Option<Vec<String>>,
    #[serde(default = "default_format")]
    format: String,
}

// Default format is RDF/XML for backwards compatibility
fn default_format() -> String {
    "rdfxml".to_string()
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    results: Vec<QueryResult>,
}

#[derive(Debug, Serialize)]
struct QueryResult {
    query_index: usize,
    query: String,
    data: Vec<Vec<String>>,
    execution_time_ms: f64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

fn main() {
    println!("Starting Kolibrie HTTP Server on 0.0.0.0:8080");
    
    let listener = TcpListener::bind("0.0.0.0:8080")
        .expect("Failed to bind to port 8080");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = vec![0u8; 1_048_576]; // 1MB buffer for large RDF data
    
    match stream.read(&mut buffer) {
        Ok(size) => {
            let request = String::from_utf8_lossy(&buffer[..size]);
            let response = handle_request(&request);
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
        Err(e) => {
            eprintln!("Failed to read from connection: {}", e);
        }
    }
}

fn handle_request(request: &str) -> String {
    let lines: Vec<&str> = request.lines().collect();
    
    if lines.is_empty() {
        return error_response(400, "Bad Request");
    }

    let request_line = lines[0];
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    
    if parts.len() < 2 {
        return error_response(400, "Bad Request");
    }

    let method = parts[0];
    let path = parts[1];

    if method == "GET" && path == "/" {
        return serve_playground();
    }

    if method == "POST" && path == "/query" {
        if let Some(body_start) = request.find("\r\n\r\n") {
            let body = &request[body_start + 4..];
            return execute_sparql_with_context(body);
        }
    }

    if method == "OPTIONS" {
        return cors_response();
    }

    error_response(404, "Not Found")
}

fn serve_playground() -> String {
    let html = include_str!("../../web/playground.html");
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/html; charset=UTF-8\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         \r\n\
         {}",
        html.len(),
        html
    )
}

fn execute_sparql_with_context(body: &str) -> String {
    let request: QueryRequest = match serde_json::from_str(body) {
        Ok(req) => req,
        Err(e) => {
            eprintln!("JSON parse error: {}", e);
            return json_error_response(&format!("Invalid JSON: {}", e));
        }
    };

    // Collect all queries
    let mut queries = Vec::new();
    if let Some(single_query) = request.sparql {
        queries.push(single_query);
    }
    if let Some(multi_queries) = request.queries {
        queries.extend(multi_queries);
    }

    if queries.is_empty() {
        return json_error_response("No queries provided");
    }

    // Collect all rules
    let mut rules = Vec::new();
    if let Some(single_rule) = request.rule {
        rules.push(single_rule);
    }
    if let Some(multi_rules) = request.rules {
        rules.extend(multi_rules);
    }

    println!("Processing {} query(ies) and {} rule(s)", queries.len(), rules.len());

    let mut database = SparqlDatabase::new();
    let use_optimizer = request.format == "ntriples";
    
    // Load RDF data once
    if let Some(rdf_data) = request.rdf {
        if !rdf_data.trim().is_empty() {
            match request.format.as_str() {
                "ntriples" => {
                    println!("Parsing N-Triples data with Streamertail optimizer...");
                    database.parse_ntriples_and_add(&rdf_data);
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
                "turtle" => {
                    println!("Parsing Turtle dataset...");
                    database.parse_turtle(&rdf_data);
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
                "rdfxml" | _ => {
                    println!("Parsing RDF/XML data...");
                    database.parse_rdf(&rdf_data);
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
            }
        }
    }
    
    // Process N3 logic rules (n3logic field) using parse_n3_rule + Reasoner.
    // Syntax: @prefix declarations followed by { premise } => { conclusion } .
    // This is completely separate from the SPARQL RULE syntax — it uses the
    // Reasoner class (datalog/reasoning.rs) exactly as shown in the test example.
    if let Some(ref n3_rules_text) = request.n3logic {
    if !n3_rules_text.trim().is_empty() {
        println!("Processing N3 logic rules from N3 Logic sub-tab...");

        // Mirror the database dictionary so term encodings are shared
        let mut kg = Reasoner::new();
        kg.dictionary = database.dictionary.clone();

        // Decode all triples to owned strings using database.dictionary BEFORE
        // creating kg's mutable borrow — avoids the RwLockReadGuard conflict.
        let decoded_triples: Vec<(String, String, String)> = {
            let dict_guard = kg.dictionary.read().unwrap();
            database.triples.iter()
                .map(|triple| (
                    dict_guard.decode(triple.subject).unwrap_or("").to_string(),
                    dict_guard.decode(triple.predicate).unwrap_or("").to_string(),
                    dict_guard.decode(triple.object).unwrap_or("").to_string(),
                ))
                .collect()
        };

        println!("Decoded triples are: {:?}", decoded_triples);

        // Now load into the Reasoner — no borrow conflict
        for (s, p, o) in &decoded_triples {
            kg.add_abox_triple(s, p, o);
        }

        let n3_text = n3_rules_text.trim();
        match parse_n3_rule(n3_text, &mut kg) {
            Ok((_, (prefixes, rule))) => {
                println!(
                    "N3 rule parsed ({} prefix(es), {} premise(s), {} conclusion(s))",
                    prefixes.len(),
                    rule.premise.len(),
                    rule.conclusion.len(),
                );

                // **IMPORTANT**: Register N3 prefixes with the database so SPARQL rules can use them
                for (prefix, uri) in prefixes {
                    database.prefixes.insert(prefix.to_string(), uri.to_string());
                }

                // Register the rule and infer new facts
                kg.add_rule(rule);
                let inferred = kg.infer_new_facts_semi_naive();
                println!("N3 rule inferred {} fact(s)", inferred.len());

                // Push inferred triples into the database triple store
                for triple in inferred {
                    database.triples.insert(triple);
                }

                // Sync the enriched dictionary back so SPARQL can decode the new terms
                database.dictionary = kg.dictionary.clone();

                if !database.triples.is_empty() {
                    database.invalidate_stats_cache();
                    database.get_or_build_stats();
                    database.build_all_indexes();
                }
            }
            Err(e) => {
                eprintln!("N3 rule parse error: {:?}", e);
            }
        }
    }
}

    // Process all SPARQL-syntax rules (RULE :Name :- CONSTRUCT { } WHERE { })
    for (idx, rule_def) in rules.iter().enumerate() {
        if !rule_def.trim().is_empty() {
            println!("Processing rule {}...", idx + 1);
            match process_rule_definition(&rule_def, &mut database) {
                Ok((_, inferred_facts)) => {
                    println!("Rule {} processed, inferred {} facts", idx + 1, inferred_facts.len());
                    if !inferred_facts.is_empty() {
                        database.invalidate_stats_cache();
                        database.get_or_build_stats();
                        database.build_all_indexes();
                    }
                }
                Err(e) => {
                    eprintln!("Rule {} processing error: {:?}", idx + 1, e);
                }
            }
        }
    }
    
    // Execute all queries
    let mut all_results = Vec::new();
    
    for (idx, query) in queries.iter().enumerate() {
        println!("Executing query {}/{}...", idx + 1, queries.len());
        let start_time = std::time::Instant::now();
        
        let results = if use_optimizer {
            execute_query_rayon_parallel2_volcano(query, &mut database)
        } else {
            execute_query(query, &mut database)
        };
        
        let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;
        
        all_results.push(QueryResult {
            query_index: idx,
            query: query.clone(),
            data: results,
            execution_time_ms: execution_time,
        });
    }
    
    let response = QueryResponse { results: all_results };
    let json = match serde_json::to_string(&response) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("Failed to serialize response: {}", e);
            return json_error_response("Failed to serialize results");
        }
    };
    
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         \r\n\
         {}",
        json.len(),
        json
    )
}

fn json_error_response(message: &str) -> String {
    let error = ErrorResponse {
        error: message.to_string(),
    };
    let json = serde_json::to_string(&error).unwrap_or_else(|_| {
        r#"{"error":"Internal server error"}"#.to_string()
    });
    
    format!(
        "HTTP/1.1 400 Bad Request\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         \r\n\
         {}",
        json.len(),
        json
    )
}

fn cors_response() -> String {
    "HTTP/1.1 204 No Content\r\n\
     Access-Control-Allow-Origin: *\r\n\
     Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n\
     Access-Control-Allow-Headers: Content-Type\r\n\
     \r\n".to_string()
}

fn error_response(code: u16, message: &str) -> String {
    format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {}",
        code,
        message,
        message.len(),
        message
    )
}