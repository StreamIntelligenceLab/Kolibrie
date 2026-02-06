/*
 * Copyright © 2026 Stream Intelligence Lab
 * KU Leuven — Stream Intelligence Lab, Belgium
 */

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use kolibrie::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::parser::process_rule_definition;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sparql: String,
    #[serde(default)]
    rdf: Option<String>,
    #[serde(default)]
    rule: Option<String>,
    #[serde(default = "default_format")]
    format: String,
}

// Default format is RDF/XML for backwards compatibility
fn default_format() -> String {
    "rdfxml".to_string()
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    results: Vec<Vec<String>>,
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

    println!("Received query request:");
    println!("  SPARQL: {} chars", request.sparql.len());
    println!("  Format: {}", request.format);
    if let Some(ref rdf) = request.rdf {
        println!("  RDF: {} chars", rdf.len());
    }
    if let Some(ref rule) = request.rule {
        println!("  Rule: {} chars", rule.len());
    }

    let mut database = SparqlDatabase::new();
    let use_optimizer = request.format == "ntriples";
    
    if let Some(rdf_data) = request.rdf {
        if !rdf_data.trim().is_empty() {
            // Parse based on format
            match request.format.as_str() {
                "ntriples" => {
                    println!("Parsing N-Triples data...");
                    database.parse_ntriples_and_add(&rdf_data);
                    
                    // CRITICAL: N-Triples MUST use optimizer
                    println!("Building statistics for Streamertail optimizer (required for N-Triples)...");
                    database.get_or_build_stats();
                    println!("Building indexes...");
                    database.build_all_indexes();
                    
                    println!("✓ N-Triples data loaded with optimizer, triple count: {}", database.triples.len());
                }
                "rdfxml" | _ => {
                    println!("Parsing RDF/XML data...");
                    database.parse_rdf(&rdf_data);
                    
                    // Get statistics and build indexes
                    println!("Building statistics...");
                    database.get_or_build_stats();
                    println!("Building indexes...");
                    database.build_all_indexes();
                    
                    println!("✓ RDF/XML data loaded, triple count: {}", database.triples.len());
                }
            }
        }
    }
    
    if let Some(rule_def) = request.rule {
        if !rule_def.trim().is_empty() {
            println!("Processing rule...");
            match process_rule_definition(&rule_def, &mut database) {
                Ok((_, inferred_facts)) => {
                    println!("Rule processed, inferred {} facts", inferred_facts.len());
                    
                    // IMPORTANT: Rebuild stats after adding inferred facts
                    if !inferred_facts.is_empty() {
                        println!("Rebuilding statistics after inference...");
                        database.invalidate_stats_cache();  // Clear old stats
                        database.get_or_build_stats();       // Rebuild with new triples
                        database.build_all_indexes();
                    }
                }
                Err(e) => {
                    eprintln!("Rule processing error: {:?}", e);
                }
            }
        }
    }
    
    // Execute query with appropriate executor
    let executor_name = if use_optimizer { "Streamertail optimizer" } else { "Streamertail optimizer (fallback mode)" };
    println!("Executing SPARQL query with {}...", executor_name);
    println!("Query: {}", request.sparql);
    
    // N-Triples MUST use volcano optimizer, RDF/XML tries it with fallback
    let results = if use_optimizer {
        // N-Triples: MUST use optimizer, no fallback
        println!("Using Streamertail optimizer (required for N-Triples)");
        match execute_query_rayon_parallel2_volcano(&request.sparql, &mut database) {
            res => {
                println!("Streamertail optimizer executed successfully, {} results", res.len());
                res
            }
        }
    } else {
        // RDF/XML: Try optimizer first, fallback to regular if it fails
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_query_rayon_parallel2_volcano(&request.sparql, &mut database)
        })) {
            Ok(res) => {
                println!("Streamertail optimizer executed successfully, {} results", res.len());
                res
            }
            Err(e) => {
                eprintln!("Streamertail optimizer failed: {:?}, falling back to regular executor", e);
                println!("Executing with regular query executor...");
                let res = execute_query(&request.sparql, &mut database);
                println!("Regular executor completed, {} results", res.len());
                res
            }
        }
    };
    
    // Debug: Print first few results
    if !results.is_empty() {
        println!("Sample results:");
        for (i, row) in results.iter().take(3).enumerate() {
            println!("  Row {}: {:?}", i + 1, row);
        }
    } else {
        println!("Query returned 0 results!");
        println!("Database has {} triples", database.triples.len());
    }
    
    let response = QueryResponse { results };
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