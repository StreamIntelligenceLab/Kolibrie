/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use clap::Parser;
use kolibrie::execute_query::*;
use kolibrie::sparql_database::SparqlDatabase;

#[derive(Parser)]
#[command(
    name = "kolibrie-cli",
    version = "0.1.1",
    author = "Volodymyr Kadzhaia <vkadzhaia@gmail.com>",
    author = "Pieter Bonte <pieter.bonte@kuleuven.be>",
    about = "A CLI tool for Kolibrie",
    long_about = "Kolibrie CLI - A command-line interface for loading RDF/XML files and executing SPARQL queries against them. Built with Rust using the kolibrie library for high-performance RDF processing."
)]
struct Args {
    #[arg(short, long, help = "RDF file to query", value_name = "FILE")]
    file: String,

    #[arg(short, long, help = "SPARQL query string", value_name = "QUERY")]
    query: String,
}

fn main() {
    let args = Args::parse();

    let mut database = SparqlDatabase::new();
    database.parse_rdf_from_file(&args.file);

    // Execute query
    let results = execute_query(&args.query, &mut database);
    println!("Results: {:?}", results);
}
