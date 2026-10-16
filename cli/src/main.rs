/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! `kolibrie-cli`: load one RDF file, run one SPARQL query, print the results.
//!
//! Deliberately minimal — a smoke test for the library and a way to run a query
//! without writing a program. It parses the file into a fresh in-memory store, so
//! nothing persists between invocations and a large file is re-parsed every time.
//!
//! ```text
//! kolibrie-cli --file data.rdf --query 'SELECT ?s WHERE { ?s ?p ?o }'
//! ```
//!
//! The `--version` string is hard-coded in the `clap` attributes rather than read
//! from the workspace version. It matches today; nothing keeps it matching, so it
//! will drift silently at the next version bump.

use clap::Parser;
use kolibrie::prelude::*;
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

    // A failed query exits non-zero and says why. It used to print `Results: []`
    // and exit 0, which is also what a query matching nothing prints.
    match execute(&args.query, &mut database) {
        Ok(solutions) => println!("Results: {:?}", solutions.into_rows()),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}
