/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Prints the statements of an N3 document one per line, sorted, with blank nodes named by
//! their place in the graph: `n3_canonical <file.n3>`, or `-` for standard input
//!
//! It reads without a base IRI and prints the lines `n3_reason --canonical` prints for the same
//! statements, so another reasoner's N3 output compares with this crate's line by line. Exits 1
//! when the text is not N3.

use n3::canon::canonical_lines;
use n3::parse_document;
use std::io::Read;
use std::process::ExitCode;

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: n3_canonical <file.n3 | ->");
        return ExitCode::from(2);
    };
    let read = if path == "-" {
        let mut text = String::new();
        std::io::stdin().read_to_string(&mut text).map(|_| text)
    } else {
        std::fs::read_to_string(&path)
    };
    let text = match read {
        Ok(text) => text,
        Err(error) => {
            eprintln!("cannot read {path}: {error}");
            return ExitCode::FAILURE;
        }
    };
    match parse_document(&text, None) {
        Ok(document) => {
            for line in canonical_lines(&document.statements) {
                println!("{line}");
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
