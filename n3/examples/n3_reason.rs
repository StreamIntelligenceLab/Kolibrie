/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reasons over N3 files and prints the derived facts: `n3_reason <file.n3>…`

use n3::{parse_document, N3Error, N3Reasoner};
use std::process::ExitCode;

fn file_base(path: &str) -> String {
    match std::fs::canonicalize(path) {
        Ok(absolute) => format!("file://{}", absolute.display()),
        Err(_) => format!("file://{path}"),
    }
}

fn run(paths: &[String]) -> Result<String, N3Error> {
    let mut reasoner = N3Reasoner::new();
    for path in paths {
        let text = std::fs::read_to_string(path)
            .map_err(|e| N3Error::syntax(format!("cannot read {path}: {e}")))?;
        let document = parse_document(&text, Some(&file_base(path)))?;
        reasoner.load(&document)?;
    }
    reasoner.run()?;
    Ok(reasoner.derived_n3())
}

fn main() -> ExitCode {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    if paths.is_empty() {
        eprintln!("usage: n3_reason <file.n3>…");
        return ExitCode::from(2);
    }
    match run(&paths) {
        Ok(text) => {
            print!("{text}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
