/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reasons over N3 files and prints what the run outputs:
//! `n3_reason [--max-backward-depth N] <file.n3>…`

use n3::{parse_input, N3Error, N3Reasoner};
use std::process::ExitCode;

fn file_base(path: &str) -> String {
    match std::fs::canonicalize(path) {
        Ok(absolute) => format!("file://{}", absolute.display()),
        Err(_) => format!("file://{path}"),
    }
}

fn run(paths: &[String], max_backward_depth: Option<usize>) -> Result<String, N3Error> {
    let mut reasoner = N3Reasoner::new();
    if let Some(depth) = max_backward_depth {
        reasoner.limits.max_backward_depth = depth;
    }
    for path in paths {
        let text = std::fs::read_to_string(path)
            .map_err(|e| N3Error::syntax(format!("cannot read {path}: {e}")))?;
        let trig = path.ends_with(".trig");
        let document = parse_input(&text, Some(&file_base(path)), trig)?;
        reasoner.load(&document)?;
    }
    reasoner.run()?;
    Ok(reasoner.output_n3())
}

fn main() -> ExitCode {
    let mut paths = Vec::new();
    let mut max_backward_depth = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--max-backward-depth" {
            match args.next().and_then(|n| n.parse().ok()) {
                Some(depth) => max_backward_depth = Some(depth),
                None => {
                    eprintln!("--max-backward-depth needs a number");
                    return ExitCode::from(2);
                }
            }
        } else {
            paths.push(arg);
        }
    }
    if paths.is_empty() {
        eprintln!("usage: n3_reason [--max-backward-depth N] <file.n3>…");
        return ExitCode::from(2);
    }
    match run(&paths, max_backward_depth) {
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
