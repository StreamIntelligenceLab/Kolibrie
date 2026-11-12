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
//! `n3_reason [--max-backward-depth N] [--proof] <file.n3>…`; `--proof` prints a proof of the output
//! that `eyeron --check-proof` can verify, instead of the output itself

use n3::{parse_input, N3Error, N3Reasoner};
use std::process::ExitCode;

fn file_base(path: &str) -> String {
    match std::fs::canonicalize(path) {
        Ok(absolute) => format!("file://{}", absolute.display()),
        Err(_) => format!("file://{path}"),
    }
}

fn run(
    paths: &[String],
    max_backward_depth: Option<usize>,
    proof: bool,
) -> Result<String, N3Error> {
    let mut reasoner = N3Reasoner::new();
    if proof {
        reasoner.enable_proofs();
    }
    if let Some(depth) = max_backward_depth {
        reasoner.limits.max_backward_depth = depth;
    }
    for path in paths {
        let text = std::fs::read_to_string(path)
            .map_err(|e| N3Error::syntax(format!("cannot read {path}: {e}")))?;
        let label = std::path::Path::new(path)
            .file_name()
            .map_or_else(|| path.clone(), |name| name.to_string_lossy().into_owned());
        reasoner.set_source_label(label);
        let trig = path.ends_with(".trig");
        let document = parse_input(&text, Some(&file_base(path)), trig)?;
        reasoner.load(&document)?;
    }
    reasoner.run()?;
    let output = if proof {
        reasoner.proof_n3()
    } else {
        reasoner.output_n3()
    };
    // The process exits next, so freeing a large store one allocation at a time is wasted work.
    std::mem::forget(reasoner);
    Ok(output)
}

fn main() -> ExitCode {
    let mut paths = Vec::new();
    let mut max_backward_depth = None;
    let mut proof = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--proof" {
            proof = true;
        } else if arg == "--max-backward-depth" {
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
        eprintln!("usage: n3_reason [--max-backward-depth N] [--proof] <file.n3>…");
        return ExitCode::from(2);
    }
    match run(&paths, max_backward_depth, proof) {
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
