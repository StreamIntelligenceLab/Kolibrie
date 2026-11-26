/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reasons over N3 files and prints what the run outputs, as eyeron does:
//! `n3_reason [--max-backward-depth N] [--proof | --canonical] [--no-base] [--timing] <file>…`
//!
//! The output is the derived facts, or the query answers when there are `log:query`
//! statements, as N3; when the run outputs `log:outputString` texts, it is those texts instead.
//! `.trig` files are read as TriG.
//!
//! - `--proof` prints a proof of the output that `eyeron --check-proof` can verify
//! - `--canonical` prints the output one statement per line, sorted, with blank nodes named by
//!   their place in the graph, so two runs' outputs can be compared line by line
//! - `--no-base` reads the files without a base IRI, as eyeron's own example runner does
//! - `--timing` writes `reasoning_ms=…` to stderr: loading and reasoning, not reading the files
//!
//! Exit codes: 0 output printed, 1 any other error, 2 bad arguments, 3 the program uses a
//! feature this crate reports as unsupported, 65 an inference fuse fired (eyeron's code; the
//! first line of the output then says so).

use n3::canon::canonical_lines;
use n3::{parse_input, N3Error, N3Reasoner};
use std::process::ExitCode;
use std::time::Instant;

const FUSE: u8 = 65;
const UNSUPPORTED: u8 = 3;

struct Options {
    paths: Vec<String>,
    max_backward_depth: Option<usize>,
    proof: bool,
    canonical: bool,
    base: bool,
    timing: bool,
}

fn file_base(path: &str) -> String {
    match std::fs::canonicalize(path) {
        Ok(absolute) => format!("file://{}", absolute.display()),
        Err(_) => format!("file://{path}"),
    }
}

fn run(options: &Options) -> Result<String, N3Error> {
    let mut documents = Vec::new();
    for path in &options.paths {
        let text = std::fs::read_to_string(path)
            .map_err(|e| N3Error::syntax(format!("cannot read {path}: {e}")))?;
        let label = std::path::Path::new(path)
            .file_name()
            .map_or_else(|| path.clone(), |name| name.to_string_lossy().into_owned());
        let base = options.base.then(|| file_base(path));
        let document = parse_input(&text, base.as_deref(), path.ends_with(".trig"))?;
        documents.push((label, document));
    }
    let started = Instant::now();
    let mut reasoner = N3Reasoner::new();
    if options.proof {
        reasoner.enable_proofs();
    }
    if let Some(depth) = options.max_backward_depth {
        reasoner.limits.max_backward_depth = depth;
    }
    for (label, document) in &documents {
        reasoner.set_source_label(label.clone());
        reasoner.load(document)?;
    }
    let ran = reasoner.run().map(|_| ());
    if options.timing {
        eprintln!("reasoning_ms={:.3}", started.elapsed().as_secs_f64() * 1000.0);
    }
    ran?;
    let output = if options.proof {
        reasoner.proof_n3()
    } else if let Some(text) = reasoner.output_strings() {
        text
    } else if options.canonical {
        let statements: Vec<_> = reasoner
            .output()
            .iter()
            .map(|triple| reasoner.store.statement(triple))
            .collect();
        canonical_lines(&statements)
            .into_iter()
            .map(|line| line + "\n")
            .collect()
    } else {
        reasoner.output_n3()
    };
    // The process exits next, so freeing a large store one allocation at a time is wasted work.
    std::mem::forget(reasoner);
    Ok(output)
}

fn parse_options() -> Result<Options, String> {
    let mut options = Options {
        paths: Vec::new(),
        max_backward_depth: None,
        proof: false,
        canonical: false,
        base: true,
        timing: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--proof" => options.proof = true,
            "--canonical" => options.canonical = true,
            "--no-base" => options.base = false,
            "--timing" => options.timing = true,
            "--max-backward-depth" => {
                let depth = args.next().and_then(|n| n.parse().ok());
                options.max_backward_depth =
                    Some(depth.ok_or("--max-backward-depth needs a number")?);
            }
            _ => options.paths.push(arg),
        }
    }
    if options.paths.is_empty() {
        return Err("usage: n3_reason [--max-backward-depth N] [--proof | --canonical] \
                    [--no-base] [--timing] <file>…"
            .to_string());
    }
    if options.proof && options.canonical {
        return Err("--proof and --canonical cannot be combined".to_string());
    }
    Ok(options)
}

fn main() -> ExitCode {
    let options = match parse_options() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    match run(&options) {
        Ok(text) => {
            print!("{text}");
            ExitCode::SUCCESS
        }
        Err(N3Error::Fuse(reason)) => {
            println!("# Inference fuse triggered: {reason}");
            ExitCode::from(FUSE)
        }
        Err(error @ N3Error::Unsupported(_)) => {
            eprintln!("{error}");
            ExitCode::from(UNSUPPORTED)
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
