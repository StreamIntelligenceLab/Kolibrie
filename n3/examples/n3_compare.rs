/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Compares this crate's N3 reasoning with eyeron's, case by case
//!
//! Usage: `n3_compare [--eyeron-home DIR] [--cases DIR]… [--verbose] [NAME…]`. Each `X.n3` in a
//! cases directory is one case; the expected answer is `output/X.n3` next to it when present,
//! and otherwise eyeron's own output for the file. `--eyeron-home` defaults to `$EYERON_HOME`,
//! then `../eyeron`; its `examples/` directory is always a cases directory. A mismatch for a
//! case listed in a cases directory's `divergences.txt` (`name: reason` per line) is reported as
//! a known divergence rather than a failure.

use n3::canon::canonical_lines;
use n3::{parse_document, N3Error, N3Reasoner};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::Instant;

enum Outcome {
    Pass,
    Divergence(String),
    Mismatch {
        missing: Vec<String>,
        extra: Vec<String>,
    },
    Unsupported(String),
    Failed(String),
    NoOracle(String),
}

struct Options {
    eyeron_home: PathBuf,
    case_dirs: Vec<PathBuf>,
    names: BTreeSet<String>,
    verbose: bool,
}

fn parse_options() -> Result<Options, String> {
    let mut eyeron_home = std::env::var_os("EYERON_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../eyeron"));
    let mut case_dirs = Vec::new();
    let mut names = BTreeSet::new();
    let mut verbose = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--eyeron-home" => {
                eyeron_home = PathBuf::from(args.next().ok_or("--eyeron-home needs a value")?);
            }
            "--cases" => case_dirs.push(PathBuf::from(args.next().ok_or("--cases needs a value")?)),
            "--verbose" | "-v" => verbose = true,
            name => {
                names.insert(name.trim_end_matches(".n3").to_string());
            }
        }
    }
    case_dirs.insert(0, eyeron_home.join("examples"));
    Ok(Options {
        eyeron_home,
        case_dirs,
        names,
        verbose,
    })
}

/// The case file, then its companion `input/<name>.n3` or `input/<name>.trig` if one exists
fn inputs(path: &Path) -> Vec<PathBuf> {
    let mut found = vec![path.to_path_buf()];
    let (Some(dir), Some(stem)) = (path.parent(), path.file_stem()) else {
        return found;
    };
    for extension in ["n3", "trig"] {
        let companion = dir
            .join("input")
            .join(format!("{}.{extension}", stem.to_string_lossy()));
        if companion.exists() {
            found.push(companion);
            break;
        }
    }
    found
}

/// What a run printed: N3 statements, canonicalised, or `log:outputString` text
enum Answer {
    Statements(Vec<String>),
    Text(String),
}

impl Answer {
    fn into_lines(self) -> Vec<String> {
        match self {
            Answer::Statements(lines) => lines,
            Answer::Text(text) => text
                .lines()
                .map(|line| line.trim_end().to_string())
                .filter(|line| !line.is_empty())
                .collect(),
        }
    }
}

fn ours(path: &Path) -> Result<Answer, N3Error> {
    let mut reasoner = N3Reasoner::new();
    for input in inputs(path) {
        let text = std::fs::read_to_string(&input)
            .map_err(|e| N3Error::syntax(format!("cannot read {}: {e}", input.display())))?;
        let document = parse_document(&text, None).map_err(|e| match e {
            N3Error::Syntax { .. } if input.extension().is_some_and(|x| x == "trig") => {
                N3Error::unsupported(format!("TriG companion input ({e})"))
            }
            other => other,
        })?;
        reasoner.load(&document)?;
    }
    reasoner.run()?;
    if let Some(text) = reasoner.output_strings() {
        return Ok(Answer::Text(text));
    }
    let statements: Vec<_> = reasoner
        .derived()
        .iter()
        .map(|t| reasoner.store.statement(t))
        .collect();
    Ok(Answer::Statements(canonical_lines(&statements)))
}

fn expected(path: &Path, eyeron_home: &Path) -> Result<Answer, String> {
    let golden = |extension: &str| {
        let name = path.with_extension(extension);
        path.parent()
            .map(|dir| {
                dir.join("output")
                    .join(name.file_name().unwrap_or_default())
            })
            .filter(|p| p.exists())
    };
    if let Some(markdown) = golden("md") {
        let text = std::fs::read_to_string(&markdown).map_err(|e| e.to_string())?;
        return Ok(Answer::Text(text));
    }
    let text = match golden("n3") {
        Some(stored) => std::fs::read_to_string(&stored).map_err(|e| e.to_string())?,
        None => {
            let binary = eyeron_home.join("target/release/eyeron");
            let output = Command::new(&binary)
                .args(inputs(path))
                .output()
                .map_err(|e| format!("cannot run {}: {e}", binary.display()))?;
            if !output.status.success() {
                return Err(format!(
                    "eyeron exited with {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ));
            }
            String::from_utf8_lossy(&output.stdout).into_owned()
        }
    };
    match parse_document(&text, None) {
        Ok(document) => Ok(Answer::Statements(canonical_lines(&document.statements))),
        Err(_) => Ok(Answer::Text(text)),
    }
}

/// The known divergences listed next to a case, by case name
fn divergences(path: &Path) -> BTreeMap<String, String> {
    let Some(dir) = path.parent() else {
        return BTreeMap::new();
    };
    let Ok(text) = std::fs::read_to_string(dir.join("divergences.txt")) else {
        return BTreeMap::new();
    };
    text.lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(name, reason)| (name.trim().to_string(), reason.trim().to_string()))
        .collect()
}

fn compare(path: &Path, eyeron_home: &Path) -> Outcome {
    let actual = match ours(path) {
        Ok(lines) => lines,
        Err(N3Error::Unsupported(feature)) => return Outcome::Unsupported(feature),
        Err(error) => return Outcome::Failed(error.to_string()),
    };
    let expected = match expected(path, eyeron_home) {
        Ok(lines) => lines,
        Err(reason) => return Outcome::NoOracle(reason),
    };
    let (expected, actual) = (expected.into_lines(), actual.into_lines());
    let expected_set: BTreeSet<&String> = expected.iter().collect();
    let actual_set: BTreeSet<&String> = actual.iter().collect();
    if expected_set == actual_set {
        return Outcome::Pass;
    }
    let name = path.file_stem().unwrap_or_default().to_string_lossy();
    if let Some(reason) = divergences(path).remove(&*name) {
        return Outcome::Divergence(reason);
    }
    Outcome::Mismatch {
        missing: expected_set
            .difference(&actual_set)
            .map(|s| (*s).clone())
            .collect(),
        extra: actual_set
            .difference(&expected_set)
            .map(|s| (*s).clone())
            .collect(),
    }
}

/// Groups an unsupported feature by kind, built-ins by namespace
fn feature_family(feature: &str) -> String {
    if let Some(iri) = feature.strip_prefix("built-in <") {
        let namespace = iri.split_once('#').map_or(iri, |(ns, _)| ns);
        let short = namespace.rsplit('/').next().unwrap_or(namespace);
        return format!("built-in {short}:");
    }
    feature
        .split(['(', '?'])
        .next()
        .unwrap_or(feature)
        .trim()
        .to_string()
}

fn cases(options: &Options) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for dir in &options.case_dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            eprintln!("skipping missing cases directory {}", dir.display());
            continue;
        };
        let mut files: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "n3"))
            .filter(|p| {
                options.names.is_empty()
                    || p.file_stem()
                        .is_some_and(|s| options.names.contains(&*s.to_string_lossy()))
            })
            .collect();
        files.sort();
        found.extend(files);
    }
    found
}

fn main() -> ExitCode {
    let options = match parse_options() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    let mut tally: BTreeMap<&str, usize> = BTreeMap::new();
    let mut unsupported: BTreeMap<String, usize> = BTreeMap::new();
    for path in cases(&options) {
        let name = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let started = Instant::now();
        let outcome = compare(&path, &options.eyeron_home);
        let elapsed = started.elapsed().as_millis();
        let label = match &outcome {
            Outcome::Pass => "pass",
            Outcome::Divergence(_) => "divergence",
            Outcome::Mismatch { .. } => "MISMATCH",
            Outcome::Unsupported(_) => "unsupported",
            Outcome::Failed(_) => "FAILED",
            Outcome::NoOracle(_) => "no-oracle",
        };
        *tally.entry(label).or_default() += 1;
        match outcome {
            Outcome::Pass => println!("{label:<12} {name} ({elapsed} ms)"),
            Outcome::Mismatch { missing, extra } => {
                println!(
                    "{label:<12} {name}: {} missing, {} extra",
                    missing.len(),
                    extra.len()
                );
                let shown = if options.verbose { usize::MAX } else { 3 };
                for line in missing.iter().take(shown) {
                    println!("    - {line}");
                }
                for line in extra.iter().take(shown) {
                    println!("    + {line}");
                }
            }
            Outcome::Unsupported(feature) => {
                println!("{label:<12} {name}: {feature}");
                *unsupported.entry(feature_family(&feature)).or_default() += 1;
            }
            Outcome::Divergence(reason) | Outcome::Failed(reason) | Outcome::NoOracle(reason) => {
                println!("{label:<12} {name}: {reason}");
            }
        }
    }
    println!("\nsummary:");
    for (label, count) in &tally {
        println!("  {label:<12} {count}");
    }
    if !unsupported.is_empty() {
        println!("unsupported features:");
        let mut by_count: Vec<_> = unsupported.into_iter().collect();
        by_count.sort_by_key(|entry| std::cmp::Reverse(entry.1));
        for (feature, count) in by_count {
            println!("  {count:>4}  {feature}");
        }
    }
    if tally.contains_key("MISMATCH") || tally.contains_key("FAILED") {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
