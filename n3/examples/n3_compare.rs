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
//! Usage: `n3_compare [--eyeron-home DIR] [--cases DIR]… [--proofs] [--verbose] [NAME…]`. With
//! `--proofs`, every case that matches also has its proof checked by `eyeron --check-proof`. Each `X.n3` in a
//! cases directory is one case; the expected answer is `output/X.n3` next to it when present,
//! and otherwise eyeron's own output for the file. `--eyeron-home` defaults to `$EYERON_HOME`,
//! then `../eyeron`; its `examples/` directory is always a cases directory. A mismatch for a
//! case listed in a cases directory's `divergences.txt` (`name: reason` per line) is reported as
//! a known divergence rather than a failure.

use n3::canon::canonical_lines;
use n3::{parse_document, parse_input, N3Error, N3Reasoner};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::Instant;

enum Outcome {
    Pass,
    /// Differs from the stored golden but matches eyeron run on this machine
    PassLive,
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
    proofs: bool,
}

fn parse_options() -> Result<Options, String> {
    let mut eyeron_home = std::env::var_os("EYERON_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../eyeron"));
    let mut case_dirs = Vec::new();
    let mut names = BTreeSet::new();
    let mut verbose = false;
    let mut proofs = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--eyeron-home" => {
                eyeron_home = PathBuf::from(args.next().ok_or("--eyeron-home needs a value")?);
            }
            "--cases" => case_dirs.push(PathBuf::from(args.next().ok_or("--cases needs a value")?)),
            "--verbose" | "-v" => verbose = true,
            "--proofs" => proofs = true,
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
        proofs,
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
#[derive(Clone)]
enum Answer {
    Statements(Vec<String>),
    Text(String),
    /// The run stopped at an inference fuse
    Fuse,
}

impl Answer {
    fn into_lines(self) -> Vec<String> {
        match self {
            Answer::Statements(lines) => lines,
            Answer::Fuse => vec!["(inference fuse)".to_string()],
            Answer::Text(text) => text
                .lines()
                .map(|line| line.trim_end().to_string())
                .filter(|line| !line.is_empty())
                .collect(),
        }
    }
}

/// What eyeron's checker said about the proof of a case
enum ProofVerdict {
    /// Every step verified
    Checked,
    /// No failures, but some steps are trust obligations
    Trusted(String),
    /// The checker found failures
    Invalid(String),
    /// No proof could be made or checked
    Error(String),
    /// The run stopped at an inference fuse, so there is no output to prove
    NotApplicable,
}

/// Writes this crate's proof of a case and has eyeron check it against the case's inputs
fn check_proof(path: &Path, eyeron_home: &Path) -> ProofVerdict {
    let mut reasoner = N3Reasoner::new();
    reasoner.enable_proofs();
    for input in inputs(path) {
        let Ok(text) = std::fs::read_to_string(&input) else {
            return ProofVerdict::Error(format!("cannot read {}", input.display()));
        };
        reasoner.set_source_label(input.file_name().unwrap_or_default().to_string_lossy());
        let trig = input.extension().is_some_and(|x| x == "trig");
        let loaded = parse_input(&text, Some(&file_base(&input)), trig)
            .and_then(|document| reasoner.load(&document));
        if let Err(error) = loaded {
            return ProofVerdict::Error(error.to_string());
        }
    }
    match reasoner.run() {
        Ok(_) => {}
        Err(N3Error::Fuse(_)) => return ProofVerdict::NotApplicable,
        Err(error) => return ProofVerdict::Error(error.to_string()),
    }
    let proof = reasoner.proof_n3();
    let file = std::env::temp_dir().join(format!(
        "n3-proof-{}-{}.n3",
        std::process::id(),
        path.file_stem().unwrap_or_default().to_string_lossy()
    ));
    if let Err(error) = std::fs::write(&file, proof) {
        return ProofVerdict::Error(error.to_string());
    }
    let output = Command::new(eyeron_home.join("target/release/eyeron"))
        .arg("--check-proof")
        .arg(&file)
        .args(inputs(path))
        .output();
    let _ = std::fs::remove_file(&file);
    let Ok(output) = output else {
        return ProofVerdict::Error("cannot run eyeron".to_string());
    };
    let report = String::from_utf8_lossy(&output.stdout).into_owned()
        + &String::from_utf8_lossy(&output.stderr);
    let first = report.lines().next().unwrap_or("").to_string();
    let detail = report
        .lines()
        .filter(|line| line.contains("C1") || line.contains("failure") || line.contains("detail"))
        .take(4)
        .collect::<Vec<_>>()
        .join(" | ");
    if first.starts_with("# checked with") {
        ProofVerdict::Trusted(first)
    } else if first.starts_with("# checked") {
        ProofVerdict::Checked
    } else {
        ProofVerdict::Invalid(format!("{first} {detail}"))
    }
}

fn file_base(path: &Path) -> String {
    let absolute = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    format!("file://{}", absolute.display())
}

fn ours(path: &Path) -> Result<Answer, N3Error> {
    let mut reasoner = N3Reasoner::new();
    for input in inputs(path) {
        let text = std::fs::read_to_string(&input)
            .map_err(|e| N3Error::syntax(format!("cannot read {}: {e}", input.display())))?;
        let trig = input.extension().is_some_and(|x| x == "trig");
        let document = parse_input(&text, None, trig)?;
        reasoner.load(&document)?;
    }
    match reasoner.run() {
        Err(N3Error::Fuse(_)) => return Ok(Answer::Fuse),
        other => {
            other?;
        }
    }
    if let Some(text) = reasoner.output_strings() {
        return Ok(Answer::Text(text));
    }
    let statements: Vec<_> = reasoner
        .output()
        .iter()
        .map(|t| reasoner.store.statement(t))
        .collect();
    Ok(Answer::Statements(canonical_lines(&statements)))
}

/// Runs eyeron on the case and reads what it printed
fn live(path: &Path, eyeron_home: &Path) -> Result<Answer, String> {
    let binary = eyeron_home.join("target/release/eyeron");
    let output = Command::new(&binary)
        .args(inputs(path))
        .output()
        .map_err(|e| format!("cannot run {}: {e}", binary.display()))?;
    if output.status.code() == Some(65) {
        return Ok(Answer::Fuse);
    }
    if !output.status.success() {
        return Err(format!(
            "eyeron exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(read_answer(&String::from_utf8_lossy(&output.stdout)))
}

/// Reads an oracle's output: a fuse report, N3 statements, or `log:outputString` text
fn read_answer(text: &str) -> Answer {
    if text.starts_with("# Inference fuse triggered") {
        return Answer::Fuse;
    }
    match parse_document(text, None) {
        Ok(document) => Answer::Statements(canonical_lines(&document.statements)),
        Err(_) => Answer::Text(text.to_string()),
    }
}

/// The stored golden next to a case, if it has one
fn golden(path: &Path) -> Result<Option<Answer>, String> {
    let stored = |extension: &str| {
        let name = path.with_extension(extension);
        path.parent()
            .map(|dir| {
                dir.join("output")
                    .join(name.file_name().unwrap_or_default())
            })
            .filter(|p| p.exists())
    };
    if let Some(markdown) = stored("md") {
        let text = std::fs::read_to_string(&markdown).map_err(|e| e.to_string())?;
        return Ok(Some(Answer::Text(text)));
    }
    match stored("n3") {
        Some(n3) => {
            let text = std::fs::read_to_string(&n3).map_err(|e| e.to_string())?;
            Ok(Some(read_answer(&text)))
        }
        None => Ok(None),
    }
}

/// Whether two answers agree, and if not, what each has that the other lacks
fn difference(expected: Answer, actual: Answer) -> Option<(Vec<String>, Vec<String>)> {
    let (expected, actual) = (expected.into_lines(), actual.into_lines());
    let expected_set: BTreeSet<&String> = expected.iter().collect();
    let actual_set: BTreeSet<&String> = actual.iter().collect();
    if expected_set == actual_set {
        return None;
    }
    let only = |a: &BTreeSet<&String>, b: &BTreeSet<&String>| {
        a.difference(b).map(|s| (*s).clone()).collect()
    };
    Some((
        only(&expected_set, &actual_set),
        only(&actual_set, &expected_set),
    ))
}

fn expected(path: &Path, eyeron_home: &Path) -> Result<Answer, String> {
    match golden(path)? {
        Some(answer) => Ok(answer),
        None => live(path, eyeron_home),
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
    let Some((missing, extra)) = difference(expected, actual.clone()) else {
        return Outcome::Pass;
    };
    // A stored golden records one platform's floating-point library; eyeron run here may agree.
    let has_golden = matches!(golden(path), Ok(Some(_)));
    if has_golden {
        if let Ok(here) = live(path, eyeron_home) {
            if difference(here, actual).is_none() {
                return Outcome::PassLive;
            }
        }
    }
    let name = path.file_stem().unwrap_or_default().to_string_lossy();
    if let Some(reason) = divergences(path).remove(&*name) {
        return Outcome::Divergence(reason);
    }
    Outcome::Mismatch { missing, extra }
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
    let mut proof_tally: BTreeMap<&str, usize> = BTreeMap::new();
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
        if options.proofs && matches!(outcome, Outcome::Pass | Outcome::PassLive) {
            let (label, detail) = match check_proof(&path, &options.eyeron_home) {
                ProofVerdict::Checked => ("proof-ok", String::new()),
                ProofVerdict::Trusted(report) => ("proof-trusted", report),
                ProofVerdict::Invalid(report) => ("PROOF-BAD", report),
                ProofVerdict::Error(report) => ("proof-error", report),
                ProofVerdict::NotApplicable => ("proof-n/a", String::new()),
            };
            *proof_tally.entry(label).or_default() += 1;
            if !matches!(label, "proof-ok" | "proof-n/a") {
                println!("{label:<12} {name}: {detail}");
            }
        }
        let label = match &outcome {
            Outcome::Pass => "pass",
            Outcome::PassLive => "pass-live",
            Outcome::Divergence(_) => "divergence",
            Outcome::Mismatch { .. } => "MISMATCH",
            Outcome::Unsupported(_) => "unsupported",
            Outcome::Failed(_) => "FAILED",
            Outcome::NoOracle(_) => "no-oracle",
        };
        *tally.entry(label).or_default() += 1;
        match outcome {
            Outcome::Pass | Outcome::PassLive => println!("{label:<12} {name} ({elapsed} ms)"),
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
    if options.proofs {
        for (label, count) in &proof_tally {
            println!("  {label:<12} {count}");
        }
    }
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
