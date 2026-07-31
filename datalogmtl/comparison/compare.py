#!/usr/bin/env python3
"""
Correctness harness: run every case under cases/ through both the MeTeoR
reference reasoner (baseline / ground truth) and the datalogmtl Rust engine,
then diff their normalized outputs.

Each case directory contains `program.txt` and `data.txt` in MeTeoR's past-only,
integer, arity-<=2 syntax. The comparison window [0, T] is derived identically
for both engines: T = max fact endpoint + max operator interval width.

Usage:
    PYTHONPATH is set automatically to $METEOR_HOME (default
    /Users/u0164257/Documents/Github/MeTeoR).

    python compare.py [cases_dir] [--case NAME] [--store snapshot|interval] [-v]
"""
import argparse
import difflib
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
METEOR_HOME = os.environ.get(
    "METEOR_HOME", "/Users/u0164257/Documents/Github/MeTeoR"
)
DYLD = os.environ.get(
    "DYLD_LIBRARY_PATH",
    "/opt/homebrew/Cellar/python@3.11/3.11.11/Frameworks/Python.framework/Versions/3.11/lib",
)

INTERVAL_RE = re.compile(r"\[\s*(-?\d+)(?:\s*,\s*(-?\d+))?\s*\]")


def max_interval_end(text):
    """Largest integer interval endpoint appearing in bracketed intervals."""
    best = 0
    for m in INTERVAL_RE.finditer(text):
        end = int(m.group(2) if m.group(2) is not None else m.group(1))
        best = max(best, end)
    return best


def max_fact_end(data_text):
    """Largest fact validity endpoint across the data file."""
    best = 0
    for line in data_text.splitlines():
        line = line.split("#", 1)[0].strip().replace(" ", "")
        if not line or "@" not in line:
            continue
        span = line.rsplit("@", 1)[1]
        m = INTERVAL_RE.search(span)
        if m:
            best = max(best, int(m.group(2) if m.group(2) is not None else m.group(1)))
        else:
            try:
                best = max(best, int(span))
            except ValueError:
                pass
    return best


def horizon_for(program_text, data_text):
    return max_fact_end(data_text) + max_interval_end(program_text)


def normalize(output):
    return sorted(l.strip() for l in output.splitlines() if l.strip())


def rust_binary():
    path = os.path.join(REPO_ROOT, "target", "debug", "examples", "meteor_compare")
    return path if os.path.exists(path) else None


def run_rust(program, data, horizon, store, strategy="tick"):
    args = ["--program", program, "--data", data,
            "--horizon", str(horizon), "--store", store, "--strategy", strategy]
    env = dict(os.environ, DYLD_LIBRARY_PATH=DYLD)
    binary = rust_binary()
    if binary:
        cmd = [binary] + args
    else:
        cmd = ["cargo", "run", "-q", "-p", "datalogmtl",
               "--example", "meteor_compare", "--"] + args
    proc = subprocess.run(cmd, cwd=REPO_ROOT, env=env,
                          capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError("rust engine failed:\n" + proc.stderr)
    return proc.stdout


def run_meteor(program, data, horizon, mode="seminaive"):
    env = dict(os.environ, PYTHONPATH=METEOR_HOME)
    cmd = [sys.executable, os.path.join(HERE, "run_meteor.py"),
           "--program", program, "--data", data, "--horizon", str(horizon), "--mode", mode]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError("meteor reasoner failed:\n" + proc.stderr)
    return proc.stdout


def run_case(case_dir, store, verbose, strategy="tick", meteor_mode="seminaive"):
    program = os.path.join(case_dir, "program.txt")
    data = os.path.join(case_dir, "data.txt")
    program_text = open(program).read()
    data_text = open(data).read()
    horizon = horizon_for(program_text, data_text)

    meteor = normalize(run_meteor(program, data, horizon, meteor_mode))
    rust = normalize(run_rust(program, data, horizon, store, strategy))

    ok = meteor == rust
    name = os.path.basename(case_dir)
    status = "PASS" if ok else "MISMATCH"
    print("[{}] {}  (T={})".format(status, name, horizon))
    if not ok or verbose:
        diff = difflib.unified_diff(meteor, rust, fromfile="meteor", tofile="rust", lineterm="")
        for line in diff:
            print("    " + line)
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cases_dir", nargs="?", default=os.path.join(HERE, "cases"))
    ap.add_argument("--case", help="run only this case name")
    ap.add_argument("--store", default="snapshot", choices=["snapshot", "interval"])
    ap.add_argument("--strategy", default="tick", choices=["tick", "interval"])
    ap.add_argument("--meteor-mode", default="seminaive", choices=["seminaive", "naive"])
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    cases = sorted(
        d for d in os.listdir(args.cases_dir)
        if os.path.isdir(os.path.join(args.cases_dir, d))
        and (args.case is None or d == args.case)
    )
    if not cases:
        print("no cases found", file=sys.stderr)
        return 1

    failures = 0
    for name in cases:
        try:
            if not run_case(os.path.join(args.cases_dir, name), args.store, args.verbose, args.strategy, args.meteor_mode):
                failures += 1
        except Exception as e:  # noqa: BLE001
            print("[ERROR] {}: {}".format(name, e))
            failures += 1

    print("\n{}/{} cases passed".format(len(cases) - failures, len(cases)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
