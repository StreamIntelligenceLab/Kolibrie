#!/usr/bin/env python3
"""
Performance harness: generate temporal-LUBM datasets at increasing scale, run the
past-fragment program through both MeTeoR (baseline) and the datalogmtl Rust
engine, and tabulate reasoning wall-clock time.

For each scale N:
  1. gen_lubm.py writes a dataset (identical file feeds both engines).
  2. run_meteor_perf.py times MeTeoR's materialize().
  3. meteor_compare --timing times the Rust engine's tick loop.

Optionally (--verify) the smallest scale is also run through the correctness
comparator to confirm both engines still agree on the generated workload.

Usage:
    python perf.py [--scales 100,500,1000,2000] [--horizon 20] [--intervals 3]
                   [--store snapshot|interval] [--repeats 1] [--verify]
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
COMP = os.path.dirname(HERE)                       # comparison/
REPO_ROOT = os.path.abspath(os.path.join(COMP, "..", ".."))
PROGRAMS = {
    "past": os.path.join(HERE, "programs", "lubm_past.txt"),      # 4 rules, past ops
    "deep": os.path.join(HERE, "programs", "lubm_deep.txt"),      # 8 rules, deeper chain + joins
    "future": os.path.join(HERE, "programs", "lubm_future.txt"),  # 4 rules, future ops (static)
}
METEOR_HOME = os.environ.get("METEOR_HOME", "/Users/u0164257/Documents/Github/MeTeoR")
DYLD = os.environ.get(
    "DYLD_LIBRARY_PATH",
    "/opt/homebrew/Cellar/python@3.11/3.11.11/Frameworks/Python.framework/Versions/3.11/lib",
)
RUST_BIN_RELEASE = os.path.join(REPO_ROOT, "target", "release", "examples", "meteor_compare")
RUST_BIN_DEBUG = os.path.join(REPO_ROOT, "target", "debug", "examples", "meteor_compare")
RUST_BIN = RUST_BIN_RELEASE if os.path.exists(RUST_BIN_RELEASE) else RUST_BIN_DEBUG


def gen_data(scale, horizon, intervals, seed, out, max_width=None, rich=False):
    cmd = [sys.executable, os.path.join(HERE, "gen_lubm.py"),
           "--scale", str(scale), "--horizon", str(horizon),
           "--intervals", str(intervals), "--seed", str(seed), "--out", out]
    if max_width is not None:
        cmd += ["--max-width", str(max_width)]
    if rich:
        cmd += ["--rich"]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def run_rust(program, data, store, strategy="tick", mode="streaming"):
    env = dict(os.environ, DYLD_LIBRARY_PATH=DYLD)
    cmd = [RUST_BIN, "--program", program, "--data", data,
           "--store", store, "--strategy", strategy, "--mode", mode, "--timing"]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError("rust engine failed:\n" + proc.stderr)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def run_meteor(program, data, mode="seminaive"):
    env = dict(os.environ, PYTHONPATH=METEOR_HOME)
    cmd = [sys.executable, os.path.join(HERE, "run_meteor_perf.py"),
           "--program", program, "--data", data, "--mode", mode]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError("meteor reasoner failed:\n" + proc.stderr)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def best_of(fn, repeats):
    return min(fn()["reason_ms"] for _ in range(repeats))


def verify(program, scale, horizon, intervals, seed, store, strategy="tick",
           max_width=None, rich=False, meteor_mode="seminaive", mode="streaming"):
    """Confirm both engines still agree on a generated dataset via compare.py."""
    with tempfile.TemporaryDirectory() as tmp:
        cases = os.path.join(tmp, "cases")
        case = os.path.join(cases, "gen")
        os.makedirs(case)
        subprocess.run(["cp", program, os.path.join(case, "program.txt")], check=True)
        gen_data(scale, horizon, intervals, seed, os.path.join(case, "data.txt"), max_width, rich)
        env = dict(os.environ, DYLD_LIBRARY_PATH=DYLD, METEOR_HOME=METEOR_HOME)
        proc = subprocess.run(
            [sys.executable, os.path.join(COMP, "compare.py"), cases,
             "--store", store, "--strategy", strategy, "--meteor-mode", meteor_mode,
             "--mode", mode],
            env=env, capture_output=True, text=True,
        )
        print(proc.stdout.strip())
        return proc.returncode == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", default="100,500,1000,2000")
    ap.add_argument("--program", default="past", choices=list(PROGRAMS),
                    help="workload: past (4 rules) or deep (8 rules, deeper chain+joins)")
    ap.add_argument("--horizon", type=int, default=20)
    ap.add_argument("--intervals", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--store", default="snapshot", choices=["snapshot", "interval"])
    ap.add_argument("--strategy", default="tick", choices=["tick", "interval"])
    ap.add_argument("--max-width", type=int, default=None)
    ap.add_argument("--repeats", type=int, default=1, help="take best of N runs")
    ap.add_argument("--meteor-cap", type=int, default=None,
                    help="skip MeTeoR above this scale (it is the slow baseline)")
    ap.add_argument("--meteor-mode", default="seminaive", choices=["seminaive", "naive"],
                    help="MeTeoR materialization mode (naive = ground truth for Since-over-derived)")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(RUST_BIN):
        print("Rust driver not built. Run:\n  cargo build -p datalogmtl --example meteor_compare",
              file=sys.stderr)
        return 1

    program = PROGRAMS[args.program]
    rich = args.program == "deep"
    scales = [int(s) for s in args.scales.split(",") if s.strip()]
    per_entity = 6 if rich else 3  # base atoms emitted per entity

    # Future operators need static data + the interval engine.
    mode = "static" if args.program == "future" else "streaming"
    strategy = args.strategy
    if mode == "static" and strategy == "tick":
        strategy = "interval"

    if args.verify:
        print("== parity check on generated data (scale={}) ==".format(scales[0]))
        ok = verify(program, scales[0], args.horizon, args.intervals, args.seed,
                    args.store, strategy, args.max_width, rich, args.meteor_mode, mode)
        print("parity: {}\n".format("OK" if ok else "MISMATCH"))

    print("program={}  mode={}  horizon={}  intervals/atom={}  store={}  strategy={}  repeats={}".format(
        args.program, mode, args.horizon, args.intervals, args.store, strategy, args.repeats))
    print("{:>8} {:>11} {:>13} {:>13} {:>9}".format(
        "scale", "facts", "meteor_ms", "rust_ms", "rust/mtr"))
    print("-" * 58)

    with tempfile.TemporaryDirectory() as tmp:
        for n in scales:
            data = os.path.join(tmp, "lubm_{}.txt".format(n))
            gen_data(n, args.horizon, args.intervals, args.seed, data, args.max_width, rich)
            facts = per_entity * n * args.intervals

            skip_meteor = args.meteor_cap is not None and n > args.meteor_cap
            m_ms = None if skip_meteor else best_of(lambda: run_meteor(program, data, args.meteor_mode), args.repeats)
            r_ms = min(run_rust(program, data, args.store, strategy, mode)["reason_ms"]
                       for _ in range(args.repeats))
            ratio = (r_ms / m_ms) if m_ms else float("nan")
            m_str = "-" if m_ms is None else "{:.2f}".format(m_ms)
            ratio_str = "-" if m_ms is None else "{:.3f}".format(ratio)
            print("{:>8} {:>11} {:>13} {:>13.2f} {:>9}".format(
                n, facts, m_str, r_ms, ratio_str))

    return 0


if __name__ == "__main__":
    sys.exit(main())
