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
PROGRAM = os.path.join(HERE, "programs", "lubm_past.txt")
METEOR_HOME = os.environ.get("METEOR_HOME", "/Users/u0164257/Documents/Github/MeTeoR")
DYLD = os.environ.get(
    "DYLD_LIBRARY_PATH",
    "/opt/homebrew/Cellar/python@3.11/3.11.11/Frameworks/Python.framework/Versions/3.11/lib",
)
RUST_BIN_RELEASE = os.path.join(REPO_ROOT, "target", "release", "examples", "meteor_compare")
RUST_BIN_DEBUG = os.path.join(REPO_ROOT, "target", "debug", "examples", "meteor_compare")
RUST_BIN = RUST_BIN_RELEASE if os.path.exists(RUST_BIN_RELEASE) else RUST_BIN_DEBUG


def gen_data(scale, horizon, intervals, seed, out):
    subprocess.run(
        [sys.executable, os.path.join(HERE, "gen_lubm.py"),
         "--scale", str(scale), "--horizon", str(horizon),
         "--intervals", str(intervals), "--seed", str(seed), "--out", out],
        check=True, capture_output=True, text=True,
    )


def run_rust(data, store):
    env = dict(os.environ, DYLD_LIBRARY_PATH=DYLD)
    cmd = [RUST_BIN, "--program", PROGRAM, "--data", data,
           "--store", store, "--timing"]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError("rust engine failed:\n" + proc.stderr)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def run_meteor(data):
    env = dict(os.environ, PYTHONPATH=METEOR_HOME)
    cmd = [sys.executable, os.path.join(HERE, "run_meteor_perf.py"),
           "--program", PROGRAM, "--data", data]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError("meteor reasoner failed:\n" + proc.stderr)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def best_of(fn, repeats):
    return min(fn()["reason_ms"] for _ in range(repeats))


def verify(scale, horizon, intervals, seed, store):
    """Confirm both engines still agree on a generated dataset via compare.py."""
    with tempfile.TemporaryDirectory() as tmp:
        cases = os.path.join(tmp, "cases")
        case = os.path.join(cases, "gen")
        os.makedirs(case)
        subprocess.run(["cp", PROGRAM, os.path.join(case, "program.txt")], check=True)
        gen_data(scale, horizon, intervals, seed, os.path.join(case, "data.txt"))
        env = dict(os.environ, DYLD_LIBRARY_PATH=DYLD, METEOR_HOME=METEOR_HOME)
        proc = subprocess.run(
            [sys.executable, os.path.join(COMP, "compare.py"), cases,
             "--store", store],
            env=env, capture_output=True, text=True,
        )
        print(proc.stdout.strip())
        return proc.returncode == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", default="100,500,1000,2000")
    ap.add_argument("--horizon", type=int, default=20)
    ap.add_argument("--intervals", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--store", default="snapshot", choices=["snapshot", "interval"])
    ap.add_argument("--repeats", type=int, default=1, help="take best of N runs")
    ap.add_argument("--verify", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(RUST_BIN):
        print("Rust driver not built. Run:\n  cargo build -p datalogmtl --example meteor_compare",
              file=sys.stderr)
        return 1

    scales = [int(s) for s in args.scales.split(",") if s.strip()]

    if args.verify:
        print("== parity check on generated data (scale={}) ==".format(scales[0]))
        ok = verify(scales[0], args.horizon, args.intervals, args.seed, args.store)
        print("parity: {}\n".format("OK" if ok else "MISMATCH"))

    print("horizon={}  intervals/atom={}  store={}  repeats={}".format(
        args.horizon, args.intervals, args.store, args.repeats))
    print("{:>8} {:>10} {:>13} {:>13} {:>9}".format(
        "scale", "facts", "meteor_ms", "rust_ms", "rust/mtr"))
    print("-" * 58)

    with tempfile.TemporaryDirectory() as tmp:
        for n in scales:
            data = os.path.join(tmp, "lubm_{}.txt".format(n))
            gen_data(n, args.horizon, args.intervals, args.seed, data)
            facts = 3 * n * args.intervals

            m_ms = best_of(lambda: run_meteor(data), args.repeats)
            r = run_rust(data, args.store)
            r_ms = min([r["reason_ms"]] +
                       [run_rust(data, args.store)["reason_ms"] for _ in range(args.repeats - 1)])
            ratio = r_ms / m_ms if m_ms else float("nan")
            print("{:>8} {:>10} {:>13.2f} {:>13.2f} {:>9.2f}".format(
                n, facts, m_ms, r_ms, ratio))

    return 0


if __name__ == "__main__":
    sys.exit(main())
