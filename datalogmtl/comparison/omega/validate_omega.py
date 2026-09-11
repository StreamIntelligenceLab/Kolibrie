#!/usr/bin/env python3
"""
Validate the datalogmtl ω-materialization's entailment answers against MeTeoR's
**canonical model** (its ground truth for infinite/eventually-periodic models).

For a program + data + a list of query facts `Pred(args)@[l,r]`:
  - datalogmtl:  meteor_compare --strategy omega --entail queries.txt
  - MeTeoR:      CanonicalRepresentation + find_periods + fact_entailment
Both must agree on every query (including far-future ones), which the finite
materializers cannot answer.

Usage:
    PYTHONPATH set to $METEOR_HOME automatically.
    python validate_omega.py [case_dir]   (default: this directory)
"""
import contextlib
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
METEOR_HOME = os.environ.get("METEOR_HOME", "/Users/u0164257/Documents/Github/MeTeoR")
DYLD = os.environ.get(
    "DYLD_LIBRARY_PATH",
    "/opt/homebrew/Cellar/python@3.11/3.11.11/Frameworks/Python.framework/Versions/3.11/lib",
)


def rust_binary():
    for p in ("release", "debug"):
        b = os.path.join(REPO_ROOT, "target", p, "examples", "meteor_compare")
        if os.path.exists(b):
            return b
    sys.exit("build first: cargo build -p datalogmtl --example meteor_compare")


def run_rust(program, data, queries):
    env = dict(os.environ, DYLD_LIBRARY_PATH=DYLD)
    out = subprocess.run(
        [rust_binary(), "--program", program, "--data", data,
         "--strategy", "omega", "--mode", "static", "--entail", queries],
        env=env, cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.exit("rust omega failed:\n" + out.stderr)
    ans = {}
    for line in out.stdout.splitlines():
        fact, val = line.rsplit("\t", 1)
        ans[fact.strip()] = (val.strip() == "true")
    return ans


def run_meteor(program, data, query_lines):
    sys.path.insert(0, METEOR_HOME)
    from meteor_reasoner.utils.loader import load_program, load_dataset
    from meteor_reasoner.materialization.coalesce import coalescing_d
    from meteor_reasoner.materialization.index_build import build_index
    from meteor_reasoner.canonical.canonical_representation import CanonicalRepresentation
    from meteor_reasoner.canonical.utils import find_periods, fact_entailment
    from meteor_reasoner.utils.parser import parse_str_fact
    from meteor_reasoner.classes.atom import Atom

    prog = load_program([l for l in open(program).read().splitlines() if l.strip()])
    D = load_dataset([l for l in open(data).read().splitlines() if l.strip()])
    with open(os.devnull, "w") as dn, contextlib.redirect_stdout(dn):
        coalescing_d(D)
        build_index(D)
        cr = CanonicalRepresentation(D, prog)
        cr.initilization()
        d1, common, vl, lp, ll, vr, rp, rl = find_periods(cr)

    ans = {}
    for q in query_lines:
        pred, ent, itv = parse_str_fact(q)
        with open(os.devnull, "w") as dn, contextlib.redirect_stdout(dn):
            ans[q] = bool(fact_entailment(d1, Atom(pred, ent, itv), common, lp, ll, rp, rl))
    return ans


def main():
    case = os.path.abspath(sys.argv[1]) if len(sys.argv) > 1 else HERE
    program = os.path.join(case, "program.txt")
    data = os.path.join(case, "data.txt")
    queries = os.path.join(case, "queries.txt")
    query_lines = [l.strip() for l in open(queries) if l.strip()]

    rust = run_rust(program, data, queries)
    meteor = run_meteor(program, data, query_lines)

    ok = True
    print("{:<32} {:>8} {:>8}".format("query", "meteor", "omega"))
    for q in query_lines:
        m = meteor.get(q)
        r = rust.get(q)
        mark = "OK" if m == r else "MISMATCH"
        if m != r:
            ok = False
        print("{:<32} {:>8} {:>8}   {}".format(q, str(m), str(r), mark))
    print("\n{}".format("ALL MATCH" if ok else "MISMATCH"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
