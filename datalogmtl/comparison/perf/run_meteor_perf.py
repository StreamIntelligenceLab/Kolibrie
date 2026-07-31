#!/usr/bin/env python3
"""
Time MeTeoR's materialization on a program + data file and print a JSON summary:

    {"reason_ms": <float>, "native_facts": <int>}

Only the reasoning (`materialize`) is timed, to match the Rust driver, which times
its tick loop and not parsing. `native_facts` counts the coalesced (predicate,
entity, interval) rows MeTeoR holds after materialization.

Usage:
    PYTHONPATH=$METEOR_HOME python run_meteor_perf.py --program P --data D [--K 1000]
"""
import argparse
import contextlib
import os
import time

from meteor_reasoner.utils.loader import load_program, load_dataset
from meteor_reasoner.materialization.materialize import materialize
from meteor_reasoner.materialization.coalesce import coalescing_d


def count_facts(D):
    total = 0
    for predicate in D:
        entries = D[predicate]
        if isinstance(entries, list):
            total += len(entries)
        else:
            for _entity, intervals in entries.items():
                total += len(intervals)
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--program", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--K", type=int, default=1000)
    ap.add_argument("--mode", default="seminaive", choices=["seminaive", "naive"])
    args = ap.parse_args()

    with open(os.devnull, "w") as devnull, contextlib.redirect_stdout(devnull):
        rules = load_program(args.program)
        D = load_dataset(args.data)

        start = time.perf_counter()
        materialize(D, rules, mode=args.mode, K=args.K)
        coalescing_d(D)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        facts = count_facts(D)

    print('{{"reason_ms": {:.3f}, "native_facts": {}}}'.format(elapsed_ms, facts))


if __name__ == "__main__":
    main()
