#!/usr/bin/env python3
"""
Run the MeTeoR reference reasoner on a program + data file and print the full
materialization as normalized, coalesced integer-interval lines:

    Pred(args)@[l,r]

Each MeTeoR (possibly open) interval is expanded to the integer points it covers
within the comparison window [0, T], then re-coalesced into closed integer
intervals. This matches the point-grid form emitted by the Rust `meteor_compare`
example so the two can be diffed line-for-line.

Usage:
    PYTHONPATH=/path/to/MeTeoR python run_meteor.py --program P --data D --horizon T
"""
import argparse
import contextlib
import os
import sys
from collections import defaultdict

from meteor_reasoner.utils.loader import load_program, load_dataset
from meteor_reasoner.materialization.materialize import materialize
from meteor_reasoner.materialization.coalesce import coalescing_d


def interval_int_points(interval, horizon):
    """Integer points in [0, horizon] covered by a MeTeoR interval."""
    lo = interval.left_value
    hi = interval.right_value

    # Clamp infinities / out-of-window bounds to the comparison window.
    try:
        lo_f = float(lo)
    except (ValueError, OverflowError):
        lo_f = float("-inf")
    try:
        hi_f = float(hi)
    except (ValueError, OverflowError):
        hi_f = float("inf")

    if lo_f == float("-inf") or lo_f < 0:
        first = 0
    else:
        first = int(lo)
        if interval.left_open and first == lo:
            first += 1
        elif first < lo:            # non-integer closed lower bound
            first += 1

    if hi_f == float("inf") or hi_f > horizon:
        last = horizon
    else:
        last = int(hi)
        if interval.right_open and last == hi:
            last -= 1
        # closed upper bound with fractional value: int() already floors

    if last < first:
        return []
    first = max(first, 0)
    last = min(last, horizon)
    return list(range(first, last + 1))


def coalesce(points):
    """Sorted set of ints -> list of maximal closed (lo, hi) intervals."""
    out = []
    for t in sorted(points):
        if out and out[-1][1] + 1 == t:
            out[-1] = (out[-1][0], t)
        elif out and out[-1][1] == t:
            pass
        else:
            out.append((t, t))
    return out


def atom_string(predicate, entity):
    """MeTeoR atom text: propositional P, or P(a,b)."""
    names = [term.name for term in entity if term.name != "nan"]
    if not names:
        return predicate
    return "{}({})".format(predicate, ",".join(names))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--program", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--horizon", type=int, required=True)
    ap.add_argument("--mode", default="seminaive")
    ap.add_argument("--K", type=int, default=1000)
    args = ap.parse_args()

    # MeTeoR's materialize() prints progress to stdout; keep our stdout clean.
    with open(os.devnull, "w") as devnull, contextlib.redirect_stdout(devnull):
        rules = load_program(args.program)
        D = load_dataset(args.data)
        materialize(D, rules, mode=args.mode, K=args.K)
        coalescing_d(D)

    # atom string -> set of integer timepoints
    points = defaultdict(set)
    for predicate in D:
        entries = D[predicate]
        if isinstance(entries, list):  # propositional predicate
            atom = predicate
            for interval in entries:
                points[atom].update(interval_int_points(interval, args.horizon))
        else:
            for entity, intervals in entries.items():
                atom = atom_string(predicate, entity)
                for interval in intervals:
                    points[atom].update(interval_int_points(interval, args.horizon))

    lines = []
    for atom, pts in points.items():
        for lo, hi in coalesce(pts):
            lines.append("{}@[{},{}]".format(atom, lo, hi))
    lines.sort()
    print("\n".join(lines))


if __name__ == "__main__":
    sys.exit(main())
