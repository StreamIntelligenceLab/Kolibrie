#!/usr/bin/env python3
"""
Generate a scalable temporal-LUBM dataset for the past-fragment program
`perf/programs/lubm_past.txt`, mirroring MeTeoR's methodology of taking LUBM-style
entities/relations and assigning each atom several random temporal intervals.

Base predicates produced (arity <= 2, matching lubm_past.txt):
  - UndergraduateStudent(u_i)
  - GraduateStudent(g_i)
  - publicationAuthor(pub_i, u_i)   # publication authored by student u_i

Scaling is by `--scale N` (entity count). Each atom gets `--intervals k` random
closed integer intervals within `[0, --horizon H]`. Output is MeTeoR fact syntax
(`Pred(args)@[l,r]`), the same format consumed by the correctness harness and by
the Rust `meteor_compare` driver, so both engines read identical input.

Usage:
    python gen_lubm.py --scale 500 --horizon 20 --intervals 3 --seed 42 --out data.txt
"""
import argparse
import random
import sys


def random_intervals(rng, k, horizon, max_width=None):
    """k random closed integer intervals within [0, horizon], coalesced.

    `max_width` caps each interval's width (default: unbounded, i.e. up to the
    horizon). Small widths over a large horizon model bursty/sparse event data.

    Overlapping OR integer-adjacent intervals (gap <= 1) are merged. This keeps
    the workload unambiguous between integer-time (datalogmtl) and real-time
    (MeTeoR): without it, two integer-adjacent intervals like [3,12] and [13,15]
    look contiguous on the integer grid but have a real gap (12,13) for MeTeoR,
    making universal operators (Box) disagree.
    """
    raw = []
    for _ in range(k):
        a = rng.randint(0, horizon)
        hi = horizon if max_width is None else min(horizon, a + max_width)
        b = rng.randint(a, hi)
        raw.append((a, b))
    raw.sort()
    merged = []
    for (a, b) in raw:
        if merged and a <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, required=True, help="entity count N")
    ap.add_argument("--horizon", type=int, default=20)
    ap.add_argument("--intervals", type=int, default=3, help="intervals per atom")
    ap.add_argument("--max-width", type=int, default=None,
                    help="cap each interval's width (default: up to horizon)")
    ap.add_argument("--rich", action="store_true",
                    help="also emit teachingAssistant/takesCourse/advisor (for lubm_deep)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="-", help="output file ('-' for stdout)")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    lines = []

    def emit(atom):
        for (a, b) in random_intervals(rng, args.intervals, args.horizon, args.max_width):
            lines.append("{}@[{},{}]".format(atom, a, b))

    n = args.scale
    for i in range(n):
        emit("UndergraduateStudent(u{})".format(i))
        emit("GraduateStudent(g{})".format(i))
        emit("publicationAuthor(pub{},u{})".format(i, i))
        if args.rich:
            emit("takesCourse(u{},c{})".format(i, i))
            emit("advisor(u{},p{})".format(i, i))
            if i % 2 == 0:
                emit("teachingAssistant(u{})".format(i))

    text = "\n".join(lines) + "\n"
    if args.out == "-":
        sys.stdout.write(text)
    else:
        with open(args.out, "w") as f:
            f.write(text)
        print("wrote {} facts ({} atoms x {} intervals) to {}".format(
            len(lines), 3 * n, args.intervals, args.out), file=sys.stderr)


if __name__ == "__main__":
    main()
