#!/usr/bin/env python3
"""Summarise and compare criterion benchmark results.

Criterion stores its data under `target/criterion`, which is gitignored and
therefore vanishes on `cargo clean` and is never comparable across machines or
across time. This script extracts a small, diffable summary that can be committed
to `perf-baseline/`, and compares a later run against it.

Usage:
    scripts/perf_report.py collect                       # summarise the last run
    scripts/perf_report.py collect -o /tmp/after.json
    scripts/perf_report.py baseline RUN1 RUN2 -o perf-baseline/baseline.json
    scripts/perf_report.py compare perf-baseline/baseline.json
    scripts/perf_report.py compare BASE --current /tmp/after.json
    scripts/perf_report.py noise RUN1 RUN2               # measure the floor

How much to trust a delta (measured on this repo, Oct 2026, M-series laptop)
---------------------------------------------------------------------------
Two kinds of variation were measured, and they differ by ~4x:

    within-session  (two passes back-to-back)     median 1.5%   max  8.7%
    cross-session   (hot machine vs cooled)       median 3.0%   max 12.5%

The cross-session figure is the one that bites. A baseline taken weeks ago on a
differently-loaded machine can disagree with an unmodified checkout by ~10% on
individual benchmarks; this was observed here, where `micro_pattern/all_variables`
read 1.244 ms in one session and 1.119 ms in the next with identical source. Two
back-to-back passes measure short-term REPEATABILITY, not reproducibility.

So:

  * To judge a change, run before AND after in the SAME session, back-to-back,
    and compare the two collected files. This is the authoritative procedure.
  * The committed `perf-baseline/baseline.json` is a historical reference for
    catching gross drift, not a precision instrument. Treat sub-12% differences
    against it as unproven.
  * Always compare like-for-like: the same tier, run the same way. A single-bench
    run is not comparable to a full-tier baseline.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys

CRITERION_DIR = pathlib.Path("target/criterion")
# Below this, a difference is indistinguishable from scheduling noise on a typical
# developer machine. Override with --threshold once the real noise floor is known.
DEFAULT_THRESHOLD_PCT = 5.0
# A delta must clear this multiple of the benchmark's own measured noise. 2.0 is a
# judgement call, not a statistic: it keeps the quiet benchmarks sensitive while
# not flagging the allocation-heavy ones on every run.
DEFAULT_NOISE_MARGIN = 2.0


def _iter_estimates() -> list[tuple[str, float, float]]:
    """Yield (benchmark id, median ns, median absolute deviation ns)."""
    if not CRITERION_DIR.is_dir():
        sys.exit(f"{CRITERION_DIR} not found — run `cargo bench` first.")

    out: list[tuple[str, float, float]] = []
    for est in CRITERION_DIR.rglob("new/estimates.json"):
        # .../target/criterion/<group>/<bench>/new/estimates.json
        bench_id = str(est.parent.parent.relative_to(CRITERION_DIR))
        try:
            data = json.loads(est.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        median = data.get("median", {}).get("point_estimate")
        mad = data.get("median_abs_dev", {}).get("point_estimate", 0.0)
        if median is None:
            continue
        out.append((bench_id, float(median), float(mad)))
    out.sort()
    return out


def _fmt(ns: float) -> str:
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.3f} {unit}"
    return f"{ns:.1f} ns"


def cmd_collect(args: argparse.Namespace) -> int:
    rows = _iter_estimates()
    if not rows:
        sys.exit("no criterion estimates found — did `cargo bench` actually run?")

    summary = {
        "benchmarks": {
            bench: {"median_ns": median, "mad_ns": mad} for bench, median, mad in rows
        }
    }

    if args.output:
        path = pathlib.Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(f"wrote {len(rows)} benchmarks to {path}")
    else:
        for bench, median, mad in rows:
            # MAD relative to the median is the per-benchmark noise indicator: a
            # benchmark with 10% spread cannot detect a 5% regression.
            spread = (mad / median * 100) if median else 0.0
            print(f"  {_fmt(median):>12}  ±{spread:4.1f}%  {bench}")
        print(f"\n  {len(rows)} benchmarks")
    return 0


def cmd_baseline(args: argparse.Namespace) -> int:
    """Build a baseline from two unchanged runs, recording per-benchmark noise.

    Two runs are required rather than one because a baseline without a noise
    figure cannot answer "is this delta real?". Benchmarks here differ in noise by
    more than an order of magnitude (0.1% to 10.7%), so a single global threshold
    is either blind to real regressions in the quiet ones or cries wolf on the
    noisy ones.
    """
    a = json.loads(pathlib.Path(args.first).read_text())["benchmarks"]
    b = json.loads(pathlib.Path(args.second).read_text())["benchmarks"]

    shared = sorted(set(a) & set(b))
    if not shared:
        sys.exit("the two runs share no benchmarks")

    benchmarks = {}
    for bench in shared:
        m_a, m_b = a[bench]["median_ns"], b[bench]["median_ns"]
        lo = min(m_a, m_b)
        benchmarks[bench] = {
            # Mean of the two medians: a single run's median is itself a sample.
            "median_ns": (m_a + m_b) / 2,
            "mad_ns": (a[bench].get("mad_ns", 0.0) + b[bench].get("mad_ns", 0.0)) / 2,
            "noise_pct": (abs(m_a - m_b) / lo * 100) if lo > 0 else 0.0,
        }

    only_a, only_b = sorted(set(a) - set(b)), sorted(set(b) - set(a))
    if only_a or only_b:
        print(f"warning: {len(only_a) + len(only_b)} benchmarks appeared in only one "
              f"run and were excluded")

    summary = {"benchmarks": benchmarks}
    path = pathlib.Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

    noises = [v["noise_pct"] for v in benchmarks.values()]
    print(f"wrote {len(benchmarks)} benchmarks to {path}")
    print(f"noise: median {statistics.median(noises):.1f}%  max {max(noises):.1f}%")
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    base_path = pathlib.Path(args.baseline)
    if not base_path.is_file():
        sys.exit(f"baseline not found: {base_path}")
    base = json.loads(base_path.read_text())["benchmarks"]

    if args.current:
        # Comparing two saved summaries: lets you re-check an old comparison, or
        # diff results captured on two different machines, without re-running.
        current = json.loads(pathlib.Path(args.current).read_text())["benchmarks"]
    else:
        current = {b: {"median_ns": m, "mad_ns": d} for b, m, d in _iter_estimates()}

    if not current:
        sys.exit("no current results — run `cargo bench` first.")

    regressions, improvements, missing, added = [], [], [], []

    for bench, ref in sorted(base.items()):
        if bench not in current:
            missing.append(bench)
            continue
        before = ref["median_ns"]
        after = current[bench]["median_ns"]
        if before <= 0:
            continue
        delta = (after - before) / before * 100

        # A benchmark that varies 10% between identical runs cannot report a 6%
        # regression. Where the baseline recorded per-benchmark noise, require the
        # delta to clear a multiple of it; otherwise fall back to the global figure.
        noise = ref.get("noise_pct")
        limit = args.threshold
        if noise is not None and not args.flat_threshold:
            limit = max(limit, noise * args.noise_margin)

        if delta > limit:
            regressions.append((bench, before, after, delta, limit))
        elif delta < -limit:
            improvements.append((bench, before, after, delta, limit))

    added = sorted(set(current) - set(base))

    def show(title: str, rows: list) -> None:
        if not rows:
            return
        print(f"\n{title}")
        for bench, before, after, delta, limit in rows:
            print(f"  {delta:+6.1f}%  (>{limit:4.1f}%)  "
                  f"{_fmt(before)} -> {_fmt(after):<12}  {bench}")

    show(f"REGRESSIONS (floor +{args.threshold:g}%, noise-aware)", regressions)
    show(f"improvements (floor -{args.threshold:g}%, noise-aware)", improvements)

    if missing:
        print(f"\nin baseline but not measured ({len(missing)}):")
        for bench in missing:
            print(f"  {bench}")
    if added:
        print(f"\nnew since baseline ({len(added)}):")
        for bench in added:
            print(f"  {bench}")

    unchanged = len(base) - len(regressions) - len(improvements) - len(missing)
    print(
        f"\n{unchanged} within ±{args.threshold:g}%, "
        f"{len(regressions)} regressed, {len(improvements)} improved"
    )

    # Exit non-zero on regression so this can gate a local workflow if wanted.
    # Deliberately NOT wired into CI: shared-runner variance routinely exceeds the
    # regressions worth catching, and a gate that cries wolf gets ignored.
    return 1 if regressions and args.fail_on_regression else 0


def cmd_noise(args: argparse.Namespace) -> int:
    """Compare two baseline files taken with no code change between them."""
    a = json.loads(pathlib.Path(args.first).read_text())["benchmarks"]
    b = json.loads(pathlib.Path(args.second).read_text())["benchmarks"]
    deltas = []
    print("per-benchmark run-to-run variation with no code change:\n")
    for bench in sorted(set(a) & set(b)):
        before, after = a[bench]["median_ns"], b[bench]["median_ns"]
        if before <= 0:
            continue
        delta = abs(after - before) / before * 100
        deltas.append(delta)
        print(f"  {delta:5.1f}%  {bench}")
    if deltas:
        print(
            f"\n  median {statistics.median(deltas):.1f}%   "
            f"p90 {sorted(deltas)[int(len(deltas) * 0.9)]:.1f}%   "
            f"max {max(deltas):.1f}%"
        )
        print(
            "\n  Use a threshold above the max: anything smaller is noise, not signal."
        )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("collect", help="summarise the last cargo bench run")
    c.add_argument("-o", "--output", help="write JSON summary here")
    c.set_defaults(func=cmd_collect)

    p = sub.add_parser("compare", help="compare the last run against a baseline")
    p.add_argument("baseline")
    p.add_argument("--current", help="compare against this saved summary instead of "
                                     "the last cargo bench run")
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD_PCT,
                   help=f"percent change treated as signal (default {DEFAULT_THRESHOLD_PCT})")
    p.add_argument("--noise-margin", type=float, default=DEFAULT_NOISE_MARGIN,
                   help="multiple of a benchmark's recorded noise that a delta must "
                        f"clear (default {DEFAULT_NOISE_MARGIN})")
    p.add_argument("--flat-threshold", action="store_true",
                   help="ignore recorded per-benchmark noise and use --threshold alone")
    p.add_argument("--fail-on-regression", action="store_true")
    p.set_defaults(func=cmd_compare)

    bl = sub.add_parser("baseline",
                        help="build a noise-aware baseline from two unchanged runs")
    bl.add_argument("first")
    bl.add_argument("second")
    bl.add_argument("-o", "--output", required=True)
    bl.set_defaults(func=cmd_baseline)

    n = sub.add_parser("noise", help="measure the noise floor from two unchanged runs")
    n.add_argument("first")
    n.add_argument("second")
    n.set_defaults(func=cmd_noise)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
