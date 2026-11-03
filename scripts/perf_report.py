#!/usr/bin/env python3
"""Summarise and compare criterion benchmark results.

Criterion stores its data under `target/criterion`, which is gitignored and
therefore vanishes on `cargo clean` and is never comparable across machines or
across time. This script extracts a small, diffable summary that can be committed
to `perf-baseline/`, and compares a later run against it.

Usage:
    scripts/perf_report.py collect                       # summarise the last run
    scripts/perf_report.py collect -o /tmp/after.json
    scripts/perf_report.py baseline RUN1 RUN2 RUN3 RUN4 -o perf-baseline/baseline.json
    scripts/perf_report.py compare perf-baseline/baseline.json
    scripts/perf_report.py compare BASE --current /tmp/after.json
    scripts/perf_report.py noise RUN1 RUN2               # measure the floor

How much to trust a delta (measured on this repo, Oct 2026, M-series laptop)
---------------------------------------------------------------------------
Three kinds of variation, each larger than the last:

    two passes back-to-back        median 1.5%   max  8.7%
    across sessions, same code     median 3.0%   max 12.5%
    repeated runs, one benchmark                 up to ~9-12%

The third row is the one that matters and the one that was missed at first. Noise
was originally derived from two back-to-back passes, which measures short-term
REPEATABILITY — and two samples cannot estimate a spread at all. That produced two
false positives:

    micro_scan_selective_subject/dyn_trait   recorded 2.0%,  actual ~10%
    execution_hot_paths/filter_numeric       recorded 0.22%, actual ~9%

Four runs of one unchanged `filter_numeric` binary spanned 2.056-2.242 ms, so a
single before/after pair reported a 5.7% "regression" that four runs in each state
showed to be absent. `baseline` now takes N runs and records the full observed
range; it warns when given fewer than four.

So:

  * Build the baseline from 4+ runs. `compare` labels results from a thinner one
    `[thin baseline]`, and flags deltas under 1.5x their limit as borderline.
  * To judge a change, run before AND after in the SAME session. If a delta is
    near its limit, repeat both states 3-4 times before believing it — that is
    what distinguished a real regression from noise in every case so far.
  * `compare` against a `collect` summary silently falls back to the flat floor,
    because a collect file carries no noise data. It now warns; prefer a baseline.
  * Always compare like-for-like: the same tier, same filter, run the same way.
    Noise depends on the invocation, not just the benchmark — `filter_numeric`
    spreads 1.9% across full tier passes and 9.0% when run as a filtered single
    case after a rebuild. A 4-run baseline fixes the arithmetic; it cannot
    anticipate an invocation pattern it never saw.
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
# Below this many runs, a baseline's noise figures are not worth trusting: two
# samples cannot estimate a spread at all, and three barely can.
MIN_USEFUL_RUNS = 4


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
    """Build a baseline from N unchanged runs, recording per-benchmark noise.

    Why N and not 2
    ---------------
    This took two runs and derived noise from the gap between them. That measures
    short-term *repeatability*, not the variance a real before/after comparison has
    to survive, and it produced two false positives that each cost an afternoon:

        micro_scan_selective_subject/dyn_trait   recorded 2.0%, actual ~10%
        execution_hot_paths/filter_numeric       recorded 0.22%, actual ~9%

    The second is the clearest case: four runs of one unchanged binary spanned
    2.056-2.242 ms, so a single before/after pair reported a 5.7% "regression"
    against a baseline claiming 0.22% noise. Two samples cannot estimate a spread.

    Noise is the full observed range, (max - min) / min, which is deliberately
    conservative: it is better to miss a marginal regression than to cry wolf, since
    a harness that reports phantom regressions stops being read. If that makes a
    benchmark too insensitive to be useful, the fix is to make the benchmark quieter
    — `micro_scan` dropped SUBJECTS from 20k to 5k for exactly this reason — not to
    narrow the threshold until it fires.
    """
    runs = []
    for path in args.runs:
        data = json.loads(pathlib.Path(path).read_text())["benchmarks"]
        runs.append((path, data))

    if len(runs) < 2:
        sys.exit("need at least 2 runs; 4 or more gives a usable spread")
    if len(runs) < MIN_USEFUL_RUNS:
        print(f"warning: {len(runs)} runs is thin. Two samples cannot estimate a "
              f"spread, and three barely can; prefer {MIN_USEFUL_RUNS} or more.")

    # Keys only: Counter.update on a dict-of-dicts tries to add the values.
    seen: set[str] = set()
    for _, data in runs:
        seen.update(data.keys())

    benchmarks, thin = {}, []
    for bench in sorted(seen):
        medians = [d[bench]["median_ns"] for _, d in runs if bench in d]
        if len(medians) < 2:
            thin.append(bench)
            continue
        mads = [d[bench].get("mad_ns", 0.0) for _, d in runs if bench in d]
        lo, hi = min(medians), max(medians)
        benchmarks[bench] = {
            # Median of the run medians: more robust than a mean once N > 2, and it
            # does not let one slow run drag the reference point.
            "median_ns": statistics.median(medians),
            "mad_ns": statistics.median(mads),
            # Full range, not a deviation: see the note above.
            "noise_pct": ((hi - lo) / lo * 100) if lo > 0 else 0.0,
            # Recorded so `compare` can say how much to trust each figure.
            "runs": len(medians),
        }

    if not benchmarks:
        sys.exit("the runs share no benchmarks")
    if thin:
        print(f"warning: {len(thin)} benchmarks appeared in only one run and were "
              f"excluded (no spread can be computed from one sample)")

    path = pathlib.Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(
        {"runs": len(runs), "benchmarks": benchmarks}, indent=2, sort_keys=True) + "\n")

    noises = sorted(v["noise_pct"] for v in benchmarks.values())
    print(f"wrote {len(benchmarks)} benchmarks from {len(runs)} runs to {path}")
    print(f"noise: median {statistics.median(noises):.1f}%  "
          f"p90 {noises[int(len(noises) * 0.9)]:.1f}%  max {max(noises):.1f}%")
    worst = sorted(benchmarks.items(), key=lambda kv: -kv[1]["noise_pct"])[:5]
    print("\nnoisiest, which are the ones least able to detect a regression:")
    for bench, v in worst:
        print(f"  {v['noise_pct']:5.1f}%  ({v['runs']} runs)  {bench}")
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    base_path = pathlib.Path(args.baseline)
    if not base_path.is_file():
        sys.exit(f"baseline not found: {base_path}")
    base_doc = json.loads(base_path.read_text())
    base = base_doc["benchmarks"]

    # A `collect` summary has no noise data, so every comparison against one silently
    # falls back to the flat floor. That is how filter_numeric's phantom +5.7% got
    # through: it was compared against a collect file, not a baseline.
    if not any("noise_pct" in v for v in base.values()):
        print(f"warning: {base_path} carries no noise data — it looks like a "
              f"`collect` summary rather than a `baseline`.\n"
              f"         Falling back to the flat ±{args.threshold:g}% floor, which "
              f"over-reports on noisy benchmarks.\n"
              f"         Build a real baseline with: perf_report.py baseline RUN1 "
              f"RUN2 RUN3 RUN4 -o {base_path}\n")
    elif base_doc.get("runs", 0) and base_doc["runs"] < MIN_USEFUL_RUNS:
        print(f"warning: baseline was built from only {base_doc['runs']} runs, so its "
              f"noise figures likely understate the real spread.\n")

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

        runs = ref.get("runs")
        if delta > limit:
            regressions.append((bench, before, after, delta, limit, runs))
        elif delta < -limit:
            improvements.append((bench, before, after, delta, limit, runs))

    added = sorted(set(current) - set(base))

    def show(title: str, rows: list) -> None:
        if not rows:
            return
        print(f"\n{title}")
        for bench, before, after, delta, limit, runs in rows:
            # A delta that only just clears a limit derived from few runs is the
            # shape both past false positives had; say so at the point of reporting.
            weak = " [thin baseline]" if runs and runs < MIN_USEFUL_RUNS else ""
            close = " [within 1.5x of the limit]" if abs(delta) < limit * 1.5 else ""
            print(f"  {delta:+6.1f}%  (>{limit:4.1f}%)  "
                  f"{_fmt(before)} -> {_fmt(after):<12}  {bench}{weak}{close}")

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
                        help="build a noise-aware baseline from N unchanged runs")
    bl.add_argument("runs", nargs="+", metavar="RUN",
                    help=f"two or more collect summaries of unchanged code; "
                         f"{MIN_USEFUL_RUNS}+ recommended")
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
