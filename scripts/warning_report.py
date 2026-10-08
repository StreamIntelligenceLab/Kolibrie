#!/usr/bin/env python3
"""Report compiler warnings grouped by lint — the burn-down metric for the
architecture work (see the architecture doc, §8).

Usage:
    scripts/warning_report.py                 # whole workspace
    scripts/warning_report.py --by-crate      # also break down per crate
    scripts/warning_report.py --json          # machine-readable, for CI

Counts are per *target*, so a lint firing in a lib and again in that lib's tests
counts twice. That is intentional: it is the number a developer actually sees.
"""
from __future__ import annotations

import argparse
import collections
import json
import subprocess
import sys

CARGO_CMD = [
    "cargo", "check", "--workspace", "--all-targets", "--message-format=json",
]


def collect() -> tuple[collections.Counter, dict[str, collections.Counter]]:
    proc = subprocess.run(CARGO_CMD, capture_output=True, text=True)
    totals: collections.Counter = collections.Counter()
    per_crate: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)

    for line in proc.stdout.splitlines():
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("reason") != "compiler-message":
            continue
        inner = msg["message"]
        if inner.get("level") != "warning":
            continue
        code = (inner.get("code") or {}).get("code") or "other"
        totals[code] += 1
        per_crate[msg.get("target", {}).get("name", "?")][code] += 1

    return totals, per_crate


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--by-crate", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    totals, per_crate = collect()

    if args.json:
        json.dump(
            {
                "total": sum(totals.values()),
                "by_lint": dict(totals),
                "by_target": {k: dict(v) for k, v in per_crate.items()},
            },
            sys.stdout,
            indent=2,
        )
        print()
        return 0

    print("warnings by lint")
    for code, n in totals.most_common():
        print(f"  {n:6d}  {code}")
    print(f"  {sum(totals.values()):6d}  TOTAL")

    if args.by_crate:
        print("\nwarnings by target")
        ranked = collections.Counter({k: sum(v.values()) for k, v in per_crate.items()})
        for target, n in ranked.most_common():
            print(f"  {n:6d}  {target}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
