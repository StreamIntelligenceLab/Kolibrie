#!/usr/bin/env python3
"""Run every example that can finish on its own, and report the ones that cannot.

`verify.sh` compile-checks the examples but never ran them, which is how three of
them stayed broken: one panicked on a rule syntax the parser does not accept, one
read an absolute path into a personal directory, and one read a gitignored dataset.
A compile check sees none of that.

Only examples with no `required-features` are run. The eleven behind `ml` need
preconditions this cannot supply -- a trusted-local training directory, a model
allowlist, the MNIST corpus -- so running them here would report environment setup
as failure.

Usage:
    scripts/smoke_examples.py [--timeout SECONDS] [--list] [outdir]

Exits non-zero if any example fails, naming each one. Output is kept per example so
a failure can be diagnosed without re-running.

Takes about a minute, and two streaming demos are most of it: `continious_stream`
and `automate_policy` sleep through timed windows for roughly 24s and 21s, against
2.4s for the other 43 together. They are waiting, not computing, so this costs
wall-clock rather than CPU.
"""

import argparse
import json
import pathlib
import subprocess
import sys

# These cannot finish unattended, and that is what they are for rather than a
# defect. Each needs something this script will not start:
SKIP = {
    "drone_traffic_safety_demo": "serves HTTP until interrupted",
    "http_check": "expects kolibrie-http-server to be running",
    "mqtt_example": "needs an MQTT broker",
    "mqtt_real_scenario": "needs an MQTT broker",
}


def examples(root: pathlib.Path) -> list[str]:
    """Example targets of the `kolibrie` package that need no extra features.

    Read from `cargo metadata` rather than a list kept here, so an example added
    tomorrow is covered without anyone remembering to add it.
    """
    meta = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=root, capture_output=True, text=True, check=True,
        ).stdout
    )
    package = next(p for p in meta["packages"] if p["name"] == "kolibrie")
    return sorted(
        target["name"]
        for target in package["targets"]
        if "example" in target["kind"] and not target.get("required-features")
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("outdir", nargs="?", default="/tmp/kolibrie-verify")
    parser.add_argument("--timeout", type=int, default=120,
                        help="seconds per example (default: 120)")
    parser.add_argument("--list", action="store_true",
                        help="print what would run, and exit")
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parent.parent
    names = examples(root)
    runnable = [n for n in names if n not in SKIP]

    if args.list:
        for n in names:
            print(f"  {'skip' if n in SKIP else 'run ':4}  {n}"
                  + (f"   ({SKIP[n]})" if n in SKIP else ""))
        return 0

    out = pathlib.Path(args.outdir) / "examples"
    out.mkdir(parents=True, exist_ok=True)

    missing, failed, timed_out, passed = [], [], [], []
    for name in runnable:
        exe = root / "target" / "debug" / "examples" / name
        if not exe.exists():
            missing.append(name)
            continue
        try:
            result = subprocess.run(
                [str(exe)], cwd=root, capture_output=True, text=True,
                timeout=args.timeout,
            )
        except subprocess.TimeoutExpired:
            timed_out.append(name)
            continue
        if result.returncode == 0:
            passed.append(name)
        else:
            failed.append((name, result.returncode))
            (out / f"{name}.log").write_text(result.stdout + result.stderr)

    print(f"   ran: {len(passed) + len(failed) + len(timed_out)}"
          f"  passed: {len(passed)}"
          f"  failed: {len(failed) + len(timed_out)}"
          f"  skipped: {len(SKIP)}")

    for name, code in failed:
        print(f"     FAILED (exit {code}) {name}  -> {out / (name + '.log')}")
    for name in timed_out:
        print(f"     TIMEOUT ({args.timeout}s)  {name}")
    if missing:
        print(f"     NOT BUILT: {', '.join(missing)}")
        print("     run `cargo build --workspace --examples` first")

    return 1 if (failed or timed_out or missing) else 0


if __name__ == "__main__":
    sys.exit(main())
