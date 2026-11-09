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

## Why this is slow, and what it is not

Roughly 185s after a build, of which only about 16s is the examples doing anything --
and 13s of that is three of them (`sparql_syntax`, `continious_stream`, `replay_profile`).
The rest is a flat ~3.3s per binary, charged on an example's *first* execution after it
is linked. Run the suite twice with no rebuild between and the second pass takes 16s.

That floor is macOS Gatekeeper, not this script and not the examples. Measured over 10
freshly linked binaries: 36s wall, during which `syspolicyd` burned 30.1s of CPU and
`XprotectService` 9.5s, while the example processes themselves reported 0.00 user and
0.00 sys -- blocked, not computing. It is the malware/notarisation assessment of an
executable the system has never seen, so every rebuild pays it again and a byte-identical
*copy* of an already-run binary does not (0.18s), since the assessment is cached by
content.

So there is no slow example to find and nothing here to optimise. The fix is a machine
setting -- exempting the terminal that runs cargo under Privacy & Security > Developer
Tools -- which is why this docstring exists rather than a workaround.
"""

import argparse
import json
import pathlib
import subprocess
import sys
import time

# These cannot finish unattended, and that is what they are for rather than a
# defect. Each needs something this script will not start:
SKIP = {
    "drone_traffic_safety_demo": "serves HTTP until interrupted",
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
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
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
    parser.add_argument(
        "--timeout", type=int, default=120, help="seconds per example (default: 120)"
    )
    parser.add_argument(
        "--dump-timings",
        metavar="PATH",
        help="write every example's wall time to PATH, slowest first",
    )
    parser.add_argument("--list", action="store_true", help="print what would run, and exit")
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parent.parent
    names = examples(root)
    runnable = [n for n in names if n not in SKIP]

    if args.list:
        for n in names:
            print(
                f"  {'skip' if n in SKIP else 'run ':4}  {n}"
                + (f"   ({SKIP[n]})" if n in SKIP else "")
            )
        return 0

    out = pathlib.Path(args.outdir) / "examples"
    out.mkdir(parents=True, exist_ok=True)

    missing, failed, timed_out, passed = [], [], [], []
    # Per-example wall time, so a slow example is visible rather than hiding inside
    # a single total.
    durations: dict[str, float] = {}

    # Serial on purpose: the streaming examples use wall-clock RSP windows, and
    # running them concurrently slows them enough that a pass would not show they
    # stay correct.
    begun = time.monotonic()
    for name in runnable:
        exe = root / "target" / "debug" / "examples" / name
        if not exe.exists():
            missing.append(name)
            continue
        started = time.monotonic()
        try:
            result = subprocess.run(
                [str(exe)],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=args.timeout,
            )
        except subprocess.TimeoutExpired:
            durations[name] = time.monotonic() - started
            timed_out.append(name)
            continue
        durations[name] = time.monotonic() - started
        if result.returncode == 0:
            passed.append(name)
        else:
            failed.append((name, result.returncode))
            (out / f"{name}.log").write_text(result.stdout + result.stderr)
    wall = time.monotonic() - begun

    print(
        f"   ran: {len(passed) + len(failed) + len(timed_out)}"
        f"  passed: {len(passed)}"
        f"  failed: {len(failed) + len(timed_out)}"
        f"  skipped: {len(SKIP)}"
    )

    total = sum(durations.values())
    ranked = sorted(durations.items(), key=lambda pair: -pair[1])
    # The median matters more than the maximum here. The cost of this phase is a flat
    # per-binary floor paid on an example's first execution after it is linked, not a
    # handful of slow examples, so a top-N list alone is misleading -- it ranks noise.
    print(f"   wall: {wall:.0f}s; slowest:")
    for name, seconds in ranked[:8]:
        if seconds >= 0.5:
            share = 100 * seconds / total if total else 0
            print(f"     {seconds:6.1f}s  {share:4.1f}%  {name}")
    if ranked:
        mid = ranked[len(ranked) // 2]
        print(f"   median {mid[1]:.1f}s ({mid[0]}); floor {ranked[-1][1]:.1f}s ({ranked[-1][0]})")
    if args.dump_timings:
        path = pathlib.Path(args.dump_timings)
        path.write_text("".join(f"{s:.3f}\t{n}\n" for n, s in ranked))
        print(f"   all {len(ranked)} timings -> {path}")

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
