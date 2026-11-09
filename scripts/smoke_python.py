#!/usr/bin/env python3
"""Build the Python extension module and run every example against it.

CI builds `kolibrie-python` and stops there, so the Python API has only ever been
compile-checked. That is the same gap `smoke_examples.py` exists to close on the Rust
side, where it found three examples that compiled and did not run. Nothing had ever
imported the module in this repository's automation.

What it does: builds the cdylib, stages it under a temporary directory as
`kolibrie.so` -- the name Python needs to import it -- copies the examples beside it
so their relative data paths resolve, and runs each one.

Usage:
    scripts/smoke_python.py [--release] [--timeout SECONDS] [outdir]

Exits non-zero if the build fails or any example does, naming each one. Output is
kept per example so a failure can be read without re-running.
"""

import argparse
import os
import pathlib
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "python" / "examples"

# The extension module links against the host Python, so an interpreter whose
# version differs from the one the build was configured for cannot load it. That is
# an environment mismatch rather than a failing example, and is reported as such.
SKIP = {}


def build(release: bool) -> pathlib.Path:
    """Builds the cdylib and returns its path."""
    command = ["cargo", "build", "-p", "kolibrie-python"]
    if release:
        command.append("--release")
    print(f"$ {' '.join(command)}")
    if subprocess.run(command, cwd=ROOT).returncode != 0:
        sys.exit("the extension module did not build")

    profile = "release" if release else "debug"
    for name in ("libkolibrie.dylib", "libkolibrie.so"):
        candidate = ROOT / "target" / profile / name
        if candidate.exists():
            return candidate
    sys.exit(f"no cdylib found in target/{profile}")


def stage(cdylib: pathlib.Path, outdir: pathlib.Path) -> pathlib.Path:
    """Puts the module and the examples in one directory and returns it."""
    staged = outdir / "module"
    staged.mkdir(parents=True, exist_ok=True)
    # Python imports by file name, so the cdylib has to be called `kolibrie.so`
    # whatever the linker named it.
    shutil.copy2(cdylib, staged / "kolibrie.so")
    for example in EXAMPLES.iterdir():
        if example.is_file():
            shutil.copy2(example, staged / example.name)
    return staged


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("outdir", nargs="?", default=None)
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--timeout", type=int, default=120)
    arguments = parser.parse_args()

    outdir = (
        pathlib.Path(arguments.outdir) if arguments.outdir else ROOT / "target" / "smoke-python"
    )
    outdir.mkdir(parents=True, exist_ok=True)

    staged = stage(build(arguments.release), outdir)

    scripts = sorted(path.name for path in EXAMPLES.glob("*.py"))
    if not scripts:
        sys.exit("no Python examples found")

    # The staged directory has to come first on the path, or an installed `kolibrie`
    # would be imported instead of the one just built.
    environment = dict(os.environ, PYTHONPATH=str(staged))

    passed, failed, skipped = [], [], []
    for script in scripts:
        if script in SKIP:
            skipped.append(script)
            print(f"SKIP  {script}  ({SKIP[script]})")
            continue
        log = outdir / f"{script}.log"
        with log.open("w") as handle:
            try:
                code = subprocess.run(
                    [sys.executable, script],
                    cwd=staged,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    timeout=arguments.timeout,
                ).returncode
            except subprocess.TimeoutExpired:
                code = None
        if code == 0:
            passed.append(script)
            print(f"ok    {script}")
        else:
            failed.append(script)
            reason = "timed out" if code is None else f"exit {code}"
            print(f"FAIL  {script}  ({reason}); see {log}")
            print("".join(log.read_text(errors="replace").splitlines(keepends=True)[-8:]))

    print(
        f"\n   ran: {len(passed) + len(failed)}  passed: {len(passed)}  "
        f"failed: {len(failed)}  skipped: {len(skipped)}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
