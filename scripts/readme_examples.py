#!/usr/bin/env python3
"""Compile every Rust example in the README, so an API change cannot silently rot it.

The README is the first thing a reader tries, and nothing checked it. Its examples
were written against whatever the API looked like at the time and then left alone
while signatures moved underneath them -- the same failure the hand-written API
section had, with the same cause: no build touched it.

This extracts every ```rust block and compiles it. Compiles, not runs: an example may
reasonably read a file that is not in the repository or start a server, and whether it
*runs* is `scripts/smoke_examples.py`'s question about `kolibrie/examples`. What this
answers is narrower and is the thing that rots -- does it still say something the
compiler accepts.

Most blocks are fragments shown in the middle of a narrative -- `db.parse_turtle(..)`
with the `db` from two paragraphs up -- which is how a README should read and is not
something a compiler accepts. So a section can declare a preamble, prepended to every
block after it, the way rustdoc hides setup behind `#` lines:

    <!-- readme-examples: preamble
    use kolibrie::prelude::*;
    let mut db = SparqlDatabase::new();
    -->

A preamble holds until the next one. `<!-- readme-examples: preamble none -->` clears
it, for a block that is a whole program.

A block that cannot compile at all is marked, and the reason is required:

    <!-- readme-examples: skip (illustrates a signature, not a program) -->

Usage:
    scripts/readme_examples.py                 # extract and compile
    scripts/readme_examples.py --list          # what would be compiled, and skips
    scripts/readme_examples.py --keep          # leave the generated crate in place

Exits non-zero if any block fails, naming the README line it starts at.
"""

import argparse
import pathlib
import re
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
WORKSPACE = ROOT / "target" / "readme-examples"
SKIP = re.compile(r"<!--\s*readme-examples:\s*skip\s*(?:\(([^)]*)\))?\s*-->")
PREAMBLE_OPEN = re.compile(r"<!--\s*readme-examples:\s*preamble\s*(none)?\s*$")


def blocks(text: str):
    """Every ```rust block, as (line number, code, preamble, skip reason or None)."""
    lines = text.split("\n")
    found = []
    preamble = ""
    index = 0
    while index < len(lines):
        opened = PREAMBLE_OPEN.search(lines[index])
        if opened:
            if opened.group(1) == "none":
                preamble = ""
                index += 1
                continue
            collected = []
            index += 1
            while index < len(lines) and "-->" not in lines[index]:
                collected.append(lines[index])
                index += 1
            preamble = "\n".join(collected)
            index += 1
            continue
        if lines[index].startswith("```rust"):
            start = index
            index += 1
            body = []
            while index < len(lines) and not lines[index].startswith("```"):
                body.append(lines[index])
                index += 1
            # A skip marker sits on the line before the fence, or the line before a
            # blank line before the fence.
            reason = None
            for back in (start - 1, start - 2):
                if back >= 0:
                    match = SKIP.search(lines[back])
                    if match:
                        reason = (match.group(1) or "no reason given").strip()
                        break
            found.append((start + 1, "\n".join(body), preamble, reason))
        index += 1
    return found


def as_program(code: str, preamble: str = "") -> str:
    """The block as something rustc will accept.

    A block with `fn main` is a program already. One without is a body, and gets
    wrapped. Lints are relaxed because an example is written to be read: an unused
    import in a snippet is not a defect worth failing on.
    """
    header = "#![allow(unused_imports, unused_variables, unused_mut, dead_code, unreachable_code)]\n"
    if re.search(r"\bfn\s+main\s*\(", code):
        return header + code + "\n"
    # `use` declarations have to stay at the top level, so they are hoisted out of
    # the wrapper rather than left inside it.
    uses, body = [], []
    for line in f"{preamble}\n{code}".split("\n"):
        (uses if line.strip().startswith("use ") else body).append(line)
    return header + "\n".join(uses) + "\n\nfn main() {\n" + "\n".join(body) + "\n}\n"


def write_crate(cases: list[tuple[int, str, str]]) -> None:
    """A crate whose examples are the README's blocks."""
    if WORKSPACE.exists():
        shutil.rmtree(WORKSPACE)
    (WORKSPACE / "examples").mkdir(parents=True)
    (WORKSPACE / "src").mkdir()
    (WORKSPACE / "src" / "lib.rs").write_text("//! Nothing: the examples are the point.\n")
    # Paths are absolute so the crate can live under `target/` without being part of
    # the workspace, which it must not be -- a member would be built by every
    # `cargo build --workspace` in the repository.
    (WORKSPACE / "Cargo.toml").write_text(
        f"""[package]
name = "readme-examples"
version = "0.0.0"
edition = "2021"
publish = false

[workspace]

[lib]
path = "src/lib.rs"

[dependencies]
kolibrie = {{ path = "{ROOT / 'kolibrie'}" }}
shared = {{ path = "{ROOT / 'shared'}" }}
datalog = {{ path = "{ROOT / 'datalog'}" }}
datalogmtl = {{ path = "{ROOT / 'datalogmtl'}" }}
"""
    )
    for line, code, preamble in cases:
        (WORKSPACE / "examples" / f"readme_line_{line}.rs").write_text(
            as_program(code, preamble)
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true", help="show what would run")
    parser.add_argument("--keep", action="store_true", help="keep the generated crate")
    arguments = parser.parse_args()

    found = blocks(README.read_text())
    cases = [
        (line, code, preamble) for line, code, preamble, reason in found if reason is None
    ]
    skipped = [(line, reason) for line, _, _, reason in found if reason is not None]

    if arguments.list:
        print(f"{len(found)} rust blocks in README.md")
        for line, _, _ in cases:
            print(f"  compile  README.md:{line}")
        for line, reason in skipped:
            print(f"  skip     README.md:{line}  ({reason})")
        return 0

    if not cases:
        print("no compilable blocks found, which is itself suspicious")
        return 1

    write_crate(cases)
    print(f"compiling {len(cases)} blocks from README.md ({len(skipped)} skipped)")
    result = subprocess.run(
        ["cargo", "build", "--examples", "--quiet"],
        cwd=WORKSPACE,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print(f"   ok: all {len(cases)} compile")
        if not arguments.keep:
            shutil.rmtree(WORKSPACE, ignore_errors=True)
        return 0

    # Name the README line, not the generated file: the reader has to fix the README.
    failing = sorted(set(re.findall(r"readme_line_(\d+)\.rs", result.stderr)))
    print(result.stderr.rstrip())
    print()
    if failing:
        print("failing blocks, by the README line their fence is on:")
        for line in failing:
            print(f"   README.md:{line}")
    print(f"\nthe generated crate is at {WORKSPACE} for inspection")
    return 1


if __name__ == "__main__":
    sys.exit(main())
