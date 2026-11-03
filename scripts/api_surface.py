#!/usr/bin/env python3
"""The workspace's public API surface, read out of the source.

Generates `docs/api.md`. The README used to carry 371 hand-written lines mirroring
these signatures, and it drifted: it documented `add_constraint`,
`query_with_repairs` and `infer_new_facts_semi_naive_with_repairs` for as long as
they existed and then for a while after, and four translated READMEs carried the same
three sections. A listing nobody regenerates is a listing nobody can trust.

Nightly is deliberately not used. `cargo doc --output-format json` and
`cargo public-api` both want it, and this project does not take a nightly
dependency, so the surface is read from the source the way
`scripts/module_graph.py` reads the module graph: by parsing it.

What that costs, stated plainly: this reads `pub` items lexically. It does not
resolve types, expand macros, or follow `pub use` re-exports to their definitions, so
a re-exported item is listed where it is written rather than where it is reachable.
It also cannot tell a `pub` item inside a private module -- which is not reachable --
from one that is. `cargo doc` is the authority on what a consumer can actually name;
this is the index, kept honest by being generated.

Usage:
    scripts/api_surface.py                  # write docs/api.md
    scripts/api_surface.py --stdout         # print it instead
    scripts/api_surface.py --check          # exit 1 if docs/api.md is out of date

`--check` is the one for CI: it fails when someone changes the public surface without
regenerating, which is exactly how the hand-written version went stale.
"""

import argparse
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "docs" / "api.md"

# The crates a consumer depends on, in the order they are worth reading. `python` is
# absent on purpose: its surface is the Python API, not its Rust shim, and its own
# `[lib] doc = false` says so.
CRATES = [
    ("kolibrie-api", "The facade: one dependency that re-exports the rest."),
    ("kolibrie", "The engine: store, parser, query execution, RSP."),
    ("shared", "Types every crate shares: terms, triples, rules, dictionary."),
    ("datalog", "Datalog reasoning: forward and backward chaining, N3 rules."),
    ("datalogmtl", "Metric temporal Datalog."),
    ("ml", "Machine learning integration, behind the `ml` feature."),
]

# A `pub` item worth listing, and what to call it. `pub(crate)` and `pub(super)` are
# not public and must not match, which is why the pattern requires whitespace rather
# than allowing a parenthesis.
ITEM = re.compile(
    r"^\s*pub\s+(?P<kind>fn|struct|enum|trait|type|const|static|mod|use)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
)
# `pub async fn` and `pub unsafe fn` carry a qualifier before the keyword.
QUALIFIED = re.compile(
    r"^\s*pub\s+(?:async|unsafe|extern\s+\"[^\"]*\")\s+(?P<kind>fn)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
)


def signature(lines: list[str], start: int, kind: str) -> str:
    """The declaration as one line, gathering continuations and dropping the body.

    An item may be written across several lines -- a `use` group, a long parameter
    list, a `where` clause -- and taking only the first produced
    `pub use kolibrie::execute_query::` with the group it imports cut off.
    """
    text = ""
    for raw in lines[start : start + 12]:
        text = f"{text} {raw.strip()}".strip()
        if kind in ("use", "type", "const", "static"):
            # These end at a semicolon, and a `use` group may span lines before it.
            if ";" in text:
                break
        else:
            # A signature ends where its body or its `where` clause begins, but not
            # at a brace inside `<...>` or `(...)`.
            depth = 0
            ended = False
            for index, character in enumerate(text):
                if character in "(<":
                    depth += 1
                elif character in ")>":
                    depth -= 1
                elif character in "{;" and depth <= 0:
                    text, ended = text[:index], True
                    break
            if ended or text.rstrip().endswith(";"):
                break
            if " where " in text:
                text = text[: text.index(" where ")]
                break
    text = " ".join(text.split())
    # A `use` group's braces are part of the item; everything else's open its body.
    cuts = (";",) if kind in ("use", "type", "const", "static") else ("{", ";")
    for cut in cuts:
        index = text.find(cut)
        if index != -1:
            text = text[:index]
    return text.strip().rstrip(",").strip()


def items_of(path: pathlib.Path) -> list[tuple[str, str]]:
    """The public items a file declares, as (kind, signature)."""
    found = []
    in_block_comment = False
    lines = path.read_text(errors="replace").splitlines()
    for number, raw in enumerate(lines):
        line = raw.strip()
        # Block comments can hold anything, including example code with `pub fn`.
        if in_block_comment:
            if "*/" in line:
                in_block_comment = False
            continue
        if line.startswith("/*"):
            if "*/" not in line:
                in_block_comment = True
            continue
        if line.startswith("//"):
            continue
        match = ITEM.match(raw) or QUALIFIED.match(raw)
        if not match:
            continue
        kind = match.group("kind")
        # A module declaration is structure, not surface, and `module_graph.py`
        # already covers it. A re-export is listed, since it is how the facade works.
        if kind == "mod":
            continue
        found.append((kind, signature(lines, number, kind)))
    return found


def crate_sources(crate: str) -> list[pathlib.Path]:
    source = ROOT / crate.replace("-", "_") if (ROOT / crate.replace("-", "_")).exists() else ROOT / crate
    return sorted((source / "src").rglob("*.rs")) if (source / "src").exists() else []


def render() -> str:
    out = [
        "# Public API surface",
        "",
        "Generated by `scripts/api_surface.py`. Do not edit: run the script.",
        "",
        "This is an index, not a reference. `cargo doc --workspace --open` is the",
        "authority on what a consumer can name, with the types, the bounds and the",
        "prose; this exists so that a change to the surface is visible in a diff, and",
        "so no hand-written list can quietly disagree with the code again.",
        "",
        "It is read lexically from the source, so a `pub` item inside a private module",
        "appears here although nothing outside can reach it, and a re-export is listed",
        "where it is written rather than where it is reachable.",
        "",
    ]
    for crate, blurb in CRATES:
        sources = crate_sources(crate)
        if not sources:
            continue
        out += [f"## `{crate}`", "", blurb, ""]
        total = 0
        for path in sources:
            items = items_of(path)
            if not items:
                continue
            total += len(items)
            relative = path.relative_to(ROOT)
            out += [f"### `{relative}`", "", "```rust"]
            out += [sig for _, sig in items]
            out += ["```", ""]
        files = sum(1 for path in sources if items_of(path))
        out += [
            f"_{total} public {'item' if total == 1 else 'items'} across "
            f"{files} {'file' if files == 1 else 'files'}._",
            "",
        ]
    return "\n".join(out).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stdout", action="store_true", help="print instead of writing")
    parser.add_argument("--check", action="store_true", help="exit 1 if out of date")
    arguments = parser.parse_args()

    generated = render()
    if arguments.stdout:
        print(generated, end="")
        return 0
    if arguments.check:
        if not OUTPUT.exists():
            print(f"{OUTPUT.relative_to(ROOT)} does not exist; run scripts/api_surface.py")
            return 1
        if OUTPUT.read_text() != generated:
            print(
                f"{OUTPUT.relative_to(ROOT)} is out of date; run scripts/api_surface.py"
            )
            return 1
        print(f"{OUTPUT.relative_to(ROOT)} is up to date")
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(generated)
    print(f"wrote {OUTPUT.relative_to(ROOT)} ({len(generated.splitlines())} lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
