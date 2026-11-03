#!/usr/bin/env python3
"""The `kolibrie` crate's module dependency graph: layers, edges and a cycle check.

Every edge is a `crate::<module>` reference that exists in the source. Lines starting
`///` or `//!` are excluded, because a documentation link is not a dependency —
counting them once inflated a reported cycle from 15 modules to 17 and made two
modules look irreducible when nothing but a doc link held them there.

The cycle check is the part worth running in anger. The crate was once 87% one
strongly-connected component; it is now acyclic, and a new module that reintroduces a
cycle is easier to fix the day it lands than a year later.

Usage:
    scripts/module_graph.py              # layers, per-module dependencies, cycle check
    scripts/module_graph.py --mermaid    # the diagram in docs/architecture.md
    scripts/module_graph.py --check      # exit non-zero if a cycle exists

Exits non-zero under --check when any strongly-connected component holds more than
one module. Reports the largest such component, NOT the largest component: a single
6,000-line module outranks a real five-module cycle by line count, which once read as
though the graph were already acyclic.
"""

import argparse
import collections
import pathlib
import re
import sys


def modules(src: pathlib.Path) -> dict[str, list[pathlib.Path]]:
    """Top-level modules of the crate, each with the files it spans.

    A directory module and a sibling `foo.rs` are the same module, which is how
    `rsp_engine.rs` plus `rsp_engine/builder.rs` count as one.
    """
    found: dict[str, list[pathlib.Path]] = {}
    for path in sorted(src.iterdir()):
        if path.name == "lib.rs":
            continue
        if path.is_file() and path.suffix == ".rs":
            found.setdefault(path.stem, []).append(path)
        elif path.is_dir():
            for f in sorted(path.rglob("*.rs")):
                found.setdefault(path.name, []).append(f)
    for name in list(found):
        sibling = src / f"{name}.rs"
        if sibling.exists() and sibling not in found[name]:
            found[name].append(sibling)
    return found


def graph(src: pathlib.Path):
    mods = modules(src)
    code, lines = {}, {}
    for name, files in mods.items():
        text = "\n".join(f.read_text() for f in files)
        code[name] = "\n".join(
            l for l in text.splitlines() if not l.lstrip().startswith(("///", "//!"))
        )
        lines[name] = len(text.splitlines())
    edges = {
        (a, b)
        for a in mods
        for b in mods
        if a != b and re.search(r"\bcrate::" + b + r"\b", code[a])
    }
    return lines, edges


def components(nodes, edges):
    """Tarjan's strongly-connected components, largest first by module count."""
    succ = collections.defaultdict(list)
    for a, b in edges:
        succ[a].append(b)
    index, low, on_stack, stack, out, counter = {}, {}, set(), [], [], [0]

    def walk(v):
        index[v] = low[v] = counter[0]
        counter[0] += 1
        stack.append(v)
        on_stack.add(v)
        for w in succ[v]:
            if w not in index:
                walk(w)
                low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                comp.append(w)
                if w == v:
                    break
            out.append(comp)

    sys.setrecursionlimit(10000)
    for v in nodes:
        if v not in index:
            walk(v)
    return sorted(out, key=len, reverse=True)


def depths(nodes, edges):
    """Longest-path layer per module: one above its deepest dependency."""
    succ = collections.defaultdict(list)
    for a, b in edges:
        succ[a].append(b)
    depth, visiting = {}, set()

    def of(m):
        if m in depth:
            return depth[m]
        if m in visiting:
            raise ValueError(f"cycle through {m}; run --check")
        visiting.add(m)
        depth[m] = 1 + max((of(b) for b in succ[m]), default=-1)
        visiting.discard(m)
        return depth[m]

    for m in nodes:
        of(m)
    return depth


def reduction(nodes, edges):
    """Transitive reduction: drop an edge implied by a longer path."""
    succ = collections.defaultdict(set)
    for a, b in edges:
        succ[a].add(b)
    cache: dict[str, set[str]] = {}

    def reach(m):
        if m not in cache:
            out: set[str] = set()
            for b in succ[m]:
                out.add(b)
                out |= reach(b)
            cache[m] = out
        return cache[m]

    for m in nodes:
        reach(m)
    return {
        (a, b) for a, b in edges if not any(b in reach(c) for c in succ[a] if c != b)
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mermaid", action="store_true", help="print the diagram")
    parser.add_argument("--check", action="store_true", help="exit 1 on a cycle")
    parser.add_argument("--src", default=None, help="crate src (default: kolibrie/src)")
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parent.parent
    src = pathlib.Path(args.src) if args.src else root / "kolibrie" / "src"
    lines, edges = graph(src)

    cycles = [c for c in components(lines, edges) if len(c) > 1]
    if cycles:
        worst = max(cycles, key=lambda c: sum(lines[m] for m in c))
        print(f"CYCLE: {len(cycles)} component(s) hold more than one module")
        print(f"  largest: {len(worst)} modules, {sum(lines[m] for m in worst)} lines")
        for m in sorted(worst):
            print(f"    {m}")
        return 1
    if args.check:
        print(f"acyclic: {len(lines)} modules, {len(edges)} edges")
        return 0

    depth = depths(lines, edges)
    reduced = reduction(lines, edges)
    by_layer = collections.defaultdict(list)
    for m, lv in depth.items():
        by_layer[lv].append(m)

    if args.mermaid:
        short = {
            m: m.replace("streamertail_optimizer", "streamertail").replace(
                "neural_relations_disabled", "neural_disabled"
            )
            for m in lines
        }
        print("```mermaid")
        print("graph TD")
        for lv in sorted(by_layer):
            print('    subgraph L%d["layer %d"]' % (lv, lv))
            for m in sorted(by_layer[lv], key=lambda m: -lines[m]):
                print('        %s["%s<br/>%d lines"]' % (m, short[m], lines[m]))
            print("    end")
        for a, b in sorted(reduced):
            print(f"    {a} --> {b}")
        print("```")
        return 0

    out = collections.defaultdict(list)
    for a, b in edges:
        out[a].append(b)
    print(
        f"{len(lines)} modules, {len(edges)} edges "
        f"({len(reduced)} after transitive reduction), "
        f"{max(depth.values()) + 1} layers, acyclic\n"
    )
    for lv in sorted(by_layer):
        total = sum(lines[m] for m in by_layer[lv])
        print(f"  layer {lv}  ({len(by_layer[lv])} modules, {total} lines)")
        for m in sorted(by_layer[lv], key=lambda m: -lines[m]):
            deps = ", ".join(sorted(out[m])) or "(nothing in the crate)"
            print(f"      {lines[m]:5}  {m:26} -> {deps}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
