# Contributing to Kolibrie

Conventions for anyone changing this repository — human or AI coding assistant.
This file is the single source of truth; tool-specific instruction files
(`CLAUDE.md`, `AGENTS.md`, editor configs) should point here rather than restate
it, so the rules cannot drift apart.

For issues, pull requests and licensing, see **How to Contribute** in
[`README.md`](README.md).

---

## Every commit updates `changes.txt`

`changes.txt` is the project changelog and the only human-readable record of what
shipped. It is not optional and not generated.

- Add a numbered entry under the **current version heading** at the top of the file.
- Match the descriptive style of the surrounding entries: say what changed and,
  where it is not obvious, what it affects.
- Version headings are newest-first; numbering restarts at `1` under each.
- Open a new heading only when starting a new version, not per commit.

```
0.3.0
1. First change in this version
2. Second change

0.2.0
1. ...
```

## Commit message structure

```
<Verb>s <what changed>

<Why it was needed — not just what.>

- bullets for multi-part changes, one per logical change

<Evidence: test counts, flake rates, benchmark deltas, before/after numbers.>
```

**Subject** — third person present, ≤72 characters, no trailing period.
Matching existing history: `Adds …`, `Fixes …`, `Solves …`, `Removes …`.
Not `Add`, not `Added`.

**Body** — required for anything non-trivial. Wrap at 72 characters, blank line
after the subject.

Rules of thumb that have paid off here:

- **Explain why, not only what.** The diff already shows what changed.
- **Record decisions a future reader would otherwise re-litigate** — why a CI
  gate is advisory, why a dependency version is pinned, why an approach was
  rejected. These are the facts that get rediscovered expensively.
- **Include evidence.** Test pass/fail counts against the known baseline, flake
  rates, benchmark deltas. A claim without a number cannot be checked later.
- **Say what you deliberately did not do, and why.** Absence of work is
  otherwise indistinguishable from oversight.
- **Never bundle an unrelated fix into a refactor commit.** It destroys
  bisectability. Split them, even when the fix is one line.

### Example

```
Fixes order-dependent flake in scan conflict test

scan_rejects_repeated_variable_conflicts_before_extending_rows failed
roughly 20% of the time (5 of 25 isolated runs). The code under test was
fine; the test asserted a positional property of a result that has no
defined order.

DatasetIndex::query_graph returns hash order, and Rust re-seeds
RandomState per process, so the fixture quads come back differently each
run. The precondition now checks the fixture by content, and the result is
compared as a multiset via the module's existing sorted() helper.

0 failures in 30 runs, from 5 in 25.

Eight other tests in this file compare Bindings with assert_eq! on a Vec
and are latently exposed to the same variation.
```

## Before you commit

1. `scripts/verify.sh` — runs tests, `cargo check --all-targets`, examples,
   benches, and both feature configurations, then prints a summary. Compare the
   test count against the known-good baseline rather than against zero, and name
   any pre-existing failure you are not fixing.
2. `python3 scripts/warning_report.py` — the warning count is a tracked metric
   and should not go up.
3. Update `changes.txt`.

Do not use `cargo test --workspace --all-targets`: `--all-targets` pulls the four
heavy benchmark sweeps into the test run in debug mode, where
`citybench_cross_window_compare` does not finish — it ran over 36 minutes without
completing. `verify.sh` runs the tests without `--all-targets` and compile-checks
the benches separately with `--no-run`.

Summarise test output only while also keeping it. A pipeline that counted passes
through `awk` once reported "642 passed, 1 failed" and discarded the failing
test's name, which cost an afternoon to recover. `verify.sh` writes full logs.

### When a test fails intermittently

`scripts/flakehunt.sh [passes] [name filters]` stresses the prebuilt test binaries
directly and reports the name of anything that fails, with full logs.

```bash
scripts/flakehunt.sh 40          # 40 passes over every test binary
scripts/flakehunt.sh 40 ml       # only binaries matching "ml"
```

Prefer it over looping `cargo test`: a full workspace run takes ~3 minutes, and
eight of them once failed to reproduce a 1-in-25 flake that this found on its
first pass. Exits non-zero if anything failed.

On macOS, anything linking `ml`/`python` needs the Python dylib on the library
path:

```bash
export DYLD_LIBRARY_PATH=/opt/homebrew/Cellar/python@3.11/<version>/Frameworks/Python.framework/Versions/3.11/lib
```

## Writing documentation

`missing_docs` is being burned down per crate, and each crate that reaches zero gets
a blocking gate (see `scripts/check_lint.sh`). Two rules are worth knowing before you
start, because both have cost real debugging time:

- **A module with its own `//!` must not also carry an outer `///` on its `pub mod`
  declaration.** The two concatenate, and rustdoc then resolves the module file's
  bare intra-doc links in `lib.rs`'s scope, where that module's own items are not
  visible. It looks like the links are wrong; they are not. Put the summary in one
  place — the module file — and rustdoc uses its first line.
- **Bracketed notation in a doc comment is read as a link.** `Box[a,b]` and friends
  need backticks, or `cargo doc` fails. The `docs` CI job is blocking and runs with
  `-D rustdoc::broken_intra_doc_links`.

A doc comment that restates the item's name passes the lint and tells a reader
nothing, which is worse than the warning because it looks like documentation. Prefer
writing down the invariant, the unit, the panic, or the thing a caller would
otherwise discover by experiment.

## Notes for AI coding assistants

- Do not commit, push, or create branches unless explicitly asked.
- Do not weaken a test to make it pass. If a test is wrong, say so and explain
  why before changing it.
- Report outcomes faithfully: if tests fail, show the output; if a step was
  skipped, say so.
- Prefer reusing an existing helper over adding a parallel one — check the
  surrounding module first.
