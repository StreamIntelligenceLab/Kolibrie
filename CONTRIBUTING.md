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

1. `cargo build --workspace --all-targets`
2. `cargo test --workspace --all-targets` — compare against the known-good
   baseline rather than against zero, and name any pre-existing failure you are
   not fixing.
3. `python3 scripts/warning_report.py` — the warning count is a tracked metric
   and should not go up.
4. Update `changes.txt`.

On macOS, anything linking `ml`/`python` needs the Python dylib on the library
path:

```bash
export DYLD_LIBRARY_PATH=/opt/homebrew/Cellar/python@3.11/<version>/Frameworks/Python.framework/Versions/3.11/lib
```

## Notes for AI coding assistants

- Do not commit, push, or create branches unless explicitly asked.
- Do not weaken a test to make it pass. If a test is wrong, say so and explain
  why before changing it.
- Report outcomes faithfully: if tests fail, show the output; if a step was
  skipped, say so.
- Prefer reusing an existing helper over adding a parallel one — check the
  surrounding module first.
