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

1. `scripts/verify.sh` — runs tests, `cargo check --all-targets`, both feature
   configurations, the benches with `--no-run`, and the examples both compiled and
   *run*, then prints a summary. Compare the test count against the known-good
   baseline rather than against zero, and name any pre-existing failure you are not
   fixing.
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

### Examples are run, not just compiled

`scripts/smoke_examples.py` runs every example that can finish on its own, and
`verify.sh` calls it. It exists because compiling an example proves very little:
three were broken for months while compiling cleanly — one used a rule syntax the
parser does not accept, one read an absolute path into a personal directory, and one
read a dataset that `.gitignore` excludes.

```bash
scripts/smoke_examples.py --list      # what runs, what is skipped and why
scripts/smoke_examples.py             # run them; non-zero names each failure
```

Three are skipped, listed in `SKIP` in that script with a reason each: two need an
MQTT broker and one serves HTTP until interrupted.
The eleven behind the `ml` feature are not run either — they need a trusted-local
training directory, a model allowlist, or the MNIST corpus, so running them here
would report environment setup as failure.

Adds about a minute, almost all of it two streaming demos sleeping through timed
windows. If you add an example, it is picked up automatically: the list comes from
`cargo metadata`, not from a list in the script. An example that cannot finish
unattended belongs in `SKIP` with a reason, not left to time out.

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
  **In practice:** when you add a `//!` to a module, delete the outer `///` from its
  `pub mod` line in the same change. Doing one without the other is what breaks it,
  and it has happened three times in this burn-down.
- **Bracketed notation in a doc comment is read as a link.** `Box[a,b]` and friends
  need backticks, or `cargo doc` fails. The `docs` CI job is blocking and runs with
  `-D rustdoc::broken_intra_doc_links`.

A doc comment that restates the item's name passes the lint and tells a reader
nothing, which is worse than the warning because it looks like documentation. Prefer
writing down the invariant, the unit, the panic, or the thing a caller would
otherwise discover by experiment.

## Scripted edits must preserve line endings

The tree has mixed line endings — 15 CRLF `.rs` files against 221 LF, with
`kolibrie/src/rsp/simple_r2r.rs` mixed *within* the file — and `.gitattributes`
carries no `text` or `eol` rule, so nothing normalises them.

A script that reads a file as text and writes it back will silently convert the
whole file, because Python's `read_text()` and most equivalents apply universal
newlines on the way in and a single ending on the way out. The result compiles,
passes tests, and shows up as every line deleted and re-added, which buries the
real change and destroys `git blame` on the file.

This has happened twice, to these same two files. Read bytes, and either leave
existing lines untouched or re-apply each line's original ending:

```python
raw = path.read_bytes()
lines = raw.splitlines(keepends=True)   # keeps \r\n per line
# insert whole lines; do not round-trip the rest through str
```

Check it before committing: `git diff --numstat` on a comment-only change should
show additions and no deletions. If it shows the whole file rewritten, the endings
moved.

Normalising the tree to one ending is a reasonable thing to want, but as its own
commit with a `.gitattributes` rule, not as a side effect.

## Public API surface is not tracked automatically

There was a `public-api` CI job that diffed each crate's public surface against a
checked-in `api-baseline/`. It is gone, and the baseline was never created, so it
never reported anything.

It was dropped for one reason: `cargo-public-api` works by asking rustdoc for JSON,
which is unstable and nightly-only. That made nightly a requirement for the job and
for anyone refreshing a baseline, and the format shifting between nightly releases
would have produced diffs that are not API changes. The repository now has no
reference to nightly at all — `rust-toolchain.toml` pins 1.96.0 and the MSRV job
enforces 1.85.

What this costs: nothing warns when a `pub` item is added, removed or changed shape.
That matters most when crates are split or merged, since a lot of surface moves at
once. Until something replaces it, a change to the public API is something a reviewer
has to notice, so say so in the commit message when you change one — the `unreachable_pub`
gate catches a module becoming reachable, but nothing catches a signature changing.

If this is picked up again, the thing to check first is whether rustdoc JSON has
stabilised, which removes the only real objection.

## Known gaps

Two things are deliberately left undone, recorded so they are not rediscovered as
surprises.

**The HTTP status mapping exists twice.** `kolibrie::sparql_protocol::query_result`
and `kolibrie-http-server`'s `ml_error_response` both map a `QueryError` to a status
and a wire code, in two crates, and they have to be changed together. Each carries a
comment naming the other. Deduplicating means `kolibrie-http-server` growing a
`lib.rs` — it is binary-only today, which is also why nothing can reuse its request
handling.

**Eleven rustdoc `redundant explicit link target` warnings** remain across five
crates, from the documentation burn-down: `[`Name`](path::to::Name)` where the text
resolves on its own. The `docs` job denies only broken links, so these do not fail
it. Worth one mechanical pass; not worth touching a file for on its own.

## Notes for AI coding assistants

- Do not commit, push, or create branches unless explicitly asked.
- Do not weaken a test to make it pass. If a test is wrong, say so and explain
  why before changing it.
- Report outcomes faithfully: if tests fail, show the output; if a step was
  skipped, say so.
- Prefer reusing an existing helper over adding a parallel one — check the
  surrounding module first.
