# Contributing to Kolibrie

How to change this repository, for people and AI coding assistants alike. This file
is the single source of truth: tool-specific files (`CLAUDE.md`, `AGENTS.md`, editor
configs) point here instead of repeating it.

For issues, pull requests and licensing, see **How to Contribute** in
[`README.md`](README.md).

---

## Setup

- **Rust:** any stable toolchain from 1.85 (the MSRV, checked in CI). No version is
  pinned.
- **uv:** install it once, system-wide, from
  [docs.astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/). It
  manages the Python environment; nothing else is installed globally.
- **Python 3.10 or newer**, already on the system. uv uses it rather than
  downloading one, because the `ml` crate embeds the interpreter and links against
  its libpython.

Then, in the repository root:

```bash
uv sync
```

This creates `.venv` with the ML runtime, `mlschema` and the dev tools (ruff,
pytest), at the versions pinned in `uv.lock`.

Before building with `ml` on Linux or macOS, also put Python's shared library on the
loader's path:

```bash
. scripts/python_env.sh
```

The script derives the path from the interpreter pyo3 uses, so do not write a path
down anywhere.

## Workflow

1. Branch from `main`.
2. Make one logical change per commit (see [Commit messages](#commit-messages)).
3. Run the checks in [Before you commit](#before-you-commit).
4. Open a pull request. It is squash-merged, with a short summary as the message.

## Before you commit

1. **`scripts/verify.sh`** runs everything CI blocks on and prints a summary:
   tests, both feature configurations, benches compiled with `--no-run`, `cargo doc`
   with links denied, the module-graph cycle check, the README examples, the
   examples run end to end, and the Python checks. Compare the test count with the
   known-good baseline, and name any failure you are not fixing.
2. **`scripts/warning_report.py`**: the warning count must not go up.
3. **`changes.txt`**: add a line if users will notice the change.

Do not run `cargo test --workspace --all-targets`. It pulls the heavy benchmark
sweeps into a debug test run that does not finish. `verify.sh` runs the tests
without it and compiles the benches separately.

### What CI blocks on

| Check | Command |
|---|---|
| Tests, default and `--no-default-features` | `cargo test` |
| Minimum Rust version | build with 1.85 |
| Documentation | `cargo doc` with `-D rustdoc::broken_intra_doc_links` |
| Rust formatting | `cargo fmt --check` |
| Lints held at zero | `scripts/check_lint.sh <lint>` |
| Module graph has no cycles | `scripts/module_graph.py --check` |
| README examples compile | `scripts/readme_examples.py` |
| `uv.lock` matches `pyproject.toml` | `uv lock --check` |
| Python lint | `uv run ruff check` |
| Python formatting | `uv run ruff format --check` |
| Python tests | `uv run pytest` |

### Examples

`scripts/smoke_examples.py` runs every example that can finish on its own:

```bash
scripts/smoke_examples.py --list   # what runs, what is skipped and why
scripts/smoke_examples.py          # run them; non-zero names each failure
```

New examples are picked up automatically. An example that cannot finish unattended
(it needs a broker, or serves until stopped) goes in `SKIP` with a reason.

### Intermittent failures

```bash
scripts/flakehunt.sh 40       # 40 passes over every test binary
scripts/flakehunt.sh 40 ml    # only binaries matching "ml"
```

It runs the test binaries directly and reports the name of each failing test, with
full logs. Prefer it to looping `cargo test`.

## Python tooling

Cargo builds all Rust code. uv manages everything Python: the environment in
`.venv`, the dependencies declared in `pyproject.toml`, and the exact versions in
`uv.lock`. Ruff lints and formats; pytest runs the tests.

| Rust | Python |
|---|---|
| `cargo fmt` | `uv run ruff format` |
| `cargo clippy` | `uv run ruff check` |
| `cargo test` | `uv run pytest` |

```bash
uv run ruff check --fix    # lint, applying safe fixes
uv run ruff format         # format
uv run pytest              # Python tests
```

- **Run anything built with `ml` through `uv run`**, for example
  `uv run cargo test -p kolibrie --features ml`. The embedded interpreter then
  loads packages from `.venv` instead of whatever is installed for the user.
- **Dependencies live in groups:** `ml` for the runtime the `ml` feature needs,
  `dev` for tools. Add one with `uv add --group ml <package>` or
  `uv add --group dev <package>`.
- **Commit `pyproject.toml` and `uv.lock` together.** Never edit `uv.lock` by hand;
  run `uv lock` instead.
- **Ruff's settings live in `pyproject.toml`**, with the same 100-column limit as
  `rustfmt.toml`.

## Commit messages

```
<Verb>s <what changed>

<Why it was needed.>

- one bullet per logical change, for multi-part commits

<What it changes for a user, if anything.>
```

- **Subject:** third person present (`Adds …`, `Fixes …`, `Removes …`), at most
  72 characters, no trailing period.
- **Body:** required for anything non-trivial. Wrap at 72 characters.
- **Explain why.** The diff already shows what.
- **No measurements or failed attempts:** no timings, speedups, benchmark results
  or approaches that were tried and lost. They belong in internal notes.
- **Describe user impact in words**, such as "lower memory use" or "a new error".
- **Keep unrelated fixes out of refactor commits**, even one-line ones.
- **Squash merges** get a short summary of the pull request, not the collected
  commit messages.

Example:

```
Caches parsed prefixes per query

Queries that repeat the same PREFIX declarations parsed them every
time they were used. They are now parsed once per query, which
shortens parsing for long queries. The public API is unchanged.
```

## Changelog

`changes.txt` lists what changed for people who use Kolibrie, in the spirit of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

- One line per user-visible change, under the current version.
- Grouped as Added, Changed, Deprecated, Removed, Fixed or Security.
- Say what changed for a user, not how or why.
- Internal refactors need no entry.
- Newest version first. Open a new heading only for a new version.

```
0.4.0
Added
- What a user can now do
Fixed
- What no longer goes wrong for them
```

## Comments and documentation

Comments are short and say what something is or does. One line gives the main
idea.

**Doc comments (`///`, `//!`)**

- One sentence saying what the item does, including a module's `//!`.
- No period at the end.
- No measurements, timings, known weaknesses, history or rejected approaches.
- Public fields and enum variants get one short line (`missing_docs` is blocking).
  Private fields get none.
- A tested code example may follow the sentence after an empty `///` line.

**Comments inside code (`//`)**

- None by default; the code and its doc comment should be enough.
- One short line only where a reader would otherwise get it wrong.
- `// SAFETY:` is required above every `unsafe` block or impl.

**Python:** one-line docstrings without a period; no `#` comments except the
licence header and tool pragmas. Layout is whatever `ruff format` produces.

**Every file** keeps its MPL licence header.

```rust
/// Parses N-Triples and adds them
pub fn parse_ntriples_and_add(&mut self, ntriples_data: &str) {
```

**Two rules that break `cargo doc`:**

- A module with its own `//!` must not also have a `///` on its `pub mod` line.
  When you add one, delete the other.
- Bracketed notation such as `Box[a,b]` is read as a link. Put it in backticks.

## Code conventions

- **No `unsafe` in library crates.** Where it is unavoidable outside them, add
  `// SAFETY:` and a scoped `#[allow(unsafe_code)]`.
- **The module graph has no cycles.** Run `scripts/module_graph.py` when adding a
  module.
- **Public API changes are not tracked automatically.** Say so in the commit
  message when you add, remove or change a `pub` item.
- **Scripted edits preserve line endings.** Read and write bytes, not text, so a
  file's CRLF or LF endings stay as they were. A comment-only change should show
  no whole-file rewrite in `git diff --numstat`.

## Notes for AI coding assistants

Everything above applies to you. In addition:

**How you work**

- Do not commit, push or create branches unless asked.
- Read the module you are changing and its callers before writing code.
- Ask before changing anything the task did not mention.
- Report outcomes faithfully: show failing output, and say when a step was skipped.
- Do not call a change done until `scripts/verify.sh` passes. Compiling is not
  enough.
- Run Python and `ml` builds through `uv run`. Do not use `pip install`, and do not
  install anything globally.

**How you write code**

- **Stay in scope.** Change only what the task needs: no drive-by refactors,
  renames, reformatting or reordering of code you did not otherwise touch.
- **Follow the surrounding code.** Match its naming, error handling and structure
  rather than introducing a new style.
- **Reuse before adding.** Search for an existing helper, type or trait before
  writing a parallel one.
- **Do not guess APIs.** Check that a function, method or field exists before you
  call it.
- **Return errors, do not panic,** on anything a caller or input controls. No new
  `unwrap()` or `expect()` on those paths.
- **No new dependencies** without asking, in `Cargo.toml` or `pyproject.toml`,
  including new features of existing ones. When one is approved, add it with
  `cargo add` or `uv add` so the lock file follows.
- **Keep the public API stable.** Do not add, remove or change a `pub` item unless
  the task asks for it.
- **Fix warnings, do not silence them.** No new `#[allow(...)]` except the scoped
  `unsafe_code` case above, and no new `# noqa` or ruff ignores.
- **Test what you change.** A behaviour change comes with a test; a bug fix comes
  with a test that fails without the fix.
- **Do not weaken a test to make it pass.** If a test is wrong, say why first.
- **Leave no traces.** No commented-out code, no `TODO`s, no `println!`, `dbg!` or
  `print()` left behind, no comments describing your own edit.
- **Do not edit generated files by hand.** Regenerate `uv.lock` with `uv lock`.
