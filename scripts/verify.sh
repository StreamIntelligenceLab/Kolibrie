#!/usr/bin/env bash
# Standard verification for a refactor step. Prints a summary AND keeps the full
# logs, so a failure can be diagnosed without re-running.
#
# Why this exists: an earlier ad-hoc `cargo test | awk '{sum}'` pipeline reported
# "642 passed, 1 failed" and discarded the failing test's NAME, making a real flake
# unidentifiable. Never summarise test output without retaining it.
#
# The example smoke run is here for the same reason in a different form: for months
# this script only compiled the examples, and three of them were broken anyway -- a
# rule syntax the parser does not accept, an absolute path into a personal directory,
# and a gitignored dataset. None is visible to `cargo build`.
#
# Usage: scripts/verify.sh [outdir]      (default: /tmp/kolibrie-verify)
set -u

OUT="${1:-/tmp/kolibrie-verify}"
mkdir -p "$OUT"

: "${DYLD_LIBRARY_PATH:=/opt/homebrew/Cellar/python@3.11/3.11.11/Frameworks/Python.framework/Versions/3.11/lib:$HOME/miniconda3/lib}"
export DYLD_LIBRARY_PATH

fail=0

# Deliberately NOT --all-targets: that pulls the four heavy bench sweeps into the
# test run in debug mode, where citybench_cross_window_compare does not finish.
# Benches are compile-checked separately below.
echo "== tests =="
cargo test --workspace --no-fail-fast > "$OUT/test.log" 2>&1
awk '/^test result/ {p+=$4; f+=$6} END {printf "   passed: %d  failed: %d\n", p, f}' "$OUT/test.log"
if grep -qE '^test .* FAILED$' "$OUT/test.log"; then
  echo "   FAILING TESTS:"
  grep -E '^test .* FAILED$' "$OUT/test.log" | sed 's/^/     /'
  fail=1
fi

for check in \
  "check:cargo check --workspace --all-targets" \
  "examples:cargo build --workspace --examples" \
  "benches:cargo bench --workspace --no-run" \
  "nodefault:cargo check -p kolibrie --no-default-features" \
  "ml:cargo check -p kolibrie --features ml"
do
  name="${check%%:*}"; cmd="${check#*:}"
  echo "== $name =="
  $cmd --message-format short > "$OUT/$name.log" 2>&1
  n=$(grep -cE '^[^ ]+: error|^error' "$OUT/$name.log")
  echo "   errors: $n"
  [ "$n" -gt 0 ] && { grep -E '^[^ ]+: error|^error' "$OUT/$name.log" | head -5 | sed 's/^/     /'; fail=1; }
done

# Mirrors the blocking `rustdoc` CI job exactly, including the deny flag: a broken
# intra-doc link fails there even though `missing_docs` is only a warning. Not in the
# loop above because RUSTDOCFLAGS contains a space and would not survive its word
# splitting.
#
# Why this is worth the extra minute: four broken links reached commits in one
# refactoring session while every other check here passed. `cargo check` does not
# resolve doc links at all, so nothing else in this script can see them. Two came
# from the same trap twice -- a `///` summary written on a `pub mod` whose file
# already has a `//!` header, which merge and then resolve in the parent module's
# scope -- and one from removing an import that only a doc link still used.
echo "== rustdoc =="
RUSTDOCFLAGS="-D rustdoc::broken_intra_doc_links" \
  cargo doc --workspace --no-deps > "$OUT/doc.log" 2>&1
n=$(grep -cE '^error' "$OUT/doc.log")
echo "   errors: $n"
if [ "$n" -gt 0 ]; then
  grep -E '^error' -A 3 "$OUT/doc.log" | head -12 | sed 's/^/     /'
  fail=1
fi

# Runs the examples, which the compile check above cannot speak for: three examples
# were broken for months while compiling cleanly. Adds about a minute, most of it two
# streaming demos sleeping through timed windows.
echo "== example smoke run =="
if scripts/smoke_examples.py "$OUT" > "$OUT/smoke.log" 2>&1; then
  grep -E '^   ran:' "$OUT/smoke.log"
else
  cat "$OUT/smoke.log"
  fail=1
fi

echo "== warnings (default features) =="
grep -oE 'warning: [a-z].*' "$OUT/check.log" 2>/dev/null | wc -l | sed 's/^/   raw warning lines: /'
echo "   (run scripts/warning_report.py after a clean build for the per-lint count)"

echo
echo "logs in $OUT"
exit "$fail"
