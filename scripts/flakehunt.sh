#!/usr/bin/env bash
# Hunt intermittent test failures by stressing the prebuilt test binaries directly.
#
# Why not just loop `cargo test`: a full workspace run takes ~3 minutes, and eight of
# them failed to reproduce a 1-in-25 flake. The test binaries themselves take 1-4
# seconds, so running them directly gives roughly two orders of magnitude more
# iterations per minute. That found the same flake on the first pass.
#
# Unlike `scripts/verify.sh` (run once per change, tells you if it is good), this is
# for investigation: run it when a count disagrees with the known baseline, or when
# CI fails something that passes locally.
#
# Usage:
#   scripts/flakehunt.sh                 # 15 passes over every test binary
#   scripts/flakehunt.sh 40              # 40 passes
#   scripts/flakehunt.sh 40 ml shared    # only binaries matching these names
#
# Reports which binaries failed and, crucially, the NAME of each failing test.
# Full logs for every failure land in $OUT.
set -u

cd "$(dirname "$0")/.." || exit 1

# Match scripts/verify.sh: only fill this in if the caller has not, so an
# environment that already works is never overridden.
: "${DYLD_LIBRARY_PATH:=/opt/homebrew/Cellar/python@3.11/3.11.11/Frameworks/Python.framework/Versions/3.11/lib:$HOME/miniconda3/lib}"
export DYLD_LIBRARY_PATH

ITER="${1:-15}"
shift || true
FILTERS=("$@")

OUT="${FLAKEHUNT_OUT:-/tmp/flakehunt}"
rm -rf "$OUT"; mkdir -p "$OUT"
: > "$OUT/failures.txt"
: > "$OUT/summary.txt"

# Build and discover the binaries ourselves — requiring the caller to produce a
# binary list first is how this script was initially unusable by anyone else.
echo "building test binaries..."
if ! cargo test --workspace --no-run --message-format short > "$OUT/build.log" 2>&1; then
  echo "build failed; see $OUT/build.log" >&2
  grep -E '^[^ ]+: error|^error' "$OUT/build.log" | head -10 >&2
  exit 1
fi

# Exact paths, not basenames: two distinct binaries are both called `kolibrie-*`,
# and picking the newest would silently skip one.
BINS=()
while IFS= read -r line; do
  [ -z "$line" ] && continue
  [ -x "$line" ] || continue
  if [ "${#FILTERS[@]}" -gt 0 ]; then
    keep=0
    for f in "${FILTERS[@]}"; do
      case "$(basename "$line")" in *"$f"*) keep=1 ;; esac
    done
    [ "$keep" -eq 1 ] || continue
  fi
  BINS+=("$line")
done < <(grep -oE '\(target/[a-z]+/deps/[^)]+\)' "$OUT/build.log" | tr -d '()' | sort -u)

if [ "${#BINS[@]}" -eq 0 ]; then
  echo "no test binaries matched" >&2
  exit 1
fi

echo "stressing ${#BINS[@]} binaries, $ITER passes"
start=$(date +%s)

for i in $(seq 1 "$ITER"); do
  printf '  pass %d/%d\n' "$i" "$ITER"
  for bin in "${BINS[@]}"; do
    name=$(basename "$bin")
    log="$OUT/current.log"
    "$bin" > "$log" 2>&1
    rc=$?
    if [ "$rc" -ne 0 ] || grep -qE '^test .* FAILED$' "$log"; then
      echo "    FAIL $name (rc=$rc)"
      grep -E '^test .* FAILED$' "$log" >> "$OUT/summary.txt" || echo "$name rc=$rc" >> "$OUT/summary.txt"
      {
        echo "########## $name  pass $i  rc=$rc ##########"
        sed -n '/^failures:/,/^test result/p' "$log" | head -80
        grep 'panicked' "$log" | head -5 || true
      } >> "$OUT/failures.txt"
      cp "$log" "$OUT/fail_${name}_pass${i}.log"
    fi
  done
done

total=$(( ${#BINS[@]} * ITER ))
echo
echo "=== $total binary runs in $(( $(date +%s) - start ))s ==="
if [ -s "$OUT/summary.txt" ]; then
  echo
  echo "failing tests (count out of $ITER passes):"
  sort "$OUT/summary.txt" | uniq -c | sort -rn | sed 's/^/  /'
  echo
  echo "details: $OUT/failures.txt"
  exit 1
fi
echo "no failures"
