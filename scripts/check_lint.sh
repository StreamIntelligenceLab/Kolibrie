#!/usr/bin/env bash
# Fail if a named lint fires anywhere in the checked targets.
#
#   scripts/check_lint.sh <lint> [--only <path-prefix> | --crate-src] -- <cargo args>
#
#   scripts/check_lint.sh unreachable_pub -- --workspace --all-targets
#   scripts/check_lint.sh missing_docs --only shared/src -- -p shared --all-targets
#   scripts/check_lint.sh missing_docs --crate-src -- --workspace --all-targets
#
# `--only` restricts the gate to a path prefix, which is how a lint still burning
# down elsewhere can be held at zero in the crates that have reached it.
#
# `--crate-src` restricts it to `<crate>/src/**` across every crate at once. That is
# the library surface: it excludes tests, examples, benches and build scripts, which
# between them still hold ~110 missing_docs that are one crate-level comment per
# target and not part of anything public. Twelve of those come from criterion's own
# macros and cannot be fixed here at all, so a truly workspace-wide gate on this lint
# is not reachable.
#
# Prefer `--crate-src` to a list of per-crate `--only` lines: a new crate is covered
# the day it is added, where a per-crate list silently fails to cover it. The failure
# output names every offending path, so it still says which crate regressed.
#
# The lints stay at `warn` in [workspace.lints] rather than `deny`. Making a module
# private, or adding a public item mid-change, is exactly what trips them, and a hard
# local gate stops the build halfway through an edit. The gate lives here, at PR time.
#
# Checks must run per feature configuration: a default build cannot see items behind
# `#[cfg(feature = "ml")]`, and eight unreachable-pub items were missed that way.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

: "${DYLD_LIBRARY_PATH:=/opt/homebrew/Cellar/python@3.11/3.11.11/Frameworks/Python.framework/Versions/3.11/lib:$HOME/miniconda3/lib}"
export DYLD_LIBRARY_PATH

LINT="${1:-}"
[ -z "$LINT" ] && { echo "usage: $0 <lint> [--only <prefix>] -- <cargo args>" >&2; exit 2; }
shift

ONLY=""
MODE="prefix"
if [ "${1:-}" = "--only" ]; then
  ONLY="$2"; shift 2
elif [ "${1:-}" = "--crate-src" ]; then
  MODE="crate-src"; shift
fi
[ "${1:-}" = "--" ] && shift

LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT

# JSON, not text: `cargo check` emits warnings only for crates it actually
# recompiles, and the text form gives no reliable way to tell a stale cache from a
# clean result. Counting text lines once produced 69 where the real figure was 162.
if ! cargo check "$@" --message-format=json > "$LOG" 2>/dev/null; then
  echo "cargo check failed for: $*" >&2
  cargo check "$@" --message-format short 2>&1 | grep -E '^[^ ]+: error|^error' | head -10 >&2
  exit 2
fi

python3 - "$LOG" "$LINT" "$ONLY" "$*" "$MODE" <<'PY'
import json, sys, pathlib, os
log, lint, only, cfg, mode = sys.argv[1:6]
cwd = os.getcwd() + os.sep
found = set()


def in_scope(rel):
    if mode == "crate-src":
        parts = rel.split("/")
        # <crate>/src/** , or src/** for a crate at the repository root
        return (len(parts) > 2 and parts[1] == "src") or parts[0] == "src"
    return not only or rel.startswith(only)

for line in pathlib.Path(log).read_text().splitlines():
    try:
        msg = (json.loads(line) or {}).get("message") or {}
    except Exception:
        continue
    if ((msg.get("code") or {}).get("code")) != lint:
        continue
    for span in msg.get("spans", []):
        if not span.get("is_primary"):
            continue
        rel = span["file_name"].replace(cwd, "")
        if not in_scope(rel):
            continue
        found.add(f"{rel}:{span['line_start']}")

scope = " under <crate>/src" if mode == "crate-src" else (f" under {only}" if only else "")
if found:
    print(f"{lint} must stay at zero{scope}; {len(found)} found ({cfg}):")
    for loc in sorted(found):
        print(f"  {loc}")
    sys.exit(1)

print(f"{lint}: 0{scope} ({cfg})")
PY
