#!/usr/bin/env bash
# Fail if `unreachable_pub` fires anywhere. Arguments are passed straight to
# `cargo check`, so the caller chooses the feature configuration:
#
#   scripts/check_unreachable_pub.sh --workspace --all-targets
#   scripts/check_unreachable_pub.sh -p kolibrie --features ml --all-targets
#
# This lint is deliberately `warn` in [workspace.lints] rather than `deny`: making a
# module private is exactly what produces the warning, so a hard local gate would
# break the build in the middle of the module moves that steps 5-6 require. The gate
# lives here, at PR time, instead.
#
# Checks must run per feature configuration. A default build cannot see items behind
# `#[cfg(feature = "ml")]`, and eight such items were missed on the first pass of the
# step-4b burn-down for precisely that reason.
#
# Reports the warning's own `--> file:line`, not the matching log line, so the output
# points at the offending item.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

: "${DYLD_LIBRARY_PATH:=/opt/homebrew/Cellar/python@3.11/3.11.11/Frameworks/Python.framework/Versions/3.11/lib:$HOME/miniconda3/lib}"
export DYLD_LIBRARY_PATH

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

python3 - "$LOG" "$*" <<'PY'
import json, sys, pathlib, os
log, cfg = sys.argv[1], sys.argv[2]
cwd = os.getcwd() + os.sep
found = set()
for line in pathlib.Path(log).read_text().splitlines():
    try:
        msg = (json.loads(line) or {}).get("message") or {}
    except Exception:
        continue
    if ((msg.get("code") or {}).get("code")) != "unreachable_pub":
        continue
    for span in msg.get("spans", []):
        if span.get("is_primary"):
            found.add(f"{span['file_name'].replace(cwd, '')}:{span['line_start']}")

if found:
    print(f"unreachable_pub must stay at zero; {len(found)} found ({cfg}):")
    for loc in sorted(found):
        print(f"  {loc}")
    print()
    print("Make the item `pub(crate)`, or add a scoped #[allow(unreachable_pub)] with")
    print("a comment if the lint cannot be satisfied (test and example crate roots).")
    sys.exit(1)

print(f"unreachable_pub: 0 ({cfg})")
PY
