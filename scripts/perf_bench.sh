#!/usr/bin/env bash
# Run a tier of benchmarks, with a per-bench wall-clock cap.
#
# Why tiers exist
# ---------------
# `cargo bench --workspace` is not a usable command in this repo. Four benches are
# large parameter sweeps (49–85 cases, up to size_5000 at ~3.9 s per iteration), so
# at criterion's default 100 samples a single case costs minutes. One of them
# (citybench_cross_window_compare) ran >36 minutes without finishing, and reducing
# the sample size does not rescue them — the cost is the workload per iteration,
# not criterion's sampling.
#
# So the benches are split by what they are FOR:
#
#   routine  Guards the refactor. Isolates one mechanism each, runs in minutes.
#            This is the tier to run before/after a refactor step.
#   heavy    Scaling sweeps comparing naive vs incremental strategies. Research
#            results, not refactor guards. Run deliberately, never routinely.
#
# Usage:
#   scripts/perf_bench.sh routine                   # full sampling (authoritative)
#   scripts/perf_bench.sh routine --quick           # reduced sampling (fast check)
#   scripts/perf_bench.sh census                    # classify every bench by cost
#   scripts/perf_bench.sh heavy --cap 600
set -u

ROUTINE=(
  micro_dictionary
  micro_pattern
  micro_scan
  micro_join
  micro_field_access
  optimizer_hotpath
  hybrid_probability
  my_benchmark
)

HEAVY=(
  cross_window_benchmark
  citybench_cross_window_compare
  rsp_citybench_cross_window
  family_tree_cross_window_compare
)

TIER="${1:-routine}"
shift || true

CAP=1800
QUICK=0
EXTRA=()
while [ $# -gt 0 ]; do
  case "$1" in
    --cap)   CAP="$2"; shift 2 ;;
    --quick) QUICK=1; shift ;;
    *)       EXTRA+=("$1"); shift ;;
  esac
done

if [ "$QUICK" -eq 1 ]; then
  # Enough to catch a gross regression, not enough to trust a 5% delta.
  EXTRA+=(--sample-size 10 --warm-up-time 1 --measurement-time 1)
fi

case "$TIER" in
  routine) BENCHES=("${ROUTINE[@]}") ;;
  heavy)   BENCHES=("${HEAVY[@]}") ;;
  census)  BENCHES=("${ROUTINE[@]}" "${HEAVY[@]}") ;;
  *)       echo "unknown tier: $TIER (routine|heavy|census)" >&2; exit 2 ;;
esac

printf '%-36s %8s  %s\n' BENCH SECONDS STATUS
total_start=$(date +%s)
failed=0
for name in "${BENCHES[@]}"; do
  bin=$(ls -t target/release/deps/"$name"-* 2>/dev/null | grep -v '\.d$' | head -1)
  if [ -z "$bin" ]; then
    printf '%-36s %8s  %s\n' "$name" - "NOT BUILT (cargo bench --no-run)"
    failed=1
    continue
  fi

  start=$(date +%s)
  # `--bench` is REQUIRED. With `harness = false` criterion defaults to test mode
  # (one iteration per case, no measurement) when it is not passed; cargo supplies
  # it, a direct invocation must too. Without it this script silently reports
  # "ok" in 0s and records nothing.
  "$bin" --bench --noplot --quiet ${EXTRA+"${EXTRA[@]}"} >/dev/null 2>&1 &
  pid=$!
  ( sleep "$CAP"; kill -9 "$pid" 2>/dev/null ) &
  watchdog=$!
  wait "$pid" 2>/dev/null
  rc=$?
  kill "$watchdog" 2>/dev/null
  wait "$watchdog" 2>/dev/null
  elapsed=$(( $(date +%s) - start ))

  if [ "$rc" -eq 0 ]; then
    status=ok
  elif [ "$elapsed" -ge "$CAP" ]; then
    status="CAPPED at ${CAP}s"; failed=1
  else
    status="failed rc=$rc"; failed=1
  fi
  printf '%-36s %8s  %s\n' "$name" "$elapsed" "$status"
done

echo
echo "tier '$TIER' total: $(( $(date +%s) - total_start ))s"
exit "$failed"
