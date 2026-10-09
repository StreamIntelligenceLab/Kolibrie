# Performance baseline

`baseline.json` is a committed summary of benchmark medians plus each benchmark's
measured noise. It exists because criterion writes to `target/criterion`, which is
gitignored — so numbers vanish on `cargo clean`, are not comparable across machines,
and leave no reference point in history.

## Running benchmarks

`cargo bench --workspace` is **not** a usable command in this repo. Benches are split
into two tiers by `scripts/perf_bench.sh`:

```bash
scripts/perf_bench.sh routine          # 8 benches, 32 cases, ~350s — the refactor guard
scripts/perf_bench.sh routine --quick  # reduced sampling, ~30s — gross errors only
scripts/perf_bench.sh heavy            # 4 scaling sweeps — minutes to hours
scripts/perf_bench.sh census           # everything, for classification
```

The `heavy` tier is four parameter sweeps (49–85 cases each, up to `size_5000` at
~3.9 s per iteration). `citybench_cross_window_compare` ran **over 36 minutes without
finishing** under `cargo bench --workspace`. Reducing the sample size does not help:
the cost is the workload per iteration. These are research results comparing naive
against incremental strategies, not refactor guards — run them deliberately.

## Judging a change

Two kinds of variation were measured here (Oct 2026, M-series laptop):

| | median | p90 | max |
|---|---|---|---|
| within-session (two passes back-to-back) | 1.5% | 4.5% | 8.7% |
| cross-session (hot machine vs cooled) | 3.0% | 8.1% | **12.5%** |

The cross-session figure is the one that matters, and it was observed concretely:
`micro_pattern/all_variables` measured 1.244 ms in one session and 1.119 ms in the
next **with identical source**. Two back-to-back passes measure short-term
repeatability, not reproducibility across sessions.

**Authoritative procedure — before/after in the same session:**

```bash
# on the unmodified checkout
rm -rf target/criterion && scripts/perf_bench.sh routine
python3 scripts/perf_report.py collect -o /tmp/before.json

# apply the change, rebuild, then
rm -rf target/criterion && scripts/perf_bench.sh routine
python3 scripts/perf_report.py collect -o /tmp/after.json

python3 scripts/perf_report.py compare /tmp/before.json --current /tmp/after.json
```

`baseline.json` is a historical reference for catching gross drift, not a precision
instrument. Treat sub-12% differences against it as unproven.

Always compare like-for-like — the same tier, run the same way. A single-bench run is
not comparable to a full-tier baseline.

## Regenerating the baseline

Needs two passes, because a baseline without a per-benchmark noise figure cannot
answer "is this delta real?". Benchmarks here differ in noise by two orders of
magnitude (0.1% to 10.7%), so one global threshold is either blind to the quiet ones
or cries wolf on the noisy ones.

```bash
rm -rf target/criterion && scripts/perf_bench.sh routine
python3 scripts/perf_report.py collect -o /tmp/p1.json
rm -rf target/criterion && scripts/perf_bench.sh routine
python3 scripts/perf_report.py collect -o /tmp/p2.json
python3 scripts/perf_report.py baseline /tmp/p1.json /tmp/p2.json -o perf-baseline/baseline.json
```

Clear `target/criterion` first: `collect` cannot tell when data was produced, so a
stale directory silently mixes runs — including runs at different sample sizes.

## Gotchas

- **Pass `--bench`.** With `harness = false`, criterion defaults to *test mode* (one
  iteration per case, no measurement) when invoked directly without it. `cargo bench`
  supplies it; a direct binary invocation must too. `perf_bench.sh` handles this.
  Symptom: benches report success in 0 s and record nothing.
- **No CI gate.** Shared-runner variance exceeds the regressions worth hunting, and a
  gate that cries wolf gets ignored.
