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

Three kinds of variation were measured here (Oct 2026, M-series laptop), each larger
than the last:

| | median | p90 | max |
|---|---|---|---|
| two passes back-to-back | 1.5% | 4.5% | 8.7% |
| across sessions, same code | 3.0% | 8.1% | 12.5% |
| **repeated runs, per benchmark (4-run baseline)** | **2.6%** | **11.4%** | **38.3%** |

The third row is the one that matters, and getting it wrong cost real time twice.
Noise was originally derived from two back-to-back passes — which measures short-term
repeatability, and in any case cannot estimate a spread from two samples. It
understated 25 of 32 benchmarks, some by more than an order of magnitude:

| benchmark | 2-run estimate | 4-run reality |
|---|---|---|
| `micro_dictionary/decode` | 0.95% | **38.3%** |
| `micro_field_access/outlined_accessor` | 0.03% | 11.4% |
| `micro_join/schema_plus_tuples` | 1.20% | 11.8% |
| `execution_hot_paths/filter_numeric` | 0.22% | ~9% |

Both false positives had the same shape: a single before/after pair produced a delta
just over a floor derived from a baseline that claimed near-zero noise.
`execution_hot_paths/filter_numeric` was flagged at +5.7% against a recorded 0.22%,
and four runs in *each* state then showed the distributions overlapping with the
unchanged state marginally slower.

Cross-session drift is real too: `micro_pattern/all_variables` measured 1.244 ms in
one session and 1.119 ms in the next with identical source.

### Benchmarks that cannot currently detect a regression

A benchmark whose own spread exceeds the effect you are looking for is not evidence.
As of the 4-run baseline:

| benchmark | noise | note |
|---|---|---|
| `micro_dictionary/decode` | 38.3% | three runs cluster at ~70 µs, one at 97 µs |
| `micro_scan_broad_all/dyn_boxed_iter` | 19.6% | allocation-dominated at 40k quads |
| `micro_join/schema_plus_tuples` | 11.8% | |
| `micro_field_access/*_accessor` | 11.4% | |

These still answer the *design* questions they were written for, where the effects are
large (the join representation differs by 4.7×, not 12%). They are poor regression
guards. The fix is to make them quieter — `micro_scan` already dropped `SUBJECTS` from
20k to 5k for this reason — not to narrow the threshold until they fire.

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

**Then judge anything it flags.** Two `collect` summaries carry no noise data, so that
comparison falls back to the flat ±5% floor and will over-report on noisy benchmarks —
it tells you the delta, not whether the delta is real. `compare` warns when this
happens. For each flagged benchmark:

1. Look up its `noise_pct` in `baseline.json`. A delta under ~2× that figure is not
   evidence. This alone would have dismissed both historical false positives.
2. If it is still borderline, run **both** states 3–4 times and compare the
   distributions, not single points. That is what settled every ambiguous case so far:

```bash
bin=$(ls -t target/release/deps/<bench>-* | grep -v '\.d$' | head -1)
for i in 1 2 3 4; do
  rm -rf target/criterion
  "$bin" --bench --noplot --quiet '<case filter>' >/dev/null 2>&1
  python3 -c "import json,glob; f=glob.glob('target/criterion/**/<case>/new/estimates.json',recursive=True); print(json.load(open(f[0]))['median']['point_estimate']/1e6)"
done
```

`baseline.json` is a historical reference for catching gross drift, not a precision
instrument. Treat differences against it below that benchmark's recorded noise as
unproven.

### Like-for-like is not optional, and the 4-run baseline does not rescue you from it

Noise is a property of the benchmark **and how it was invoked**, not of the benchmark
alone. Measured on `execution_hot_paths/filter_numeric`, same code, same machine, same
afternoon:

| how it was run | spread |
|---|---|
| inside four full `routine` tier passes | **1.9%** |
| filtered to that one case, four times, after a rebuild | **9.0%** |

So the committed baseline — built from full-tier passes — understates what you will see
if you investigate a single case in isolation, which is exactly what the follow-up
procedure above tells you to do. Two of the three historical false positives would
still clear their limits against this baseline for that reason.

The 4-run baseline fixes the arithmetic (two samples cannot estimate a spread) but it
cannot anticipate an invocation pattern it never saw. **Compare like-for-like: same
tier, same filter, same way.** When a flagged delta matters, re-measure both states
using the same invocation you are judging them by.

## Regenerating the baseline

Needs **four or more** passes. A baseline without a per-benchmark noise figure cannot
answer "is this delta real?", and one built from two passes answers it wrongly — see
above. `baseline` warns if given fewer than four.

```bash
for i in 1 2 3 4; do
  rm -rf target/criterion && scripts/perf_bench.sh routine
  python3 scripts/perf_report.py collect -o /tmp/p$i.json
done
python3 scripts/perf_report.py baseline /tmp/p{1,2,3,4}.json -o perf-baseline/baseline.json
```

Four routine-tier passes take about 24 minutes. Noise is recorded as the full observed
range, which is deliberately conservative: missing a marginal regression is cheaper
than reporting phantom ones, because a harness that cries wolf stops being read.

Clear `target/criterion` first: `collect` cannot tell when data was produced, so a
stale directory silently mixes runs — including runs at different sample sizes.

## Gotchas

- **Pass `--bench`.** With `harness = false`, criterion defaults to *test mode* (one
  iteration per case, no measurement) when invoked directly without it. `cargo bench`
  supplies it; a direct binary invocation must too. `perf_bench.sh` handles this.
  Symptom: benches report success in 0 s and record nothing.
- **No CI gate.** Shared-runner variance exceeds the regressions worth hunting, and a
  gate that cries wolf gets ignored.
