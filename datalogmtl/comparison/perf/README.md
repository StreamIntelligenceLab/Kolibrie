# Performance harness (datalogmtl vs MeTeoR)

Mirrors MeTeoR's LUBM methodology — LUBM-style entities/relations with several
random temporal intervals per atom — but with a **scalable synthetic generator**
so we can grow the dataset arbitrarily and stay inside the shared past-fragment.

## Workload

- Program: `programs/lubm_past.txt` — the past-fragment subset of MeTeoR's
  `Π¹_L` (`p1.txt`): `Boxminus`/`Diamondminus` rules + one binary temporal join,
  plain heads, arity ≤ 2 (drops the `Boxplus` rule, which is future-only).
- Data: `gen_lubm.py --scale N` emits `UndergraduateStudent`, `GraduateStudent`,
  and `publicationAuthor` atoms for `N` entities, each atom with
  `--intervals k` random closed integer intervals in `[0, --horizon H]`.
- Both engines read the identical generated file.

## Running

```bash
cargo build --release -p datalogmtl --example meteor_compare
python3 perf.py --scales 100,500,1000,2000 [--horizon 20] [--intervals 3]
                [--store snapshot|interval] [--repeats 3] [--verify]
```

`perf.py` times only the reasoning step on each side (MeTeoR `materialize`; the
Rust tick loop), not parsing/loading.

## Results (horizon=20, intervals/atom=3, release build)

After the hot-path optimization (indexed joins — see below):

| scale | facts | meteor_ms | rust_ms | rust/mtr |
|------:|------:|----------:|--------:|---------:|
|   100 |   900 |     19.8  |    20.1 |    1.02  |
|   200 |  1800 |     39.1  |    33.1 |    0.84  |
|   400 |  3600 |     81.2  |    67.3 |    0.83  |
|   800 |  7200 |    149.7  |   120.7 |    0.81  |
|  1600 | 14400 |    303.2  |   264.2 |    0.87  |
|  3200 | 28800 |    601.1  |   590.7 |    0.98  |

- Both engines now scale **≈ linearly** in dataset size.
- datalogmtl is **at par with MeTeoR (~0.8–1.0×)**, occasionally slightly faster.

### The optimization

The original engine was `O(facts²)` per tick and ~20–160× slower (e.g. scale 400:
**13648 ms → 67 ms, ~200× faster**). Three fixes, all in the base-atom seeding /
`Base`-atom evaluation path:

1. **Indexed `query_at`** (`store.rs`) — use `UnifiedIndex::get_matching_triples`
   (constant-position SPO/POS/OSP lookup) instead of scanning every fact at `t`.
2. **Indexed base join** (`evaluator.rs::seed_base_atoms`) — join a rule's `Base`
   atoms by substituting the running binding into each next pattern and probing
   the index, instead of a nested loop over all current facts (`join_base_atoms`,
   removed).
3. **Specialized inner `Base` queries** (`evaluator.rs::eval_atom_at`) — substitute
   already-bound variables into the pattern before querying, so `Diamond`/`Box`
   inner lookups are constrained/indexed too.

## Deeper evaluation: two workloads, larger scale, more dimensions

Two programs (select with `perf.py --program`):
- **`past`** (`programs/lubm_past.txt`, 4 rules) — `Boxminus`/`Diamondminus` + a binary join.
- **`deep`** (`programs/lubm_deep.txt`, 8 rules) — a longer derivation chain
  (`UndergraduateStudent → RAC → RA → ExperiencedRA → Eligible`), multi-way temporal
  joins (`advisor` ⋈ `Diamondminus takesCourse`), and a guarded `Since`. `gen_lubm.py
  --rich` emits the extra base predicates (`teachingAssistant`/`takesCourse`/`advisor`).

**MeTeoR baseline mode matters.** MeTeoR's default **seminaive** mode is *incorrect*
for the `deep` program: a `Since` whose trigger (`RAC`) is a *derived* predicate is
missed (e.g. `DedicatedTA` is never derived), whereas MeTeoR **naive** derives it and
**matches our interval engine**. So the correct baseline for `deep` is `--meteor-mode
naive`; `perf.py`/`compare.py` expose `--meteor-mode`. This is a MeTeoR limitation, not
ours — our naive interval fixpoint is correct (parity OK vs MeTeoR naive).

### Scale (release, horizon=20, intervals/atom=3)

`deep` — interval vs MeTeoR **naive** (correct baseline, capped, it is slow):

| scale | facts   | meteor_ms | interval_ms | interval/mtr |
|------:|--------:|----------:|------------:|-------------:|
|   500 |   9,000 |     723   |     13.5    |    0.019     |
| 2,000 |  36,000 |   2,941   |     55.4    |    0.019     |
| 20,000| 360,000 |     —     |    673.6    |      —       |
| 50,000| 900,000 |     —     |  2,161.7    |      —       |

`past` — interval vs MeTeoR **seminaive** (correct & fast here):

| scale | facts   | meteor_ms | interval_ms | interval/mtr |
|------:|--------:|----------:|------------:|-------------:|
| 1,000 |   9,000 |     188   |      7.9    |    0.042     |
|10,000 |  90,000 |   1,951   |     76.5    |    0.039     |
|50,000 | 450,000 |     —     |    501.6    |      —       |

Interval is **linear** in dataset size (~2–3 µs/fact) to ~1M facts in a couple of
seconds, and **~25× (past) / ~50× (deep) faster than MeTeoR**.

### Fact density and horizon (`deep`, scale=2,000)

| intervals/atom | facts   | meteor_ms | interval_ms |     | horizon | interval_ms |
|---------------:|--------:|----------:|------------:|-----|--------:|------------:|
|              1 |  12,000 |   1,960   |    37.7     |     |      20 |    55.0     |
|              3 |  36,000 |   2,941   |    55.0     |     |     100 |    62.4     |
|              6 |  72,000 |   3,194   |    59.8     |     |     500 |    63.6     |
|             10 | 120,000 |   3,186   |    59.6     |     |   2,000 |    74.4     |

- **Near-flat in fact density**: coalescing bounds the number of *distinct* intervals,
  so 10× the raw intervals barely moves interval time (37→60 ms) while facts grow 10×.
- **Near-flat in horizon** (55→74 ms over 100× horizon) — event-complexity confirmed.

## Future operators (`--program future`, static mode)

`programs/lubm_future.txt` (4 rules) mirrors `lubm_past` but with **future**
operators (`Diamondplus`/`Boxplus`) over a derived chain + a binary join. These
require static data, so `perf.py --program future` runs `--mode static` on the
interval engine. Parity vs MeTeoR is **OK** (unary future ops over derived are
seminaive-correct). Same profile as the past ops:

| scale | facts   | meteor_ms | interval_ms | interval/mtr |
|------:|--------:|----------:|------------:|-------------:|
|   500 |   4,500 |     104   |      4.5    |    0.043     |
| 2,000 |  18,000 |     421   |     17.0    |    0.040     |
| 5,000 |  45,000 |   1,086   |     40.4    |    0.037     |
|50,000 | 450,000 |     —     |    601.2    |      —       |

- **Linear** to 450k facts (0.6 s); **~25× faster** than MeTeoR.
- **Density-flat** (1→10 intervals/atom: 13.8→16.5 ms) and **horizon-flat**
  (H 20→2000: 16.7→17.7 ms) — same event-complexity as the past fragment.

Correctness: `comparison/cases_future/` — 5 cases (diamondplus, boxplus, until,
mixed past+future, stacked future) all parity-checked vs MeTeoR.

## Interval-native "automata" strategy — `--strategy interval` (recommended)

A second evaluation strategy (`datalogmtl/src/automata/`) replaces the per-tick loop
with **interval-arithmetic transducers** (a Rust port of MeTeoR's operator arithmetic)
over a **semi-naive interval fixpoint**. Facts stay as intervals (no densification);
temporal operators are endpoint transforms (`Diamond`=dilate, `Box`=erode, `Since`=
anchor-intersect+shift). Cost scales with interval **endpoints**, not horizon or width.

Because it uses MeTeoR's real-line interval semantics, it is an **exact** MeTeoR match —
`perf.py --strategy interval --verify` reports **parity OK** on the LUBM workload (the
ℤ-vs-ℝ artifact is gone), and it is **horizon-independent**:

Horizon sweep @ scale=400 (`reason_ms`):

| horizon | meteor | tick engine | interval (automata) |
|--------:|-------:|------------:|--------------------:|
|      20 |   77   |     60      |        **3.3**      |
|     100 |   86   |    315      |        **3.6**      |
|     500 |   88   |   1650      |        **3.7**      |
|    2000 |   89   |   6839      |        **3.8**      |

Scale sweep @ horizon=20: interval is **~0.04–0.09× MeTeoR** (11–25× faster) and
~20× faster than the tick engine, with near-flat scaling. Milestone-1 limits: finite
(non-periodic) programs only (iteration cap `K=1000`); `Prev` and future operators are
out of the ported fragment. Unbounded/periodic programs are the ω-automaton phase 2.

## Tick engine (historical) — horizon-dependence + idle-tick skipping

The engine advances tick-by-tick, so a naive loop over `0..=H` pays `O(H)` even
when the data occupies a tiny slice of the timeline. The driver now **skips
provably-empty ticks**: a tick is evaluated only if some base fact lies within a
rule's temporal *reach* of it (`reach = num_rules * w_max`, a safe bound on how
far a fact propagates forward through the rule chain; recursive programs fall
back to every tick). Skipped ticks have no active base fact and none in any
operator's lookback, so they derive nothing — output is bit-identical (`--no-skip`
disables it for A/B).

This makes reasoning **independent of idle horizon**. With `N=5` narrow-interval
entities and a growing horizon:

| horizon | `--no-skip` | skip (default) |
|--------:|------------:|---------------:|
|   2,000 |     2.8 ms  |     1.55 ms |
|  20,000 |    12.1 ms  |     1.62 ms |
| 200,000 |   123.0 ms  |     1.84 ms |

no-skip is linear in `H`; skip is flat. (Extreme case: `H=200k` with ~27 facts,
**125 ms → 1 ms**.)

### Remaining cost: fact density, not idle time

Skipping helps only when the horizon has genuine *gaps*. When facts densely cover
the timeline (wide intervals), there is little to skip and cost is proportional to
the facts' actual temporal extent × entities — real work, not idle horizon. After
the indexing fix, empty ticks are already nearly free (one indexed miss per rule),
so this residual is *not* removable by skipping. Collapsing it further would need
interval-native / delta-based (semi-naive) evaluation that reasons over interval
endpoints instead of per tick — a larger redesign.

## Correctness on this workload — integer time vs real time

Box/Since now use **dense integer semantics** (every integer point in the window),
which matches MeTeoR on data whose intervals are not integer-adjacent. The hand-
crafted correctness suite (`../cases/`, adjacency-free) passes exactly, and
`gen_lubm.py` coalesces each atom's *base* intervals (merging gaps ≤ 1).

A residual divergence remains and is **fundamental**, not an engine bug:
datalogmtl reasons over integer time (ℤ, `u64` ms ticks) while MeTeoR reasons over
real/rational time (ℝ). A temporal operator can expand two real-separated
intervals into integer-adjacency. Example (`g28`): `Diamond[0,2]` over
`GS@[2,17]` and `GS@[20,20]` gives `RAC = [2,19] ∪ [20,22]`. MeTeoR keeps the real
gap `(19,20)`, so `Box[0,5]RAC → RA=[7,19]`; datalogmtl has `RAC` at ticks 19 and
20 with nothing between, so it is contiguous `[2,22]` and `Box[0,5]RAC → RA=[7,22]`.

Consequently `perf.py --verify` reports boundary-only MISMATCHes on the generated
LUBM workload. These are the ℤ-vs-ℝ artifact, confined to interval edges, and do
not affect the timing conclusion (both engines do comparable work). Exact output
parity between an integer-time and a real-time reasoner is not achievable on
arbitrary chained-operator programs; the adjacency-free `../cases/` suite is the
exact-correctness oracle, and MeTeoR here is the performance baseline.
