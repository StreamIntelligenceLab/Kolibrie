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

### Horizon-dependence: idle-tick skipping

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
