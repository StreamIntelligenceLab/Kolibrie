# ω-automaton phase — unbounded-time reasoning

The finite interval evaluator caps at `K` iterations, so recursive-in-time programs
(whose minimal model is **infinite**) get truncated/wrong answers. This phase adds
unbounded-time materialization + entailment queries, validated against MeTeoR's
**canonical model** (its ground truth for infinite / eventually-periodic models).

## What ships — full lasso (prefix + repeating period)

`materialize_omega` returns a `PeriodicModel` = finite prefix + period, the
automata-theoretic canonical model.

- **Infinity-aware intervals** (`automata/interval.rs`): `NEG_INF`/`POS_INF`
  sentinels, MeTeoR's inf-openness rules, inf-guarded metric shifts, `contains_point`,
  `shifted`.
- **Materialization + acceleration** (`automata/omega.rs`): past-only recursion
  propagates **forward**. Beyond `R = max_data_endpoint + max_reach` the model is
  periodic with period dividing `P = lcm` of the metric constants. So:
  1. materialize a prefix clipping ends to `H = R + 3P`;
  2. **eventually-always** — a fact holding contiguously over `[R, R+P]` is widened
     to `[l, +∞)` (represented exactly);
  3. **genuinely periodic (gappy)** — verify the tail is `P`-periodic (two
     consecutive period windows agree) and keep it. No longer refused.
- **Entailment** (`automata::entails`, driver `--strategy omega --entail`): answers
  `Pred(args)@[l,r]` at *any* time in O(1). Far-future queries are reduced modulo `P`
  into the materialized period window and checked by **real-interval inclusion** (so
  a query spanning a gap correctly fails; continuous saturation via `[l,+∞)`).

## Soundness note (ℤ vs ℝ)

Saturation to `[l,+∞)` requires the recursion to produce **overlapping** copies
(operator interval including 0, e.g. `Diamondminus[0,1]`). `Diamondminus[1,1]` of a
point yields disjoint points (`[5,5]`,`[6,6]` have a real gap) — that is genuinely
gappy under real semantics and is correctly refused, matching MeTeoR.

## Running / validating

```bash
cargo build --release -p datalogmtl --example meteor_compare
# Entailment (far-future answered instantly):
./target/release/examples/meteor_compare \
    --program comparison/omega/program.txt --data comparison/omega/data.txt \
    --strategy omega --entail comparison/omega/queries.txt
# Cross-check against MeTeoR's canonical model:
cd comparison/omega && python3 validate_omega.py     # -> ALL MATCH
```

Two validated cases (`validate_omega.py <case>` → **ALL MATCH** vs MeTeoR canonical):
- **always** (`Alarm:-Trigger; Alarm:-Diamondminus[0,1]Alarm`, `Trigger(a)@[5,5]`):
  `Alarm(a)=[5,+∞)`; entailed at `t=1e6`, and over the whole span `[5,1e6]`.
- **periodic** (`P:-Seed; P:-Diamondminus[2,2]P`, `Seed(a)@[0,0]`): period 2, holds at
  even ticks forever — `P(a)@[1000,1000]`=true, `@[1001,1001]`=false,
  `@[1000,1001]`=false (no continuous coverage across the gap).

## Next sub-milestones

1. **Left (past) periods** — needed once future operators (`Diamondplus`/`Boxplus`)
   enter the fragment (recursion could then propagate into the past).
2. **Multi-constant period tightening** — `P = lcm` is a safe period; the true period
   may be a divisor (MeTeoR's gcd-aligned detection finds the minimal one).
