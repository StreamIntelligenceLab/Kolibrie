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
- **Bidirectional materialization + acceleration** (`automata/omega.rs`): past
  operators propagate **forward** (a *right* period toward +∞); future operators
  propagate **backward** (a *left* period toward −∞). Beyond `R = max_end + reach`
  and before `L = min_start − reach` the model is periodic with period dividing
  `P = lcm` of the metric constants. So:
  1. materialize a prefix clipping intervals to `[Lb, H]` (`H=R+3P`, `Lb=L−3P`);
  2. **eventually-always** — a fact contiguous over `[R,R+P]` → `[l,+∞)`, or over
     `[L−P,L]` → `(−∞,e]` (represented exactly);
  3. **genuinely periodic (gappy)** — verify each tail is `P`-periodic (two
     consecutive period windows agree) and keep it.
- **Entailment** (`automata::entails`, driver `--strategy omega --mode static
  --entail`): answers `Pred(args)@[l,r]` at *any* time (far future OR far past) in
  O(1) — reduce the query modulo `P` into the materialized right/left period window
  and check **real-interval inclusion** (a query spanning a gap fails; continuous
  saturation via `[l,+∞)` / `(−∞,e]`).

### MeTeoR `fact_entailment` limitations found

MeTeoR's canonical `fact_entailment` is the oracle, but two limitations surfaced
(our engine answers these correctly):
- **Wide queries in the *left* (past) periodic tail** return False even when the
  fact holds continuously there (e.g. `Q=(−∞,100]`, `Q@[10,20]` → MeTeoR False, ours
  True). Point queries are handled correctly, so the cross-check uses point queries.
- (Right-tail wide queries *are* handled by MeTeoR — the asymmetry is left-only.)

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

Five validated cases (`validate_omega.py <case>` → **ALL MATCH** vs MeTeoR canonical):
- **.** (right always): `Alarm:-Diamondminus[0,1]Alarm` → `[5,+∞)`; entailed at `t=1e6`.
- **periodic** (right gappy): `P:-Diamondminus[2,2]P` → even ticks forever (+∞ side).
- **left_periodic** (future recursion): `P:-Diamondplus[2,2]P`, `Seed@[100,100]` → even
  ticks toward −∞; `P(a)@[10,10]`=true, `@[11,11]`=false.
- **both_periodic**: past + future recursion (`Diamondminus[2,2]` and `Diamondplus[2,2]`)
  → period-2 both directions; far-past `[10,10]` and far-future `[10000,10000]` true.
- **past_always**: `Q:-Diamondplus[0,1]Q` → `(−∞,100]` (continuous past saturation).

## Next sub-milestones

1. **Multi-constant period tightening** — `P = lcm` is a safe period; the true period
   may be a divisor (MeTeoR's gcd-aligned detection finds the minimal one).
2. **Rational (sub-integer) time** — currently integer endpoints; MeTeoR uses `Decimal`.
