# datalogmtl ↔ MeTeoR correctness harness

Uses the [MeTeoR](https://github.com/wdimmy/MeTeoR) reasoner as a **ground-truth
baseline** to validate the `datalogmtl` engine on a shared corpus of small
programs, authored once in MeTeoR's native syntax and fed to both engines.

## Supported fragment

The corpus is restricted to the fragment both engines share:

- **Past operators only**: `Boxminus[a,b]`, `Diamondminus[a,b]`, `L Since[a,b] R`
  (operators may stack, e.g. `Boxminus[1,2]Diamondminus[0,1]A(X)`).
- **Plain (operator-free) rule heads.**
- **Closed integer intervals** `[a,b]` (or a point `[a]` / `@t`).
- **Predicate arity ≤ 2**, mapped to RDF triples:
  - unary `A(x)`  ↔ `(x, rdf:type, A)`
  - binary `C(x,y)` ↔ `(x, C, y)`
  - arity 0 (propositional) and arity ≥ 3 are rejected by the parser.

Out-of-fragment input (future operators `Boxplus`/`Diamondplus`/`Until`, open
interval bounds, head operators, non-integer times) produces a descriptive error.

## How comparison works

MeTeoR is batch + continuous-interval; datalogmtl is streaming + discrete point
time. They are reconciled on the integer grid `[0, T]`:

- `T = max fact endpoint + max operator interval width` (computed identically on
  both sides in `compare.py`).
- Interval facts `A(a)@[l,r]` are **densified** — inserted at every integer tick
  in `[l,r]` — for the Rust engine, which is advanced over ticks `0..=T`.
- Both outputs are normalized to the set of `(atom, integer t)` that hold, then
  re-coalesced into closed integer intervals `Pred(args)@[l,r]` and diffed.

## Running

```bash
# Build the Rust driver once
DYLD_LIBRARY_PATH="/opt/homebrew/.../python@3.11/.../lib" \
  cargo build -p datalogmtl --example meteor_compare

# Run all cases (MeTeoR imported from $METEOR_HOME, default the local checkout)
python3 compare.py                 # snapshot store (Phase 1)
python3 compare.py --store interval  # interval store (Phase 2)
python3 compare.py --case diamond_basic -v   # one case, show output
```

Environment:
- `METEOR_HOME` — path to the MeTeoR checkout (default
  `/Users/u0164257/Documents/Github/MeTeoR`).
- `DYLD_LIBRARY_PATH` — Python 3.11 dylib (see repo `CLAUDE.md`).

## Layout

| File | Role |
|---|---|
| `cases/<name>/{program.txt,data.txt}` | one test case in MeTeoR syntax |
| `run_meteor.py` | runs MeTeoR, emits normalized integer-interval lines |
| `compare.py` | computes `T`, runs both engines, diffs, reports PASS/MISMATCH |
| `../examples/meteor_compare.rs` | Rust driver (parses corpus, runs engine) |
| `../src/parser.rs` | MeTeoR-syntax → `DatalogMTLRule` + facts |
| `../src/meteor_fmt.rs` | RDF triples → MeTeoR atom text + coalescing |

## Cases

| Case | Exercises |
|---|---|
| `diamond_basic` | `Diamondminus` (existential lookback) |
| `boxminus_basic` | `Boxminus` (universal window) — regression for the leading-boundary fix |
| `since_basic` | `Since` (reset + continuation) |
| `reach` | non-temporal join + recursion (transitive closure) |
| `chain_box_diamond` | two rules: `Box` feeding `Diamond` |
| `nested_box_diamond` | nested operators in one atom (`Boxminus[..]Diamondminus[..]A`) |
| `diamond_conj` | conjunction of two `Diamond`s joined on a variable |
| `temporal_binary_join` | binary predicates joined, one side under `Diamond` (wasNear-style) |
| `since_derived` | `Since` whose continuation is a derived predicate (guarded) |
| `recursive_temporal_path` | temporal transitive closure (recursion + `Diamond`) |
| `multi_op_conj` | `Box` and `Diamond` conjoined in one body |

## Status

All corpus cases pass against MeTeoR under both store backends (Phase 1 snapshot
and Phase 2 interval).

### Rule-safety divergence (corpus convention, not a bug)

MeTeoR will not bind a head variable that occurs **only** inside a binary
`Since`/`Until` literal — such a variable needs a *guard* (an ordinary body atom
binding it), otherwise MeTeoR derives nothing. datalogmtl is more permissive and
binds the variable from within `Since`. Corpus rules therefore guard such
variables (e.g. `Alarm(X):-Warn(X),Warn(X)Since[1,8]Spike(X)`) to stay in the
common fragment. Found via `since_derived`.

### Bugs found & fixed via this harness

- **`Box` leading-boundary over-derivation** (found by `boxminus_basic`).
  For `Boxminus[1,2]A(X)` with `A(a)@[0,10]`, MeTeoR yields `B(a)@[2,11]` but
  datalogmtl originally yielded `B(a)@[1,11]`. At `t=1` the required window
  `[t-2,t-1]=[-1,0]` extends before time 0; `Interval::absolute_range`'s
  `saturating_sub` clamped the lower bound to 0, so `Box` saw only `A@0` and
  wrongly succeeded. Fixed by guarding `eval_box` on `t < interval.end` (the
  whole universal window must lie within observable time `[0, t]`).
  See `evaluator.rs:eval_box`.
