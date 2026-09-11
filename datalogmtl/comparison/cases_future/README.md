# Future-operator cases (static mode)

Future operators (`Diamondplus`, `Boxplus`, `Until`) look into not-yet-arrived time,
so they are valid **only on static (whole-timeline) data**, never on a stream. They
require `--mode static`, which uses the interval (static) engine — the tick engine is
streaming/forward-time and cannot evaluate them.

```bash
cargo build --release -p datalogmtl --example meteor_compare
# validate these cases against MeTeoR:
cd datalogmtl/comparison
python3 compare.py cases_future --mode static --strategy interval
```

Mode gating:
- `--mode streaming` (default) + a future operator → parse error.
- `--mode static --strategy tick` → error (tick engine can't do future ops).
- `--mode static` with no `--strategy` → auto-selects the interval engine.

Cases (all parity-checked vs MeTeoR):
- `diamondplus_basic`: `B(X):-Diamondplus[1,5]A(X)`, `A(a)@[10,10]` → `B(a)@[5,9]`.
- `boxplus_basic`: `B(X):-Boxplus[1,2]A(X)`, `A(a)@[10,20]` → `B(a)@[9,18]`.
- `until_basic`: `C(X):-A(X)Until[1,5]B(X)`, `A(a)@[0,20]`,`B(a)@[15,15]` → `C(a)@[10,14]`.

Scope: finite static materialization. Unbounded-time + future operators (left/past
periods in the ω-automaton) is a deferred follow-up.
