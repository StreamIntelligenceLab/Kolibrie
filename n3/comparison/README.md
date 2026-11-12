# Comparing with eyeron

[eyeron](https://github.com/eyereasoner/eyeron) is the reference N3 reasoner this crate
is checked against. The harness runs both on the same programs and compares their derived
facts, up to blank node names.

## Setup

```bash
git clone https://github.com/eyereasoner/eyeron.git ../eyeron
(cd ../eyeron && cargo build --release)
```

Results below were taken against eyeron commit `ee67291`.

## Run

```bash
cargo build --release -p n3 --examples
./target/release/examples/n3_compare --cases n3/comparison/cases            # everything
./target/release/examples/n3_compare -v socrates ancestor                    # a few cases, full diffs
```

`--eyeron-home DIR` (or `$EYERON_HOME`) points at the eyeron checkout, `../eyeron` by
default. Its `examples/` are always run. The expected answer is the stored golden
`output/<name>.n3` beside a case, and otherwise eyeron's own output, run live.

## Outcomes

| Label | Meaning |
|---|---|
| `pass` | Same derived facts, or both runs stopped at an inference fuse |
| `pass-live` | Differs from the stored golden but matches eyeron run on this machine; the golden was made with another platform's floating-point library |
| `MISMATCH` | Different derived facts; the missing (`-`) and extra (`+`) lines follow |
| `unsupported` | The program uses a feature this crate reports instead of guessing at |
| `divergence` | A mismatch listed in `divergences.txt`, where eyeron and N3 disagree |
| `FAILED` | This crate stopped with an error |
| `no-oracle` | No usable expected answer |

The summary also counts unsupported features, which shows what to implement next.

## Known divergences

`cases/divergences.txt` lists cases where eyeron's answer is not the N3 one. Each needs a
reason, and a regression test in `n3/tests/` pins our behaviour.
