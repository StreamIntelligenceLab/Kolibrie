# Milestones 4–5: usage and compatibility

Base: `4269332` on `optimizer-improvement`.

## ML is opt-in

Ordinary `cargo build` uses the core, shared, datalog, CLI and HTTP packages.
The ML crate and Python binding remain workspace members, but are not default
members. `--workspace` explicitly includes them and therefore requires Python.

Enable execution with `cargo build -p kolibrie --features ml`, or forward the
same feature through `cli`, `kolibrie-http-server`, or the Python package.
ML syntax still parses without the feature. Attempting execution then returns
`ML_FEATURE_DISABLED`; the HTTP handler uses 503. With the feature enabled,
the default database still denies ML execution.

The host selects authority using `MlExecutionContext` and the additive
`execute_sparql_with_ml_context(query, &mut database, &context)` entry point.
It returns errors instead of the compatibility wrapper's empty result.
`SparqlDatabase::with_ml_context(context)` explicitly configures a local
database for APIs that take a database directly. Existing constructors remain
restrictive. External code constructing the database with a struct literal
must supply the new `ml_context` field.

## HTTP approval

The server loads `KOLIBRIE_MODEL_ALLOWLIST` once during startup. The value must
be an absolute path to an administrator-managed JSON file. Unconfigured means
an empty registry. A missing, invalid or unverifiable configured registry
prevents an ML-enabled server from starting. Restart after registry changes.

Each registry entry supplies a public model name, backend, absolute artifact
path and SHA-256 digest. Native entries also supply input dimensions, hidden
layers and optional categorical labels. Empty labels mean a binary probability.
Python entries require an explicit module name, absolute module path, module
digest and `allow_pickle: true`.

See `model-allowlist.example.json`. Its paths and zero digests are placeholders:
it deliberately cannot approve any real model until the administrator replaces
them. Use `{"models":[]}` for an intentionally empty registry.

HTTP never selects trusted-local mode, discovers model directories, generates
missing models, trains, writes artifacts or registers query declarations. Named
inference uses only registry entries. Artifact bytes are verified and those same
bytes are deserialized. Python module origins and cached source are checked.
ML policy responses contain stable codes, not query text or Python tracebacks.
The legacy database HTTP adapter strips trusted-local authority as well.
Streaming HTTP endpoints retain disabled ML execution and preflight their query
and rule batch before creating a session; approved inference is available through
the ordinary SPARQL HTTP endpoint.

Approved inference inputs are numeric feature columns in SELECT order. Native
models enforce their configured input dimensions. Model-generated rows retain
input columns and append the prediction. Rule inference uses the existing
conclusion materialization contract; it does not register models.

Registry files, artifacts, Python modules and the interpreter/dependencies must
be administrator-controlled. Deploy them outside training directories and
mount them read-only. Directory ownership and dependency trust are deployment
requirements, not an application sandbox. In particular, pickle can execute
code: a checksum proves identity, not safety. See
[Python's pickle guidance](https://docs.python.org/3/library/pickle.html).

## Trusted-local training

Embedding code explicitly calls
`MlExecutionContext::trusted_local(absolute_output_directory)`.
The local examples require `KOLIBRIE_TRAINING_OUTPUT` to name that directory.
This environment variable is an example/host setting, never an HTTP authority
switch. Use `cargo run -p kolibrie --features ml --example predict_after_train`.

Query `SAVE_TO` accepts a filename such as `new_model.bin`, not a path.
It rejects traversal, separators, drive/UNC/alternate-stream syntax, Windows
device names and invalid characters. Writes use exclusive creation: a second
save to the same filename fails. Choose a fresh name on subsequent runs.
Keep the output directory private and administrator-owned; do not let other
users replace it or its ancestors while the process is running.

Direct native `save/load` methods and Python discovery helpers are trusted-local
host APIs, not HTTP APIs. Native `to_bytes/from_bytes` allow callers to choose
their own persistence policy. Example Python modules no longer train on import;
generation is explicit. Local generation does not approve its outputs.

To publish a new artifact, an administrator reviews it and its dependencies,
copies it outside the training directory, computes artifact/module hashes,
updates the registry and restarts the server.

## Docker

The default image does not install Python/ML packages or generate models.
Use `--build-arg ENABLE_ML=true` to build the optional runtime. Mount an approved
registry and artifacts read-only and set `KOLIBRIE_MODEL_ALLOWLIST` at runtime.
Image building is never an approval step.

## Aggregates

Supported variable aggregates are COUNT, SUM, AVG, MIN and MAX, optionally with
DISTINCT. Only COUNT accepts `*`. Aliases and existing projection tuples are
preserved. Static kind strings include `COUNT_DISTINCT`, `SUM_DISTINCT`,
`AVG_DISTINCT`, `MIN_DISTINCT` and `MAX_DISTINCT`; execution interprets them
through a shared descriptor.

COUNT(variable) excludes absent bindings; COUNT(*) counts rows.
COUNT(DISTINCT *) compares complete mappings, including unprojected variables,
using canonical variable ordering. An absent binding differs from a bound empty
string. Other DISTINCT aggregates deduplicate input values before evaluation.

An ungrouped empty input produces one row: COUNT/SUM/AVG are zero and MIN/MAX
aliases are unbound. Explicit grouping over empty input produces no groups.
Bound nonnumeric operands invalidate SUM/AVG for that group, leaving the alias
unbound instead of silently discarding the operand.
These rules follow the supported subset of
[SPARQL aggregate definitions](https://www.w3.org/TR/sparql11-query/#setFunctions).

Existing collapsed RDF term identity remains a limitation: this milestone does
not introduce tagged RDF terms or arbitrary aggregate-expression parsing.

## Ordered QueryBuilder API

`get_ordered_triples()` and `get_ordered_decoded_triples()` return vectors
preserving result order. Filtering and joins retain the existing unique triple
selection. Natural triple order is the default and the stable tie order for
equal custom sort keys. OFFSET/LIMIT apply after ordering.

Existing `get_triples()` and `get_decoded_triples()` retain their existing
return types and set ordering. Streaming behavior is unchanged.

## Acceptance status

The implementation is present in the working tree. Delivery remains qualified
by the uncommitted changes and verification limitations below; the broader
consensus is not closed.

A fresh, isolated export of base `4269332` produced 318 passing tests, one
failure and one ignored test. The failure is
`rsp_engine_test::rsp_ql_dstream_semantics` (two callbacks instead of one).
A repeated baseline run also reproduced the intermittent
`rsp_ql_multi_window_integration` failure. Neither RSP behavior was changed.

Completed Windows verification:

- Core, CLI, HTTP and Python-binding compilation with ML both off and on;
  all applicable Kolibrie examples compile in both configurations.
- Full ML-off Kolibrie tests with `exec-stats`: 317 passed, with only the
  baseline DSTREAM failure. Shared tests: 98 passed. Datalog tests: 75 passed.
- Focused aggregate coverage: 2 COUNT tests, 3 DISTINCT/aggregate parity tests
  and 16 aggregate-scope tests passed. All 3 ordered QueryBuilder tests passed.
- ML-off security: 5 passed. ML-on security: 10 passed, including default-policy
  entry points, rules, verified inference and rejection without registration.
- HTTP tests: 7 passed with ML off and 8 with ML on, including streaming policy
  checks and subprocess output capture for sensitive canaries.
- Native runtime tests: 12 passed. Lower-level `ml` tests: 5 passed, including
  real Python byte-buffer prediction and native dimension validation.
- Allocation regression and all 3 work-counter regressions passed.
- The Python example import-safety structural test passed.
- Targeted formatting checks for new Rust files and both staged/unstaged
  `git diff --check` passed.

The independent Milestone 4 export at `bd66387`, with the subsequent Unicode
filename validation and safe legacy HTTP error-prefix fixes applied, passed
its full ML-off and ML-on suites except the existing DSTREAM failure. This
does not claim that the unpatched commit contains those follow-up fixes.

The final full ML-on run on 2026-09-07 passed 353 tests across Kolibrie, `ml`
and HTTP, with only the baseline DSTREAM failure (Kolibrie: 340 passed and one
failed; HTTP: 8 passed; `ml`: 5 passed). All milestone-specific tests passed.
Two binary training fixtures exposed random hidden-ReLU convergence failures
after training began honoring the declared architecture. Both now use linear
models for their linearly separable data, retaining their exact inference and
probability assertions. Each revised fixture passed 20 independent runs before
the final suite. No production training algorithm was changed for this fix.

Ordinary Clippy with ML off and on still rejects the pre-existing non-looping
`while let` in `sparql_database.rs`; the same code is present in `4269332`.
A diagnostic run allowing only `clippy::never_loop` completed with warnings.
Workspace-wide formatting checks also report existing differences reproduced
on the pristine base. These are not clean Clippy or workspace formatting results.

Linux ML-on core compilation and all 10 security tests passed, including the
symlink canary and approved Python/native prediction. The additional Linux
package/feature matrix was declined and remains unverified. Docker image builds
were not run because the Docker engine was unavailable. A full release-runtime
matrix was not run. Final test reruns use command-line test-profile overrides
to disable optimization/LTO; the repository's build profiles are unchanged.

Seven logical commits are recorded, ending at `3f94a60`. The ordered QueryBuilder
API and tests are staged; the final commit command was declined. Follow-up
security/compatibility fixes, the training fixture adjustments and this report
remain unstaged. Completion of the commit series and the remaining platform
checks must not be inferred from the presence of the local implementation.

Read-only SELECT, Empty, dictionary ID changes, concurrency, RDF identity,
dense bindings and streaming remain outside these milestones.

## Relationship to the final consensus

This implements the approved ML trust-policy gate in section H and the security
checks, the supported variable/star aggregate work in section D, and the additive
ordered-result API in section E. Existing unordered result methods remain
source-compatible. Section D still inherits the documented RDF term-identity
limitation; arbitrary aggregate expressions were not added.

The previous FILTER, ordering, statistics, planner and allocation work is retained,
not reclassified as new work here. No public operator enum, dictionary ID range or
`cached_stats` representation is changed. The original consensus document is an
unchanged historical decision record, not a checklist automatically closed by
these two milestones. Its broader identity, concurrency, performance and
architectural requirements remain separate work.
