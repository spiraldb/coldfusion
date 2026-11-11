# Generic expression pushdown: local TPC-DS SF1

The generic bridge replaces the integer-only membership adapter. Compatible
native subtrees stay in Vortex; remaining synchronous DataFusion expressions
run through a typed process-local scalar function. Static filters and computed
projections share the converter. Unknown fallibility remains accepted inside a
callback; a conservative tree walk controls speculative execution. Volatile
expressions remain in DataFusion.

| Query | Previous bridge | Generic bridge | Arbor direct |
| --- | ---: | ---: | ---: |
| Q21 | 9.19 ms | 8.91 ms | 8.68 ms |
| Q39 | 27.35 ms | 28.57 ms | 23.44 ms |
| Q62 | 12.36 ms | 12.75 ms | 13.12 ms |
| Q99 | 30.68 ms | 28.17 ms | 20.13 ms |
| Q72 | 113.04 ms | 107.87 ms | 901.09 ms |

Arithmetic means of three timed samples after one warmup. These are the four
requested guide queries plus Q72 as a regression guard. Sum of guide-query means:
previous 79.58 ms; generic
78.40 ms. This small difference is not
a general speedup claim. The large Q72 advantage over Arbor already existed with
the specialized membership bridge. Q39 and Q99 remain slower than Arbor direct.
This work broadens expression coverage while preserving prior performance;
it does not establish a Parquet improvement or parity for every query.

All 15 query/configuration checks passed the harness's reference validation,
reported as `answer_match=exact`. That label includes floating comparisons with
absolute and relative tolerance 1e-9: it is not a bitwise equality claim. Q39's
floating result digest differs between the previous bridge and the other two
lanes. The other four digests agree across all configurations.

## Method and source identities

Same laptop (Apple M3 Max, 14 cores, 96 GiB), local canonical SF1 Vortex files,
four query workers and target partitions, `TOKIO_WORKER_THREADS=4`, unchanged
DataFusion 55.2. Iceberg imports the generator files without rewriting them.
Dataset cache is warm. No compiler or other benchmark ran concurrently.
SQL timeout: 120 seconds. Only the selected queries ran. Controls ran before the
final helper cleanup; there is no randomized repeated cohort, so small differences
remain unresolved noise.

[Source identities](source-identity.json), command receipts, binary SHA-256s,
and raw samples are retained. Raw `value_ns` is a median; the table recomputes
means from `all_runtimes_ns`. The raw `commit_sha` comes from the runner's current
Git context and is **not** the provider identity; use the explicit identities
and binary hashes. Three binaries were used: previous `6751…`, Arbor `2124…`,
and final generic `d910…`. Arbor ran on the intermediate build with unchanged
Arbor driver code and dependencies. The final measured provider is `34045d5`; this report adds no
production changes. Global Arbor Vortex remains unchanged. The native Iceberg
update changes only the Vortex dependency/lockfile to expose an advisory converter
hook. No DataFusion source changes were made.

## Iteration and profiles

An eligibility prepass initially regressed Q21 to 14.70 ms and Q39 to 36.84 ms
([regressed samples](final-after.jsonl)). It failed to recognize constant literal
IN as infallible, hiding bounds-and-IN inside an opaque DF callback. A checked
same-type primitive literal-list proof restores native conversion eligibility
([isolated correction](corrected-after.jsonl)); a regression test checks this.
Nonconstant lists remain conditional.

Hint-only AND/OR lowering also retains native bounds alongside DF callbacks,
allowing metadata pruning before membership evaluation. Each child conservatively
retains original TRUE rows; AND/OR preserve that property. A failed advisory leaf
widens to TRUE. NOT/CASE stay intact, and exact callback errors propagate.
[Pre-helper samples](native-bounds-after.jsonl) and [final samples](verified-after.jsonl)
retain both stages. The final helper shares one eligibility memo and runs
whole-tree volatility/async/dynamic checks once.

macOS sampling call trees for Q39 are retained as
[previous](sample-before.txt) and [regressed generic](sample-after.txt).
They include preparation and repeated query executions; they are qualitative
profiles, not percentages of query wall time. No wall-time attribution to a
sampled frame percentage is claimed. The profiler needed authorized macOS process
inspection after the sandbox blocked it. `--profile-out` produced no flamegraph
on this DataFusion path. Operator diagnostics showed identical emitted row
counts; restoring native IN conversion then removed the measured regression.

## Correctness and fallbacks

[18 focused tests](focused-tests.txt) pass: typed projections, wrapping arithmetic,
native extraction, nullability, conditional CASE, composite integer/string hash
keys, live generations, pushed filter plus computed projection, volatile rejection,
and advisory error relaxation. The [single real-file decimal test](decimal-test.txt)
passed before the Hint-only pruning follow-up; the exact decimal path was unchanged.
No full suite ran.

Async expressions stay with DataFusion's async executor. Valid Arrow outputs that
Vortex cannot represent remain in DataFusion rather than failing planning.
Callback decimal outputs remain there because Arrow permits coefficients outside
declared precision; importing them into Vortex would introduce errors. Decimal
intermediates within Boolean or other supported results are accepted, and existing
proven native decimal operations remain native. Exact projections containing live
predicates stay in DataFusion because future snapshots can change volatility or
representability; advisory join filters are accepted and refreshed per generation.
