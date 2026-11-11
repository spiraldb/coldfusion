# Full local SF10 comparison · November 11, 2026

All **605 query/configuration checks passed**: 22 TPC-H plus 99 TPC-DS queries,
across five configurations. No query failed or timed out. Each query has one
reference-checked warmup followed by three timed samples: **1,815 timed samples**.
The same final executable was used for all ten suite/configuration runs.

| Configuration | TPC-H SF10 · 22 queries | TPC-DS SF10 · 99 queries |
| --- | ---: | ---: |
| Spiral SDK · Arbor | 5.615 s | 26.885 s |
| Spiral direct · Arbor | 4.004 s | 26.017 s |
| ColdFusion · Vortex | 5.105 s | 18.019 s |
| ColdFusion · Zstd Parquet | 6.810 s | 40.915 s |
| ColdFusion · LZ4_RAW Parquet | 5.154 s | 41.761 s |

Times are sums of per-query arithmetic means; lower is better. This is a
serialized query-suite metric, not an official TPC throughput score. ColdFusion
Vortex has 1.444× the derived TPC-DS query throughput of Spiral direct and
1.492× that of SDK in this cohort. Spiral direct remains faster on TPC-H.
LZ4_RAW beats Zstd on TPC-H but does not improve the TPC-DS total here.
These are single sequential cohorts; no claim that a small difference is
statistically significant follows from them.

| Configuration | TPC-DS Q72 mean |
| --- | ---: |
| Spiral SDK · Arbor | 10.390 s |
| Spiral direct · Arbor | 11.873 s |
| ColdFusion · Vortex | 1.072 s |
| ColdFusion · Zstd Parquet | 13.920 s |
| ColdFusion · LZ4_RAW Parquet | 13.506 s |

The native DF-expression callback keeps join membership inside the Vortex scan.
These results compare the final configurations, not an isolated before/after
experiment on that callback. The large Q72 gap must not be described as the
measured effect of only the new generic implementation: the prior specialized
membership bridge already improved it substantially.

## Matched execution

- Local Apple M3 Max, 14 cores, 96 GiB RAM; all engines run on this same laptop.
- 14 query workers/target partitions; `TOKIO_WORKER_THREADS=14` for every run.
- Same DataFusion 55.2; no DataFusion modifications. Native CF Vortex and both
  Parquet codecs go through the same Iceberg provider. Parallel scan output is retained.
- Reference-checked warmup, then three timed plan-and-collect samples. Fixture
  creation, file conversion, reference generation and validation are outside timings.
- SQL execution timeout 120 seconds; fixture preparation timeout 300 seconds.
  The latter is not a SQL timeout. No compiler, profiler, test suite or other
  benchmark ran concurrently with a timing run.
- Parquet page pruning enabled for both codecs. LZ4 transcodes retain row groups,
  schemas and row/file order; encoded data-page boundaries may differ. These are
  end-to-end configurations, not a controlled codec-only experiment.

Within each suite the order was SDK → CF Vortex → CF Zstd → CF LZ4 → direct.
TPC-H completed before TPC-DS data generation/reference preparation and timing.
This is sequential, not a randomized/bracketed repeated experiment. The inputs
are local canonical generator Parquet files. Iceberg Vortex imports canonical
Vortex files without rewriting; Arbor builds its usual local fragments from the
same Parquet rows. Both Arbor lanes have identical per-table encoded payload
fingerprints: 88 files/2,761,969,312 bytes for TPC-H and
200 files/2,902,052,132 bytes for TPC-DS. Fragment maximum: 1,048,576 rows.

[Layout receipts](layout-receipts.json) retain per-file hashes and row-group/codec
metadata. [Arbor receipts](arbor-layout-receipts.json) retain per-table fingerprints.
ColdFusion Vortex uses 16 files/2,761,455,768 bytes for TPC-H and
33 files/2,896,400,576 bytes for TPC-DS. Different file/fragment layouts are part
of the configurations being compared; the validated logical inputs match.

## Validation and provenance

All 605 records have the harness label `answer_match=exact`; no approximate
fallback or row-count-only validation was used. That label still permits the
normal floating tolerance: |actual−expected| ≤ 1e−9 + 1e−9·|expected|.
It does not establish bit-identical floating-point results. Validation applies
to the initial warmup; the three timed executions are not checked individually.
Reference answers come from the independent DataFusion ListingTable/VortexFormat
path, not the ColdFusion or Arbor providers. All 99 fresh TPC-DS references were
prepared successfully; the reference watchdog never expired. Q72 reference
[telemetry](reference-q72.telemetry.json) and [plan](reference-q72.plan.txt) are saved.

[Source identities](source-identity.json) bind the production code to CF
`34045d5`, native Iceberg `32ac258`, and CF Vortex `5d6cfc1`; the measured branch tip
`822758c` adds only the SF1 report. The common driver is Spiral `4837bf9`, global
Arbor Vortex remains `d92f467`, and the executable SHA-256 is
`d91040bbf8fed4a3264465ba4a9c11e90e262c5a3694fec464e0d397b83de8f7`. Public dependency pins are paired with the
[local override build command](build-command.json) and
[CF-only import alias change](consumer-aliases.patch). Raw runner `commit_sha`
comes from its working Git context and is not a provider source identity.

Each `*-command.json` records actual argv, working directory, environment,
binary hash and start time. Each raw `*.jsonl` preserves every timed sample,
validation label and result digest. [analysis.json](analysis.json) and
[analysis script](analyze.py) recompute means from `all_runtimes_ns`; raw
`value_ns` is a median. CSVs provide the complete
[TPC-H](tpch-query-means.csv) and [TPC-DS](tpcds-query-means.csv) per-query comparison.
The old four-worker SF1 experiment is separate and should not be numerically
combined with this 14-worker SF10 cohort.

TPC-DS preparation initially used an incorrect CLI format spelling, corrected
before reference generation/timings. Completed Parquet data was reused; no
production code changed and no timing run was repeated. The data preparation
and reference watchdog script is retained for reproduction.
