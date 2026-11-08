# Coldfusion

Coldfusion is a read-only Iceberg table provider for DataFusion 55.2. It reads
Parquet and Vortex through the native readers in `spiraldb/iceberg-rust` and
exposes parallel scan streams. A table may contain either format or a mixture
of both. DataFusion is unmodified.

The dependency direction is `ColdFusion → Iceberg + DataFusion`. This repository
owns the provider, partition planning, and DataFusion expression adaptation. The
Iceberg reader exposes format-neutral scan tasks and native reader hooks; it
has no DataFusion dependency.

```toml
[dependencies]
coldfusion = { git = "https://github.com/spiraldb/coldfusion", branch = "develop" }
datafusion = { version = "=55.2.0", default-features = false, features = ["sql"] }
```

Use the re-exported Iceberg crate when constructing the table, so its types match
the pinned reader dependency:

```rust,no_run
use std::sync::Arc;

use coldfusion::{IcebergTableProvider, iceberg::table::Table};
use datafusion::common::Result;
use datafusion::prelude::{SessionConfig, SessionContext};

fn register(table: Table) -> Result<SessionContext> {
    let context = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(14),
    );
    context.register_table("events", Arc::new(IcebergTableProvider::try_new(table)?))?;
    Ok(context)
}
```

## Scan behavior

Scans expose at least two output partitions, using the session target when
larger. Each partition owns its reader stream and disjoint Parquet row groups
or Vortex row ranges.
Small files can leave partitions empty. Keep the table's configured runtimes
alive while queries execute. Its I/O runtime must be driven; entering its I/O
context does not drive a current-thread runtime's reactor. Background decoding
also uses the table's CPU executor.

The Arrow reader decodes on DataFusion's workers when they cover the smaller of
the output partition count and available CPU count. Smaller query runtimes use
the table's CPU executor with bounded prefetching and cancellation on consumer
drop. Native Vortex scans use the official Vortex DataSource and are polled on
DataFusion's workers, independently of that Arrow-reader offload policy.

The provider retains the loaded schema and snapshot. Construct another provider
to observe catalog changes. Reads use Iceberg's field-ID projection, schema
evolution, defaults, and positional and equality delete handling. Output uses
canonical Iceberg Arrow types, including `Utf8` and `LargeBinary`. View types are
available through `try_new_with_view_types` and remain opt-in.

Supported required-field predicates use exact pushdown by default, removing
redundant DataFusion residuals. Supported Vortex scans use the native array source
and reuse opened file metadata; other scans use the Arrow reader, with Vortex
layout batches filtered before Arrow output is sliced. The
`with_exact_filter_pushdown`, `with_native_vortex_scan`, and
`with_vortex_layout_splits` methods can disable these paths independently.

Static primitive predicates prune manifests, row groups, and Parquet pages.
Page-statistics pruning follows DataFusion's
`datafusion.execution.parquet.enable_page_index` setting (enabled by default).
Files without indexes fall back to row-group and decoded-row filtering.
DataFusion retains conditions outside the exact predicate subset. Parquet live
dynamic filters use cheap
bounds, null checks, and short literal lists. DataFusion can represent live join
filters using `CASE` and hash lookups; evaluating these for every scanned row can
cost more than it saves when little is pruned. The provider declines that optional
reader pushdown and retains DataFusion's join and residual checks. This does not
skip SQL `CASE` evaluation required by the query. Vortex uses the native static
predicate path and DataFusion residuals; the Parquet-specific live row-filter
hook does not run on Vortex files. The Arrow reader evaluates live filters on
decoded Arrow batches; it does not currently turn arbitrary `CASE` expressions
into metadata pruning. Native Vortex scans delegate supported live expressions to
the official Vortex adapter, which evaluates them over Vortex arrays.
The Arrow-reader scan exposes output rows, compute time, bytes read, and dynamic
pruning through DataFusion metrics. Native Vortex scans expose the official
Vortex execution metrics, but do not report per-query or per-partition
`bytes_scanned`: file handles and their IO counters are shared across scans.
Byte statistics estimate projected Arrow output,
independently of file compression.

## Iceberg patches

The dependency starts from `spiraldb/iceberg-rust` main at
`ea48146de861ee71d32ba747ca0d182ea7746ed1` and is pinned in `Cargo.toml`.
The [13-patch series](patches/iceberg-rust/) contains nonempty native changes.
Its first nine patches preserve the historical reader tree `930277e0`; four
additional commits align Vortex with Arbor, improve input/output batching, expose
a native array source, and reuse file metadata on the table IO runtime. The
extended tip has its own tree and replay proof. Cancelling experiments and the
obsolete native DataFusion adapter remain omitted.

The series includes Parquet split ownership, promoted-literal and page-bound
fixes, runtime and cancellation support, and generic reader hooks. DataFusion
expression adaptation stays in this repository.

## Validation

```sh
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all -- --check
```

Real-file tests cover independent parallel consumers, projections, renames,
defaults, promoted types, nulls, both delete kinds, live filter updates, and split
I/O and compute runtimes.

## Benchmarks

The integration adds separate ColdFusion lanes to `spiraldb/spiraldb`, alongside
its existing Apache DataFusion–Iceberg lane. It compares the Spiral DataFusion
Arbor configuration with canonical Zstd Parquet, a separately named LZ4_RAW
Parquet layout, and canonical Vortex files imported unchanged into Iceberg.
The LZ4 lane performs untimed transcoding, preserving values, schema, file order,
and row-group boundaries; re-encoding can change page boundaries.

The read-only 22-query TPC-H suite runs at SF10. Every answer is checked against
the canonical references. Each query reports the median of three timed samples.
Derived query throughput is `22 / sum(query median seconds)`; this is not the
official TPC-H power/refresh/concurrent-stream metric.

The final seven-configuration cohort completed November 7, 2026 UTC, using
ColdFusion `19558c4b` and Iceberg `930277e0`. All 154 query checks passed exact
validation using one executable and identical SQL and input values.

| Configuration | Parquet page pruning | Sum of query medians | Queries/s | Encoded data GB | Files |
| --- | --- | ---: | ---: | ---: | ---: |
| Spiral DataFusion SDK · Arbor | N/A | 5.783 s | 3.804 | 2.762 | 88 |
| DataFusion · Arbor direct Scan API | N/A | 4.025 s | 5.466 | 2.762 | 88 |
| ColdFusion · original Zstd Parquet | Off | 7.049 s | 3.121 | 2.357 | 16 |
| ColdFusion · original Zstd Parquet | On | 7.077 s | 3.109 | 2.357 | 16 |
| ColdFusion · LZ4_RAW Parquet | Off | 5.519 s | 3.986 | 3.540 | 16 |
| ColdFusion · LZ4_RAW Parquet | On | 5.763 s | 3.818 | 3.540 | 16 |
| ColdFusion · Vortex-Iceberg | N/A | 5.803 s | 3.791 | 2.761 | 16 |

Direct Arbor was faster than all ColdFusion configurations. LZ4_RAW was near SDK
Arbor or faster in this single cohort; the samples do not establish a reliable
throughput lead. Vortex-Iceberg was near SDK Arbor. Canonical Zstd remained slower
than both Arbor configurations.

Page pruning changed the Zstd sum by only 0.4%. The 4.4% larger LZ4 sum with page
pruning enabled is almost entirely Q21's variable timings: both settings produced
samples around 0.45 s and around 0.7 s, and their three-sample medians landed on
different modes. This cohort does not establish a page-pruning regression.
The [index audit](benchmarks/parquet-page-pruning-2026-11-07/page-index-audit.json)
verifies indexes in all 16 canonical Parquet files; their broad bounds exclude no
lineitem pages for the audited Q6 and Q15 predicates. The real-file regression
separately verifies that pruning saves reads on selective pages while preserving
projection and delete semantics.

Storage totals include complete encoded data files and their internal metadata,
excluding external catalogs, snapshots, and manifests consistently. GB is decimal.
LZ4_RAW used about 28.2% more data-file bytes than Arbor. Vortex-Iceberg used about
the same bytes as Arbor, with a different file layout. LZ4 transcoding and
verification took 33.681 s with pruning off and 33.184 s with it on; Vortex
footer/schema verification and hashing took 1.338 s. These helper costs exclude
catalog/manifest setup and query timing. LZ4 off/on files have identical hashes;
both Arbor configurations have identical payload fingerprints.

The native Iceberg Vortex reader is crates.io version 0.86.1; Arbor and the
canonical Vortex generator use `vortex-data/vortex@7d003295`. This compares whole
configurations with different layouts and reader versions. It does not isolate
Iceberg overhead or compression alone.

The machine is an Apple M3 Max, 14 CPUs and 96 GiB RAM, with local NVMe storage.
All seven configurations use 14 query workers and 14 target partitions. Each
query has one validation/warmup execution before its three measured samples.
The configurations ran sequentially on a shared machine with uncontrolled OS
page cache, separated by 30-second cooldowns. Builds, tests and profiles ran
separately from measurements. A one-second process guard detected no competing
builds or benchmark runners during the completed cohort. Incomplete attempts
interrupted by other builds were excluded.

The [full seven-configuration table and raw timing samples](benchmarks/parquet-page-pruning-2026-11-07/)
record measured identities, all 22 per-query medians, execution order, layout costs
and file hashes. Earlier [five-configuration](benchmarks/tpch-sf10-vortex-2026-11-07/)
and [four-configuration](benchmarks/tpch-sf10-2026-11-06/) cohorts remain available
separately. The [original benchmark PR](https://github.com/spiraldb/spiraldb/pull/9815)
is merged; the [Vortex-Iceberg and page-pruning PR](https://github.com/spiraldb/spiraldb/pull/9822)
adds the new lane and explicit pruning controls. Analysis scripts are in [scripts](scripts/).
