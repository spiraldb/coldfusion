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
coldfusion = { git = "https://github.com/spiraldb/coldfusion", rev = "98ec669d786f2fb0d72d8b95b94469b846f191e0" }
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
Native Vortex scans retain parallel outputs through optimizer rewrites. Limits
and ordering stay above the scan in DataFusion rather than collapsing it to one
output partition.

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

The native Vortex source uses one converter for static filters and computed
projections. It extracts compatible native subtrees and embeds remaining synchronous
DataFusion expressions as a typed, process-local `DFScalarFn`. Volatile expressions
stay in DataFusion. Conditional branches remain together unless extraction preserves
their evaluation domain. A small conservative tree walk marks only proven infallible
operations as safe for speculative evaluation; unknown operations are still accepted
inside the callback.

Live join filters use generation-aware Vortex expressions and the same callback.
An evaluation error in an advisory hint retains rows; exact expressions preserve
DataFusion errors. Exact projections containing live predicates stay in DataFusion,
because later snapshots may introduce unsupported or volatile expressions. Other
fallbacks are async expressions and Arrow result types Vortex cannot safely carry.
In particular, a callback returning a decimal stays in DataFusion because Arrow can
produce coefficients outside the declared precision; decimal intermediates inside
Boolean or other representable results are supported. Existing proven native decimal
operations remain native. DataFusion itself is unchanged.

The Arrow-reader scan exposes output rows, compute time, bytes read, and dynamic
pruning through DataFusion metrics. Native Vortex scans expose the official
Vortex execution metrics, but do not report per-query or per-partition
`bytes_scanned`: file handles and their IO counters are shared across scans.
Arrow-reader byte estimates describe projected Arrow output independently of
file compression. Both Arrow-reader scans (Parquet and Vortex fallback) and
native Vortex scans expose top-level Int/Long column bounds from Iceberg manifests as
conservative scan statistics when every selected nonempty file supplies
compatible bounds. Field IDs preserve these facts through renames and
Int-to-Long promotion. Missing bounds remain unknown; distinct counts and null
counts are not inferred. These bounds help DataFusion choose join order and
build sides without additional payload or Parquet metadata reads.

## Iceberg patches

The dependency starts from `spiraldb/iceberg-rust` main at
`ea48146de861ee71d32ba747ca0d182ea7746ed1` and is pinned in `Cargo.toml`.
The [19-patch series](patches/iceberg-rust/) contains nonempty native changes.
Its first nine patches preserve the historical reader tree `930277e0`; four
additional commits align Vortex with Arbor, improve input/output batching, expose
a native array source, and reuse file metadata on the table IO runtime. A fifth
exposes manifest bounds on scan tasks. A sixth reconciles integer predicate
literals with the physical Vortex type, including signed/unsigned boundaries
and null-preserving range simplification. A seventh shares footer-to-manifest
metrics for file imports and exposes conservative integer bounds through the native
Vortex source; missing statistics remain unknown. An eighth pins Vortex
`e2d8b0fd`, which preserves supported integer bounds in dynamic filter snapshots
without pushing unsupported hash lookups into the reader. The ninth extension pins Vortex
`5d6cfc1b`, adding an advisory dynamic-conversion hook for this provider. The tenth
extension updates to current Vortex (`94babf7` plus that hook at `32f0a47`) and
adapts the Arrow export API. Native casts use the shared Vortex/DataFusion
converter, including integer/Boolean-to-Float64 rounding and lossless unsigned-to-signed
widening; unsupported casts retain the DataFusion fallback. The extended tip has its own tree and
replay proof. Cancelling experiments and the obsolete native DataFusion adapter
remain omitted.

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

The [full local SF10 comparison](benchmarks/full-sf10-2026-11-11/) covers all
22 TPC-H and 99 TPC-DS queries across both Arbor integrations and ColdFusion
Vortex, Zstd Parquet, and LZ4_RAW Parquet. All 605 reference checks passed.

The [November 11 generic expression study](benchmarks/generic-expression-pushdown-2026-11-11/)
compares the previous membership bridge, generic bridge, and Arbor direct on
TPC-DS SF1 Q21/Q39/Q62/Q99, with Q72 as a regression guard.


The final November 8, 2026 UTC local TPC-H SF10 cohort uses one locked executable
built from committed ColdFusion `98ec669`, Iceberg `3a409b31`, and benchmark harness
`2c44c06ba`. DataFusion is unchanged. Vortex-Iceberg and both Arbor configurations
use Vortex Git `7d003295`; Parquet is measured separately.

All **198 query checks passed exactly** across nine process runs and **990 timed
samples**. Each query has two warmups and five timed samples per process. Arbor
runs bracket the five Iceberg configurations; their rows below average the two
controls. SDK and direct Arbor mean sums drifted only 0.02% and 0.07% respectively.
The shared Apple M3 Max has 14 CPUs and 96 GiB RAM; query workers and target
partitions are 14, and the DataFusion batch-size setting is 8192. Actual output
batch sizes can differ by adapter and were not measured. Each prepared fixture
settles for 60 seconds before warmups.

The primary metric is the sum of per-query arithmetic means. Derived query rate
is `22 / sum(query mean seconds)`, not an official TPC-H throughput score. Query
timing excludes fixture preparation, LZ4 transcoding, answer checking and settling.

| Configuration | Parquet page pruning | Sum of query means | Queries/s | Encoded data GB | Files |
| --- | --- | ---: | ---: | ---: | ---: |
| Spiral DataFusion SDK · Arbor | N/A | 5.776 s | 3.809 | 2.762 | 88 |
| DataFusion · Arbor direct Scan API | N/A | 3.964 s | 5.550 | 2.762 | 88 |
| ColdFusion · original Zstd Parquet | Off | 6.963 s | 3.160 | 2.357 | 16 |
| ColdFusion · original Zstd Parquet | On | 6.682 s | 3.292 | 2.357 | 16 |
| ColdFusion · LZ4_RAW Parquet | Off | 5.499 s | 4.001 | 3.540 | 16 |
| ColdFusion · LZ4_RAW Parquet | On | 5.434 s | 4.049 | 3.540 | 16 |
| ColdFusion · Vortex-Iceberg | N/A | 3.916 s | 5.618 | 2.761 | 16 |

LZ4_RAW measured 5.0–6.3% more query throughput than SDK Arbor, while remaining
slower than direct Arbor. Native Vortex measured 47.5% more than SDK Arbor and
1.2% more than direct Arbor in this cohort; the small direct-Arbor difference is
not a general performance guarantee. Zstd remains slower than both Arbors.

Paired on/off studies retained Exact predicate pushdown (+2.8% Zstd and +3.8%
LZ4_RAW query rate) and the native Vortex source bundle (+18.2% over its
layout-batched Arrow-reader path). Global view types produced severe Q21 tails
and remain opt-in. Broad repeated filtering, staged conjuncts and a Parquet
metadata cache failed to improve their measured workloads and were discarded.
The [results and profiles](benchmarks/adapter-optimizations-2026-11-08/) include
all samples, per-query means, rejected experiments and source-identity limits.

Page settings must be interpreted cautiously: Q21 timing mixtures account for
69% of the Zstd and 78% of the LZ4 on/off difference. Q6 was slightly slower with
page pruning on in this cohort. These observations do not establish a causal
page-pruning speedup. The [page-index audit](benchmarks/parquet-page-pruning-2026-11-07/page-index-audit.json)
confirms indexes in all 16 canonical Parquet files, but their broad bounds exclude
no lineitem pages for the audited Q6/Q15 predicates. Selective real-file tests
separately verify reduced reads with correct projection and delete handling.

Storage totals count complete encoded data files and internal metadata, excluding
external catalogs, snapshots and manifests consistently; GB is decimal. LZ4_RAW
uses 28.2% more data-file bytes than Arbor. Vortex-Iceberg uses almost the same
bytes with a different file layout. Canonical Zstd and Vortex inputs are imported
unchanged; LZ4 transcoding preserves values, schema, file order and row-group
boundaries, while page boundaries can change. OS page cache is uncontrolled.

The [commit-by-commit rationale](benchmarks/adapter-optimizations-2026-11-08/commit-series.md)
separates reader fixes, integration APIs and packaging from measured optimizations.
Reviewable draft PRs: [provider #2](https://github.com/spiraldb/coldfusion/pull/2)
and [benchmark integration #9833](https://github.com/spiraldb/spiraldb/pull/9833),
stacked on the existing Iceberg benchmark lanes in
[#9822](https://github.com/spiraldb/spiraldb/pull/9822). Earlier
[November 7 measurements](benchmarks/parquet-page-pruning-2026-11-07/) remain
archived with their original median-based methodology and reader versions.
