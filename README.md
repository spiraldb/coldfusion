# Coldfusion

Coldfusion is a read-only Iceberg table provider for DataFusion 55.2. It reads
Parquet through the native reader in `spiraldb/iceberg-rust` and exposes parallel
scan streams. DataFusion is unmodified. The normal dependency graph excludes
Vortex, and the provider rejects data files other than Parquet.

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
larger. Each partition owns its reader stream and disjoint Parquet row groups.
Small files can leave partitions empty. Keep the table's configured runtimes
alive while queries execute. Its I/O runtime must be driven; entering its I/O
context does not drive a current-thread runtime's reactor. Background decoding
also uses the table's CPU executor.

Decoding runs on DataFusion's workers when they cover the smaller of the output
partition count and available CPU count. Smaller query runtimes use the table's
CPU executor with bounded prefetching and cancellation on consumer drop.

The provider retains the loaded schema and snapshot. Construct another provider
to observe catalog changes. Reads use Iceberg's field-ID projection, schema
evolution, defaults, and positional and equality delete handling. Output uses
canonical Iceberg Arrow types, including `Utf8` and `LargeBinary`.

Conservative primitive predicates prune manifests and Parquet row groups.
DataFusion retains residual conditions. Live dynamic filters use cheap bounds,
null checks, and short literal lists; expensive expressions remain residual.
The scan exposes output rows, compute time, bytes read, and dynamic pruning
through DataFusion metrics. Byte statistics estimate projected Arrow output,
independently of file compression.

## Iceberg patches

The dependency starts from `spiraldb/iceberg-rust` main at
`ea48146de861ee71d32ba747ca0d182ea7746ed1` and is pinned in `Cargo.toml`.
The [patch series](patches/iceberg-rust/) reproduces the patched fork tree.
It includes Parquet split ownership and promoted-literal fixes, runtime and
cancellation support, and the per-file row-filter extension used by this crate.

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

The integration adds separate Coldfusion lanes to `spiraldb/spiraldb`, alongside
its existing Apache DataFusion–Iceberg lane. It compares the Spiral DataFusion
Arbor configuration with canonical Zstd Parquet and a separately named LZ4_RAW
Parquet layout. The LZ4 lane performs untimed transcoding, preserves file order,
values, schema and row-group boundaries, and reports codecs, hashes, storage
bytes and preparation time. Re-encoding can change page boundaries.

The workload is the read-only 22-query TPC-H suite at SF10. Every answer is checked
against the canonical references. Each query reports the median of three timed
samples. Aggregate query throughput is `22 / sum(query median seconds)`; this is
not the official TPC-H power/refresh/concurrent-stream metric.

The final local cohort on November 6, 2026 passed all 22 exact answer checks on
each lane, using ColdFusion revision `6b84466b` and one shared executable:

| Configuration | Sum of query medians | Queries/s |
| --- | ---: | ---: |
| Spiral DataFusion SDK · Arbor | 5.714 s | 3.850 |
| DataFusion · Arbor direct Scan API | 4.001 s | 5.498 |
| ColdFusion · original Zstd Parquet | 7.039 s | 3.126 |
| ColdFusion · LZ4_RAW Parquet | 5.690 s | 3.866 |

Canonical Zstd remained slower than both Arbor configurations. LZ4_RAW was within
0.5% of the SDK configuration in this single cohort, which does not establish a
reliable throughput lead; direct Arbor was faster. LZ4 layout preparation and
reporting took 32.954 s outside query timing. Files grew from 2.357 GB to 3.540 GB
(decimal units), about 50.2%. The canonical lane reads the original bytes.

The machine is an Apple M3 Max, 14 CPUs and 96 GiB RAM, with local NVMe storage.
All four lanes use the same executable, SQL, input values, 14 query workers and
14 target partitions. This is one sequential cohort on a shared machine, with
the OS page cache uncontrolled. Builds and profiles run separately from measurements.
The [results and raw timing samples](benchmarks/tpch-sf10-2026-11-06/) record
source and binary identities, execution order, configuration and layout costs.
The [benchmark PR](https://github.com/spiraldb/spiraldb/pull/9815) retains the
existing Apache Iceberg lane. Analysis scripts are in [scripts](scripts/).
