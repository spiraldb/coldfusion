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
`with_exact_filter_pushdown` and `with_native_vortex_scan` methods can disable
those optimizations independently. Vortex layout batching is unconditional.

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

## Iceberg dependency

Native reader changes are maintained in
[spiraldb/iceberg-rust](https://github.com/spiraldb/iceberg-rust).
`Cargo.toml` and `Cargo.lock` pin the Git revision Cargo builds directly.
DataFusion expression adaptation stays in this repository.

## Validation

```sh
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all -- --check
```

Real-file tests cover independent parallel consumers, projections, renames,
defaults, promoted types, nulls, both delete kinds, live filter updates, and split
I/O and compute runtimes.

## Benchmarking

Measure Parquet and Vortex separately with matched inputs and reference checks.
Benchmark source and reusable analysis tools may be committed. Keep generated
results, raw samples, profiles, query plans, datasets, and run-specific reports
out of Git; use ignored local directories or external CI artifact storage.
