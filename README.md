# Coldfusion

Coldfusion is a read-only Iceberg table provider for DataFusion 55.2. It reads
Parquet through [vortex-parquet](https://github.com/spiraldb/vortex-parquet) and
Vortex through its native layouts, using one expression and scan pipeline. A table may contain either format or a mixture
of both. DataFusion is unmodified.

The dependency direction is `ColdFusion → Iceberg + DataFusion`. This repository
owns the readers, schema evolution, delete handling, partition planning, and
DataFusion expression adaptation. Iceberg plans snapshot files and their applicable
deletes; it has no DataFusion dependency. ColdFusion uses the unmodified main branch
of `spiraldb/iceberg-rust`.

```toml
[dependencies]
coldfusion = { git = "https://github.com/spiraldb/coldfusion", rev = "e8effc17850f7249db256ccca2e72b8812520682" }
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

Scans expose at least two output partitions, using the session target when larger.
Work units follow natural Parquet row-group and Vortex layout boundaries, chosen
with the projection and filter. Small files can leave output partitions empty.
Limits and ordering stay above the scan in DataFusion to preserve parallel outputs.

Readers share bounded opened-file metadata and payload caches across queries.
Expression caches belong to each planned scan; decoded delete keys are shared
between its files. Create providers on a Tokio runtime with I/O and timers enabled,
and keep that runtime alive while executing their scans.

The provider retains the loaded schema and snapshot. Construct another provider
to observe catalog changes. ColdFusion matches fields by Iceberg IDs, applies
schema evolution, initial defaults and identity partition values, and resolves
positional and equality deletes before producing output. Output uses canonical
Iceberg Arrow types, including `Utf8` and `LargeBinary`. Hidden delete keys are
retained internally even after a column is dropped. Encrypted files and delete
formats other than Parquet or Vortex are rejected explicitly.

Static primitive predicates prune Iceberg manifests. Both formats then evaluate
Vortex expressions inside their layouts, including filter and projection pushdown.
Parquet uses row-group and page statistics whenever available; files without page
indexes still support row-group and row filtering. Page pruning is unconditional.
Supported required-field predicates use exact pushdown by default, removing
redundant DataFusion residuals. `with_exact_filter_pushdown(false)` retains those
residuals. Both formats always use the shared Vortex readers and expression converter.

The shared source uses one converter for static filters and computed
projections. It extracts compatible native subtrees and embeds remaining synchronous
DataFusion expressions as a typed, process-local `DFScalarFn`. Volatile expressions
stay in DataFusion. Conditional branches remain together unless extraction preserves
their evaluation domain. A small conservative tree walk marks only proven infallible
operations as safe for speculative evaluation; unknown operations are still accepted
inside the callback.

Vortex and mixed-format scans use generation-aware live join filters and the same
callback. Parquet-only scans push native dynamic bounds; expensive optional
CASE/hash callbacks remain in DataFusion, which enforces the complete filter.
Static filter and projection callbacks are supported for both formats. When files
have row deletes, unproven fallible filters stay in DataFusion above the resolved
scan: layout filters may evaluate rows before applying a selection. Projections
remain inside Vortex because their inputs have already been selected.
An evaluation error in an advisory hint retains rows; exact expressions preserve
DataFusion errors. Exact projections containing live predicates stay in DataFusion,
because later snapshots may introduce unsupported or volatile expressions. Other
fallbacks are async expressions and Arrow result types Vortex cannot safely carry.
In particular, a callback returning a decimal stays in DataFusion because Arrow can
produce coefficients outside the declared precision; decimal intermediates inside
Boolean or other representable results are supported. Existing proven native decimal
operations remain native. DataFusion itself is unchanged.

Scans expose the official Vortex execution metrics. I/O handles and payload
caches are shared, so their byte counters are not reported as per-query
`bytes_scanned`. Top-level Int/Long bounds come from file footers and remain
conservative through renames and promotion. Missing bounds remain unknown;
distinct counts and null counts are not inferred. These statistics help DataFusion
choose join order and build sides. They require opening the selected files, which
scan planning also needs to discover natural work units.

## Dependencies

`Cargo.toml` and `Cargo.lock` pin exact Git revisions for
[spiraldb/iceberg-rust](https://github.com/spiraldb/iceberg-rust) and
[vortex-parquet](https://github.com/spiraldb/vortex-parquet). No local Cargo patches
are required. Reader and DataFusion integration changes stay in ColdFusion;
Parquet layout improvements belong in vortex-parquet. That repository is currently
Internal on GitHub, so building requires SpiralDB access and Git credentials.
ColdFusion retains its existing Vortex Git pin for advisory expression conversion;
Iceberg uses its own released Vortex internally. Vortex arrays do not cross that
boundary.

## Validation

```sh
cargo test --locked --test provider
cargo test --locked --lib
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
