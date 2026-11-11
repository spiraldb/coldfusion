# Iceberg Rust patch series

These 18 patches contain the native Iceberg changes used by ColdFusion, based on
`spiraldb/iceberg-rust` main. The provider remains in ColdFusion; this native
workspace has no DataFusion dependency or adapter.

- Repository: https://github.com/spiraldb/iceberg-rust
- Branch: `ngates/vortex-5e08d3a9`
- Base: `ea48146de861ee71d32ba747ca0d182ea7746ed1`
- Tip: `53a3b2859f436f23f3be53a6661c2c9bd7706b7c`
- Resulting tree: `411adb6d73486c333b8821bbdbfdf77771910cbc`

The first nine patches preserve the previous compaction: their tree is exactly
`7f9e7822a40c5bf67e711ac0d1cf755d23d17877`, matching the historical 25-commit tip
`930277e0209bec6ff321d06ae9b4a0f2e794000f`. Their exported bytes, numbering
headers, checksums and manifest entries are unchanged. That equality applies to
**the first nine patches**, not to this extended tip. Patches 10–13 intentionally
change the Vortex dependency and reader behavior. Existing benchmark records keep
the source revisions actually measured. Patch 14 preserves manifest bounds for
consumers without changing native pruning or decoding. Patch 15 narrows integer filter
literals to the physical Vortex type and folds out-of-domain comparisons.
Patch 16 shares footer-to-manifest metrics between writers and importers and exposes
conservative integer bounds from scan tasks through the native Vortex source.

Apply every numbered patch in lexical order to a clean checkout of the base:

```sh
git checkout --detach ea48146de861ee71d32ba747ca0d182ea7746ed1
for patch in /path/to/coldfusion/patches/iceberg-rust/*.patch; do
    git apply --index "$patch"
done
git write-tree
```

The resulting tree must be `411adb6d73486c333b8821bbdbfdf77771910cbc`. Verification replayed all 18 patches and the
four-patch extension separately through isolated Git indexes and object stores.
Each intermediate tree matched its source commit. No checkout or commits were
created. The retained series has no empty commit, repeated boundary tree, or
exact inverse patch pair.

[manifest.json](manifest.json) records ordered commits, checksums, the preserved
first-nine `compaction` proof and removed empty groups, the Vortex `extension`
range, the `metadata_bounds_extension`, `literal_domain_extension` and `vortex_statistics_extension`, and the
full-series replay. [base-tip.txt](base-tip.txt) records these
identities in plain text.

## Remaining patches

| # | Commit | Purpose |
| --- | --- | --- |
| 1 | `6c22c0f4` | Assign disjoint Parquet row groups using physical offsets and expose validated task byte ranges. |
| 2 | `18ba96a7` | Preserve decoded predicate comparisons after schema promotion. |
| 3 | `33a62e31` | Support bounded background decoding on the existing CPU executor. |
| 4 | `cd721649` | Observe producer compute time through consumer cancellation. |
| 5 | `d412cf8d` | Yield cooperatively between background batches. |
| 6 | `b75c074e` | Add generic per-file Parquet row-filter factories; ColdFusion owns expression adaptation. |
| 7 | `09af27a9` | Expose opt-in Arrow string/binary view output. |
| 8 | `13545bbe` | Poll foreground Parquet reads with the configured I/O runtime. |
| 9 | `45376aeb` | Promote physical Int32/Float page bounds to Long/Double before pruning. |
| 10 | `f00af1ca` | Align the native Vortex dependency and reader/writer APIs with the Arbor revision. |
| 11 | `71886fef` | Filter native Vortex layout batches before bounding Arrow output batches, preserving configured output types. |
| 12 | `0038736e` | Expose supported Iceberg Vortex tasks as a generic native Vortex array source, with conservative whole-scan fallback. |
| 13 | `3a409b31` | Reuse bounded immutable Vortex file metadata handles on the table's I/O runtime while keeping query state separate. |
| 14 | `8a36a080` | Preserve inclusive manifest column bounds in scan tasks, shared across byte ranges, without engine-specific types or additional reads. |
| 15 | `25bfc0fd` | Bind integer predicates to physical signed/unsigned widths using checked literals, range simplification and null-safe filter selection. |
| 16 | `9ab632ff` | Reuse footer metrics for no-rewrite imports and expose complete, conservative integer column bounds through the native Vortex source. |
| 17 | `a82bafad` | Align Vortex with conservative recovery of integer bounds in live DataFusion filter snapshots. |
| 18 | `53a3b285` | Move Vortex to `5e08d3a9`, the revision SpiralDB pins, so one Vortex build serves both. |

## Native Vortex source access

`iceberg::arrow::VortexScanSource` implements Vortex's `DataSource` interface.
It keeps supported predicates and projections in Vortex arrays until a consumer
needs Arrow. `try_new` creates a source for one scan. Repeated scans can use
`VortexScanCache::new(file_io, session, runtime)` with `try_new_with_cache`;
`Table::runtime()` exposes the configured execution handles. Keep the underlying
I/O runtime alive and driven while planning and reading, even if a shorter-lived
planning runtime stops.

Construction validates every task and file schema before returning a usable
source. It supports a checked subset of top-level primitive schemas, field-ID
renames, safe promotions, and missing optional fields without defaults. Keep the
original tasks for fallback: `Ok(None)` means the **whole scan** must use the
Arrow reader. Deletes, partition constants, metadata columns and other unsupported
shapes must not be silently omitted or combined with partially emitted source
rows. Partition row ranges and selections use ordinals relative to stable source
partitions. Unordered scans share a global row limit across partitions. Ordered
scans apply the limit per partition; callers must trim the combined ordered output
to enforce a global limit.

The cache reuses at most 128 opened immutable file handles. Schema adaptation,
expressions and pruning state remain query-local. Its byte counter is cumulative
cache I/O, not an exact per-query physical-disk byte count. The source and cache
APIs contain no DataFusion types; ColdFusion owns the engine adapter.

## Removed empty history

The first-nine compaction removed the Vortex feature-gating change and its exact
reversal, the staged-conjunct experiment and its exact reversal, and the obsolete
DataFusion adapter's complete lifetime including its lockfile edits. The native
portions of mixed commits remain. The compaction's `Cargo.lock` equaled its base;
patch 10 now intentionally changes the lockfile for the aligned Vortex revision.
Vortex remains built-in throughout the retained series.
