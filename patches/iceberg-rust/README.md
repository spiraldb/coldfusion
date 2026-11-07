# Iceberg Rust patch series

These nine patches contain the remaining native Iceberg changes used by
ColdFusion, based on `spiraldb/iceberg-rust` main. Changes that cancel out have
been removed from the series.

- Repository: https://github.com/spiraldb/iceberg-rust
- Branch: `codex/coldfusion-reader`
- Base: `ea48146de861ee71d32ba747ca0d182ea7746ed1`
- Tip: `45376aebbfc5fa0ba94b18837cb2443eb87a68ae`
- Resulting tree: `7f9e7822a40c5bf67e711ac0d1cf755d23d17877`

The full tree is identical to the former 25-commit tip,
`930277e0209bec6ff321d06ae9b4a0f2e794000f`. This includes `Cargo.lock`, which is
also identical to the base. Existing benchmark records retain the source commits
actually measured; the historical tip remains available for provenance.

Apply the numbered patches in lexical order to a clean checkout of the base:

```sh
git checkout --detach ea48146de861ee71d32ba747ca0d182ea7746ed1
for patch in /path/to/coldfusion/patches/iceberg-rust/*.patch; do
    git apply --index "$patch"
done
git write-tree
```

The resulting tree must be `7f9e7822a40c5bf67e711ac0d1cf755d23d17877`.
All nine exports were replayed this way, and the temporary worktree was removed.
[manifest.json](manifest.json) records the ordered commits, original source
commits, checksums, removed empty groups, and replay verification.
[base-tip.txt](base-tip.txt) records the identities in plain text.

## Remaining patches

| # | Commit | Purpose |
| --- | --- | --- |
| 1 | `6c22c0f4` | Assign disjoint Parquet row groups using physical offsets and expose validated task byte ranges. |
| 2 | `18ba96a7` | Preserve decoded predicate comparisons when schema promotion makes a literal wider than the physical column type. |
| 3 | `33a62e31` | Add optional background decoding on the existing CPU executor, with bounded buffering, cancellation, and error propagation. |
| 4 | `cd721649` | Observe producer compute time even when the consumer cancels. |
| 5 | `d412cf8d` | Yield cooperatively between background reader batches. |
| 6 | `b75c074e` | Add a generic per-file Parquet row-filter factory alongside native predicates and deletes. ColdFusion owns the DataFusion expression adaptation. |
| 7 | `09af27a9` | Support opt-in Arrow string/binary views. This native API remains a real change; ColdFusion currently uses canonical Arrow output. |
| 8 | `13545bbe` | Enter the configured I/O context while polling foreground Parquet reads, supporting separate I/O and compute runtimes. |
| 9 | `45376aeb` | Promote physical Int32/Float page bounds to Long/Double before pruning, with indexed-file regression tests. |

## Removed empty groups

- The Vortex feature-gating change and its exact reversal: former commits
  `eed7a41c` and `278b205d`. Vortex remains built-in, as it was at the base.
- The staged-conjunct experiment and its exact reversal: former commits
  `f8d72b03` and `943df5d1`.
- The obsolete DataFusion adapter's addition, later edits, and eventual removal,
  including its lockfile changes. The adapter directory is absent at both
  endpoints. Native changes from mixed commits were retained separately.

Only native reader code, tests, and API inventory remain in this series. The
DataFusion provider is maintained in this ColdFusion repository and depends on
Iceberg and DataFusion; the native Iceberg workspace has no DataFusion adapter.
