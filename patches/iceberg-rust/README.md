# Iceberg Rust patch series

These 24 patches reproduce the public `spiraldb/iceberg-rust` development history used by ColdFusion.

- Repository: https://github.com/spiraldb/iceberg-rust
- Base: `ea48146de861ee71d32ba747ca0d182ea7746ed1`
- Tip: `d28e192ceba4105ff4b4480021d94e3df747915b`
- Resulting tree: `03a4656b29c9fb2d847b1977abfbbcb4a7154939`

The series includes every commit in that range, including intermediate changes and their reverts. It contains only public Iceberg repository changes. The standalone ColdFusion provider is maintained in this repository. The first 23 patch files retain their original numbering headers and checksums. Patch #23 restores built-in Vortex support and #24 removes the duplicate adapter without rewriting the published history.

Apply the numbered patches in lexical order to a clean checkout of the base:

```sh
git checkout --detach ea48146de861ee71d32ba747ca0d182ea7746ed1
for patch in /path/to/coldfusion/patches/iceberg-rust/*.patch; do
    git apply --index "$patch"
done
git write-tree
```

The resulting tree must be `03a4656b29c9fb2d847b1977abfbbcb4a7154939`. This verification creates no commits. The exported series was verified in a detached temporary worktree, which was removed afterward.

[manifest.json](manifest.json) records patch order, public commit identities, SHA-256 checksums, and the successful tree comparison. [base-tip.txt](base-tip.txt) provides the pinned identities in plain text.

## Why this history has so many commits

This is a chronological development series, not a list of upstream defects or a
minimal upstream submission. The native Vortex reader and writer already existed
at the base revision. The DataFusion provider was new work, initially developed
inside this fork and subsequently copied into standalone ColdFusion. Patch #24
removes only that obsolete adapter and its unused lockfile dependencies from the
native workspace; native Iceberg reader and writer code is unchanged. ColdFusion
depends directly on Iceberg and unchanged DataFusion, and is the maintained adapter.

The two native Parquet correctness fixes found during this work concern physical
row-group split ownership and predicate literals after schema promotion. The I/O
context fix supports callers with separate compute and I/O runtimes. Other native
changes add integration hooks or optional behavior. Several commits only affect
our provider, tests, documentation, or experiments.

| # | Commit | Purpose and current relevance |
| --- | --- | --- |
| 1 | `eed7a41c` | Made Vortex optional after interpreting the initial constraint too broadly. Reverted by #23; it was not needed to optimize Parquet. |
| 2 | `636504bb` | Correctness: use actual Parquet column offsets for disjoint row-group ownership and expose validated task byte ranges. Required by our parallel split design. |
| 3 | `5ca9dac1` | Added the historical DataFusion provider prototype and parallel source partitions. Removed from the native workspace by #24; the maintained provider is standalone ColdFusion. |
| 4 | `a21a8d20` | Added conservative static predicate pushdown to the provider. An optimization; DataFusion retains residual filters. |
| 5 | `77e43577` | Added real-file validation for deletes, schema evolution, and filtering. No new execution policy. |
| 6 | `b75bcdb0` | Documented the provider API, immutable snapshot, and parallel scan contract. |
| 7 | `efedf229` | Correctness: preserve comparisons when a promoted column's literal cannot fit its physical type. Prevents incorrectly discarded rows. |
| 8 | `b88bac0a` | Added optional decoding on the existing CPU executor, bounded buffering, cancellation, and error propagation. Useful with small query runtimes; not required merely to expose multiple output partitions. |
| 9 | `6acff8d5` | Initially enabled background decoding for every provider partition. Superseded by the automatic choice in #21. |
| 10 | `1486e2b6` | Added producer compute-time accounting that survives consumer cancellation. Improves measurement accuracy. |
| 11 | `f8d72b03` | Experimented with staged Parquet conjunct evaluation. Benchmarks regressed, so this was not retained. |
| 12 | `943df5d1` | Reverted #11. The pair has no net final behavior. |
| 13 | `3a8a76c5` | Added cooperative yielding between background batches. Relevant to the optional background mode. |
| 14 | `0f05fc5e` | Balanced provider byte budgets across file boundaries so consumers receive more even work. Provider policy, not a native reader bug. |
| 15 | `b4f0f71b` | Added a generic per-file Parquet row-filter factory alongside native predicates and deletes. Enables the provider's dynamic-filter optimization. |
| 16 | `f125ab64` | Experimented with Arrow string/binary views. The native opt-in API remains available, but ColdFusion does not use it. It can be omitted from a minimal native patch set. |
| 17 | `69f157b7` | Connected DataFusion live filters to the provider's Parquet hook. Its initially broad policy was refined in #19. |
| 18 | `2e4c372c` | Restored canonical provider Arrow output after view-heavy joins regressed. Leaves #16's native opt-in API unused. |
| 19 | `6bc2ff7a` | Restricted live pushdown to cheap, safe predicates and cached decisions by filter generation, avoiding expensive CASE/hash-filter evaluation. |
| 20 | `277fa343` | Correctness for split runtimes: enter the configured I/O context while polling foreground reads. The separate CPU/I/O runtime architecture already existed. |
| 21 | `252ab569` | Let sufficiently sized DataFusion runtimes decode directly; use background decoding when query worker capacity is smaller. Supersedes #9's unconditional offload. |
| 22 | `04c96e42` | Corrected our provider's statistics to estimate projected Arrow output bytes instead of reporting compressed file bytes. This was our integration error. |
| 23 | `278b205d` | Reverted #1, restoring the native Vortex reader, writer, and delete-file support as mandatory built-in functionality while preserving the Parquet changes. |
| 24 | `d28e192c` | Removed the duplicate DataFusion adapter crate and its unused lockfile dependencies from the native workspace. Native core code is unchanged; ColdFusion depends directly on Iceberg and DataFusion. |

For upstream review, the native fixes and hooks should be consolidated separately
from provider code and development experiments. Retaining the full history here
preserves the already published commit identities and reproduces the exact fork
tree; it does not imply that every intermediate patch is necessary today.
