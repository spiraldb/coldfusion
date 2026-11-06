# Iceberg Rust patch series

These 22 patches reproduce the public `spiraldb/iceberg-rust` development history used by ColdFusion.

- Repository: https://github.com/spiraldb/iceberg-rust
- Base: `ea48146de861ee71d32ba747ca0d182ea7746ed1`
- Tip: `04c96e42614ccf36f89fb3f699739eeb45e9838b`
- Resulting tree: `c429d4810e39683eda4b80f88b6a7b569ace182f`

The series includes every commit in that range, including intermediate changes and their reverts. It contains only public Iceberg repository changes. The standalone ColdFusion provider is maintained in this repository.

Apply the numbered patches in lexical order to a clean checkout of the base:

```sh
git checkout --detach ea48146de861ee71d32ba747ca0d182ea7746ed1
for patch in /path/to/coldfusion/patches/iceberg-rust/*.patch; do
    git apply --index "$patch"
done
git write-tree
```

The resulting tree must be `c429d4810e39683eda4b80f88b6a7b569ace182f`. This verification creates no commits. The exported series was verified in a detached temporary worktree, which was removed afterward.

[manifest.json](manifest.json) records patch order, public commit identities, SHA-256 checksums, and the successful tree comparison. [base-tip.txt](base-tip.txt) provides the pinned identities in plain text.
