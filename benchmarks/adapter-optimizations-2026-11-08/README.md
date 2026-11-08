# ColdFusion reader optimization measurements

LZ4_RAW Parquet with page indexes ON was 6.3% ahead of SDK Arbor and 27.0% behind Direct Arbor. Vortex Iceberg was 1.2% ahead of Direct Arbor in this cohort. Parquet and Vortex results remain separate.

All nine process runs completed: 198 exact query checks and 990 timed samples. This is one forward cohort with Arbor controls before/after. The primary metric sums per-query arithmetic means; controls average the two run sums. Rates use 22/primary seconds, without drift normalization.

| Configuration | Mean sum (s), primary | Median sum (s) | Queries/s | vs SDK | vs Direct | Encoded GB | Files | Max sample (ms) | Q21 max (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SDK Arbor | 5.776301 | 5.770214 | 3.809 | 1.000x | 0.686x | 2.761969312 | 88 | 747.454 | 715.425 |
| Direct Arbor | 3.964080 | 3.963697 | 5.550 | 1.457x | 1.000x | 2.761969312 | 88 | 582.366 | 446.024 |
| Iceberg Zstd Parquet / page OFF | 6.962942 | 6.954642 | 3.160 | 0.830x | 0.569x | 2.356798022 | 16 | 963.064 | 963.064 |
| Iceberg Zstd Parquet / page ON | 6.681887 | 6.674229 | 3.292 | 0.864x | 0.593x | 2.356798022 | 16 | 854.771 | 604.544 |
| Iceberg LZ4_RAW Parquet / page OFF | 5.499161 | 5.396584 | 4.001 | 1.050x | 0.721x | 3.539865160 | 16 | 841.577 | 764.160 |
| Iceberg LZ4_RAW Parquet / page ON | 5.433898 | 5.378326 | 4.049 | 1.063x | 0.730x | 3.539865160 | 16 | 838.865 | 684.600 |
| Iceberg Vortex / native source | 3.915994 | 3.910636 | 5.618 | 1.475x | 1.012x | 2.761455768 | 16 | 810.550 | 275.036 |

All samples were below 2 seconds; the configured outlier count was zero (requires both >2 seconds and >5 times that query/run median). The table reports observed maxima, not a percentile or latency guarantee.

Before/after control mean sums:

- arbor-sdk: 5.775696683s → 5.776904375s (+0.021% time).
- arbor-direct: 3.962791616s → 3.965368701s (+0.065% time).

Page ON/OFF differences are observations from this cohort. Zstd’s 193.958 ms Q21 difference accounts for 69.0% of the 281.055 ms total; LZ4_RAW’s 51.068 ms accounts for 78.2% of 65.264 ms. Q6 was slightly slower with page ON for both codecs. Earlier Q6/Q15 audits found broad page bounds. These timings do not establish a causal pruning gain; retain both configurations and link the raw samples/contributor table.

Both Iceberg Vortex and Arbor use Git Vortex 7d003295 in this executable. ColdFusion returns canonical Arrow types; Direct Arbor uses view-based strings. Encoded file layout differs (16 versus 88 files). Parquet rows use Parquet; Vortex results do not establish Parquet throughput.

Reproduction identity: binary `07f53ac24bbb517264808abf0827c70364a99a0a19a85e9e71a249154155389d`, provider `98ec669`, native `3a409b31`, benchmark consumer `2c44c06ba`. Samples, means, medians, and counters were independently recomputed from sanitized raw records. No new measurement ran during this analysis.

Configuration: SF10, 22 queries, 14 workers/partitions, batch 8192, 2 warmups and 5 timed samples/query, 60 seconds settling per prepared lane; SQL planning and collection timed, fixture preparation excluded. Local files and OS cache are uncontrolled.

Final ColdFusion settings: Exact predicates ON, native Vortex scans ON, Vortex layout batching ON, canonical Arrow types (views OFF). The Parquet metadata-cache prototype was removed. DataFusion was not forked or modified; scan output retains at least two partitions under the tested optimizer rewrites.

Encoded bytes include complete data files and internal metadata/footers/indexes; catalogs and Iceberg manifests are excluded. Page OFF/ON pairs use identical payloads, and both Arbor configurations share payload fingerprints. Arbor uses the matched 1,048,576-row fragment setting. Safe file hashes and table aggregate fingerprints are retained in cohorts.json.

The final executable uses clean committed, locked sources, distinct from the earlier saved-snapshot experiments. [provenance.json](provenance.json) records exact revisions, manifest/lock hashes, toolchain, compiler-cache identity and matching binary/symbol UUIDs, without private paths or environment contents.

See [all per-query means](per-query-means.md), [page contributors and samples](page-setting-contributors.md), [earlier studies](studies.md), [rejected-experiment profiles](profile-notes.md), [retained commit scope](commit-series.md), and [validation](validation.md). All 12 retained runtime/integration commits received CLEAR reviews; individual commits without isolated timings have no individual speedup claim.

The provider passed 37 tests and the harness 10 scoped tests, with local Clippy/formatting checks passing. Public provider CI is green; private CI stopped in external release-tool setup with HTTP 504, without an observed project-code failure.

The package contains ten cohorts: 1,936 exact records and 9,372 timed samples in measurements.jsonl. The final committed cohort accounts for 198 records and 990 samples. summary.json retains primary means, upper medians and tails; profiles.json retains profile/rejection evidence. artifact-manifest.json is the exact publication allowlist and checksum manifest.
