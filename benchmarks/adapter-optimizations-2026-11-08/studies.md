# Earlier optimization studies

These cohorts are separate from the final committed comparison. Saved-snapshot studies include uncommitted changes and dependency overrides; their parent commits alone do not identify the executable. Do not pool timings across different binaries, sample counts or preparation controls.

| Study | Classification | Queries | Exact checks | Timed samples |
|---|---|---:|---:|---:|
| views | rejected | 22 | 352 | 1760 |
| static-filter | rejected | 22 | 264 | 1320 |
| split-conjunct-screen | rejected-screen | 4 | 48 | 240 |
| vortex-layout-screen | positive-screen | 5 | 40 | 200 |
| exact-filter | accepted | 22 | 264 | 1320 |
| native-vortex | accepted | 22 | 176 | 880 |
| metadata-cache | rejected | 22 | 264 | 1320 |
| native-vortex-initial | superseded | 22 | 176 | 880 |
| initial-seven-baseline | baseline | 22 | 154 | 462 |

All per-configuration means, upper medians, maxima, slow-sample counts and control drift are retained in summary.json and cohorts.json. Every sample and exact-result digest is retained in measurements.jsonl.

Exact pushdown improved paired full-suite derived rate by 2.8% for Zstd and 3.8% for LZ4_RAW. The latest full native-source bundle improved paired rate by 1.182x; that result is not divided among API, cache, converter or adapter changes. Layout batching improved a five-query screen by 1.42x, which is not a full-suite claim. Staged split-conjunct filtering was a four-query screen.

Views, broad static filters and the metadata cache were rejected; initial native-source measurements are superseded history. See [profile-notes.md](profile-notes.md) for the measured regressions, profile evidence and limitations.
