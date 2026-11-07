# Parquet page pruning and storage comparison

All 154 configuration/query results passed exact reference validation at SF10. Each query median uses three measured samples after one validation/warmup, with 14 query workers and target partitions. All seven configurations use the same executable, SQL catalog and logical input rows.

The table reports query planning and collection only. Layout preparation is untimed. GB is decimal; storage excludes external table/catalog/manifest metadata.

| Configuration | Page index | Sum of medians (s) | Queries/s | Data GB | Files |
| --- | --- | ---: | ---: | ---: | ---: |
| Spiral SDK Arbor | — | 5.783314 | 3.804 | 2.762 | 88 |
| Direct Scan API Arbor | — | 4.025244 | 5.466 | 2.762 | 88 |
| ColdFusion Zstd Parquet · page index off | off | 7.048860 | 3.121 | 2.357 | 16 |
| ColdFusion Zstd Parquet · page index on | on | 7.076743 | 3.109 | 2.357 | 16 |
| ColdFusion LZ4_RAW Parquet · page index off | off | 5.519257 | 3.986 | 3.540 | 16 |
| ColdFusion LZ4_RAW Parquet · page index on | on | 5.762572 | 3.818 | 3.540 | 16 |
| ColdFusion Vortex-Iceberg | — | 5.802730 | 3.791 | 2.761 | 16 |

## Interpreting page-index differences

This is one sequential cohort, with one run per configuration. Earlier interrupted attempts are excluded; there is no completed balanced A/B comparison in these results.

Zstd page-on took 0.4% longer in the sum of query medians. LZ4 page-on took 4.4% longer, but its aggregate gap is accounted for by Q21: off samples were 433, 753 and 449 ms; on samples were 698, 463 and 735 ms. These overlapping fast/slow samples placed the three-sample medians in different bands. The other 21 LZ4 query medians sum to 5.95 ms less with page indexes on. This does not establish a reliable causal on/off ranking.

The independent canonical-file audit found no min/max page exclusions for the examined Q6 and Q15 bounds, so it predicts little page-pruning benefit for those predicates. It does not establish that every query is unprunable.

## Query medians

Values are milliseconds. Page-index settings apply to the Parquet variants; the Vortex configurations use their native reader paths.

| Query | SDK Arbor | Direct Arbor | Zstd off | Zstd on | LZ4 off | LZ4 on | Vortex |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 346.454 | 218.166 | 382.377 | 382.877 | 343.831 | 348.274 | 374.571 |
| Q2 | 94.243 | 71.785 | 130.523 | 128.616 | 100.697 | 100.344 | 92.683 |
| Q3 | 203.584 | 120.871 | 251.067 | 249.701 | 208.056 | 207.916 | 213.650 |
| Q4 | 147.300 | 69.440 | 97.023 | 97.110 | 92.766 | 91.560 | 134.640 |
| Q5 | 329.493 | 226.652 | 337.206 | 342.521 | 267.498 | 266.651 | 275.945 |
| Q6 | 37.480 | 31.145 | 117.680 | 116.071 | 100.124 | 98.664 | 135.351 |
| Q7 | 260.147 | 235.570 | 309.928 | 320.781 | 226.877 | 227.519 | 507.319 |
| Q8 | 385.357 | 230.815 | 391.998 | 376.254 | 280.597 | 279.523 | 261.372 |
| Q9 | 649.889 | 423.628 | 586.320 | 584.160 | 480.196 | 479.222 | 446.498 |
| Q10 | 318.613 | 224.772 | 466.766 | 468.189 | 342.305 | 341.225 | 233.383 |
| Q11 | 58.961 | 37.133 | 69.749 | 70.611 | 50.120 | 51.048 | 70.740 |
| Q12 | 155.927 | 79.829 | 174.953 | 184.819 | 172.103 | 170.455 | 200.806 |
| Q13 | 219.437 | 158.720 | 261.425 | 266.313 | 210.655 | 206.202 | 199.590 |
| Q14 | 85.614 | 45.116 | 170.036 | 185.102 | 92.496 | 94.497 | 108.579 |
| Q15 | 86.612 | 74.291 | 338.122 | 365.640 | 197.436 | 199.470 | 198.332 |
| Q16 | 70.641 | 50.260 | 92.384 | 95.591 | 81.776 | 81.797 | 56.633 |
| Q17 | 519.969 | 448.589 | 542.597 | 559.417 | 469.454 | 471.176 | 441.159 |
| Q18 | 751.345 | 577.994 | 837.913 | 864.465 | 821.239 | 813.501 | 831.739 |
| Q19 | 59.878 | 50.185 | 317.643 | 330.468 | 246.561 | 245.878 | 167.987 |
| Q20 | 219.752 | 119.790 | 302.479 | 301.885 | 195.401 | 199.925 | 185.039 |
| Q21 | 663.521 | 443.036 | 733.538 | 654.403 | 448.784 | 698.054 | 615.938 |
| Q22 | 119.098 | 87.458 | 137.134 | 131.750 | 90.285 | 89.672 | 50.775 |

[Metadata and source/input identities](metadata.json) · [All 154 records and timing samples](measurements.jsonl) · [Actual canonical page-index audit](page-index-audit.json)

Canonical Vortex and Arbor use the pinned Git Vortex writer; native Iceberg Vortex decoding uses the registry version recorded in metadata. LZ4 re-encoding preserves row order and row-group row counts but may change page boundaries. LZ4 off/on outputs have identical encoded-file hashes. The page-index audit states its separate limits for historical LZ4 temporary files.

This is one sequential cohort on a shared machine with uncontrolled OS cache, not an official TPC-H throughput result. Builds, tests and profiles were held outside query measurements.
