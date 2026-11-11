# Q72 runtime membership: laptop SF1

ColdFusion's native Vortex scan now evaluates DataFusion's runtime membership
predicate before returning rows to the joins. This is the existing batched DF
lookup, not a new SIMD hash table. No DataFusion, Iceberg, or Vortex changes are
required by these commits.

## Measured query times

One measured sample per configuration, sequentially on the same Apple M3 Max
laptop (96 GiB RAM, 14 cores), with four DataFusion partitions and
`TOKIO_WORKER_THREADS=4`. TPC-DS SF1, Q72 only, local canonical inputs. Each lane
first executes once for untimed reference verification. The configured warmup
count is zero; it does not remove this verification execution. Table preparation
and verification are excluded from the measured sample; SQL planning is included.

| Configuration | Seconds | Time relative to new Viceberg |
| --- | ---: | ---: |
| ColdFusion Viceberg + membership ScalarFn | 0.113242917 | 1.00x |
| ColdFusion Piceberg, page pruning enabled | 1.021898750 | 9.02x |
| Spiral direct (`datafusion-arbor`) | 0.884944916 | 7.81x |

All three passed exact answer validation with digest
`b3dc4524079fd5e2079508829720ed12574d5532a917a6f7db200112771c87b7`.
Every timed lane used the same binary, SHA-256 `6751e261da70cca0bd00ae67952d5519fb4887cd0a04b86ae101dd24ccef71ca`.
The SQL timeout was 120 seconds. These are small local measurements, not an SF10
result or a full-suite throughput claim.

The final timing cohort uses Spiral's reviewed PR #9991 tree at
`4837bf9bcd4a2da24aea39d85da834cac6581534`, equivalent to squash merge
`49afe5b517ef41593633e46c11287d977a1c8ecd`, with a local ColdFusion override at
`8d9bb759af53820bf20d34a5088d211e4e09370d` and native Iceberg at `a82bafad2b6ea12fefa110fdc02e6e41a6096231`.
It keeps the benchmark's existing separate Vortex revisions: Spiral direct uses
`d92f4674b68b0b562e17f93c4cb0c364f0bf4544`; ColdFusion uses
`e2d8b0fd1fc4708a38946352a3925e58c22cef5f`. Both use DF 55.2.0 / Arrow 59.3.2.
An initial older-driver comparison used Vortex 7d for direct and measured 5.520 s;
that outdated baseline is superseded by the 0.885 s direct result above.

## Evidence for early filtering

A separate controlled diagnostic used the earlier local driver at `504c2e41b1`,
the same canonical SF1 inputs, and four partitions. The intervention removed
only the three-line converter hook, then restored it. It did not change join
order, query text, native reader statistics, or files. Both executions passed
exact reference validation. This diagnostic is separate from the final timing
cohort above.

| Operator output | Hook disabled | Hook enabled |
| --- | ---: | ---: |
| Catalog sales scan | 280,639 | 9,100 |
| Inventory scan | 11,745,000 | 4,991,135 |
| Sales/inventory join | 11,698,173 | 385,033 |

The first fact join produces about 30.4x fewer rows. The later joins still enforce
exact join semantics. The control timing record was overwritten by the later
operator diagnostic; no before/after ColdFusion timing ratio is claimed.
`operator-control.json` contains the full plans and operator counts. Its generic
diagnostic helper labels the table format `parquet`; the physical plans identify
the actual `VortexScanSource` used for both sides of this intervention.

## Implementation and checks

- A query-local, nonserializable ScalarFn retains DF's expression and shared map.
- Only referenced integer key columns are materialized to Arrow. DF's seeds,
  null handling, and complete partition-routing CASE expression are retained.
- The converter accepts a bounded, value-infallible grammar: single-key membership,
  integer hash routing, typed comparisons/Boolean operators, typed CASE, and
  unsigned routing modulo a nonzero literal. It declines other expressions.
- Existing Vortex dynamic-expression generation tracking refreshes the snapshots;
  the initial TRUE predicate is not frozen. Parallel output partitions are retained.
- Seven focused tests passed: null hashing/projection/nonserialization, routing
  CASE with both accepted/rejected rows, snapshot replacement, actual source
  filtering across updates, volatile rejection, fallible-expression rejection,
  and malformed shapes/type/nullability rejection. No exhaustive suite was run.

The two implementation commits are `ffcd81fc0e50a377f94806015663cb40f6af8d09`
and `8d9bb759af53820bf20d34a5088d211e4e09370d`; both were reviewed together in the designated
review chat. Raw timing records, invocation receipts, source hashes and metadata
are included alongside this file. The invocation receipts contain local paths;
replace the workspace and dataset paths when reproducing elsewhere.
