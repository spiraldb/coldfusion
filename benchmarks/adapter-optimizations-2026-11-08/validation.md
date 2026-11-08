# Validation

The measured provider revision passed 16 unit tests and 21 integration tests: **37 tests total**. Locked checks, Clippy and formatting passed on the October 23 nightly toolchain. Provider CI on `98ec669d` is green.

The benchmark integration passed 10 scoped tests, local Clippy and formatting. Its CI retries stopped during external GitHub release-tool installation with HTTP 504 after one retry; the Go retry passed. No project-code failure was observed in the completed checks. The blocked private CI run is not reported as passing.

All 12 retained runtime/integration commits received CLEAR reviews. The frozen executable completed **198 exact query checks and 990 timed samples**, with input hashes unchanged. The independent audit matched all mean, median and tail counters. The largest sample was 963.064 ms; all 990 samples were below two seconds. Later documentation edits are outside the measured executable.
