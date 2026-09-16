# Prepare complete entry batches despite earlier shred gaps

Live tracing found five large external blocks whose final assembly-to-ready time was 20–39 ms. Most fallback verification work was already available more than 20 ms before full assembly. For one 33,760-transaction block, 6,506 transactions in later complete batches waited 44–116 ms for discovery behind an earlier missing shred.

The old discovery cursor stopped at the first missing data index. The new bounded bitmap index discovers each complete DATA_COMPLETE range independently. Receiving a data shred or recovering one through FEC can release its containing batch and, when it supplies a boundary, the next batch. A batch still needs every data shred and its preceding boundary, unless it begins at index zero. Results are queued in discovery order and final assembly restores wire order using the existing exact range and byte-identity checks.

The index uses about 24 KiB per retained slot and is allocated only with streaming preparation enabled. Successor/predecessor queries have bounded cost even with reverse or adversarial arrival order. Existing worker counts, verifier batching, retained-byte limits, cancellation, generation ownership, final block checks and signature validation remain unchanged.

Regression coverage includes a delayed earlier shred, delayed preceding boundary, FEC-recovered boundary, disabled preparation, randomized arrival against a reference oracle, duplicate discovery, index word/group/slot boundaries, and the existing final-validation and cancellation tests. Native Turbine/replay race tests and Turbine vet passed.

## Controlled Zen 5 benchmark

AMD Ryzen 7 9700X, Go 1.26.4, Narya r51, GOMAXPROCS=8, two transaction verification workers, target batch size eight. 33,760 generated signed transactions arrive as complete component bursts across 200 ms. One data shred in a component three quarters through the block is withheld until after the footer. Both versions run with identical fixtures in baseline/candidate/candidate/baseline order, three iterations per case in each run, while the validator remains running. Ranges below are the two run medians, not a confidence interval.

| Wire transaction size | Prior assembly → ready | New assembly → ready |
| --- | ---: | ---: |
| 228 bytes, delayed shred | 21.43–22.77 ms | 2.82–2.83 ms |
| 1,232 bytes, delayed shred | 35.46–36.34 ms | 7.04–8.08 ms |
| 228 bytes, ordered | 2.50–2.56 ms | 2.40–2.52 ms |
| 1,232 bytes, ordered | 6.91–7.67 ms | 6.87–7.89 ms |

The delayed-shred case now verifies roughly 33.5–33.7k transactions before assembly, compared with 25.3–25.6k previously. The benchmark asserts every retained signature is verified exactly once and block metadata remains correct. It includes assembly, decoding, final validation and transaction verification; it excludes network I/O, replay execution and actual FAST-certificate inclusion. This modeled case is not an end-to-end validator speedup or a prediction of overall FAST percentage.

Reproduce with `MITHRIL_SIGVERIFY_FLOW_BACKEND=r51 GOMAXPROCS=8 go test ./pkg/turbine -run '^$' -bench '^BenchmarkEntryPrefetchGapArrival$' -benchtime=3x` on each implementation, copying the same benchmark file to the baseline.

Historical live trials and their limitations are in the
[archived evidence](streaming-preparation-evidence.md). Component results do not establish
a sustained FAST-inclusion improvement.
