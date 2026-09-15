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

## Initial live measurement

The fixed post-warmup window, 2026-09-15 01:27:00–01:30:00 UTC, contains 27 sampled large blocks: 25 external and two own-leader observations, which are excluded from these latency statistics. Every retained transaction had known batch availability, timing order and transaction accounting passed validation, and no trace reports were dropped.

Across the 25 external blocks, maximum complete-availability-to-discovery delay was 0.018715 ms (18.7 microseconds). Assembly-to-ready was 4.709 ms median and 14.710 ms maximum; none exceeded 20 ms. The earlier diagnostic window had five external blocks above20ms among30, including the38.99ms example. These are small observational samples with different blocks/leaders, not matched causal estimates or a guarantee that all long tails are gone. The direct discovery timings and controlled delayed-shred benchmark support the mechanism specifically.

The first completed current-build FAST capture, filtered to source slots first observed after01:27UTC and deduplicated by full proof key, included us in461/466FAST certificates (98.93%); large blocks were79/82 (96.34%). This is a local footer sample, not the rolling Puffin score or Titan reward inclusion. It does not establish a lasting overall FAST improvement. All751clean replay observations had notarize events; no reservation exhaustion was observed. A separate30second probe sanity check also had112/112replay/notarize, no rejection/exhaustion and no parse/truncation errors.

At01:30:27UTC voting was current within3slots, allfourservices active,88/88peer connections, zero broadcast drops, peer-send errors, queue drops/discards/timeouts or connection errors. Three completed own-leader load windows produced12blocks with zero sender errors, up to48,622transactions/block. Counts by window:3798364=[22447,48622,44343,43730];3799208=[17139,48622,36041,40611];3799488=[22814,46680,41221,42949]. The existing continuous loader and all monitoring remain running.

