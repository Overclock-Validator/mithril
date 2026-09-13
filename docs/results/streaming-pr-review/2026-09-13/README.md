# Standalone streaming verification PR review

This branch extracts the receiver, transaction pool, Narya update and completion
work from trial commit `15ad18458dea3b7ff89ea21ef3bae7df04b323de` onto
`alpenglow-dev` at `33dde405`. The dependency planner is already merged in that
base. It does not include the FEC acceleration, producer batching, or subsequent
leader-packing experiments from the combined trial.

The only shared prerequisite with FEC PR #259 is the small generator correctness
fix that places DATA_COMPLETE at the end of the serialized component, rather
than at every intermediate FEC boundary. Boundary tests cover exact unsigned
and resigned capacities, multiple sets, signature proofs, and round-trip bytes.
The assembly fixture uses the public generation API and derives roots from
packet proofs; fixture generation remains outside timing. The root cache keeps
alpenglow-dev's existing private shredSigCache type.

## Final allocation improvement

Early preparation knows the decoded transaction count. Allocate its pointer view
once, preserving order and pointers into the original entry storage. This avoids
repeated slice growth without copying transaction objects or changing ownership.

Apple M4 Pro, Go 1.26.4, three 200 ms microbenchmark samples per case:

| Transactions/component | Before bytes / allocations | After bytes / allocations |
|---|---:|---:|
| 4 | 56 / 3 | 32 / 1 |
| 49 | 1,016 / 7 | 416 / 1 |
| 269 | 4,472 / 9 | 2,304 / 1 |
| 33,760 | approximately 1,301,624 / 23 | 270,336 / 1 |

These measure pointer-view construction alone. The 33,760 case is a helper
stress case, not a typical prefetched component. Concurrent local validation
makes the timing samples unsuitable for an end-to-end speedup claim; allocation
counts are the result of interest. Raw output is in pointer-before.txt and
pointer-after.txt. The before version is the committed trial's append-growth
implementation; this is not an alpenglow-dev comparison.

## Earlier performance evidence and live validation

The [streaming report](../../sigverify-streaming/2026-09-12-zen5/README.md),
[direct byte comparison report](../../sigverify-direct-cache/2026-09-12-zen5/README.md),
and [completion follow-up](../../completion-followup/2026-09-12-zen5/README.md)
retain their original raw evidence, source hashes, and baseline definitions.
Those runs used the combined trial branch. They are not a fresh comparison
against this standalone branch's current alpenglow-dev base.

A five-minute live Alpenglow sample on September 13, 04:03:06–04:08:06 UTC, used
combined trial `15ad1845` before the later leader-packing changes. On the Ryzen 7
9700X with GOMAXPROCS=8, two signature workers and target eight:

- 1,241 replayed blocks, 5,700,024 transactions; 118 blocks had at least 20,000
  transactions, with median and maximum transaction count 33,760.
- Those large blocks had median full replay 78.11 ms, p95 98.67 ms; median
  final-shred-to-ready 7.53 ms, p95 34.74 ms. The two stages are distinct.
- 3,449,937 of 3,844,618 large-block transactions (89.7%) were signature-verified
  before full assembly.
- RPC-tip checks stayed current; last-vote lag was 1–3 slots, median two. No
  logged runtime errors, bankhash mismatches, or internal verifier fallbacks
  were observed in the sample.

This was an observational trial, not a paired whole-validator speed comparison.
The live binary also contained the FEC/producer work excluded from this PR.

## Fresh standalone Zen 5 check

The source archive identified by source-sha256.txt contains the final production
code and tests. Documentation-only additions followed that archive. Run details
are in run-native.sh; native-assembly.txt contains all measurements. Eight
physical cores (CPUs 0–7), GOMAXPROCS=8, Go 1.26.4, native Narya r51, nice 10;
the existing validator remained active with the same PID throughout.

For 33,760 distinct generated one-signature transactions, two workers, target
eight, three timed blocks per condition:

| Wire bytes/tx | Tip final-shred-to-ready, overlap off → on | Tip total arrival-to-ready, off → on | Catch-up total, off → on |
|---|---:|---:|---:|
| 228 | 85.72 → 1.831 ms | 286.93 → 202.94 ms | 85.31 → 86.39 ms |
| 1,232 | 99.89 → 6.504 ms | 302.61 → 207.77 ms | 111.97 → 117.38 ms |

Residual values are within-run medians; total times are Go benchmark means.
This small run validates the extracted implementation and illustrates overlap;
it is not a stable speedup estimate. Both sides use this PR's new batching,
Narya and completion code. It is explicitly **not old alpenglow-dev versus PR**.
Catch-up offers all shreds immediately and showed modest extra overlap overhead
in this run. The default favors latency at the tip, with no mode-switch timer.

The tip workload distributes complete component bursts over 200 ms, using a
60 KiB transaction-byte component budget plus actual entry overhead/FEC padding.
Collection took 200.9–201.4 ms. These are synthetic arrivals, not a network trace.
Timed work includes assembly, decoding, Merkle/block identity, signatures and
publication; it excludes packet parsing, shred authentication, loss/recovery,
transaction execution, PoH, disk I/O and commit. Assertions checked exact
signature coverage, block metadata, and zero internal native fallbacks.

Native and local race suites passed for Turbine, replay, sigverify, txverify,
block, statsd and node; vet and complete Mithril builds passed on both machines.
Raw race logs are included. Root-selection fuzzing compared the new selector
against the original sorted algorithm; local-root-fuzz.txt records the result.
