Large blocks can finish shred assembly with avoidable parsing, message-identity and signature-verification work pending. Discover complete entry components independently, including those beyond an earlier missing shred, prepare them during arrival, and restore final wire order using exact range/byte checks.

Combines #259 with streaming preparation: specialize exactly-one-missing-data recovery for 32+32 FEC sets, pool the fixed encoder, reuse generated packets/roots, and track canonical batch bytes. Preserve newer slot reservations and the asynchronous shred worker. The two-FEC producer target and once-per-component DATA_COMPLETE fix land atomically, so no intermediate commit emits split components. The producer target is 61,632 bytes; other recovery shapes retain the general decoder. Also includes the Narya update, bounded job groups, cached identity/FEC-root reuse, retention-sweep scheduling and owned relay buffers. Completion/recovery requests have priority at request admission and one of the existing four permits is reserved against prefetch. This protects completion as a class; it does not preempt admitted work or prioritize exactly the next replay slot. Worker defaults stay unchanged. Entry serialization failures now emit an error log and increment `block_production_entry_serialization_errors_total`; this diagnoses the existing drop path rather than changing bank rollback behavior.

Metadata inconsistencies join existing readers and reverify the final block. Invalid signatures, cancellation and verifier failure still reject it. Generation ownership, retained-memory bounds and final block checks remain enforced.

| Historical Zen 5 fixture | Earlier implementation → result |
|---|---|
| 33,760 × 228-byte transactions; one delayed shred; assembly → ready | **21.43–22.77 → 2.82–2.83 ms** |
| Same, 1,232-byte transactions | **35.46–36.34 → 7.04–8.08 ms** |
| Saturated prefetch; 32-transaction completion p99 | **47.31 → 1.488 ms** |

The first two comparisons use the preceding streaming implementation and arrivals spread over 200 ms; the last uses the preceding admission policy. They are incremental component comparisons, not #278-versus-PR or live FAST results. Ordered arrivals were roughly unchanged, and total-work p99 in the admission experiment did not improve.

[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/91129784a0e6bb675a83b65f8a388739cd935aa0/docs/transaction_sigverify_streaming.md).
[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/91129784a0e6bb675a83b65f8a388739cd935aa0/docs/out-of-order-entry-prefetch.md).
Fresh local race suites passed for repair, Turbine/recovery/simulator, block production, cost model, replay/blockstream, signature verification, block, stats and starter config. Combined integration validation also covers #278 and the other performance reviews. Historical native benchmarks retain their original baselines; this preparation pass ran locally, without touching the live validator.

The historical #259 producer comparison on September 6 used development head `7e4e8af1`: 50,000 × 1,232-byte legacy transactions across three slots took **734.052 → 282.296 ms** on one pinned Zen 5 CPU. At an equal 30,816-byte target, it was **734.052 → 288.378 ms**. This excludes execution, admission verification, worker overlap and network delivery; it does not measure the rebased combined review. [Original FEC evidence](https://github.com/Overclock-Validator/mithril/tree/a3b16ebaaf803807ad04a7975f3eccf1c15649ea/docs/results/producer-batch/2026-09-06-zen5/alpenglow-dev-head).

The legacy-size fixtures now explicitly retain the 1,232-byte limit; they must not inherit the newer 4,096-byte transport maximum. Tests check canonical sizes, accumulated slot bytes, generated roots/packets, all missing-data/coding-row pairs, simulator traces and general-decoder equivalence. Leader packing and spool publication remain separate dependent reviews. This combined review supersedes #259's implementation.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/1c1171d3661d0404b013a9bf9391e23eb660706e/docs/results) is preserved outside the proposed merge; reusable benchmarks and maintained contracts remain in source.

Based on #278 at `e1204b32`; retarget to `alpenglow-dev` after that PR merges.

[Rebased validation and exact source heads](https://github.com/Overclock-Validator/mithril/blob/7layer/review-integration-20260915/docs/results/review-preparation/2026-09-16/README.md).

Recovery authentication is fixed in a separate commit: both decoders reconstruct missing coding shards, rebuild the complete Merkle tree and require its root to match the signed received root before returning data. Recovered data receives complete proofs. Tests reject leader-signed inconsistent sets (including inconsistent missing parity) and altered recovered bytes, accept valid recovery, and reproduce the original roots and recovered bytes of four captured Agave FEC sets. The unchained 1+17 layout is also covered.

This correctness check has a cost: on M4 Pro (`GOMAXPROCS=2`, five 200 ms samples), the warmed one-missing-data / all-coding recovery benchmark increases from **2.47 to 30.17 µs** per FEC set. Exactly-threshold arrivals measure **104.52–118.01 µs**, including reconstruction of missing parity. These are local component measurements, not live-validator results. The all-data-present path is unchanged. [Authentication contract, Agave reference and benchmark method](https://github.com/Overclock-Validator/mithril/blob/91129784a0e6bb675a83b65f8a388739cd935aa0/docs/fec-recovery-authentication.md).
