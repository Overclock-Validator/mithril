Large blocks can finish shred assembly with avoidable decoding and signature-verification work still pending. Prepare complete entry batches during arrival, including batches beyond an earlier missing shred, and reuse results only after exact range/byte validation. Final transaction order, block identity checks, signature checks, bounded ownership and cancellation remain intact.

Includes the Narya update, bounded ready-batch scheduling, direct cached-byte comparisons, authenticated FEC-root reuse, retention-sweep reduction, early message identities, independent batch discovery and the queued-prefetch-reset correction from #279. Opt-in sampled pipeline tracing defaults off. Leader packing, voting/persistence, certificate processing and transaction-status expiry are separate PRs. FEC acceleration remains #259.

### Benchmark

Zen5 Ryzen7 9700X, Go1.26.4, Narya r51, two verification workers, batch target8, GOMAXPROCS8. Identical 33,760-transaction fixtures arrive over200ms; one shred three quarters through the block is held until after the footer. Two runs per version, three iterations per case, alternating baseline/candidate/candidate/baseline. Ranges are run medians.

| Transaction size | Previous streaming implementation | Independent batch discovery |
|---|---:|---:|
| 228 bytes, delayed shred | 21.43–22.77ms | 2.82–2.83ms |
| 1,232 bytes, delayed shred | 35.46–36.34ms | 7.04–8.08ms |

These are final-assembly-to-ready times against the **previous streaming implementation**, not alpenglow-dev or a whole-validator speedup. Ordered arrivals were roughly unchanged. The benchmark verifies every retained signature exactly once. Historical component comparisons keep their original baselines.

### Review and validation

Start with `docs/transaction_sigverify_streaming.md`, `docs/out-of-order-entry-prefetch.md`, and `docs/streaming_message_identities.md`. Raw delayed-shred benchmarks are in `docs/results/out-of-order-prefetch/2026-09-15`; other historical evidence remains beside its original method.

Fresh standalone race suites passed for Turbine, block, txverify, txstatus, sigverify and replay; vet and the full validator build passed. Includes both the new gap regressions and #279's reset-accounting correction. Fresh logs: `docs/results/pr-split-2026-09-15/streaming`.

### Cache recovery and configuration ownership

Move the signature-verification template and test here, alongside the code that reads those settings. Do not enable unrelated VM pooling in the standalone template; the runtime branch supplies that default with its ownership fix.

On inconsistent cached identity/range metadata, join old readers and re-verify every final transaction. Valid blocks recover; invalid signatures, cancellation and verifier failure remain errors. Bounds are checked before slicing. Normal signature failures still reject the block, and the default worker count is unchanged. Local standalone and native combined race suites, vet and builds passed. Regression failures before the fix and passing evidence: `docs/results/review-fixes/2026-09-15`. Not deployed.
