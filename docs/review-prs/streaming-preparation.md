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

On inconsistent cached identity/range metadata, join old readers and re-verify every final transaction. Valid blocks recover; invalid signatures, cancellation and verifier failure remain errors. Bounds are checked before slicing. Normal signature failures still reject the block, and the default worker count is unchanged. Local standalone and native combined race suites, vet and builds passed. Regression failures before the fix and passing evidence: `docs/results/review-fixes/2026-09-15`. These recovery changes are now included in the combined testnet deployment.

### Relay buffer reuse

Reuse per-worker peer result storage and exclusively owned packet copies through synchronous sends and retries; queue rejection and shutdown return copies safely. Routing and authentication remain unchanged. The incremental Zen 5 comparison against c40ac9e8 reduces allocation from 3,784 to about 729 B/shred (8 to 6 allocations). Median in-memory pipeline time improved about 4–7% across 90/512-contact fixtures, with noisy samples; this does not establish network latency or FAST gains. See `docs/turbine-relay-buffers.md` for ownership, methodology and limitations. Local/native Turbine race tests, native vet and combined build passed. The relay follow-up was deployed and live-tested on Zen 5 at 2026-09-15 04:43 UTC. In the first completed five-minute window, 5.9 million destination packets had zero relay queue drops or send errors, and own blocks reached 48,505 transactions. FAST inclusion was 825/835 (98.80%) versus 839/843 (99.53%) immediately before; this lower short sample does not establish causation or a live gain. Median replay-to-vote was 1.56 versus 1.53 ms. Monitoring continues with the same worker and load settings.


### Reserve verifier admission for completion

Commit `1c1171d3` caps prefetch at three of the existing four request permits with the default two workers. Waiting completion/recovery requests get priority over prefetch at admission; job queues, vector grouping, rolling windows and worker count remain unchanged. Accepted jobs are still verified and joined before memory reuse. Prefetch resumes after the completion backlog drains. This is completion-class protection, not exact next-replay-slot priority; future-slot completions qualify too and already-admitted prefetch is not preempted. No persistence or voting-recovery contract changes.

Zen 5 / Narya r51 / GOMAXPROCS8, three runs of100 saturated iterations: four ready4,096-transaction prefetch components plus a32-transaction completion. Compared with the previously deployed production source, completion-finished p99 was **47.31→1.488ms**, and admission p99 **45.78→0.004599ms** (medians of per-run statistics). Total-work median stayed **39.43→39.11ms**; total-work p99 **49.09→50.65ms**, so no total-work tail gain is claimed. This is a controlled saturation result, not live FAST or whole-PR performance. Method, ranges and smaller components: `docs/transaction_sigverify_streaming.md`.

Full local/native turbine and blockstream race suites, native node recovery race tests, local/native vet and combined build passed. Regression coverage includes request bounds, completion priority, prefetch resumption, cancellation, shutdown joining and slot-reset memory ownership. Raw evidence stays server-side at `/srv/mithril-verifier-reservation-20260915`.
