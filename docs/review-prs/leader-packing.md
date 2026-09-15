Prepare owned transactions before leadership, recheck bank-dependent state at use, reduce scratch/entry-root overhead, correct slot-duration-dependent resource limits and expose bounded queue/finalization-reserve settings. Remove consumed, evicted and expired scheduler entries from both indexed heaps so stale transaction references do not accumulate.

Includes deterministic single-signature load fixtures, near-limit bank generation, actual cost-limit assertions and emitted-entry shred round trips. The existing slot-local transaction retry policy is unchanged; later deferred-retry experiments remain outside this PR. Voting reservations and transport are separate.

### Benchmark

Zen5 Ryzen7 9700X, Go1.26.4; three alternating paired rounds against current alpenglow-dev33dde405. A48,622-transaction bank using unique198-byte single-signature transactions improved from209.64 to150.30ms from wire bytes, and189.73 to132.40ms from decoded transactions. This excludes signature verification, real AccountsDB, network broadcast and consensus. Heap counterpart removal has a small measured removal-cost tradeoff; see the queue evidence rather than interpreting it as a throughput gain.

Start with `docs/leader_block_packing.md`. Raw evidence is under `docs/results/leader-block-packing/2026-09-13` and `docs/results/queue-retention/2026-09-14`.

Fresh race suites passed for leader/scheduler, accounts, config, cost model, Merkle, replay, transaction fixtures, Turbine and node startup; vet passed. Logs: `docs/results/pr-split-2026-09-15/leader`.

This is stacked on the streaming-preparation PR; the GitHub diff shows only leader-specific changes. It extracts the existing leader portion of #279 without adding new block-production work.

Split from #279; base development commit: `33dde4050d9250557583395810799aaac2f54017`. Historical native measurements retain their original tested source; these reorganized heads have fresh local validation.
