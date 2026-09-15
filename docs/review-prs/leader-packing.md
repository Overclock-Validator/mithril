Prepare owned transactions before leadership, recheck bank-dependent state at use, reduce scratch/entry-root overhead, correct slot-duration-dependent resource limits and expose bounded queue/finalization-reserve settings. Remove consumed, evicted and expired scheduler entries from both indexed heaps so stale transaction references do not accumulate.

Includes deterministic single-signature load fixtures, near-limit bank generation, actual cost-limit assertions and emitted-entry shred round trips. The existing slot-local transaction retry policy is unchanged; later deferred-retry experiments remain outside this PR. Voting reservations and transport are separate.

### Benchmark

Zen5 Ryzen7 9700X, Go1.26.4; three alternating paired rounds against current alpenglow-dev33dde405. A48,622-transaction bank using unique198-byte single-signature transactions improved from209.64 to150.30ms from wire bytes, and189.73 to132.40ms from decoded transactions. This excludes signature verification, real AccountsDB, network broadcast and consensus. Heap counterpart removal has a small measured removal-cost tradeoff; see the queue evidence rather than interpreting it as a throughput gain.

Start with `docs/leader_block_packing.md`. Raw evidence is under `docs/results/leader-block-packing/2026-09-13` and `docs/results/queue-retention/2026-09-14`.

Fresh race suites passed for leader/scheduler, accounts, config, cost model, Merkle, replay, transaction fixtures, Turbine and node startup; vet passed. Logs: `docs/results/pr-split-2026-09-15/leader`.

This is stacked on the streaming-preparation PR; the GitHub diff shows only leader-specific changes. It extracts the existing leader portion of #279 without adding new block-production work.

### Live sender contention follow-up (operational experiment)

On the unchanged combined validator, the synthetic sender was changed from 200,000 transactions at 150,000/s starting 20 slots before leadership to the same count at 75,000/s starting 27 slots before leadership. At 200 ms/slot this preserves theoretical completion headroom (2.67 → 2.73 seconds) while spreading sender/TPU work. This is a separate load-generator setting, not a validator scheduling change or new default in this PR.

The first matched comparison had only **10 baseline and 4 candidate blocks**: median replay **128.5 → 85.1 ms**, with median excess above similar nearby no-send blocks **52.3 → 12.6 ms**. Controls match leader, position within its four-slot run, binary, transaction count and rounded CU within 10%, and time within five minutes. Build/test intervals are excluded. These observational samples do **not** establish causation, p99, or sustained FAST improvement.

Five candidate sends each submitted all 200,000 with zero local send errors and finished 15–17 slots before leadership. This does not prove network receipt or inclusion of all submitted transactions. Four-block inclusion totals ranged from 121,358 to 147,680; equal block fullness is not established. Native sender race tests, vet, build and selfcheck passed. Machine-specific sender services, funding logic, runtime state and monitoring scripts remain outside this validator PR; the portable offline fixtures above remain the reproduction for its bank-building benchmark.
