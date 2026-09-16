Static decoding, preparation, allocation and queue retention consume the leader's packing window. Prepare owned transactions before leadership, recheck the feature snapshot and all bank-dependent conditions at use, reuse execution/hash scratch, and immediately remove consumed/evicted/expired entries from both priority heaps.

Apply slot-duration-dependent resource budgets and expose bounded queue/completion-reserve settings. New arrivals preserve priority/FIFO selection. The existing slot-local retry policy is unchanged; deferred-retry experiments are excluded. Voting persistence and the coordinated traffic tool are outside this review.

| Historical Zen 5 whole-bank fixture | `alpenglow-dev` at `33dde405` → candidate |
|---|---|
| 48,622 transactions from wire bytes | **209.64 → 150.30 ms** |
| Same bank from decoded transactions | **189.73 → 132.40 ms** |

Three alternating paired rounds use unique 198-byte, one-signature transactions and an in-memory bank. The benchmark excludes signature verification, real AccountsDB, network and consensus; it is not replay time. A separate test checks the cost limit, rejection of the next transaction, and entry/shred round trips. Immediate counterpart-heap removal has a small measured per-removal cost. No live block-fill or FAST gain is claimed.

[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/fea3bdde4a1933962b493a6a3e163a5c8e85f494/docs/leader_block_packing.md).
Fresh local leader/scheduler, account/config, cost-model, Merkle, fixture and node race tests passed. The post-FEC round-trip test uses the public component serializer. Combined integration validation also covers #278 and the other performance reviews. Historical native benchmarks retain their original baselines; this preparation pass ran locally, without touching the live validator.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/06ef067798c99947e8cc527450ad28430a9a7333/docs/results) is preserved outside the proposed merge; reusable benchmarks and maintained contracts remain in source.

Stacked on `7layer/review-streaming-preparation`, which includes #278 at `e1204b32`. Review this diff against that parent.

[Rebased validation and exact source heads](https://github.com/Overclock-Validator/mithril/blob/7layer/review-integration-20260915/docs/results/review-preparation/2026-09-16/README.md).
